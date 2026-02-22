package provider

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	rand "math/rand/v2"
	"net"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	log "github.com/sirupsen/logrus"

	"sigs.k8s.io/external-dns/endpoint"
	"sigs.k8s.io/external-dns/plan"
	externaldnsprovider "sigs.k8s.io/external-dns/provider"
)

const (
	DefaultGraphQLEndpoint = "https://console.ps.kz/dns/graphql"

	defaultHTTPTimeout      = 30 * time.Second
	defaultPageSize         = 100
	defaultMaxZonePages     = 1000
	defaultRecordTTL        = 300
	defaultMaxRetries       = 3
	maxGraphQLResponseBytes = 10 << 20

	defaultRetryInitialBackoff = 200 * time.Millisecond
	defaultRetryMaxBackoff     = 2 * time.Second
	defaultRetryJitter         = 100 * time.Millisecond

	headerContentType = "Content-Type"
	headerUserToken   = "X-User-Token"
	headerUserAgent   = "User-Agent"

	contentTypeJSON = "application/json"
	userAgent       = "external-dns-pscloud-webhook"
)

const (
	opListZones       = "list_zones"
	opListZoneRecords = "list_zone_records"
	opCreateRecord    = "create_record"
	opUpdateRecord    = "update_record"
	opDeleteRecord    = "delete_record"
)

var (
	graphQLRequestsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "external_dns_pscloud_webhook",
			Name:      "graphql_requests_total",
			Help:      "Total number of GraphQL requests to PS Cloud DNS API.",
		},
		[]string{"operation", "result"},
	)
	graphQLRequestDurationSeconds = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "external_dns_pscloud_webhook",
			Name:      "graphql_request_duration_seconds",
			Help:      "Duration of GraphQL requests to PS Cloud DNS API in seconds.",
			Buckets:   prometheus.DefBuckets,
		},
		[]string{"operation"},
	)
)

func init() {
	prometheus.MustRegister(graphQLRequestsTotal, graphQLRequestDurationSeconds)
}

const listZonesQuery = `
query ListZones($page: Int!, $perPage: Int!) {
  dns {
    zones(page: $page, perPage: $perPage) {
      count
      items {
        id
        name
        enabled
        type
      }
    }
  }
}
`

const listZoneRecordsQuery = `
query ZoneRecords($zoneName: String!) {
  dns {
    zone(name: $zoneName) {
      id
      name
      records {
        id
        name
        type
        ttl
        value
      }
    }
  }
}
`

const createRecordMutation = `
mutation CreateRecord($zoneName: String!, $name: String!, $type: RecordType!, $ttl: Int, $value: String!) {
  dns {
    record {
      create(
        zoneName: $zoneName
        createData: {
          name: $name
          type: $type
          ttl: $ttl
          value: $value
        }
      ) {
        id
      }
    }
  }
}
`

const updateRecordMutation = `
mutation UpdateRecord($zoneName: String!, $recordId: String!, $input: RecordUpdateInput!) {
  dns {
    record {
      update(zoneName: $zoneName, recordId: $recordId, input: $input) {
        id
      }
    }
  }
}
`

const deleteRecordMutation = `
mutation DeleteRecord($zoneName: String!, $recordId: String!) {
  dns {
    record {
      delete(zoneName: $zoneName, recordId: $recordId) {
        id
      }
    }
  }
}
`

type Config struct {
	Endpoint            string
	Token               string
	DomainFilter        *endpoint.DomainFilter
	DryRun              bool
	HTTPClient          *http.Client
	ZonesPageSize       *int
	MaxRetries          *int
	RetryInitialBackoff *time.Duration
	RetryMaxBackoff     *time.Duration
	RetryJitter         *time.Duration
}

type PSProvider struct {
	externaldnsprovider.BaseProvider

	client      *http.Client
	endpoint    string
	token       string
	domainFilt  *endpoint.DomainFilter
	pageSize    int
	dryRun      bool
	maxRetries  int
	retryBase   time.Duration
	retryMax    time.Duration
	retryJitter time.Duration
	sleepFn     func(context.Context, time.Duration) error

	opMu            sync.Mutex
	mu              sync.RWMutex
	zoneNameMap     externaldnsprovider.ZoneIDName
	zoneMapInit     bool
	recordIDByKV    map[recordKey]string
	recordIndexInit bool
}

type recordKey struct {
	zone       string
	name       string
	recordType string
	value      string
}

type graphQLRequest struct {
	Query     string         `json:"query"`
	Variables map[string]any `json:"variables,omitempty"`
}

type graphQLError struct {
	Message string `json:"message"`
}

type graphQLResponse struct {
	Data   json.RawMessage `json:"data"`
	Errors []graphQLError  `json:"errors"`
}

type listZonesData struct {
	DNS struct {
		Zones struct {
			Count int              `json:"count"`
			Items []graphqlZoneDTO `json:"items"`
		} `json:"zones"`
	} `json:"dns"`
}

type listZoneRecordsData struct {
	DNS struct {
		Zone *graphqlZoneRecordsDTO `json:"zone"`
	} `json:"dns"`
}

type createRecordData struct {
	DNS struct {
		Record struct {
			Create struct {
				ID string `json:"id"`
			} `json:"create"`
		} `json:"record"`
	} `json:"dns"`
}

type updateRecordData struct {
	DNS struct {
		Record struct {
			Update struct {
				ID string `json:"id"`
			} `json:"update"`
		} `json:"record"`
	} `json:"dns"`
}

type deleteRecordData struct {
	DNS struct {
		Record struct {
			Delete struct {
				ID string `json:"id"`
			} `json:"delete"`
		} `json:"record"`
	} `json:"dns"`
}

type graphqlZoneDTO struct {
	ID      string `json:"id"`
	Name    string `json:"name"`
	Enabled bool   `json:"enabled"`
	Type    string `json:"type"`
}

type graphqlZoneRecordsDTO struct {
	ID      string             `json:"id"`
	Name    string             `json:"name"`
	Records []graphqlRecordDTO `json:"records"`
}

type graphqlRecordDTO struct {
	ID    string `json:"id"`
	Name  string `json:"name"`
	Type  string `json:"type"`
	TTL   *int   `json:"ttl"`
	Value string `json:"value"`
}

type endpointGroupKey struct {
	dnsName    string
	recordType string
	recordTTL  endpoint.TTL
}

type endpointGroup struct {
	dnsName    string
	recordType string
	recordTTL  endpoint.TTL
	targets    map[string]struct{}
}

func NewPSProvider(cfg Config) (*PSProvider, error) {
	if strings.TrimSpace(cfg.Endpoint) == "" {
		cfg.Endpoint = DefaultGraphQLEndpoint
	}
	validatedEndpoint, err := validateEndpointURL(cfg.Endpoint)
	if err != nil {
		return nil, err
	}
	cfg.Endpoint = validatedEndpoint

	if strings.TrimSpace(cfg.Token) == "" {
		return nil, errors.New("provider token cannot be empty")
	}

	httpClient := cfg.HTTPClient
	if httpClient == nil {
		httpClient = &http.Client{Timeout: defaultHTTPTimeout}
	}

	pageSize := defaultPageSize
	if cfg.ZonesPageSize != nil {
		pageSize = *cfg.ZonesPageSize
		if pageSize <= 0 {
			return nil, fmt.Errorf("zones page size must be > 0, got %d", pageSize)
		}
	}

	maxRetries := defaultMaxRetries
	if cfg.MaxRetries != nil {
		maxRetries = *cfg.MaxRetries
		if maxRetries < 0 {
			return nil, fmt.Errorf("max retries must be >= 0, got %d", maxRetries)
		}
	}

	retryBase := defaultRetryInitialBackoff
	if cfg.RetryInitialBackoff != nil {
		retryBase = *cfg.RetryInitialBackoff
		if retryBase <= 0 {
			return nil, fmt.Errorf("retry initial backoff must be > 0, got %s", retryBase)
		}
	}

	retryMax := defaultRetryMaxBackoff
	if cfg.RetryMaxBackoff != nil {
		retryMax = *cfg.RetryMaxBackoff
		if retryMax <= 0 {
			return nil, fmt.Errorf("retry max backoff must be > 0, got %s", retryMax)
		}
	}
	if retryMax < retryBase {
		return nil, fmt.Errorf("retry max backoff must be >= retry initial backoff (%s), got %s", retryBase, retryMax)
	}

	retryJitter := defaultRetryJitter
	if cfg.RetryJitter != nil {
		retryJitter = *cfg.RetryJitter
		if retryJitter < 0 {
			return nil, fmt.Errorf("retry jitter must be >= 0, got %s", retryJitter)
		}
	}

	return &PSProvider{
		client:       httpClient,
		endpoint:     cfg.Endpoint,
		token:        cfg.Token,
		domainFilt:   cfg.DomainFilter,
		pageSize:     pageSize,
		dryRun:       cfg.DryRun,
		maxRetries:   maxRetries,
		retryBase:    retryBase,
		retryMax:     retryMax,
		retryJitter:  retryJitter,
		sleepFn:      sleepWithContext,
		zoneNameMap:  externaldnsprovider.ZoneIDName{},
		recordIDByKV: map[recordKey]string{},
	}, nil
}

func (p *PSProvider) GetDomainFilter() endpoint.DomainFilterInterface {
	return p.domainFilt
}

func (p *PSProvider) Records(ctx context.Context) ([]*endpoint.Endpoint, error) {
	p.opMu.Lock()
	defer p.opMu.Unlock()
	return p.records(ctx)
}

func (p *PSProvider) records(ctx context.Context) ([]*endpoint.Endpoint, error) {
	zones, err := p.listManagedZones(ctx)
	if err != nil {
		return nil, err
	}

	zoneMap := externaldnsprovider.ZoneIDName{}
	recordIDs := make(map[recordKey]string)
	groups := map[endpointGroupKey]*endpointGroup{}

	for _, zone := range zones {
		zoneName := normalizeDNSName(zone.Name)
		zoneMap.Add(zoneName, zoneName)

		zoneRecords, err := p.listZoneRecords(ctx, zoneName)
		if err != nil {
			return nil, err
		}

		for _, record := range zoneRecords {
			recordType := normalizeRecordType(record.Type)
			if !externaldnsprovider.SupportedRecordType(recordType) {
				log.WithFields(log.Fields{
					"zone": zoneName,
					"name": record.Name,
					"type": record.Type,
				}).Debug("PS Cloud provider: skipping unsupported record type")
				continue
			}

			dnsName := normalizeDNSName(record.Name)
			value := normalizeRecordValue(recordType, record.Value)
			ttl := endpoint.TTL(defaultRecordTTL)
			if record.TTL != nil && *record.TTL > 0 {
				ttl = endpoint.TTL(*record.TTL)
			}

			groupKey := endpointGroupKey{
				dnsName:    dnsName,
				recordType: recordType,
				recordTTL:  ttl,
			}
			group, found := groups[groupKey]
			if !found {
				group = &endpointGroup{
					dnsName:    dnsName,
					recordType: recordType,
					recordTTL:  ttl,
					targets:    map[string]struct{}{},
				}
				groups[groupKey] = group
			}
			group.targets[value] = struct{}{}

			recordIDs[newRecordKey(zoneName, dnsName, recordType, value)] = record.ID
		}
	}

	records := make([]*endpoint.Endpoint, 0, len(groups))
	for _, group := range groups {
		targets := make([]string, 0, len(group.targets))
		for target := range group.targets {
			targets = append(targets, target)
		}
		sort.Strings(targets)
		records = append(records, endpoint.NewEndpointWithTTL(group.dnsName, group.recordType, group.recordTTL, targets...))
	}

	p.replaceState(zoneMap, recordIDs)
	return records, nil
}

func (p *PSProvider) ApplyChanges(ctx context.Context, changes *plan.Changes) error {
	p.opMu.Lock()
	defer p.opMu.Unlock()

	if changes == nil {
		return nil
	}

	if err := p.ensureZoneMap(ctx); err != nil {
		return err
	}

	needsRecordIDs := len(changes.Delete) > 0 || len(changes.UpdateOld) > 0 || len(changes.UpdateNew) > 0
	if needsRecordIDs {
		if err := p.ensureRecordIndex(ctx); err != nil {
			return err
		}
	}

	if err := p.applyUpdatePairs(ctx, changes.UpdateOld, changes.UpdateNew); err != nil {
		return err
	}
	if err := p.applyCreates(ctx, changes.Create); err != nil {
		return err
	}
	if err := p.applyDeletes(ctx, changes.Delete); err != nil {
		return err
	}

	return nil
}

func (p *PSProvider) applyUpdatePairs(ctx context.Context, oldEndpoints, newEndpoints []*endpoint.Endpoint) error {
	maxLen := len(oldEndpoints)
	if len(newEndpoints) > maxLen {
		maxLen = len(newEndpoints)
	}

	for i := 0; i < maxLen; i++ {
		switch {
		case i >= len(oldEndpoints):
			if err := p.applyCreates(ctx, []*endpoint.Endpoint{newEndpoints[i]}); err != nil {
				return err
			}
		case i >= len(newEndpoints):
			if err := p.applyDeletes(ctx, []*endpoint.Endpoint{oldEndpoints[i]}); err != nil {
				return err
			}
		default:
			if err := p.applyUpdatePair(ctx, oldEndpoints[i], newEndpoints[i]); err != nil {
				return err
			}
		}
	}

	return nil
}

func (p *PSProvider) applyUpdatePair(ctx context.Context, oldEndpoint, newEndpoint *endpoint.Endpoint) error {
	if oldEndpoint == nil || newEndpoint == nil {
		return nil
	}

	oldRecordType := normalizeRecordType(oldEndpoint.RecordType)
	newRecordType := normalizeRecordType(newEndpoint.RecordType)
	oldName := normalizeDNSName(oldEndpoint.DNSName)
	newName := normalizeDNSName(newEndpoint.DNSName)

	if !externaldnsprovider.SupportedRecordType(oldRecordType) || !externaldnsprovider.SupportedRecordType(newRecordType) {
		return nil
	}

	oldZone, err := p.zoneForDNSName(ctx, oldName)
	if err != nil {
		return err
	}
	newZone, err := p.zoneForDNSName(ctx, newName)
	if err != nil {
		return err
	}

	if oldZone != newZone || oldName != newName || oldRecordType != newRecordType {
		if err := p.applyDeletes(ctx, []*endpoint.Endpoint{oldEndpoint}); err != nil {
			return err
		}
		if err := p.applyCreates(ctx, []*endpoint.Endpoint{newEndpoint}); err != nil {
			return err
		}
		return nil
	}

	sharedTargets, oldOnlyTargets, newOnlyTargets := diffTargets(targetsForEndpoint(oldEndpoint), targetsForEndpoint(newEndpoint))
	ttlToUpdate := ttlForUpdate(oldEndpoint, newEndpoint)

	for _, value := range sharedTargets {
		if ttlToUpdate == nil {
			continue
		}

		key := newRecordKey(newZone, newName, newRecordType, value)
		recordID, err := p.findRecordID(ctx, key)
		if err != nil {
			return err
		}
		if err := p.updateRecord(ctx, newZone, recordID, value, ttlToUpdate); err != nil {
			return err
		}
	}

	pairedCount := len(oldOnlyTargets)
	if len(newOnlyTargets) < pairedCount {
		pairedCount = len(newOnlyTargets)
	}

	for i := 0; i < pairedCount; i++ {
		oldValue := oldOnlyTargets[i]
		newValue := newOnlyTargets[i]
		oldKey := newRecordKey(newZone, oldName, oldRecordType, oldValue)

		recordID, err := p.findRecordID(ctx, oldKey)
		if err != nil {
			return err
		}
		if err := p.updateRecord(ctx, newZone, recordID, newValue, ttlToUpdate); err != nil {
			return err
		}

		newKey := newRecordKey(newZone, newName, newRecordType, newValue)
		p.replaceRecordID(oldKey, newKey, recordID)
	}

	if pairedCount < len(newOnlyTargets) {
		ttl := ttlForCreate(newEndpoint)
		for _, value := range newOnlyTargets[pairedCount:] {
			if err := p.createRecord(ctx, newZone, newName, newRecordType, value, ttl); err != nil {
				return err
			}
		}
	}

	if pairedCount < len(oldOnlyTargets) {
		for _, value := range oldOnlyTargets[pairedCount:] {
			key := newRecordKey(newZone, oldName, oldRecordType, value)
			recordID, err := p.findRecordID(ctx, key)
			if err != nil {
				return err
			}
			if err := p.deleteRecord(ctx, newZone, recordID); err != nil {
				return err
			}
			log.WithFields(log.Fields{
				"zone":     newZone,
				"name":     oldName,
				"type":     oldRecordType,
				"value":    value,
				"recordID": recordID,
			}).Info("PS Cloud provider: deleted record")
			p.deleteRecordID(key)
		}
	}

	return nil
}

func (p *PSProvider) applyCreates(ctx context.Context, endpoints []*endpoint.Endpoint) error {
	for _, ep := range endpoints {
		if ep == nil {
			continue
		}

		recordType := normalizeRecordType(ep.RecordType)
		if !externaldnsprovider.SupportedRecordType(recordType) {
			continue
		}

		zoneName, err := p.zoneForDNSName(ctx, ep.DNSName)
		if err != nil {
			return err
		}

		ttl := ttlForCreate(ep)
		dnsName := normalizeDNSName(ep.DNSName)
		for _, value := range targetsForEndpoint(ep) {
			if err := p.createRecord(ctx, zoneName, dnsName, recordType, value, ttl); err != nil {
				return err
			}
		}
	}

	return nil
}

func (p *PSProvider) applyDeletes(ctx context.Context, endpoints []*endpoint.Endpoint) error {
	for _, ep := range endpoints {
		if ep == nil {
			continue
		}

		recordType := normalizeRecordType(ep.RecordType)
		if !externaldnsprovider.SupportedRecordType(recordType) {
			continue
		}

		zoneName, err := p.zoneForDNSName(ctx, ep.DNSName)
		if err != nil {
			return err
		}

		dnsName := normalizeDNSName(ep.DNSName)
		for _, value := range targetsForEndpoint(ep) {
			key := newRecordKey(zoneName, dnsName, recordType, value)
			recordID, err := p.findRecordID(ctx, key)
			if err != nil {
				if !errors.Is(err, externaldnsprovider.SoftError) {
					return err
				}
				log.WithFields(log.Fields{
					"zone":  zoneName,
					"name":  dnsName,
					"type":  recordType,
					"value": value,
				}).Warn("PS Cloud provider: record to delete is absent in local index, skipping")
				continue
			}

			if err := p.deleteRecord(ctx, zoneName, recordID); err != nil {
				return err
			}
			log.WithFields(log.Fields{
				"zone":     zoneName,
				"name":     dnsName,
				"type":     recordType,
				"value":    value,
				"recordID": recordID,
			}).Info("PS Cloud provider: deleted record")
			p.deleteRecordID(key)
		}
	}

	return nil
}

func (p *PSProvider) listManagedZones(ctx context.Context) ([]graphqlZoneDTO, error) {
	zones, err := p.listZones(ctx)
	if err != nil {
		return nil, err
	}

	filtered := make([]graphqlZoneDTO, 0, len(zones))
	for _, z := range zones {
		if !z.Enabled {
			continue
		}

		z.Name = normalizeDNSName(z.Name)
		if p.domainFilt != nil && !p.domainFilt.Match(z.Name) {
			continue
		}

		filtered = append(filtered, z)
	}

	return filtered, nil
}

func (p *PSProvider) listZones(ctx context.Context) ([]graphqlZoneDTO, error) {
	zones := make([]graphqlZoneDTO, 0)
	page := 1
	for {
		if page > defaultMaxZonePages {
			return nil, fmt.Errorf("list zones exceeded max pages limit (%d)", defaultMaxZonePages)
		}

		var data listZonesData
		if err := p.doGraphQL(ctx, opListZones, listZonesQuery, map[string]any{
			"page":    page,
			"perPage": p.pageSize,
		}, &data); err != nil {
			return nil, fmt.Errorf("list zones failed: %w", err)
		}

		items := data.DNS.Zones.Items
		if len(items) == 0 {
			break
		}

		zones = append(zones, items...)
		if data.DNS.Zones.Count > 0 && len(zones) >= data.DNS.Zones.Count {
			break
		}
		if len(items) < p.pageSize {
			break
		}
		page++
	}

	return zones, nil
}

func (p *PSProvider) listZoneRecords(ctx context.Context, zoneName string) ([]graphqlRecordDTO, error) {
	var data listZoneRecordsData
	if err := p.doGraphQL(ctx, opListZoneRecords, listZoneRecordsQuery, map[string]any{
		"zoneName": normalizeDNSName(zoneName),
	}, &data); err != nil {
		return nil, fmt.Errorf("list zone records for %q failed: %w", zoneName, err)
	}

	if data.DNS.Zone == nil {
		return nil, nil
	}

	return data.DNS.Zone.Records, nil
}

func (p *PSProvider) createRecord(ctx context.Context, zoneName, name, recordType, value string, ttl int) error {
	if p.dryRun {
		log.WithFields(log.Fields{
			"zone":  zoneName,
			"name":  name,
			"type":  recordType,
			"value": value,
			"ttl":   ttl,
		}).Info("PS Cloud provider: dry-run create record")
		return nil
	}

	var resp createRecordData
	err := p.doGraphQL(ctx, opCreateRecord, createRecordMutation, map[string]any{
		"zoneName": zoneName,
		"name":     normalizeDNSName(name),
		"type":     recordType,
		"ttl":      ttl,
		"value":    value,
	}, &resp)
	if err != nil {
		// API can return "Record already exist" when state changed between planning and apply.
		if isAlreadyExistsError(err) {
			log.WithFields(log.Fields{
				"zone":  zoneName,
				"name":  name,
				"type":  recordType,
				"value": value,
			}).Warn("PS Cloud provider: create reported already-existing record, treating as idempotent success")
			_ = p.refreshZoneRecordIndex(ctx, zoneName)
			return nil
		}
		return fmt.Errorf("create record %q (%s) in zone %q failed: %w", name, recordType, zoneName, err)
	}

	log.WithFields(log.Fields{
		"zone":  zoneName,
		"name":  name,
		"type":  recordType,
		"value": value,
		"ttl":   ttl,
	}).Info("PS Cloud provider: created record")

	return nil
}

func (p *PSProvider) updateRecord(ctx context.Context, zoneName, recordID, value string, ttl *int) error {
	input := map[string]any{
		"value": value,
	}
	if ttl != nil {
		input["ttl"] = *ttl
	}

	if p.dryRun {
		log.WithFields(log.Fields{
			"zone":     zoneName,
			"recordID": recordID,
			"value":    value,
			"ttl":      ttl,
		}).Info("PS Cloud provider: dry-run update record")
		return nil
	}

	var resp updateRecordData
	err := p.doGraphQL(ctx, opUpdateRecord, updateRecordMutation, map[string]any{
		"zoneName": zoneName,
		"recordId": recordID,
		"input":    input,
	}, &resp)
	if err != nil {
		return fmt.Errorf("update record %q in zone %q failed: %w", recordID, zoneName, err)
	}

	log.WithFields(log.Fields{
		"zone":     zoneName,
		"recordID": recordID,
		"value":    value,
		"ttl":      ttl,
	}).Info("PS Cloud provider: updated record")

	return nil
}

func (p *PSProvider) deleteRecord(ctx context.Context, zoneName, recordID string) error {
	if p.dryRun {
		log.WithFields(log.Fields{
			"zone":     zoneName,
			"recordID": recordID,
		}).Info("PS Cloud provider: dry-run delete record")
		return nil
	}

	var resp deleteRecordData
	err := p.doGraphQL(ctx, opDeleteRecord, deleteRecordMutation, map[string]any{
		"zoneName": zoneName,
		"recordId": recordID,
	}, &resp)
	if err != nil {
		return fmt.Errorf("delete record %q in zone %q failed: %w", recordID, zoneName, err)
	}

	return nil
}

func (p *PSProvider) doGraphQL(ctx context.Context, operation, query string, variables map[string]any, out any) error {
	startedAt := time.Now()
	result := "success"
	defer func() {
		graphQLRequestsTotal.WithLabelValues(operation, result).Inc()
		graphQLRequestDurationSeconds.WithLabelValues(operation).Observe(time.Since(startedAt).Seconds())
	}()

	requestBody := graphQLRequest{
		Query:     query,
		Variables: variables,
	}

	body, err := json.Marshal(requestBody)
	if err != nil {
		result = "marshal_request_error"
		return fmt.Errorf("marshal graphql request: %w", err)
	}

	for attempt := 0; attempt <= p.maxRetries; attempt++ {
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, p.endpoint, bytes.NewReader(body))
		if err != nil {
			result = "create_request_error"
			return fmt.Errorf("create graphql request: %w", err)
		}

		req.Header.Set(headerContentType, contentTypeJSON)
		req.Header.Set(headerUserToken, p.token)
		req.Header.Set(headerUserAgent, userAgent)

		resp, err := p.client.Do(req)
		if err != nil {
			if attempt < p.maxRetries && shouldRetryTimeoutError(ctx, err) {
				if waitErr := p.waitBeforeRetry(ctx, operation, attempt+1, err); waitErr != nil {
					result = "retry_wait_error"
					return fmt.Errorf("wait before graphql retry: %w", waitErr)
				}
				continue
			}
			result = "execute_request_error"
			return fmt.Errorf("execute graphql request: %w", err)
		}

		responseBody, readErr := io.ReadAll(io.LimitReader(resp.Body, maxGraphQLResponseBytes+1))
		_ = resp.Body.Close()
		if readErr != nil {
			if attempt < p.maxRetries && shouldRetryTimeoutError(ctx, readErr) {
				if waitErr := p.waitBeforeRetry(ctx, operation, attempt+1, readErr); waitErr != nil {
					result = "retry_wait_error"
					return fmt.Errorf("wait before graphql retry: %w", waitErr)
				}
				continue
			}
			result = "read_response_error"
			return fmt.Errorf("read graphql response: %w", readErr)
		}
		if len(responseBody) > maxGraphQLResponseBytes {
			result = "response_too_large"
			return fmt.Errorf("graphql response exceeds maximum size of %d bytes", maxGraphQLResponseBytes)
		}

		if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
			if attempt < p.maxRetries && shouldRetryStatus(resp.StatusCode) {
				statusErr := fmt.Errorf("graphql request returned status %d: %s", resp.StatusCode, strings.TrimSpace(string(responseBody)))
				if waitErr := p.waitBeforeRetry(ctx, operation, attempt+1, statusErr); waitErr != nil {
					result = "retry_wait_error"
					return fmt.Errorf("wait before graphql retry: %w", waitErr)
				}
				continue
			}
			result = "upstream_http_error"
			return fmt.Errorf("graphql request returned status %d: %s", resp.StatusCode, strings.TrimSpace(string(responseBody)))
		}

		var payload graphQLResponse
		if err := json.Unmarshal(responseBody, &payload); err != nil {
			result = "decode_response_error"
			return fmt.Errorf("decode graphql response: %w", err)
		}

		if len(payload.Errors) > 0 {
			msgs := make([]string, 0, len(payload.Errors))
			for _, gqlErr := range payload.Errors {
				if gqlErr.Message != "" {
					msgs = append(msgs, gqlErr.Message)
				}
			}
			if len(msgs) == 0 {
				msgs = append(msgs, "unknown graphql error")
			}
			result = "graphql_error"
			return fmt.Errorf("graphql errors: %s", strings.Join(msgs, "; "))
		}

		if out == nil || len(payload.Data) == 0 || string(payload.Data) == "null" {
			return nil
		}

		if err := json.Unmarshal(payload.Data, out); err != nil {
			result = "decode_data_error"
			return fmt.Errorf("decode graphql data: %w", err)
		}

		return nil
	}

	result = "retry_exhausted"
	return errors.New("graphql request retries exhausted")
}

func (p *PSProvider) waitBeforeRetry(ctx context.Context, operation string, retryNumber int, cause error) error {
	delay := p.retryDelay(retryNumber)
	log.WithFields(log.Fields{
		"operation":   operation,
		"retry":       retryNumber,
		"max_retries": p.maxRetries,
		"delay":       delay.String(),
		"error":       cause.Error(),
	}).Warn("PS Cloud provider: retrying graphql request")

	return p.sleepFn(ctx, delay)
}

func (p *PSProvider) retryDelay(retryNumber int) time.Duration {
	if retryNumber < 1 {
		retryNumber = 1
	}

	delay := p.retryBase
	for i := 1; i < retryNumber; i++ {
		if delay >= p.retryMax {
			delay = p.retryMax
			break
		}
		delay *= 2
		if delay > p.retryMax {
			delay = p.retryMax
		}
	}

	if p.retryJitter > 0 {
		delay += time.Duration(rand.Int64N(int64(p.retryJitter) + 1))
	}

	return delay
}

func shouldRetryStatus(statusCode int) bool {
	return statusCode == http.StatusTooManyRequests || statusCode >= http.StatusInternalServerError
}

func shouldRetryTimeoutError(ctx context.Context, err error) bool {
	if err == nil || ctx.Err() != nil {
		return false
	}

	var netErr net.Error
	if errors.As(err, &netErr) && netErr.Timeout() {
		return true
	}

	return strings.Contains(strings.ToLower(err.Error()), "timeout")
}

func sleepWithContext(ctx context.Context, d time.Duration) error {
	timer := time.NewTimer(d)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func validateEndpointURL(rawURL string) (string, error) {
	parsedURL, err := url.Parse(strings.TrimSpace(rawURL))
	if err != nil {
		return "", fmt.Errorf("parse graphql endpoint: %w", err)
	}
	if parsedURL.Scheme == "" || parsedURL.Host == "" {
		return "", fmt.Errorf("graphql endpoint must be an absolute URL, got %q", rawURL)
	}

	switch strings.ToLower(parsedURL.Scheme) {
	case "https":
		return parsedURL.String(), nil
	case "http":
		if isLoopbackHost(parsedURL.Hostname()) {
			return parsedURL.String(), nil
		}
		return "", fmt.Errorf("insecure graphql endpoint is allowed only for localhost/loopback, got host %q", parsedURL.Hostname())
	default:
		return "", fmt.Errorf("unsupported graphql endpoint scheme %q", parsedURL.Scheme)
	}
}

func isLoopbackHost(host string) bool {
	if strings.EqualFold(host, "localhost") {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}

func (p *PSProvider) zoneForDNSName(ctx context.Context, dnsName string) (string, error) {
	if err := p.ensureZoneMap(ctx); err != nil {
		return "", err
	}

	name := normalizeDNSName(dnsName)

	p.mu.RLock()
	defer p.mu.RUnlock()

	_, zoneName := p.zoneNameMap.FindZone(name)
	if zoneName == "" {
		return "", externaldnsprovider.NewSoftErrorf("no zone found for DNS name %q", dnsName)
	}

	return normalizeDNSName(zoneName), nil
}

func (p *PSProvider) ensureZoneMap(ctx context.Context) error {
	p.mu.RLock()
	zoneMapInit := p.zoneMapInit
	p.mu.RUnlock()
	if zoneMapInit {
		return nil
	}

	zones, err := p.listManagedZones(ctx)
	if err != nil {
		return err
	}

	zoneMap := externaldnsprovider.ZoneIDName{}
	for _, z := range zones {
		zoneName := normalizeDNSName(z.Name)
		zoneMap.Add(zoneName, zoneName)
	}

	p.mu.Lock()
	p.zoneNameMap = zoneMap
	p.zoneMapInit = true
	p.mu.Unlock()
	return nil
}

func (p *PSProvider) ensureRecordIndex(ctx context.Context) error {
	p.mu.RLock()
	recordIndexInit := p.recordIndexInit
	p.mu.RUnlock()
	if recordIndexInit {
		return nil
	}

	_, err := p.records(ctx)
	return err
}

func (p *PSProvider) findRecordID(ctx context.Context, key recordKey) (string, error) {
	if recordID, ok := p.getRecordID(key); ok {
		return recordID, nil
	}

	if err := p.refreshZoneRecordIndex(ctx, key.zone); err != nil {
		return "", err
	}

	recordID, ok := p.getRecordID(key)
	if !ok {
		return "", externaldnsprovider.NewSoftErrorf(
			"record id not found for zone=%q name=%q type=%q value=%q",
			key.zone, key.name, key.recordType, key.value,
		)
	}

	return recordID, nil
}

func (p *PSProvider) refreshZoneRecordIndex(ctx context.Context, zoneName string) error {
	records, err := p.listZoneRecords(ctx, zoneName)
	if err != nil {
		return err
	}

	zoneName = normalizeDNSName(zoneName)
	index := make(map[recordKey]string)
	for _, record := range records {
		recordType := normalizeRecordType(record.Type)
		if !externaldnsprovider.SupportedRecordType(recordType) {
			continue
		}

		key := newRecordKey(zoneName, record.Name, recordType, record.Value)
		index[key] = record.ID
	}

	p.mu.Lock()
	for key := range p.recordIDByKV {
		if key.zone == zoneName {
			delete(p.recordIDByKV, key)
		}
	}
	for key, recordID := range index {
		p.recordIDByKV[key] = recordID
	}
	p.recordIndexInit = true
	p.mu.Unlock()
	return nil
}

func (p *PSProvider) replaceState(zoneMap externaldnsprovider.ZoneIDName, recordIDs map[recordKey]string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.zoneNameMap = zoneMap
	p.zoneMapInit = true
	p.recordIDByKV = recordIDs
	p.recordIndexInit = true
}

func (p *PSProvider) getRecordID(key recordKey) (string, bool) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	recordID, ok := p.recordIDByKV[key]
	return recordID, ok
}

func (p *PSProvider) replaceRecordID(oldKey, newKey recordKey, recordID string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	delete(p.recordIDByKV, oldKey)
	p.recordIDByKV[newKey] = recordID
}

func (p *PSProvider) deleteRecordID(key recordKey) {
	p.mu.Lock()
	defer p.mu.Unlock()
	delete(p.recordIDByKV, key)
}

func newRecordKey(zoneName, name, recordType, value string) recordKey {
	recordType = normalizeRecordType(recordType)
	return recordKey{
		zone:       normalizeDNSName(zoneName),
		name:       normalizeDNSName(name),
		recordType: recordType,
		value:      normalizeRecordValue(recordType, value),
	}
}

func normalizeDNSName(name string) string {
	return externaldnsprovider.EnsureTrailingDot(strings.TrimSpace(name))
}

func normalizeRecordType(recordType string) string {
	return strings.ToUpper(strings.TrimSpace(recordType))
}

func normalizeRecordValue(recordType, value string) string {
	value = strings.TrimSpace(value)
	switch normalizeRecordType(recordType) {
	case endpoint.RecordTypeCNAME, endpoint.RecordTypeNS, endpoint.RecordTypePTR:
		return externaldnsprovider.EnsureTrailingDot(value)
	default:
		return value
	}
}

func targetsForEndpoint(ep *endpoint.Endpoint) []string {
	if ep == nil {
		return nil
	}

	recordType := normalizeRecordType(ep.RecordType)
	targetSet := make(map[string]struct{}, len(ep.Targets))
	for _, t := range ep.Targets {
		target := normalizeRecordValue(recordType, t)
		if target == "" {
			continue
		}
		targetSet[target] = struct{}{}
	}

	targets := make([]string, 0, len(targetSet))
	for target := range targetSet {
		targets = append(targets, target)
	}
	sort.Strings(targets)
	return targets
}

func ttlForCreate(ep *endpoint.Endpoint) int {
	if ep != nil && ep.RecordTTL.IsConfigured() {
		return int(ep.RecordTTL)
	}
	return defaultRecordTTL
}

func ttlForUpdate(oldEndpoint, newEndpoint *endpoint.Endpoint) *int {
	if newEndpoint == nil || !newEndpoint.RecordTTL.IsConfigured() {
		return nil
	}

	newTTL := int(newEndpoint.RecordTTL)
	if oldEndpoint != nil && oldEndpoint.RecordTTL.IsConfigured() && int(oldEndpoint.RecordTTL) == newTTL {
		return nil
	}

	return &newTTL
}

func diffTargets(oldTargets, newTargets []string) (shared []string, oldOnly []string, newOnly []string) {
	oldSet := make(map[string]struct{}, len(oldTargets))
	for _, target := range oldTargets {
		oldSet[target] = struct{}{}
	}

	for _, target := range newTargets {
		if _, ok := oldSet[target]; ok {
			shared = append(shared, target)
			delete(oldSet, target)
		} else {
			newOnly = append(newOnly, target)
		}
	}

	for target := range oldSet {
		oldOnly = append(oldOnly, target)
	}

	sort.Strings(shared)
	sort.Strings(oldOnly)
	sort.Strings(newOnly)
	return shared, oldOnly, newOnly
}

func isAlreadyExistsError(err error) bool {
	if err == nil {
		return false
	}
	return strings.Contains(strings.ToLower(err.Error()), "record already exist")
}

package provider

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"sigs.k8s.io/external-dns/endpoint"
	"sigs.k8s.io/external-dns/plan"
)

func TestPSProviderRecords(t *testing.T) {
	server := newGraphQLTestServer(t)
	server.zonePages[1] = []graphqlZoneDTO{
		{ID: "zone-example", Name: "example.com.", Enabled: true, Type: "MASTER"},
		{ID: "zone-disabled", Name: "disabled.example.com.", Enabled: false, Type: "MASTER"},
	}
	server.zoneRecords["example.com."] = []graphqlRecordDTO{
		{ID: "rec-a-1", Name: "a.example.com.", Type: "A", TTL: intPtr(3600), Value: "1.2.3.4"},
		{ID: "rec-txt-1", Name: "_external-dns.example.com.", Type: "TXT", TTL: intPtr(300), Value: "heritage=external-dns"},
		{ID: "rec-caa-1", Name: "a.example.com.", Type: "CAA", TTL: intPtr(300), Value: "0 issue \"letsencrypt.org\""},
	}

	domainFilter := endpoint.NewDomainFilter([]string{"example.com"})
	provider, err := NewPSProvider(Config{
		Endpoint:     server.server.URL,
		Token:        "token-123",
		DomainFilter: domainFilter,
		HTTPClient:   server.server.Client(),
	})
	require.NoError(t, err)

	records, err := provider.Records(context.Background())
	require.NoError(t, err)
	require.Len(t, records, 2)

	signatures := make([]string, 0, len(records))
	for _, record := range records {
		signatures = append(signatures, record.DNSName+"|"+record.RecordType+"|"+strings.Join(record.Targets, ","))
	}
	slices.Sort(signatures)
	require.Equal(t, []string{
		"_external-dns.example.com|TXT|heritage=external-dns",
		"a.example.com|A|1.2.3.4",
	}, signatures)

	server.assertAllRequestsHaveHeaders(t)
}

func TestNewPSProviderDefaultTunables(t *testing.T) {
	provider, err := NewPSProvider(Config{
		Endpoint: "https://example.test/graphql",
		Token:    "token-123",
	})
	require.NoError(t, err)
	require.Equal(t, defaultPageSize, provider.pageSize)
	require.Equal(t, defaultMaxRetries, provider.maxRetries)
	require.Equal(t, defaultRetryInitialBackoff, provider.retryBase)
	require.Equal(t, defaultRetryMaxBackoff, provider.retryMax)
	require.Equal(t, defaultRetryJitter, provider.retryJitter)
}

func TestNewPSProviderCustomTunables(t *testing.T) {
	pageSize := 250
	maxRetries := 7
	retryInitial := 50 * time.Millisecond
	retryMax := 3 * time.Second
	retryJitter := time.Duration(0)

	provider, err := NewPSProvider(Config{
		Endpoint:            "https://example.test/graphql",
		Token:               "token-123",
		ZonesPageSize:       &pageSize,
		MaxRetries:          &maxRetries,
		RetryInitialBackoff: &retryInitial,
		RetryMaxBackoff:     &retryMax,
		RetryJitter:         &retryJitter,
	})
	require.NoError(t, err)
	require.Equal(t, 250, provider.pageSize)
	require.Equal(t, 7, provider.maxRetries)
	require.Equal(t, 50*time.Millisecond, provider.retryBase)
	require.Equal(t, 3*time.Second, provider.retryMax)
	require.Equal(t, time.Duration(0), provider.retryJitter)
}

func TestNewPSProviderInvalidTunables(t *testing.T) {
	t.Run("negative max retries", func(t *testing.T) {
		maxRetries := -1
		_, err := NewPSProvider(Config{
			Endpoint:   "https://example.test/graphql",
			Token:      "token-123",
			MaxRetries: &maxRetries,
		})
		require.Error(t, err)
		require.Contains(t, err.Error(), "max retries")
	})

	t.Run("max backoff less than initial", func(t *testing.T) {
		retryInitial := 2 * time.Second
		retryMax := 1 * time.Second
		_, err := NewPSProvider(Config{
			Endpoint:            "https://example.test/graphql",
			Token:               "token-123",
			RetryInitialBackoff: &retryInitial,
			RetryMaxBackoff:     &retryMax,
		})
		require.Error(t, err)
		require.Contains(t, err.Error(), "retry max backoff")
	})

	t.Run("negative jitter", func(t *testing.T) {
		retryJitter := -1 * time.Millisecond
		_, err := NewPSProvider(Config{
			Endpoint:    "https://example.test/graphql",
			Token:       "token-123",
			RetryJitter: &retryJitter,
		})
		require.Error(t, err)
		require.Contains(t, err.Error(), "retry jitter")
	})

	t.Run("invalid zones page size", func(t *testing.T) {
		pageSize := 0
		_, err := NewPSProvider(Config{
			Endpoint:      "https://example.test/graphql",
			Token:         "token-123",
			ZonesPageSize: &pageSize,
		})
		require.Error(t, err)
		require.Contains(t, err.Error(), "zones page size")
	})

	t.Run("invalid retry initial backoff", func(t *testing.T) {
		retryInitial := time.Duration(0)
		_, err := NewPSProvider(Config{
			Endpoint:            "https://example.test/graphql",
			Token:               "token-123",
			RetryInitialBackoff: &retryInitial,
		})
		require.Error(t, err)
		require.Contains(t, err.Error(), "retry initial backoff")
	})

	t.Run("invalid retry max backoff", func(t *testing.T) {
		retryMax := time.Duration(0)
		_, err := NewPSProvider(Config{
			Endpoint:        "https://example.test/graphql",
			Token:           "token-123",
			RetryMaxBackoff: &retryMax,
		})
		require.Error(t, err)
		require.Contains(t, err.Error(), "retry max backoff")
	})
}

func TestNewPSProviderEndpointValidation(t *testing.T) {
	t.Run("reject non-loopback http endpoint", func(t *testing.T) {
		_, err := NewPSProvider(Config{
			Endpoint: "http://api.example.com/graphql",
			Token:    "token-123",
		})
		require.Error(t, err)
		require.Contains(t, err.Error(), "insecure graphql endpoint")
	})

	t.Run("allow loopback http endpoint", func(t *testing.T) {
		_, err := NewPSProvider(Config{
			Endpoint: "http://localhost/graphql",
			Token:    "token-123",
		})
		require.NoError(t, err)
	})
}

func TestPSProviderRecordsGroupsMultiTarget(t *testing.T) {
	server := newGraphQLTestServer(t)
	server.zonePages[1] = []graphqlZoneDTO{
		{ID: "zone-example", Name: "example.com.", Enabled: true, Type: "MASTER"},
	}
	server.zoneRecords["example.com."] = []graphqlRecordDTO{
		{ID: "rec-a-1", Name: "multi.example.com.", Type: "A", TTL: intPtr(120), Value: "1.2.3.4"},
		{ID: "rec-a-2", Name: "multi.example.com.", Type: "A", TTL: intPtr(120), Value: "5.6.7.8"},
	}

	provider, err := NewPSProvider(Config{
		Endpoint:   server.server.URL,
		Token:      "token-123",
		HTTPClient: server.server.Client(),
	})
	require.NoError(t, err)

	records, err := provider.Records(context.Background())
	require.NoError(t, err)
	require.Len(t, records, 1)
	require.Equal(t, "multi.example.com", records[0].DNSName)
	require.Equal(t, endpoint.RecordTypeA, records[0].RecordType)
	require.Equal(t, endpoint.TTL(120), records[0].RecordTTL)
	require.ElementsMatch(t, []string{"1.2.3.4", "5.6.7.8"}, []string(records[0].Targets))
}

func TestPSProviderApplyChanges(t *testing.T) {
	server := newGraphQLTestServer(t)
	server.zonePages[1] = []graphqlZoneDTO{
		{ID: "zone-example", Name: "example.com.", Enabled: true, Type: "MASTER"},
	}
	server.zoneRecords["example.com."] = []graphqlRecordDTO{
		{ID: "a-1", Name: "app.example.com.", Type: "A", TTL: intPtr(300), Value: "1.1.1.1"},
		{ID: "a-2", Name: "app.example.com.", Type: "A", TTL: intPtr(300), Value: "2.2.2.2"},
		{ID: "txt-1", Name: "txt.example.com.", Type: "TXT", TTL: intPtr(300), Value: "heritage=external-dns,owner=default"},
	}

	provider, err := NewPSProvider(Config{
		Endpoint:   server.server.URL,
		Token:      "token-123",
		HTTPClient: server.server.Client(),
	})
	require.NoError(t, err)

	// Prime internal zone and record index.
	_, err = provider.Records(context.Background())
	require.NoError(t, err)

	changes := &plan.Changes{
		Create: []*endpoint.Endpoint{
			endpoint.NewEndpointWithTTL("multi.example.com.", endpoint.RecordTypeA, endpoint.TTL(120), "4.4.4.4", "5.5.5.5"),
		},
		Delete: []*endpoint.Endpoint{
			endpoint.NewEndpointWithTTL("txt.example.com.", endpoint.RecordTypeTXT, endpoint.TTL(300), "heritage=external-dns,owner=default"),
		},
		UpdateOld: []*endpoint.Endpoint{
			endpoint.NewEndpointWithTTL("app.example.com.", endpoint.RecordTypeA, endpoint.TTL(300), "1.1.1.1", "2.2.2.2"),
		},
		UpdateNew: []*endpoint.Endpoint{
			endpoint.NewEndpointWithTTL("app.example.com.", endpoint.RecordTypeA, endpoint.TTL(600), "2.2.2.2", "3.3.3.3"),
		},
	}

	err = provider.ApplyChanges(context.Background(), changes)
	require.NoError(t, err)

	require.Len(t, server.updateCalls, 2)
	require.ElementsMatch(t, []updateCall{
		{ZoneName: "example.com.", RecordID: "a-1", Value: "3.3.3.3", TTL: intPtr(600)},
		{ZoneName: "example.com.", RecordID: "a-2", Value: "2.2.2.2", TTL: intPtr(600)},
	}, server.updateCalls)

	require.Len(t, server.createCalls, 2)
	require.ElementsMatch(t, []createCall{
		{ZoneName: "example.com.", Name: "multi.example.com.", RecordType: "A", Value: "4.4.4.4", TTL: 120},
		{ZoneName: "example.com.", Name: "multi.example.com.", RecordType: "A", Value: "5.5.5.5", TTL: 120},
	}, server.createCalls)

	require.Len(t, server.deleteCalls, 1)
	require.Equal(t, []deleteCall{
		{ZoneName: "example.com.", RecordID: "txt-1"},
	}, server.deleteCalls)

	server.assertAllRequestsHaveHeaders(t)
}

func TestPSProviderDoGraphQLRetriesOnRateLimit(t *testing.T) {
	var (
		mu    sync.Mutex
		calls int
	)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		calls++

		if calls == 1 {
			w.WriteHeader(http.StatusTooManyRequests)
			_, _ = w.Write([]byte("rate limited"))
			return
		}

		writeJSON(t, w, map[string]any{
			"data": map[string]any{
				"dns": map[string]any{
					"zones": map[string]any{
						"count": 0,
						"items": []any{},
					},
				},
			},
		})
	}))
	t.Cleanup(server.Close)

	provider, err := NewPSProvider(Config{
		Endpoint:   server.URL,
		Token:      "token-123",
		HTTPClient: server.Client(),
	})
	require.NoError(t, err)
	provider.retryBase = time.Millisecond
	provider.retryMax = time.Millisecond
	provider.retryJitter = 0
	provider.maxRetries = 2

	_, err = provider.Records(context.Background())
	require.NoError(t, err)
	require.Equal(t, 2, calls)
}

func TestPSProviderDoGraphQLRetriesOnHTTP5xx(t *testing.T) {
	var (
		mu    sync.Mutex
		calls int
	)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		calls++

		if calls <= 2 {
			w.WriteHeader(http.StatusBadGateway)
			_, _ = w.Write([]byte("temporary upstream error"))
			return
		}

		writeJSON(t, w, map[string]any{
			"data": map[string]any{
				"dns": map[string]any{
					"zones": map[string]any{
						"count": 0,
						"items": []any{},
					},
				},
			},
		})
	}))
	t.Cleanup(server.Close)

	provider, err := NewPSProvider(Config{
		Endpoint:   server.URL,
		Token:      "token-123",
		HTTPClient: server.Client(),
	})
	require.NoError(t, err)
	provider.retryBase = time.Millisecond
	provider.retryMax = time.Millisecond
	provider.retryJitter = 0
	provider.maxRetries = 2

	_, err = provider.Records(context.Background())
	require.NoError(t, err)
	require.Equal(t, 3, calls)
}

func TestPSProviderDoGraphQLRetriesOnTimeout(t *testing.T) {
	roundTripper := &timeoutThenOKRoundTripper{}
	client := &http.Client{Transport: roundTripper}

	provider, err := NewPSProvider(Config{
		Endpoint:   "http://127.0.0.1/graphql",
		Token:      "token-123",
		HTTPClient: client,
	})
	require.NoError(t, err)
	provider.retryBase = time.Millisecond
	provider.retryMax = time.Millisecond
	provider.retryJitter = 0
	provider.maxRetries = 2

	var out listZonesData
	err = provider.doGraphQL(context.Background(), opListZones, listZonesQuery, map[string]any{
		"page":    1,
		"perPage": 100,
	}, &out)
	require.NoError(t, err)
	require.Equal(t, 2, roundTripper.CallCount())
}

func TestPSProviderDoGraphQLRejectsOversizedResponse(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(strings.Repeat("x", maxGraphQLResponseBytes+1)))
	}))
	t.Cleanup(server.Close)

	provider, err := NewPSProvider(Config{
		Endpoint:   server.URL,
		Token:      "token-123",
		HTTPClient: server.Client(),
	})
	require.NoError(t, err)

	err = provider.doGraphQL(context.Background(), opListZones, listZonesQuery, map[string]any{
		"page":    1,
		"perPage": 100,
	}, nil)
	require.Error(t, err)
	require.Contains(t, err.Error(), "exceeds maximum size")
}

type createCall struct {
	ZoneName   string
	Name       string
	RecordType string
	Value      string
	TTL        int
}

type updateCall struct {
	ZoneName string
	RecordID string
	Value    string
	TTL      *int
}

type deleteCall struct {
	ZoneName string
	RecordID string
}

type graphQLTestServer struct {
	mu sync.Mutex

	t      *testing.T
	server *httptest.Server

	zonePages   map[int][]graphqlZoneDTO
	zoneRecords map[string][]graphqlRecordDTO

	createCalls []createCall
	updateCalls []updateCall
	deleteCalls []deleteCall

	seenContentTypes []string
	seenUserTokens   []string
}

type timeoutThenOKRoundTripper struct {
	mu    sync.Mutex
	calls int
}

func (t *timeoutThenOKRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.calls++
	if t.calls == 1 {
		return nil, &url.Error{
			Op:  req.Method,
			URL: req.URL.String(),
			Err: timeoutError{},
		}
	}

	return &http.Response{
		StatusCode: http.StatusOK,
		Header:     http.Header{headerContentType: []string{contentTypeJSON}},
		Body: io.NopCloser(strings.NewReader(`{
			"data": {
				"dns": {
					"zones": {
						"count": 0,
						"items": []
					}
				}
			}
		}`)),
	}, nil
}

func (t *timeoutThenOKRoundTripper) CallCount() int {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.calls
}

type timeoutError struct{}

func (timeoutError) Error() string   { return "i/o timeout" }
func (timeoutError) Timeout() bool   { return true }
func (timeoutError) Temporary() bool { return true }

func newGraphQLTestServer(t *testing.T) *graphQLTestServer {
	t.Helper()
	srv := &graphQLTestServer{
		t:           t,
		zonePages:   map[int][]graphqlZoneDTO{},
		zoneRecords: map[string][]graphqlRecordDTO{},
	}
	srv.server = httptest.NewServer(http.HandlerFunc(srv.handleRequest))
	t.Cleanup(srv.server.Close)
	return srv
}

func (s *graphQLTestServer) handleRequest(w http.ResponseWriter, r *http.Request) {
	s.mu.Lock()
	defer s.mu.Unlock()

	require.Equal(s.t, http.MethodPost, r.Method)

	s.seenContentTypes = append(s.seenContentTypes, r.Header.Get(headerContentType))
	s.seenUserTokens = append(s.seenUserTokens, r.Header.Get(headerUserToken))

	var req graphQLRequest
	err := json.NewDecoder(r.Body).Decode(&req)
	require.NoError(s.t, err)

	switch {
	case strings.Contains(req.Query, "ListZones"):
		page := int(mustNumber(req.Variables["page"]))
		items := s.zonePages[page]

		resp := map[string]any{
			"data": map[string]any{
				"dns": map[string]any{
					"zones": map[string]any{
						"count": len(s.flattenZonePages()),
						"items": items,
					},
				},
			},
		}
		writeJSON(s.t, w, resp)
	case strings.Contains(req.Query, "ZoneRecords"):
		zoneName, ok := req.Variables["zoneName"].(string)
		require.True(s.t, ok)

		resp := map[string]any{
			"data": map[string]any{
				"dns": map[string]any{
					"zone": map[string]any{
						"id":      "zone-id",
						"name":    zoneName,
						"records": s.zoneRecords[zoneName],
					},
				},
			},
		}
		writeJSON(s.t, w, resp)
	case strings.Contains(req.Query, "CreateRecord"):
		call := createCall{
			ZoneName:   req.Variables["zoneName"].(string),
			Name:       req.Variables["name"].(string),
			RecordType: req.Variables["type"].(string),
			Value:      req.Variables["value"].(string),
			TTL:        int(mustNumber(req.Variables["ttl"])),
		}
		s.createCalls = append(s.createCalls, call)
		writeJSON(s.t, w, map[string]any{
			"data": map[string]any{
				"dns": map[string]any{
					"record": map[string]any{
						"create": map[string]any{"id": "ok"},
					},
				},
			},
		})
	case strings.Contains(req.Query, "UpdateRecord"):
		input := req.Variables["input"].(map[string]any)
		var ttl *int
		if rawTTL, found := input["ttl"]; found {
			value := int(mustNumber(rawTTL))
			ttl = &value
		}

		call := updateCall{
			ZoneName: req.Variables["zoneName"].(string),
			RecordID: req.Variables["recordId"].(string),
			Value:    input["value"].(string),
			TTL:      ttl,
		}
		s.updateCalls = append(s.updateCalls, call)
		writeJSON(s.t, w, map[string]any{
			"data": map[string]any{
				"dns": map[string]any{
					"record": map[string]any{
						"update": map[string]any{"id": "ok"},
					},
				},
			},
		})
	case strings.Contains(req.Query, "DeleteRecord"):
		call := deleteCall{
			ZoneName: req.Variables["zoneName"].(string),
			RecordID: req.Variables["recordId"].(string),
		}
		s.deleteCalls = append(s.deleteCalls, call)
		writeJSON(s.t, w, map[string]any{
			"data": map[string]any{
				"dns": map[string]any{
					"record": map[string]any{
						"delete": map[string]any{"id": "ok"},
					},
				},
			},
		})
	default:
		require.FailNowf(s.t, "unexpected operation", "query=%s", req.Query)
	}
}

func (s *graphQLTestServer) flattenZonePages() []graphqlZoneDTO {
	all := make([]graphqlZoneDTO, 0)
	for _, page := range s.zonePages {
		all = append(all, page...)
	}
	return all
}

func (s *graphQLTestServer) assertAllRequestsHaveHeaders(t *testing.T) {
	t.Helper()
	require.NotEmpty(t, s.seenContentTypes)
	require.NotEmpty(t, s.seenUserTokens)
	for _, value := range s.seenContentTypes {
		require.Equal(t, contentTypeJSON, value)
	}
	for _, value := range s.seenUserTokens {
		require.Equal(t, "token-123", value)
	}
}

func intPtr(value int) *int {
	return &value
}

func mustNumber(value any) float64 {
	num, ok := value.(float64)
	if ok {
		return num
	}
	panic("value is not numeric")
}

func writeJSON(t *testing.T, w http.ResponseWriter, payload any) {
	t.Helper()
	w.Header().Set(headerContentType, contentTypeJSON)
	err := json.NewEncoder(w).Encode(payload)
	require.NoError(t, err)
}

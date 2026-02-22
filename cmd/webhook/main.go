package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	log "github.com/sirupsen/logrus"

	psprovider "external-dns-pscloud-webhook/internal/provider"

	"sigs.k8s.io/external-dns/endpoint"
	externaldnsprovider "sigs.k8s.io/external-dns/provider"
	webhookapi "sigs.k8s.io/external-dns/provider/webhook/api"
)

const tokenEnvVar = "PS_DNS_TOKEN"
const shutdownTimeout = 5 * time.Second

func main() {
	var (
		webhookAddr         = flag.String("webhook-addr", ":8888", "Webhook API listen address.")
		healthAddr          = flag.String("health-addr", ":8080", "Health endpoint listen address.")
		graphqlEndpoint     = flag.String("graphql-endpoint", psprovider.DefaultGraphQLEndpoint, "PS DNS GraphQL endpoint.")
		domainFilter        = flag.String("domain-filter", "", "Comma-separated domain filter for managed zones.")
		dryRun              = flag.Bool("dry-run", false, "Dry-run mode.")
		readTimeout         = flag.Duration("read-timeout", 5*time.Second, "Webhook read timeout.")
		writeTimeout        = flag.Duration("write-timeout", 10*time.Second, "Webhook write timeout.")
		requestTimeout      = flag.Duration("request-timeout", 30*time.Second, "HTTP timeout for GraphQL API requests.")
		zonesPageSize       = flag.Int("zones-page-size", 100, "GraphQL zones page size (perPage).")
		graphqlMaxRetries   = flag.Int("graphql-max-retries", 3, "Maximum GraphQL retries for 429/5xx/timeout errors.")
		graphqlRetryInitial = flag.Duration("graphql-retry-initial-backoff", 200*time.Millisecond, "Initial GraphQL retry backoff.")
		graphqlRetryMax     = flag.Duration("graphql-retry-max-backoff", 2*time.Second, "Maximum GraphQL retry backoff.")
		graphqlRetryJitter  = flag.Duration("graphql-retry-jitter", 100*time.Millisecond, "Additional random jitter for GraphQL retry backoff.")
		logLevel            = flag.String("log-level", "info", "Log level: debug, info, warn, error.")
	)
	flag.Parse()

	level, err := log.ParseLevel(strings.ToLower(strings.TrimSpace(*logLevel)))
	if err != nil {
		log.Fatalf("Invalid log level: %v", err)
	}
	log.SetLevel(level)

	token := strings.TrimSpace(os.Getenv(tokenEnvVar))
	if token == "" {
		log.Fatalf("Required environment variable %s is not set", tokenEnvVar)
	}

	filters := parseCSV(*domainFilter)
	df := endpoint.NewDomainFilter(filters)

	prov, err := psprovider.NewPSProvider(psprovider.Config{
		Endpoint:            *graphqlEndpoint,
		Token:               token,
		DomainFilter:        df,
		DryRun:              *dryRun,
		ZonesPageSize:       zonesPageSize,
		MaxRetries:          graphqlMaxRetries,
		RetryInitialBackoff: graphqlRetryInitial,
		RetryMaxBackoff:     graphqlRetryMax,
		RetryJitter:         graphqlRetryJitter,
		HTTPClient: &http.Client{
			Timeout: *requestTimeout,
		},
	})
	if err != nil {
		log.Fatalf("Failed to initialize provider: %v", err)
	}

	var webhookStarted atomic.Bool
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	log.Infof(
		"Starting PS Cloud DNS webhook with GraphQL endpoint=%s dry-run=%t domain-filter=%v zones-page-size=%d request-timeout=%s max-retries=%d retry-initial=%s retry-max=%s retry-jitter=%s",
		*graphqlEndpoint,
		*dryRun,
		filters,
		*zonesPageSize,
		requestTimeout.String(),
		*graphqlMaxRetries,
		graphqlRetryInitial.String(),
		graphqlRetryMax.String(),
		graphqlRetryJitter.String(),
	)

	errCh := make(chan error, 2)
	go func() {
		errCh <- startHealthServer(ctx, *healthAddr, func() bool { return webhookStarted.Load() })
	}()
	go func() {
		errCh <- startWebhookServer(ctx, *webhookAddr, *readTimeout, *writeTimeout, prov, func() {
			webhookStarted.Store(true)
			log.Infof("Webhook API started at %s", *webhookAddr)
		})
	}()

	running := 2
	ctxDone := ctx.Done()
	for running > 0 {
		select {
		case err := <-errCh:
			running--
			if err != nil {
				log.Fatalf("Server error: %v", err)
			}
		case <-ctxDone:
			log.Info("Shutdown signal received, stopping servers")
			ctxDone = nil
		}
	}
}

func parseCSV(raw string) []string {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil
	}
	parts := strings.Split(raw, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		v := strings.TrimSpace(p)
		if v != "" {
			out = append(out, v)
		}
	}
	return out
}

func startHealthServer(ctx context.Context, addr string, startedFn func() bool) error {
	server := &http.Server{
		Addr:    addr,
		Handler: newHealthMux(startedFn),
	}

	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("bind health server address %s: %w", addr, err)
	}

	go func() {
		<-ctx.Done()
		shutdownServer("health", server)
	}()

	log.Infof("Health server started at %s", addr)
	if err := server.Serve(listener); err != nil {
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return fmt.Errorf("Health server stopped: %w", err)
	}
	return nil
}

func startWebhookServer(
	ctx context.Context,
	addr string,
	readTimeout time.Duration,
	writeTimeout time.Duration,
	provider externaldnsprovider.Provider,
	onStarted func(),
) error {
	webhookServer := webhookapi.WebhookServer{Provider: provider}
	mux := http.NewServeMux()
	mux.HandleFunc("/", webhookServer.NegotiateHandler)
	mux.HandleFunc(webhookapi.UrlRecords, webhookServer.RecordsHandler)
	mux.HandleFunc(webhookapi.UrlAdjustEndpoints, webhookServer.AdjustEndpointsHandler)

	server := &http.Server{
		Addr:         addr,
		Handler:      mux,
		ReadTimeout:  readTimeout,
		WriteTimeout: writeTimeout,
	}

	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("bind webhook server address %s: %w", addr, err)
	}

	if onStarted != nil {
		onStarted()
	}

	go func() {
		<-ctx.Done()
		shutdownServer("webhook", server)
	}()

	if err := server.Serve(listener); err != nil {
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return fmt.Errorf("webhook server stopped: %w", err)
	}
	return nil
}

func shutdownServer(name string, server *http.Server) {
	shutdownCtx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer cancel()

	if err := server.Shutdown(shutdownCtx); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Errorf("%s server graceful shutdown failed: %v", name, err)
	}
}

func newHealthMux(startedFn func() bool) http.Handler {
	m := http.NewServeMux()
	readiness := func(w http.ResponseWriter, _ *http.Request) {
		if !startedFn() {
			w.WriteHeader(http.StatusServiceUnavailable)
			_, _ = w.Write([]byte("starting"))
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	}

	m.HandleFunc("/healthz", readiness)
	m.HandleFunc("/readyz", readiness)
	m.HandleFunc("/livez", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})
	m.Handle("/metrics", promhttp.Handler())
	return m
}

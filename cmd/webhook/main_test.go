package main

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestHealthzReturnsUnavailableWhenWebhookNotStarted(t *testing.T) {
	server := httptest.NewServer(newHealthMux(func() bool { return false }))
	t.Cleanup(server.Close)

	resp, err := http.Get(server.URL + "/healthz")
	require.NoError(t, err)
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)

	require.Equal(t, http.StatusServiceUnavailable, resp.StatusCode)
	require.Equal(t, "starting", strings.TrimSpace(string(body)))
}

func TestHealthzReturnsOKWhenWebhookStarted(t *testing.T) {
	server := httptest.NewServer(newHealthMux(func() bool { return true }))
	t.Cleanup(server.Close)

	resp, err := http.Get(server.URL + "/healthz")
	require.NoError(t, err)
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)

	require.Equal(t, http.StatusOK, resp.StatusCode)
	require.Equal(t, "ok", strings.TrimSpace(string(body)))
}

func TestMetricsEndpointExposed(t *testing.T) {
	server := httptest.NewServer(newHealthMux(func() bool { return true }))
	t.Cleanup(server.Close)

	resp, err := http.Get(server.URL + "/metrics")
	require.NoError(t, err)
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)

	require.Equal(t, http.StatusOK, resp.StatusCode)
	require.Contains(t, string(body), "# HELP")
}

func TestReadyzEndpointReflectsStartupState(t *testing.T) {
	server := httptest.NewServer(newHealthMux(func() bool { return false }))
	t.Cleanup(server.Close)

	resp, err := http.Get(server.URL + "/readyz")
	require.NoError(t, err)
	defer resp.Body.Close()

	require.Equal(t, http.StatusServiceUnavailable, resp.StatusCode)
}

func TestLivezEndpointAlwaysOK(t *testing.T) {
	server := httptest.NewServer(newHealthMux(func() bool { return false }))
	t.Cleanup(server.Close)

	resp, err := http.Get(server.URL + "/livez")
	require.NoError(t, err)
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, resp.StatusCode)
	require.Equal(t, "ok", strings.TrimSpace(string(body)))
}

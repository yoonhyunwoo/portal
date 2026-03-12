package tunnelruntime

import (
	"context"
	"errors"
	"io"
	"net"
	"strings"
	"testing"

	"github.com/gosuda/portal/v2/types"
)

func TestResolveRelayURLsIncludesDefaults(t *testing.T) {
	t.Parallel()

	runtime := Runtime{
		deps: runtimeDeps{
			withDefaultRelayURLs: func(_ context.Context, relayURLs ...string) []string {
				out := append([]string{"https://default.example.com"}, relayURLs...)
				return out
			},
			normalizeRelayURLs: func(relayURLs []string) ([]string, error) {
				return relayURLs, nil
			},
		},
	}

	got, err := runtime.resolveRelayURLs(context.Background(), Config{
		RelayURLs:        []string{"https://extra.example.com"},
		UseDefaultRelays: true,
	})
	if err != nil {
		t.Fatalf("resolveRelayURLs() error = %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("resolveRelayURLs() length = %d, want 2", len(got))
	}
	if got[0] != "https://default.example.com" || got[1] != "https://extra.example.com" {
		t.Fatalf("resolveRelayURLs() = %v", got)
	}
}

func TestRunReturnsErrorWhenExposureIsNil(t *testing.T) {
	t.Parallel()

	runtime := Runtime{
		deps: runtimeDeps{
			withDefaultRelayURLs: func(_ context.Context, relayURLs ...string) []string {
				return relayURLs
			},
			normalizeRelayURLs: func(relayURLs []string) ([]string, error) {
				return relayURLs, nil
			},
			expose: func(context.Context, []string, string, types.LeaseMetadata) (relayExposure, error) {
				return nil, nil
			},
		},
	}

	err := runtime.Run(context.Background(), Config{
		RelayURLs: []string{"https://relay.example.com"},
		Name:      "demo",
	})
	if err == nil || !strings.Contains(err.Error(), "no relay URLs provided") {
		t.Fatalf("Run() error = %v, want no relay URLs provided", err)
	}
}

func TestProxyConnectionWritesUnavailableResponse(t *testing.T) {
	t.Parallel()

	serverConn, clientConn := net.Pipe()
	runtime := Runtime{
		deps: runtimeDeps{
			normalizeTargetAddr: func(string) (string, error) {
				return "127.0.0.1:9999", nil
			},
			dialLocal: func(context.Context, string) (net.Conn, error) {
				return nil, errors.New("dial failed")
			},
		},
	}

	errCh := make(chan error, 1)
	go func() {
		errCh <- runtime.proxyConnection(context.Background(), "http://127.0.0.1:9999", serverConn)
	}()

	body, err := io.ReadAll(clientConn)
	_ = clientConn.Close()
	if err != nil {
		t.Fatalf("ReadAll() error = %v", err)
	}
	if err := <-errCh; err != nil {
		t.Fatalf("proxyConnection() error = %v, want nil", err)
	}

	response := string(body)
	if !strings.Contains(response, "HTTP/1.1 503 Service Unavailable") {
		t.Fatalf("proxyConnection() response missing status line: %q", response)
	}
	if !strings.Contains(response, "The local service is not currently running.") {
		t.Fatalf("proxyConnection() response missing body: %q", response)
	}
}

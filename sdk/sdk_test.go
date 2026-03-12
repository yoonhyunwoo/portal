package sdk

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gosuda/portal/v2/types"
)

func TestNewListenerRetriesInitialStartupUntilReady(t *testing.T) {
	var domainCount atomic.Int32
	var registerCount atomic.Int32
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case types.PathSDKDomain:
			if domainCount.Add(1) == 1 {
				http.Error(w, "temporarily unavailable", http.StatusBadGateway)
				return
			}
			writeSDKTestEnvelope(w, http.StatusOK, types.APIEnvelope[types.DomainResponse]{
				OK: true,
				Data: types.DomainResponse{
					Version: types.SDKProtocolVersion,
				},
			})
		case types.PathSDKRegister:
			if registerCount.Add(1) == 1 {
				http.Error(w, "temporarily unavailable", http.StatusBadGateway)
				return
			}
			writeSDKTestEnvelope(w, http.StatusCreated, types.APIEnvelope[types.RegisterResponse]{
				OK: true,
				Data: types.RegisterResponse{
					LeaseID:  "lease-1",
					Hostname: "127.0.0.1",
				},
			})
		case types.PathSDKConnect:
			writeSDKTestEnvelope(w, http.StatusForbidden, types.APIEnvelope[any]{
				OK:    false,
				Error: &types.APIError{Code: types.APIErrorCodeUnauthorized, Message: "not used in test"},
			})
		case types.PathSDKRenew:
			writeSDKTestEnvelope(w, http.StatusOK, types.APIEnvelope[types.RenewResponse]{
				OK:   true,
				Data: types.RenewResponse{LeaseID: "lease-1"},
			})
		case types.PathSDKUnregister:
			writeSDKTestEnvelope(w, http.StatusOK, types.APIEnvelope[any]{OK: true})
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	listener, err := NewListener(context.Background(), server.URL, ListenerConfig{
		Name:      "demo",
		RetryWait: 10 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("NewListener() error = %v", err)
	}
	defer listener.Close()

	waitForSDKTest(t, func() bool {
		return domainCount.Load() >= 2 && registerCount.Load() >= 2 && listener.LeaseID() == "lease-1"
	})
}

func TestNewListenerRejectsInvalidName(t *testing.T) {
	listener, err := NewListener(context.Background(), "https://relay.example.com", ListenerConfig{Name: "demo app"})
	if err == nil {
		t.Fatal("NewListener() error = nil, want invalid name error")
	}
	if listener != nil {
		t.Fatalf("NewListener() listener = %#v, want nil", listener)
	}
}

func TestNewListenerRegistersLeaseWithMainContract(t *testing.T) {
	registerReqCh := make(chan types.RegisterRequest, 1)
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case types.PathSDKDomain:
			writeSDKTestEnvelope(w, http.StatusOK, types.APIEnvelope[types.DomainResponse]{
				OK: true,
				Data: types.DomainResponse{
					Version: types.SDKProtocolVersion,
				},
			})
		case types.PathSDKRegister:
			var registerReq types.RegisterRequest
			if err := json.NewDecoder(r.Body).Decode(&registerReq); err != nil {
				t.Fatalf("decode register request: %v", err)
			}
			select {
			case registerReqCh <- registerReq:
			default:
			}
			writeSDKTestEnvelope(w, http.StatusCreated, types.APIEnvelope[types.RegisterResponse]{
				OK: true,
				Data: types.RegisterResponse{
					LeaseID:  "lease-1",
					Hostname: "127.0.0.1",
					Metadata: registerReq.Metadata,
				},
			})
		case types.PathSDKConnect:
			writeSDKTestEnvelope(w, http.StatusForbidden, types.APIEnvelope[any]{
				OK:    false,
				Error: &types.APIError{Code: types.APIErrorCodeUnauthorized, Message: "not used in test"},
			})
		case types.PathSDKRenew:
			writeSDKTestEnvelope(w, http.StatusOK, types.APIEnvelope[types.RenewResponse]{
				OK:   true,
				Data: types.RenewResponse{LeaseID: "lease-1"},
			})
		case types.PathSDKUnregister:
			writeSDKTestEnvelope(w, http.StatusOK, types.APIEnvelope[any]{OK: true})
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	listener, err := NewListener(context.Background(), server.URL, ListenerConfig{
		Name:     "Demo-App",
		Metadata: types.LeaseMetadata{Owner: "alice"},
		LeaseTTL: 42 * time.Second,
	})
	if err != nil {
		t.Fatalf("NewListener() error = %v", err)
	}
	defer listener.Close()

	var registerReq types.RegisterRequest
	waitForSDKTest(t, func() bool {
		select {
		case registerReq = <-registerReqCh:
			return true
		default:
			return false
		}
	})
	waitForSDKTest(t, func() bool {
		return listener.LeaseID() == "lease-1"
	})

	if registerReq.TTL != 42 {
		t.Fatalf("register request TTL = %d, want 42", registerReq.TTL)
	}
	if registerReq.Name != "demo-app" {
		t.Fatalf("register request Name = %q, want %q", registerReq.Name, "demo-app")
	}
	if listener.LeaseID() != "lease-1" {
		t.Fatalf("LeaseID() = %q, want %q", listener.LeaseID(), "lease-1")
	}
	if got := listener.Hostname(); got != "127.0.0.1" {
		t.Fatalf("Hostname() = %q, want %q", got, "127.0.0.1")
	}
	if got := listener.PublicURL(); got != "https://127.0.0.1" {
		t.Fatalf("PublicURL() = %q, want %q", got, "https://127.0.0.1")
	}
	if got := listener.Metadata(); got.Owner != "alice" {
		t.Fatalf("Metadata().Owner = %q, want %q", got.Owner, "alice")
	}
}

func TestNewListenerReregistersOnLeaseNotFound(t *testing.T) {
	var registerCount atomic.Int32
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case types.PathSDKDomain:
			writeSDKTestEnvelope(w, http.StatusOK, types.APIEnvelope[types.DomainResponse]{
				OK: true,
				Data: types.DomainResponse{
					Version: types.SDKProtocolVersion,
				},
			})
		case types.PathSDKRegister:
			count := registerCount.Add(1)
			leaseID := "lease-1"
			if count > 1 {
				leaseID = "lease-2"
			}
			writeSDKTestEnvelope(w, http.StatusCreated, types.APIEnvelope[types.RegisterResponse]{
				OK: true,
				Data: types.RegisterResponse{
					LeaseID:  leaseID,
					Hostname: "127.0.0.1",
				},
			})
		case types.PathSDKRenew:
			var req types.RenewRequest
			if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
				t.Fatalf("decode renew request: %v", err)
			}
			if req.LeaseID == "lease-1" {
				writeSDKTestEnvelope(w, http.StatusNotFound, types.APIEnvelope[any]{
					OK:    false,
					Error: &types.APIError{Code: types.APIErrorCodeLeaseNotFound, Message: "lease not found"},
				})
				return
			}
			writeSDKTestEnvelope(w, http.StatusOK, types.APIEnvelope[types.RenewResponse]{
				OK:   true,
				Data: types.RenewResponse{LeaseID: req.LeaseID},
			})
		case types.PathSDKConnect:
			writeSDKTestEnvelope(w, http.StatusForbidden, types.APIEnvelope[any]{
				OK:    false,
				Error: &types.APIError{Code: types.APIErrorCodeUnauthorized, Message: "not used in test"},
			})
		case types.PathSDKUnregister:
			writeSDKTestEnvelope(w, http.StatusOK, types.APIEnvelope[any]{OK: true})
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	listener, err := NewListener(context.Background(), server.URL, ListenerConfig{
		Name:        "demo",
		LeaseTTL:    80 * time.Millisecond,
		RenewBefore: 40 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("NewListener() error = %v", err)
	}
	defer listener.Close()

	waitForSDKTest(t, func() bool {
		return listener.LeaseID() == "lease-2"
	})
}

func TestNewListenerClosesAfterReverseSessionRetryBudgetExhausted(t *testing.T) {
	var connectCount atomic.Int32
	var unregisterCount atomic.Int32
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case types.PathSDKDomain:
			writeSDKTestEnvelope(w, http.StatusOK, types.APIEnvelope[types.DomainResponse]{
				OK: true,
				Data: types.DomainResponse{
					Version: types.SDKProtocolVersion,
				},
			})
		case types.PathSDKRegister:
			writeSDKTestEnvelope(w, http.StatusCreated, types.APIEnvelope[types.RegisterResponse]{
				OK: true,
				Data: types.RegisterResponse{
					LeaseID:  "lease-1",
					Hostname: "127.0.0.1",
				},
			})
		case types.PathSDKConnect:
			connectCount.Add(1)
			writeSDKTestEnvelope(w, http.StatusForbidden, types.APIEnvelope[any]{
				OK:    false,
				Error: &types.APIError{Code: types.APIErrorCodeUnauthorized, Message: "reverse session denied"},
			})
		case types.PathSDKUnregister:
			unregisterCount.Add(1)
			writeSDKTestEnvelope(w, http.StatusOK, types.APIEnvelope[any]{OK: true})
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	listener, err := NewListener(context.Background(), server.URL, ListenerConfig{
		Name:       "demo",
		RetryCount: 1,
		RetryWait:  10 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("NewListener() error = %v", err)
	}
	defer listener.Close()

	waitForSDKTest(t, func() bool {
		return listener.done()
	})
	if connectCount.Load() < 2 {
		t.Fatalf("connect count = %d, want at least 2", connectCount.Load())
	}
	if unregisterCount.Load() == 0 {
		t.Fatal("expected listener to unregister lease after retry budget exhaustion")
	}
}

func TestNewListenerRetriesForeverWhenRetryCountIsNegative(t *testing.T) {
	var connectCount atomic.Int32
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case types.PathSDKDomain:
			writeSDKTestEnvelope(w, http.StatusOK, types.APIEnvelope[types.DomainResponse]{
				OK: true,
				Data: types.DomainResponse{
					Version: types.SDKProtocolVersion,
				},
			})
		case types.PathSDKRegister:
			writeSDKTestEnvelope(w, http.StatusCreated, types.APIEnvelope[types.RegisterResponse]{
				OK: true,
				Data: types.RegisterResponse{
					LeaseID:  "lease-1",
					Hostname: "127.0.0.1",
				},
			})
		case types.PathSDKConnect:
			connectCount.Add(1)
			writeSDKTestEnvelope(w, http.StatusForbidden, types.APIEnvelope[any]{
				OK:    false,
				Error: &types.APIError{Code: types.APIErrorCodeUnauthorized, Message: "reverse session denied"},
			})
		case types.PathSDKUnregister:
			writeSDKTestEnvelope(w, http.StatusOK, types.APIEnvelope[any]{OK: true})
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	listener, err := NewListener(context.Background(), server.URL, ListenerConfig{
		Name:       "demo",
		RetryCount: -1,
		RetryWait:  10 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("NewListener() error = %v", err)
	}
	defer listener.Close()

	waitForSDKTest(t, func() bool {
		return connectCount.Load() >= 3
	})
	if listener.done() {
		t.Fatal("listener closed unexpectedly with negative RetryCount")
	}
}

func TestRunReverseSessionActivatesAfterKeepalive(t *testing.T) {
	t.Parallel()

	serverConn, clientConn := net.Pipe()
	defer clientConn.Close()

	activated := make(chan net.Conn, 1)
	errCh := make(chan error, 1)
	go func() {
		claimed, err := runReverseSession(context.Background(), serverConn, 50*time.Millisecond, func(_ context.Context, conn net.Conn) error {
			activated <- conn
			return nil
		})
		if !claimed {
			errCh <- errors.New("session was not claimed")
			return
		}
		errCh <- err
	}()

	if _, err := clientConn.Write([]byte{types.MarkerKeepalive, types.MarkerTLSStart}); err != nil {
		t.Fatalf("Write() error = %v", err)
	}

	select {
	case conn := <-activated:
		if conn != serverConn {
			t.Fatalf("activate conn = %v, want %v", conn, serverConn)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for activation")
	}

	if err := <-errCh; err != nil {
		t.Fatalf("runReverseSession() error = %v", err)
	}
}

func TestRunReverseSessionRejectsUnexpectedMarker(t *testing.T) {
	t.Parallel()

	serverConn, clientConn := net.Pipe()
	defer clientConn.Close()

	errCh := make(chan error, 1)
	go func() {
		claimed, err := runReverseSession(context.Background(), serverConn, 50*time.Millisecond, func(context.Context, net.Conn) error {
			return nil
		})
		if claimed {
			errCh <- errors.New("session unexpectedly claimed")
			return
		}
		errCh <- err
	}()

	if _, err := clientConn.Write([]byte{0xff}); err != nil {
		t.Fatalf("Write() error = %v", err)
	}

	err := <-errCh
	if err == nil || !strings.Contains(err.Error(), "unexpected reverse marker") {
		t.Fatalf("runReverseSession() error = %v, want unexpected reverse marker", err)
	}
}

func TestRenewLeaseWithRecoveryReregistersOnLeaseNotFound(t *testing.T) {
	t.Parallel()

	var renewedTTL time.Duration
	var reregistered atomic.Bool

	err := renewLeaseWithRecovery(context.Background(), "lease-1", 42*time.Second, func(_ context.Context, leaseID string, ttl time.Duration) error {
		if leaseID != "lease-1" {
			t.Fatalf("leaseID = %q, want lease-1", leaseID)
		}
		renewedTTL = ttl
		return &types.APIRequestError{Code: types.APIErrorCodeLeaseNotFound}
	}, func(context.Context) error {
		reregistered.Store(true)
		return nil
	})
	if err != nil {
		t.Fatalf("renewLeaseWithRecovery() error = %v", err)
	}
	if renewedTTL != 42*time.Second {
		t.Fatalf("renew ttl = %v, want %v", renewedTTL, 42*time.Second)
	}
	if !reregistered.Load() {
		t.Fatal("reregister was not called")
	}
}

func TestRenewLeaseWithRecoveryReturnsNonLeaseNotFound(t *testing.T) {
	t.Parallel()

	wantErr := io.EOF
	err := renewLeaseWithRecovery(context.Background(), "lease-1", 42*time.Second, func(context.Context, string, time.Duration) error {
		return wantErr
	}, func(context.Context) error {
		t.Fatal("reregister should not be called")
		return nil
	})
	if !errors.Is(err, wantErr) {
		t.Fatalf("renewLeaseWithRecovery() error = %v, want %v", err, wantErr)
	}
}

func TestExposeNoRelayInputs(t *testing.T) {
	exposure, err := Expose(context.Background(), nil, "demo", types.LeaseMetadata{})
	if err != nil {
		t.Fatalf("Expose() error = %v", err)
	}
	if exposure != nil {
		t.Fatalf("Expose() exposure = %#v, want nil", exposure)
	}
}

func writeSDKTestEnvelope[T any](w http.ResponseWriter, status int, envelope types.APIEnvelope[T]) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(envelope)
}

func waitForSDKTest(t *testing.T, fn func() bool) {
	t.Helper()

	waitForSDKTestWithTimeout(t, 5*time.Second, fn)
}

func waitForSDKTestWithTimeout(t *testing.T, timeout time.Duration, fn func() bool) {
	t.Helper()

	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if fn() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}

	t.Fatal("timed out waiting for condition")
}

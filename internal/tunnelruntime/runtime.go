package tunnelruntime

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rs/zerolog/log"

	"github.com/gosuda/portal/v2/sdk"
	"github.com/gosuda/portal/v2/types"
	"github.com/gosuda/portal/v2/utils"
)

const (
	localDialTimeout = 5 * time.Second
	shutdownTimeout  = 5 * time.Second
)

type Config struct {
	RelayURLs        []string
	UseDefaultRelays bool
	Name             string
	LocalAddr        string
	Metadata         types.LeaseMetadata
}

type relayExposure interface {
	net.Listener
	Close() error
}

type runtimeDeps struct {
	withDefaultRelayURLs func(context.Context, ...string) []string
	normalizeRelayURLs   func([]string) ([]string, error)
	expose               func(context.Context, []string, string, types.LeaseMetadata) (relayExposure, error)
	normalizeTargetAddr  func(string) (string, error)
	dialLocal            func(context.Context, string) (net.Conn, error)
}

type Runtime struct {
	deps runtimeDeps
}

func New() Runtime {
	return Runtime{
		deps: runtimeDeps{
			withDefaultRelayURLs: sdk.WithDefaultRelayURLs,
			normalizeRelayURLs:   utils.NormalizeRelayURLs,
			expose: func(ctx context.Context, relayURLs []string, name string, metadata types.LeaseMetadata) (relayExposure, error) {
				return sdk.Expose(ctx, relayURLs, name, metadata)
			},
			normalizeTargetAddr: utils.NormalizeTargetAddr,
			dialLocal: func(ctx context.Context, targetAddr string) (net.Conn, error) {
				dialer := &net.Dialer{Timeout: localDialTimeout}
				return dialer.DialContext(ctx, "tcp", targetAddr)
			},
		},
	}
}

func Run(ctx context.Context, cfg Config) error {
	return New().Run(ctx, cfg)
}

func (r Runtime) Run(ctx context.Context, cfg Config) error {
	logger := log.With().Str("component", "portal-tunnel").Logger()
	if ctx == nil {
		ctx = context.Background()
	}

	relayURLs, err := r.resolveRelayURLs(ctx, cfg)
	if err != nil {
		return fmt.Errorf("resolve relay urls: %w", err)
	}

	exposure, err := r.deps.expose(ctx, relayURLs, cfg.Name, cfg.Metadata.Copy())
	if err != nil {
		return fmt.Errorf("service %s: failed to start relays: %w", cfg.Name, err)
	}
	if exposure == nil {
		return errors.New("no relay URLs provided")
	}
	defer exposure.Close()

	logger.Info().
		Str("release_version", types.ReleaseVersion).
		Str("local", cfg.LocalAddr).
		Msg("starting portal tunnel")

	var connWG sync.WaitGroup
	var connCount atomic.Int64

	go func() {
		<-ctx.Done()
		_ = exposure.Close()
	}()

	waitErr := r.proxyRelayConnections(ctx, exposure, cfg.LocalAddr, &connWG, &connCount)
	closeErr := exposure.Close()
	if waitErr != nil {
		logger.Error().Err(waitErr).Msg("relay supervisor exited with error")
	}
	if closeErr != nil {
		logger.Error().Err(closeErr).Msg("relay shutdown failed")
	}
	if ctx.Err() != nil {
		logger.Info().Msg("tunnel shutting down")
	}

	if err := waitForConnections(&connWG, shutdownTimeout); err != nil {
		logger.Warn().Err(err).Msg("tunnel shutdown timeout; connections still active")
	}

	logger.Info().Msg("tunnel shutdown complete")
	return errors.Join(waitErr, closeErr)
}

func (r Runtime) resolveRelayURLs(ctx context.Context, cfg Config) ([]string, error) {
	relayURLs := append([]string(nil), cfg.RelayURLs...)
	if cfg.UseDefaultRelays {
		relayURLs = r.deps.withDefaultRelayURLs(ctx, relayURLs...)
	}
	return r.deps.normalizeRelayURLs(relayURLs)
}

func waitForConnections(connWG *sync.WaitGroup, timeout time.Duration) error {
	done := make(chan struct{})
	go func() {
		connWG.Wait()
		close(done)
	}()

	timer := time.NewTimer(timeout)
	defer timer.Stop()

	select {
	case <-done:
		return nil
	case <-timer.C:
		return errors.New("connection drain timeout")
	}
}

func (r Runtime) proxyRelayConnections(ctx context.Context, relayListener net.Listener, localAddr string, connWG *sync.WaitGroup, connCount *atomic.Int64) error {
	logger := log.With().Str("component", "portal-tunnel").Logger()

	for {
		relayConn, err := relayListener.Accept()
		if err != nil {
			switch {
			case ctx.Err() != nil || errors.Is(err, context.Canceled):
				return nil
			case errors.Is(err, net.ErrClosed):
				return errors.New("all relay listeners stopped")
			default:
				return err
			}
		}

		connID := connCount.Add(1)
		logger.Info().
			Int64("conn_id", connID).
			Str("remote_addr", relayConn.RemoteAddr().String()).
			Msg("accepted relay connection")

		connWG.Add(1)
		go func(connID int64, relayConn net.Conn) {
			defer connWG.Done()
			if err := r.proxyConnection(ctx, localAddr, relayConn); err != nil {
				logger.Error().Err(err).Int64("conn_id", connID).Msg("proxy connection failed")
			}
			logger.Info().Int64("conn_id", connID).Msg("proxy connection closed")
		}(connID, relayConn)
	}
}

var bufferPool = sync.Pool{
	New: func() any {
		b := make([]byte, 64*1024)
		return &b
	},
}

func (r Runtime) proxyConnection(ctx context.Context, localAddr string, relayConn net.Conn) error {
	defer relayConn.Close()

	targetAddr, err := r.deps.normalizeTargetAddr(localAddr)
	if err != nil {
		return fmt.Errorf("invalid --host value %q: %w", localAddr, err)
	}

	localConn, err := r.deps.dialLocal(ctx, targetAddr)
	if err != nil {
		return writeUnavailableResponse(relayConn)
	}
	defer localConn.Close()

	errCh := make(chan error, 2)
	stopCh := make(chan struct{})

	go func() {
		select {
		case <-ctx.Done():
			_ = relayConn.Close()
			_ = localConn.Close()
		case <-stopCh:
		}
	}()

	go func() {
		bufPtr := bufferPool.Get().(*[]byte)
		defer bufferPool.Put(bufPtr)
		_, err := io.CopyBuffer(localConn, relayConn, *bufPtr)
		if tcpConn, ok := localConn.(*net.TCPConn); ok {
			_ = tcpConn.CloseWrite()
		}
		errCh <- err
	}()

	go func() {
		bufPtr := bufferPool.Get().(*[]byte)
		defer bufferPool.Put(bufPtr)
		_, err := io.CopyBuffer(relayConn, localConn, *bufPtr)
		_ = relayConn.Close()
		errCh <- err
	}()

	var firstErr error
	for range 2 {
		if err := <-errCh; err != nil && firstErr == nil {
			firstErr = err
		}
	}

	close(stopCh)
	if errors.Is(firstErr, io.EOF) || errors.Is(firstErr, net.ErrClosed) {
		return nil
	}
	return firstErr
}

func writeUnavailableResponse(conn net.Conn) error {
	htmlBody := `<!DOCTYPE html>
<html>
<head><title>Service Unavailable</title></head>
<body style="font-family:sans-serif;text-align:center;padding:50px;">
<h1>Service Unavailable</h1>
<p>The local service is not currently running.</p>
<p>Please start your local application and refresh this page.</p>
</body>
</html>`
	response := fmt.Sprintf("HTTP/1.1 503 Service Unavailable\r\n"+
		"Content-Type: text/html; charset=utf-8\r\n"+
		"Content-Length: %d\r\n"+
		"Connection: close\r\n"+
		"\r\n%s", len(htmlBody), htmlBody)
	_, err := conn.Write([]byte(response))
	return err
}

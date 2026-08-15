package podman

import (
	"context"
	"errors"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/containers/podman/v5/pkg/bindings"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/iotready/podman-api/internal/config"
)

// countingUnixListener counts every accepted connection, so a test can prove
// ctxFor actually redialed rather than merely returning the same cached
// (possibly still-broken) connection.
type countingUnixListener struct {
	net.Listener
	accepts atomic.Int64
}

func (l *countingUnixListener) Accept() (net.Conn, error) {
	c, err := l.Listener.Accept()
	if err != nil {
		return nil, err
	}
	l.accepts.Add(1)
	return c, nil
}

// fakeLibpodServer starts a minimal HTTP server over a unix socket that
// answers just enough of the libpod REST surface (the bindings package's own
// connect-time /_ping, plus /info) for ctxFor and Ping to succeed.
func fakeLibpodServer(t *testing.T) (sockPath string, accepts *atomic.Int64) {
	t.Helper()
	sockPath = filepath.Join(t.TempDir(), "podman.sock")
	ln, err := net.Listen("unix", sockPath)
	require.NoError(t, err)
	tl := &countingUnixListener{Listener: ln}

	mux := http.NewServeMux()
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Libpod-API-Version", "5.8.2")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{}`))
	})
	srv := &http.Server{Handler: mux}
	go func() { _ = srv.Serve(tl) }()
	t.Cleanup(func() {
		_ = srv.Close()
		_ = os.Remove(sockPath)
	})
	return sockPath, &tl.accepts
}

// deadTransport is a fake http.RoundTripper that always fails the way a
// wedged SSH-multiplexed connection does in production: a read error on an
// already-established transport, not a clean close a caller could tell apart
// from "try a fresh connection". See TestRealClient_DeadCachedConnectionIsEvicted.
type deadTransport struct{}

func (deadTransport) RoundTrip(*http.Request) (*http.Response, error) {
	return nil, errors.New("read tcp 100.64.0.9:54728->100.64.0.5:22: read: connection timed out")
}

// wedgedTransport is a fake http.RoundTripper that reproduces the dominant
// #252 wedge shape for the ~30 operation methods routed through opCtxFor: the
// TCP socket accepts writes but the OS never surfaces a read error, so the
// call just hangs until its context's own deadline fires. Unlike
// deadTransport (an immediate raw error), RoundTrip here blocks on
// req.Context().Done() and returns req.Context().Err() — exactly what
// opCtxFor's fixed callTimeout produces on a wedged connection.
type wedgedTransport struct{}

func (wedgedTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	<-req.Context().Done()
	return nil, req.Context().Err()
}

// TestRealClient_WedgedConnectionIsEvictedOnRepeatedDeadline reproduces the
// #252-review gap: opCtxFor wraps every call (PodList, ContainerExec,
// HostInfo, etc. — everything but Ping/Version/raw HostInfo) in a fixed
// callTimeout, and connBroken deliberately does not treat
// context.DeadlineExceeded as a broken connection (ssh_pool.go's sshSession
// needs that distinction for its own, much shorter, caller-supplied ctx). So
// before the fix, a wedged libpod connection's own callTimeout firing never
// evicted the cached connection, and every subsequent call replayed the same
// stall. This asserts a call through opCtxFor (PodList) evicts once its own
// deadline — not the caller's — is what fires.
func TestRealClient_WedgedConnectionIsEvictedOnRepeatedDeadline(t *testing.T) {
	sock, accepts := fakeLibpodServer(t)
	c, err := NewReal([]config.Host{{ID: "h1", Addr: "unix", Socket: sock}})
	require.NoError(t, err)

	ctx := context.Background()

	// Warm the cache via Ping (raw ctxFor, undeadlined) so the cached
	// connection exists against the real, healthy server.
	require.NoError(t, c.Ping(ctx, "h1"))
	warmAccepts := accepts.Load()
	require.Greater(t, warmAccepts, int64(0), "warm-up should have dialed")

	// Mark the host already verified. In production this happens the first
	// time any opCtxFor call succeeds (ensureVerified). Ping deliberately
	// bypasses opCtxFor and never sets it, and ensureVerified's version probe
	// itself runs on the raw, undeadlined connection context — not the
	// per-call deadline this test means to exercise — so leaving the host
	// unverified would have PodList's opCtxFor call hang in that probe
	// instead of hitting the deadline-bound request against the wedged
	// transport swapped in below.
	c.mu.Lock()
	c.verified["h1"] = true
	c.mu.Unlock()

	cached := c.ctx["h1"]
	require.NotNil(t, cached, "ctxFor must have cached a connection")

	// Shrink callTimeout so the test doesn't wait 10 minutes for the wedge to
	// "surface". This is the same var opCtxFor derives its per-call deadline
	// from in production; the test only makes it small, not different.
	origTimeout := callTimeout
	callTimeout = 20 * time.Millisecond
	t.Cleanup(func() { callTimeout = origTimeout })

	// Swap in a transport that never errors on its own — it only ever
	// surfaces the request's own context deadline, mirroring a wedged
	// connection exactly.
	conn, err := bindings.GetClient(cached)
	require.NoError(t, err)
	it, ok := conn.Client.Transport.(*invalidatingTransport)
	require.True(t, ok, "ctxFor must wrap the connection's transport in an invalidatingTransport")
	it.next = wedgedTransport{}

	// PodList routes through opCtxFor, so this call is bounded by the
	// shrunken callTimeout and will time out against the wedged transport.
	_, err = c.PodList(ctx, "h1", nil)
	require.Error(t, err)

	// The fix: opCtxFor's own deadline firing (not a raw transport error, and
	// not the caller giving up early) must still evict the cached connection.
	assert.NotEqual(t, cached, c.ctx["h1"], "a connection wedged past its own callTimeout must be evicted, not cached forever")

	// Restore a working transport before the redial so the payoff is
	// verifiable the same way TestRealClient_DeadCachedConnectionIsEvicted
	// checks it: the next call actually succeeds against the real server.
	assert.NoError(t, c.Ping(ctx, "h1"), "a call after eviction must redial and succeed, not replay the stale wedge forever")
	assert.Greater(t, accepts.Load(), warmAccepts, "eviction must cause a real redial, not just a state flip")
}

// TestRealClient_DeadCachedConnectionIsEvicted reproduces #252: ctxFor must
// not keep handing out a cached connection whose transport has died. This is
// exactly the regression test the issue asks for: "cache a context, make the
// transport fail with a connection error, assert the next call redials
// rather than returning the same context."
func TestRealClient_DeadCachedConnectionIsEvicted(t *testing.T) {
	sock, accepts := fakeLibpodServer(t)
	c, err := NewReal([]config.Host{{ID: "h1", Addr: "unix", Socket: sock}})
	require.NoError(t, err)

	ctx := context.Background()

	// Warm the cache: dials and caches the connection in r.ctx["h1"]. The
	// bindings handshake (connect-time /_ping plus this call's own request)
	// may open more than one underlying TCP-level accept even though it is
	// logically "one connect" — what matters below is not the exact count
	// but that it does not grow again while the connection stays cached, and
	// does grow once eviction forces a real redial.
	require.NoError(t, c.Ping(ctx, "h1"))
	warmAccepts := accepts.Load()
	require.Greater(t, warmAccepts, int64(0), "first Ping should have dialed")

	cached := c.ctx["h1"]
	require.NotNil(t, cached, "ctxFor must have cached a connection")

	// Swap the cached connection's transport for one that fails every call
	// the way a dead-but-not-closed transport does: the connection is not
	// closed out from under it (Go's Transport would silently redial an idle
	// connection it can prove is closed — that is not the bug), it just
	// errors on use, exactly like the "same source port on every retry"
	// symptom from the issue.
	conn, err := bindings.GetClient(cached)
	require.NoError(t, err)
	it, ok := conn.Client.Transport.(*invalidatingTransport)
	require.True(t, ok, "ctxFor must wrap the connection's transport in an invalidatingTransport")
	it.next = deadTransport{}

	// This call fails — that is expected and correct; it is invalidation, not
	// a hidden retry. What matters is what it leaves behind.
	err = c.Ping(ctx, "h1")
	require.Error(t, err)

	// The bug: before the fix, r.ctx["h1"] still holds `cached` forever, so
	// every future call replays the same dead transport's error indefinitely,
	// and the accept count on the (healthy) server never increases again.
	assert.NotEqual(t, cached, c.ctx["h1"], "the dead connection must be evicted from the cache")

	// The fix's payoff: the next call redials against the real (healthy)
	// server and succeeds, exactly like engine-1 answering again after its
	// reboot.
	assert.NoError(t, c.Ping(ctx, "h1"), "a call after eviction must redial and succeed, not replay the stale failure forever")
	assert.Greater(t, accepts.Load(), warmAccepts, "eviction must cause a real redial, not just a state flip")
}

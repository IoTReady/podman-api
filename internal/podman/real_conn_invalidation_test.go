package podman

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/containers/podman/v5/pkg/bindings"
	"github.com/containers/podman/v5/pkg/bindings/system"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/iotready/podman-api/internal/config"
)

// Connection contexts are compared with == throughout, never assert.Equal:
// deep-comparing them walks into the live http.Transport they carry, which
// races its own goroutines under -race. Identity is what these tests mean
// anyway — "the same cached connection", not "an equivalent one".

// countingUnixListener counts every accepted connection, so a test can prove
// ctxFor actually redialed rather than merely returning the same cached
// (possibly still-broken) connection.
type countingUnixListener struct {
	net.Listener
	accepts atomic.Int64
	closes  atomic.Int64
}

func (l *countingUnixListener) Accept() (net.Conn, error) {
	c, err := l.Listener.Accept()
	if err != nil {
		return nil, err
	}
	l.accepts.Add(1)
	return &countingConn{Conn: c, closes: &l.closes}, nil
}

// countingConn counts server-side closes, which is how a test observes the
// client having actually dropped a connection rather than merely forgetting it.
type countingConn struct {
	net.Conn
	closes *atomic.Int64
	once   sync.Once
}

func (c *countingConn) Close() error {
	c.once.Do(func() { c.closes.Add(1) })
	return c.Conn.Close()
}

// fakeLibpodServer starts a minimal HTTP server over a unix socket that
// answers just enough of the libpod REST surface (the bindings package's own
// connect-time /_ping, plus /info) for ctxFor and Ping to succeed.
func fakeLibpodServer(t *testing.T) (sockPath string, accepts *atomic.Int64) {
	return fakeLibpodServerWith(t, nil)
}

// fakeLibpodServerWith is fakeLibpodServer with a hook for tests that need a
// specific libpod endpoint to answer with more than "{}": extra returns true
// once it has written the response, false to fall through to the default.
func fakeLibpodServerWith(t *testing.T, extra func(http.ResponseWriter, *http.Request) bool) (sockPath string, accepts *atomic.Int64) {
	sock, ln := fakeLibpodServerListener(t, extra)
	return sock, &ln.accepts
}

// fakeLibpodServerListener is fakeLibpodServerWith exposing the listener, for
// tests that need its close count as well as its accept count.
func fakeLibpodServerListener(t *testing.T, extra func(http.ResponseWriter, *http.Request) bool) (sockPath string, ln *countingUnixListener) {
	t.Helper()
	sockPath = filepath.Join(t.TempDir(), "podman.sock")
	raw, err := net.Listen("unix", sockPath)
	require.NoError(t, err)
	tl := &countingUnixListener{Listener: raw}

	mux := http.NewServeMux()
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Libpod-API-Version", "5.8.2")
		if extra != nil && extra(w, r) {
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{}`))
	})
	srv := &http.Server{Handler: mux}
	go func() { _ = srv.Serve(tl) }()
	t.Cleanup(func() {
		_ = srv.Close()
		_ = os.Remove(sockPath)
	})
	return sockPath, tl
}

// fakeAddr is the minimum net.Addr scriptedConn needs to satisfy net.Conn.
type fakeAddr struct{}

func (fakeAddr) Network() string { return "unix" }
func (fakeAddr) String() string  { return "fake" }

// scriptedConn is a net.Conn that accepts every write and then fails — or
// hangs — on read, the two shapes a broken libpod connection takes in
// production. Failures are injected at the connection, not at the
// RoundTripper, because conn.Client.Transport must stay a real *http.Transport
// (#273): podman's newUpgradeRequest type-asserts it without a comma-ok.
//
//   - readErr set: every read fails immediately, the way a dead-but-not-closed
//     SSH-multiplexed connection does ("read: connection timed out") — the
//     #252 symptom, and not something the caller could tell apart from a
//     connection worth reusing.
//   - readErr nil: reads block until the connection is closed, reproducing the
//     dominant wedge shape — the socket accepts writes but the OS never
//     surfaces a read error, so the call hangs until its own deadline fires.
type scriptedConn struct {
	readErr error
	closed  chan struct{}
	once    sync.Once
}

func newScriptedConn(readErr error) *scriptedConn {
	return &scriptedConn{readErr: readErr, closed: make(chan struct{})}
}

func (c *scriptedConn) Read(b []byte) (int, error) {
	if c.readErr != nil {
		return 0, c.readErr
	}
	<-c.closed
	return 0, net.ErrClosed
}

func (c *scriptedConn) Write(b []byte) (int, error) { return len(b), nil }

func (c *scriptedConn) Close() error {
	c.once.Do(func() { close(c.closed) })
	return nil
}

func (c *scriptedConn) LocalAddr() net.Addr             { return fakeAddr{} }
func (c *scriptedConn) RemoteAddr() net.Addr            { return fakeAddr{} }
func (c *scriptedConn) SetDeadline(t time.Time) error   { return nil }
func (c *scriptedConn) SetReadDeadline(time.Time) error { return nil }
func (c *scriptedConn) SetWriteDeadline(time.Time) error {
	return nil
}

// breakConn replaces the cached connection's transport with a fresh
// *http.Transport that only ever dials scriptedConns, then re-runs the real
// wiring over it — so the test exercises wireInvalidation itself rather than a
// stand-in for it.
func breakConn(t *testing.T, r *Real, id string, cached *connEntry, readErr error) {
	t.Helper()
	conn, err := bindings.GetClient(cached.ctx)
	require.NoError(t, err)
	conn.Client.Transport = &http.Transport{
		DialContext: func(context.Context, string, string) (net.Conn, error) {
			return newScriptedConn(readErr), nil
		},
	}
	r.wireInvalidation(cached, id, r.invalidateConn)
}

// waitEvicted waits for id's cached connection to stop being cached and
// returns whatever replaced it. Eviction reached through a transport error runs
// on its own goroutine (see wireInvalidation: it must not block net/http's read
// loop on r.mu), so asserting the moment the failing call returns would be
// asserting on a race. The deadline path in callCtx is synchronous and needs
// none of this.
func waitEvicted(t *testing.T, c *Real, id string, cached *connEntry) *connEntry {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for {
		c.mu.Lock()
		cur := c.ctx[id]
		c.mu.Unlock()
		if cur != cached || time.Now().After(deadline) {
			return cur
		}
		time.Sleep(5 * time.Millisecond)
	}
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
	// carries probeTimeout, not the per-call deadline this test means to
	// exercise — so leaving the host unverified would have PodList's
	// opCtxFor call fail in that probe instead of at the deadline-bound
	// request against the wedged transport swapped in below.
	c.mu.Lock()
	c.verified["h1"] = true
	c.mu.Unlock()

	c.mu.Lock()
	cached := c.ctx["h1"]
	c.mu.Unlock()
	require.NotNil(t, cached, "ctxFor must have cached a connection")

	// Shrink callTimeout so the test doesn't wait 10 minutes for the wedge to
	// "surface". This is the same var opCtxFor derives its per-call deadline
	// from in production; the test only makes it small, not different.
	origTimeout := callTimeout
	callTimeout = 20 * time.Millisecond
	t.Cleanup(func() { callTimeout = origTimeout })

	// Wedge the connection: writes are accepted, reads never return, so the
	// call surfaces nothing but its own context deadline.
	breakConn(t, c, "h1", cached, nil)

	// PodList routes through opCtxFor, so this call is bounded by the
	// shrunken callTimeout and will time out against the wedged transport.
	_, err = c.PodList(ctx, "h1", nil)
	require.Error(t, err)

	// The fix: opCtxFor's own deadline firing (not a raw transport error, and
	// not the caller giving up early) must still evict the cached connection.
	c.mu.Lock()
	after := c.ctx["h1"]
	c.mu.Unlock()
	// require, not assert: with the connection still cached, the redial check
	// below would hang on the wedge forever instead of failing.
	require.False(t, cached == after, "a connection wedged past its own callTimeout must be evicted, not cached forever")

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

	c.mu.Lock()
	cached := c.ctx["h1"]
	c.mu.Unlock()
	require.NotNil(t, cached, "ctxFor must have cached a connection")

	// Swap the cached connection's transport for one that fails every call
	// the way a dead-but-not-closed transport does: the connection is not
	// closed out from under it (Go's Transport would silently redial an idle
	// connection it can prove is closed — that is not the bug), it just
	// errors on use, exactly like the "same source port on every retry"
	// symptom from the issue.
	breakConn(t, c, "h1", cached,
		errors.New("read tcp 100.64.0.9:54728->100.64.0.5:22: read: connection timed out"))

	// This call fails — that is expected and correct; it is invalidation, not
	// a hidden retry. What matters is what it leaves behind.
	err = c.Ping(ctx, "h1")
	require.Error(t, err)

	// The bug: before the fix, r.ctx["h1"] still holds `cached` forever, so
	// every future call replays the same dead transport's error indefinitely,
	// and the accept count on the (healthy) server never increases again.
	after := waitEvicted(t, c, "h1", cached)
	assert.False(t, cached == after, "the dead connection must be evicted from the cache")

	// The fix's payoff: the next call redials against the real (healthy)
	// server and succeeds, exactly like engine-1 answering again after its
	// reboot.
	assert.NoError(t, c.Ping(ctx, "h1"), "a call after eviction must redial and succeed, not replay the stale failure forever")
	assert.Greater(t, accepts.Load(), warmAccepts, "eviction must cause a real redial, not just a state flip")
}

// TestRealClient_TransportStaysHTTPTransport pins #273: podman's
// newUpgradeRequest (every ExecStartAndAttach, i.e. every attaching exec)
// type-asserts conn.Client.Transport.(*http.Transport) without a comma-ok, so
// any wrapper that changes the transport's concrete type panics the daemon on
// a real host. Whatever the invalidation hook does, it must leave an
// *http.Transport in place.
func TestRealClient_TransportStaysHTTPTransport(t *testing.T) {
	sock, _ := fakeLibpodServer(t)
	c, err := NewReal([]config.Host{{ID: "h1", Addr: "unix", Socket: sock}})
	require.NoError(t, err)

	cctx, err := c.ctxFor(context.Background(), "h1")
	require.NoError(t, err)
	conn, err := bindings.GetClient(cctx)
	require.NoError(t, err)

	// The exact assertion podman makes at attach.go:569.
	_, ok := conn.Client.Transport.(*http.Transport)
	assert.True(t, ok, "conn.Client.Transport must stay an *http.Transport (podman asserts it without comma-ok); got %T", conn.Client.Transport)
}

// TestRealClient_CallerCancellationDoesNotEvict guards the other half of the
// wedge signal: opCtxFor bridges the caller's cancellation into the per-call
// context, so a client that disconnects mid-request cancels the call before
// its deadline. That says nothing about the connection's health, and evicting
// on it would turn every abandoned request into a redial for everyone else.
//
// Cancellation, specifically: since #282 a caller whose own DEADLINE expires
// does evict, because the host failing to answer inside a budget nobody
// abandoned is evidence. See real_parent_deadline_test.go for that half.
func TestRealClient_CallerCancellationDoesNotEvict(t *testing.T) {
	sock, _ := fakeLibpodServer(t)
	c, err := NewReal([]config.Host{{ID: "h1", Addr: "unix", Socket: sock}})
	require.NoError(t, err)

	require.NoError(t, c.Ping(context.Background(), "h1"))
	c.mu.Lock()
	c.verified["h1"] = true
	cached := c.ctx["h1"]
	c.mu.Unlock()
	require.NotNil(t, cached)

	// Reads hang, so the call ends only when the caller gives up — well
	// inside the (deliberately untouched, generous) callTimeout.
	breakConn(t, c, "h1", cached, nil)

	ctx, cancel := context.WithCancel(context.Background())
	time.AfterFunc(20*time.Millisecond, cancel)
	defer cancel()
	_, err = c.PodList(ctx, "h1", nil)
	require.Error(t, err)

	c.mu.Lock()
	still := c.ctx["h1"]
	c.mu.Unlock()
	assert.True(t, cached == still, "a caller walking away must not evict a connection")
}

// TestRealClient_DialedConnPreservesCloseWrite is the other half of #273:
// podman does not only assert on the transport's type, it also feeds the conn
// that DialContext returns to a `CloseWriter` interface check
// (bindings/containers/attach.go's closeWrite) to half-close STDIN on an
// attached exec. A wrapper that embeds net.Conn silently drops CloseWrite from
// the method set, so the half-close would be skipped and a stdin-attached exec
// could hang waiting for an EOF that never comes.
func TestRealClient_DialedConnPreservesCloseWrite(t *testing.T) {
	sock, _ := fakeLibpodServer(t)
	c, err := NewReal([]config.Host{{ID: "h1", Addr: "unix", Socket: sock}})
	require.NoError(t, err)

	cctx, err := c.ctxFor(context.Background(), "h1")
	require.NoError(t, err)
	conn, err := bindings.GetClient(cctx)
	require.NoError(t, err)
	tr, ok := conn.Client.Transport.(*http.Transport)
	require.True(t, ok)

	nc, err := tr.DialContext(context.Background(), "unix", sock)
	require.NoError(t, err)
	defer func() { _ = nc.Close() }()

	_, raw := nc.(*net.UnixConn)
	require.False(t, raw, "the dialer must still wrap the conn, or nothing reports transport failures")
	_, hasCloseWrite := nc.(interface{ CloseWrite() error })
	assert.True(t, hasCloseWrite, "wrapping must not hide the underlying conn's CloseWrite; got %T", nc)
}

// TestRealClient_DialedConnDoesNotFakeCloseWrite is the flip side: the wrapper
// must not advertise CloseWrite over a conn that cannot do it, or podman's
// interface check would take the half-close path and log a failure (or worse,
// believe a STDIN close happened) on a transport that never supported one.
func TestRealClient_DialedConnDoesNotFakeCloseWrite(t *testing.T) {
	sock, _ := fakeLibpodServer(t)
	c, err := NewReal([]config.Host{{ID: "h1", Addr: "unix", Socket: sock}})
	require.NoError(t, err)

	cctx, err := c.entryFor(context.Background(), "h1")
	require.NoError(t, err)
	// scriptedConn has no CloseWrite, standing in for any transport whose
	// conns cannot half-close.
	breakConn(t, c, "h1", cctx, errors.New("unused"))

	conn, err := bindings.GetClient(cctx.ctx)
	require.NoError(t, err)
	tr := conn.Client.Transport.(*http.Transport)
	nc, err := tr.DialContext(context.Background(), "unix", sock)
	require.NoError(t, err)
	defer func() { _ = nc.Close() }()

	_, hasCloseWrite := nc.(interface{ CloseWrite() error })
	assert.False(t, hasCloseWrite, "wrapper must not invent CloseWrite; got %T", nc)
}

// TestRealClient_FollowingLogStreamDoesNotEvict guards the one case where a
// per-call deadline firing is routine rather than a wedge: ContainerLogs in
// follow mode (what the UI opens for every logs tab) streams for as long as the
// viewer watches, so it outlives callTimeout by design. Evicting on that would
// drop the host's cached connection — and its version verification — every
// callTimeout for as long as a single logs tab stays open, forcing a fresh SSH
// connect for every other caller.
func TestRealClient_FollowingLogStreamDoesNotEvict(t *testing.T) {
	// A logs endpoint that behaves like follow mode: headers, then nothing
	// until the request context ends.
	sock, _ := fakeLibpodServerWith(t, func(w http.ResponseWriter, r *http.Request) bool {
		if !strings.HasSuffix(r.URL.Path, "/logs") {
			return false
		}
		w.WriteHeader(http.StatusOK)
		if f, ok := w.(http.Flusher); ok {
			f.Flush()
		}
		<-r.Context().Done()
		return true
	})
	c, err := NewReal([]config.Host{{ID: "h1", Addr: "unix", Socket: sock}})
	require.NoError(t, err)

	ctx := context.Background()
	require.NoError(t, c.Ping(ctx, "h1"))
	c.mu.Lock()
	c.verified["h1"] = true
	cached := c.ctx["h1"]
	c.mu.Unlock()
	require.NotNil(t, cached)

	origTimeout := callTimeout
	callTimeout = 50 * time.Millisecond
	t.Cleanup(func() { callTimeout = origTimeout })

	lines, err := c.ContainerLogs(ctx, "h1", "some-container", LogOptions{Follow: true, Tail: 10})
	require.NoError(t, err)
	// Drain until the stream ends — which it does when callTimeout fires.
	for range lines { //nolint:revive // draining
	}

	c.mu.Lock()
	after := c.ctx["h1"]
	c.mu.Unlock()
	assert.True(t, cached == after, "a follow stream reaching callTimeout is normal, not a wedged connection")
}

// waitPodFakeServer answers the two endpoints WaitForPodCompletion polls with
// a pod holding one forever-running container, so the wait can only end by
// running out of its own budget.
func waitPodFakeServer(t *testing.T) string {
	sock, _ := fakeLibpodServerWith(t, func(w http.ResponseWriter, r *http.Request) bool {
		switch {
		case strings.Contains(r.URL.Path, "/pods/"):
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"Containers":[{"Id":"c1"}]}`))
		case strings.Contains(r.URL.Path, "/containers/"):
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"State":{"Running":true}}`))
		default:
			return false
		}
		return true
	})
	return sock
}

// TestRealClient_WaitForPodCompletion_WedgedPollEvicts closes the #252 gap on
// the one operation that deliberately bypasses opCtxFor: WaitForPodCompletion
// honours a caller's long wait budget (a blob GC can legitimately wait 30
// minutes), so the wedge signal cannot come from that budget. It has to come
// from each individual poll, which is a single libpod call and has no business
// outlasting callTimeout.
func TestRealClient_WaitForPodCompletion_WedgedPollEvicts(t *testing.T) {
	sock := waitPodFakeServer(t)
	c, err := NewReal([]config.Host{{ID: "h1", Addr: "unix", Socket: sock}})
	require.NoError(t, err)

	ctx := context.Background()
	require.NoError(t, c.Ping(ctx, "h1"))
	c.mu.Lock()
	c.verified["h1"] = true
	cached := c.ctx["h1"]
	c.mu.Unlock()
	require.NotNil(t, cached)

	origTimeout := callTimeout
	callTimeout = 50 * time.Millisecond
	t.Cleanup(func() { callTimeout = origTimeout })

	breakConn(t, c, "h1", cached, nil)

	// The wait budget is far longer than a single call's, so what fires here
	// is the poll's own deadline, not the caller's patience running out.
	_, err = c.WaitForPodCompletion(ctx, "h1", "somepod", 10*time.Second)
	require.Error(t, err)

	c.mu.Lock()
	after := c.ctx["h1"]
	c.mu.Unlock()
	assert.False(t, cached == after, "a poll wedged past callTimeout must evict the connection")
}

// TestRealClient_WaitForPodCompletion_WaitTimeoutDoesNotEvict is the other
// direction: the caller's wait budget expiring means the pod is still running,
// which says nothing about the connection — every poll answered promptly. The
// alias integration test does exactly this on a slow pod.
func TestRealClient_WaitForPodCompletion_WaitTimeoutDoesNotEvict(t *testing.T) {
	sock := waitPodFakeServer(t)
	c, err := NewReal([]config.Host{{ID: "h1", Addr: "unix", Socket: sock}})
	require.NoError(t, err)

	ctx := context.Background()
	require.NoError(t, c.Ping(ctx, "h1"))
	c.mu.Lock()
	c.verified["h1"] = true
	cached := c.ctx["h1"]
	c.mu.Unlock()
	require.NotNil(t, cached)

	origPoll := waitPollInterval
	waitPollInterval = 10 * time.Millisecond
	t.Cleanup(func() { waitPollInterval = origPoll })

	_, err = c.WaitForPodCompletion(ctx, "h1", "somepod", 100*time.Millisecond)
	require.ErrorIs(t, err, ErrWaitTimeout)

	c.mu.Lock()
	after := c.ctx["h1"]
	c.mu.Unlock()
	assert.True(t, cached == after, "a pod outlasting the caller's wait budget is not a broken connection")
}

// waitPodServerReporting is waitPodFakeServer with a signal on the pod poll and
// a scripted container state, so a test can act between one poll and the next.
func waitPodServerReporting(t *testing.T, polled chan<- struct{}, containerState string) string {
	sock, _ := fakeLibpodServerWith(t, func(w http.ResponseWriter, r *http.Request) bool {
		switch {
		case strings.Contains(r.URL.Path, "/pods/"):
			if polled != nil {
				select {
				case polled <- struct{}{}:
				default:
				}
			}
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"Containers":[{"Id":"c1"}]}`))
		case strings.Contains(r.URL.Path, "/containers/"):
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(containerState))
		default:
			return false
		}
		return true
	})
	return sock
}

// TestRealClient_WaitForPodCompletion_ResolvesConnectionPerPoll is the
// WaitForPodCompletion counterpart to ContainerExec resolving its connection
// only after it wins the turnstile. A wait budget is deliberately long — a blob
// GC over a large registry can legitimately run for 30 minutes — which is ample
// room for a config reload to land inside it and retire the cached connection.
// A wait that resolved the connection once, up front, spends the rest of that
// budget polling the endpoint the operator just reconfigured away from; each
// poll has to resolve it afresh.
func TestRealClient_WaitForPodCompletion_ResolvesConnectionPerPoll(t *testing.T) {
	polled := make(chan struct{}, 1)
	oldSock := waitPodServerReporting(t, polled, `{"State":{"Running":true}}`)
	newSock := waitPodServerReporting(t, nil, `{"State":{"Running":false,"ExitCode":7}}`)

	c, err := NewReal([]config.Host{{ID: "h1", Addr: "unix", Socket: oldSock}})
	require.NoError(t, err)

	ctx := context.Background()
	require.NoError(t, c.Ping(ctx, "h1"))
	c.mu.Lock()
	c.verified["h1"] = true
	c.mu.Unlock()

	origPoll := waitPollInterval
	waitPollInterval = 10 * time.Millisecond
	t.Cleanup(func() { waitPollInterval = origPoll })

	type result struct {
		code int
		err  error
	}
	done := make(chan result, 1)
	go func() {
		code, err := c.WaitForPodCompletion(ctx, "h1", "somepod", 5*time.Second)
		done <- result{code, err}
	}()

	select {
	case <-polled:
	case <-time.After(2 * time.Second):
		t.Fatal("the wait never polled the host it started on")
	}
	c.SetHosts([]config.Host{{ID: "h1", Addr: "unix", Socket: newSock}})

	select {
	case got := <-done:
		require.NoError(t, got.err)
		assert.Equal(t, 7, got.code, "the wait must observe the reconfigured host, not the one it started on")
	case <-time.After(4 * time.Second):
		t.Fatal("the wait never picked up the reconfigured host")
	}
}

// TestRealClient_NonFollowLogFetchEvictsOnWedge is the counterpart to
// TestRealClient_FollowingLogStreamDoesNotEvict: a tail-N fetch (the UI's log
// page load, and /logs?follow=0) is an ordinary bounded single call, so
// reaching callTimeout means the connection hung — exactly the #252 case
// opCtxFor's eviction exists for. Only follow mode may opt out.
func TestRealClient_NonFollowLogFetchEvictsOnWedge(t *testing.T) {
	sock, _ := fakeLibpodServer(t)
	c, err := NewReal([]config.Host{{ID: "h1", Addr: "unix", Socket: sock}})
	require.NoError(t, err)

	ctx := context.Background()
	require.NoError(t, c.Ping(ctx, "h1"))
	c.mu.Lock()
	c.verified["h1"] = true
	cached := c.ctx["h1"]
	c.mu.Unlock()
	require.NotNil(t, cached)

	origTimeout := callTimeout
	callTimeout = 50 * time.Millisecond
	t.Cleanup(func() { callTimeout = origTimeout })

	breakConn(t, c, "h1", cached, nil)

	lines, err := c.ContainerLogs(ctx, "h1", "some-container", LogOptions{Tail: 200})
	require.NoError(t, err)
	for range lines { //nolint:revive // draining
	}

	c.mu.Lock()
	after := c.ctx["h1"]
	c.mu.Unlock()
	assert.False(t, cached == after, "a bounded log fetch that hung past callTimeout must evict the connection")
}

// TestRealClient_EvictionClosesTheDeadConnection pins the cost side of
// invalidation. Eviction is deliberately trigger-happy — a transport error can
// also come from an idle keep-alive close the transport itself recovered from,
// and dropping a healthy connection is the cheap mistake — but only if dropping
// it actually costs one redial. Deleting the map entry alone leaves the old
// connection's pooled sockets and their read/write goroutines alive for the
// process lifetime, with nothing holding a reference to close them later.
func TestRealClient_EvictionClosesTheDeadConnection(t *testing.T) {
	sock, ln := fakeLibpodServerListener(t, nil)
	c, err := NewReal([]config.Host{{ID: "h1", Addr: "unix", Socket: sock}})
	require.NoError(t, err)

	ctx := context.Background()
	require.NoError(t, c.Ping(ctx, "h1"))
	c.mu.Lock()
	cached := c.ctx["h1"]
	c.mu.Unlock()
	require.NotNil(t, cached)
	closedBefore := ln.closes.Load()

	c.invalidateConn("h1", cached)

	// The server sees the close asynchronously (its read loop has to notice
	// the peer went away), so poll briefly.
	deadline := time.Now().Add(2 * time.Second)
	for ln.closes.Load() <= closedBefore && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	assert.Greater(t, ln.closes.Load(), closedBefore,
		"evicting a connection must close its pooled sockets, not just forget the context")
}

// TestRealClient_ExecUsesItsOwnConnection pins the isolation the exec path
// needs. podman's newUpgradeRequest replaces conn.Client.Transport for the
// whole duration of an exec, so on a shared connection every concurrent
// PodList/Ping/health poll on that host both races that write and can park its
// keep-alive socket in podman's temporary transport — which has
// IdleConnTimeout: 0 and becomes unreachable the moment the transport is put
// back. Serialising exec against exec cannot fix either; exec needs a
// connection nothing else uses.
func TestRealClient_ExecUsesItsOwnConnection(t *testing.T) {
	sock, _ := fakeLibpodServer(t)
	c, err := NewReal([]config.Host{{ID: "h1", Addr: "unix", Socket: sock}})
	require.NoError(t, err)

	ctx := context.Background()
	require.NoError(t, c.Ping(ctx, "h1"))
	c.mu.Lock()
	c.verified["h1"] = true
	primary := c.ctx["h1"]
	c.mu.Unlock()
	require.NotNil(t, primary)

	conn, err := bindings.GetClient(primary.ctx)
	require.NoError(t, err)
	hooked := conn.Client.Transport

	// The fake server cannot complete an attach, so the exec itself fails —
	// what matters is which connection it ran on.
	_, _ = c.ContainerExec(ctx, "h1", "some-container", []string{"/bin/true"})

	c.mu.Lock()
	execConn := c.execCtx["h1"]
	stillPrimary := c.ctx["h1"]
	c.mu.Unlock()

	require.NotNil(t, execConn, "exec must dial its own connection")
	assert.False(t, primary == execConn, "exec must not share the connection every other call uses")
	assert.True(t, primary == stillPrimary, "a failed exec must not evict the primary connection")
	assert.Same(t, hooked, conn.Client.Transport, "exec must not touch the primary connection's transport")
}

// TestRealClient_UnknownHostExecCreatesNoLock guards a small unbounded-growth
// hole: the per-host exec turnstile must not be created for an id that was
// never a host, because SetHosts only ever cleans up entries it finds in
// r.hosts.
func TestRealClient_UnknownHostExecCreatesNoLock(t *testing.T) {
	sock, _ := fakeLibpodServer(t)
	c, err := NewReal([]config.Host{{ID: "h1", Addr: "unix", Socket: sock}})
	require.NoError(t, err)

	_, err = c.ContainerExec(context.Background(), "nope", "some-container", []string{"/bin/true"})
	require.Error(t, err)

	c.mu.Lock()
	_, created := c.execGate["nope"]
	c.mu.Unlock()
	assert.False(t, created, "an unknown host must not leave a turnstile behind")
}

// TestRealClient_SetHostsClosesDroppedConnections extends the invariant
// invalidateConn now states — dropping the map entry is not enough, nothing
// else holds the connection — to the other place connections are dropped. A
// config reload that removes a host, or changes where an existing one lives,
// strands its pooled sockets and their goroutines otherwise.
func TestRealClient_SetHostsClosesDroppedConnections(t *testing.T) {
	sock, ln := fakeLibpodServerListener(t, nil)
	other, _ := fakeLibpodServer(t)

	for _, tc := range []struct {
		name  string
		hosts []config.Host
	}{
		{"host removed", []config.Host{}},
		{"socket changed", []config.Host{{ID: "h1", Addr: "unix", Socket: other}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c, err := NewReal([]config.Host{{ID: "h1", Addr: "unix", Socket: sock}})
			require.NoError(t, err)
			require.NoError(t, c.Ping(context.Background(), "h1"))
			closedBefore := ln.closes.Load()

			c.SetHosts(tc.hosts)

			deadline := time.Now().Add(2 * time.Second)
			for ln.closes.Load() <= closedBefore && time.Now().Before(deadline) {
				time.Sleep(10 * time.Millisecond)
			}
			assert.Greater(t, ln.closes.Load(), closedBefore,
				"a dropped connection must have its sockets closed, not just its map entry deleted")
		})
	}
}

// TestRealClient_ExecDeadlineStartsAfterTheLock pins the ordering ContainerExec
// needs: the host is resolved before the exec lock (an unknown id must not mint
// a mutex), but the per-call clock only starts once the lock is held. Execs are
// serialised per host and one may legitimately run the full callTimeout — a
// pre_backup database dump — so a queued exec that started its budget at the
// back of the queue would fail without ever running, and take the exec
// connection down with it on the way out.
func TestRealClient_ExecDeadlineStartsAfterTheLock(t *testing.T) {
	sock, _ := fakeLibpodServer(t)
	c, err := NewReal([]config.Host{{ID: "h1", Addr: "unix", Socket: sock}})
	require.NoError(t, err)

	ctx := context.Background()
	c.mu.Lock()
	c.verified["h1"] = true
	c.mu.Unlock()

	// Warm the exec connection so the goroutine below only has the lock to
	// wait on.
	_, _ = c.ContainerExec(ctx, "h1", "some-container", []string{"/bin/true"})
	c.mu.Lock()
	execConn := c.execCtx["h1"]
	c.mu.Unlock()
	require.NotNil(t, execConn)

	origTimeout := callTimeout
	callTimeout = 100 * time.Millisecond
	t.Cleanup(func() { callTimeout = origTimeout })

	// Stand in for an exec already running on this host.
	gate, err := c.execGateFor("h1")
	require.NoError(t, err)
	gate <- struct{}{}

	errCh := make(chan error, 1)
	go func() {
		_, err := c.ContainerExec(ctx, "h1", "some-container", []string{"/bin/true"})
		errCh <- err
	}()

	// Hold the turnstile well past callTimeout, then let the queued exec run.
	time.Sleep(300 * time.Millisecond)
	<-gate

	select {
	case err := <-errCh:
		assert.NotErrorIs(t, err, context.DeadlineExceeded,
			"a queued exec spent its budget waiting its turn instead of running")
	case <-time.After(5 * time.Second):
		t.Fatal("queued exec never returned")
	}

	c.mu.Lock()
	after := c.execCtx["h1"]
	c.mu.Unlock()
	assert.True(t, execConn == after, "waiting its turn must not evict the exec connection")
}

// TestRealClient_EvictionClosesTheHookedTransport covers the window in which
// the connection's transport is not ours: podman's exec replaces it for the
// duration of the exec, so an eviction landing then would close podman's idle
// conns and leave the hooked transport's pooled sockets — and their per-socket
// goroutines — alive with nothing left holding a reference. The transport to
// close is the one that was hooked, captured when it was hooked, not whatever
// the field happens to hold later.
func TestRealClient_EvictionClosesTheHookedTransport(t *testing.T) {
	sock, ln := fakeLibpodServerListener(t, nil)
	c, err := NewReal([]config.Host{{ID: "h1", Addr: "unix", Socket: sock}})
	require.NoError(t, err)

	ctx := context.Background()
	require.NoError(t, c.Ping(ctx, "h1")) // leaves a pooled socket in the hooked transport
	c.mu.Lock()
	cached := c.ctx["h1"]
	c.mu.Unlock()
	require.NotNil(t, cached)

	conn, err := bindings.GetClient(cached.ctx)
	require.NoError(t, err)
	hooked, ok := conn.Client.Transport.(*http.Transport)
	require.True(t, ok)

	// Stand in for podman's takeover mid-exec.
	conn.Client.Transport = &http.Transport{}

	closedBefore := ln.closes.Load()
	// A failing dial through the hooked transport is the invalidation path
	// that can fire while the field points elsewhere. The bindings dialer
	// ignores the address it is handed and always dials the configured socket,
	// so the socket itself has to go away; the already-pooled connection from
	// the Ping above survives that, which is the point.
	require.NoError(t, os.Remove(sock))
	_, err = hooked.DialContext(ctx, "unix", "ignored")
	require.Error(t, err)

	deadline := time.Now().Add(2 * time.Second)
	for ln.closes.Load() <= closedBefore && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	assert.Greater(t, ln.closes.Load(), closedBefore,
		"eviction closed the transport the connection happens to hold, not the one it hooked")
}

// TestRealClient_InvalidationDoesNotBlockOnTheHostLock pins where invalidation
// runs. report is called from net/http's per-connection readLoop/writeLoop
// goroutines, and eviction needs r.mu. Since #277 no dial is held under that
// lock, so the wait is short in production — but a read loop parked on any
// lock still stalls every request multiplexed onto that connection, and the
// test holds r.mu directly to prove the eviction is not on the read loop's
// path at all.
func TestRealClient_InvalidationDoesNotBlockOnTheHostLock(t *testing.T) {
	sock, _ := fakeLibpodServer(t)
	c, err := NewReal([]config.Host{{ID: "h1", Addr: "unix", Socket: sock}})
	require.NoError(t, err)

	ctx := context.Background()
	require.NoError(t, c.Ping(ctx, "h1"))
	c.mu.Lock()
	cached := c.ctx["h1"]
	c.mu.Unlock()
	require.NotNil(t, cached)

	breakConn(t, c, "h1", cached, errors.New("read tcp 10.0.0.1:5->10.0.0.2:22: read: connection timed out"))

	// Stand in for another host's dial in progress: r.mu held, no I/O of ours.
	c.mu.Lock()
	defer c.mu.Unlock()

	// The request goes straight down the cached connection rather than through
	// a Real method, because every method starts by taking r.mu itself — what
	// is under test is the request path below that, where net/http's read loop
	// surfaces the failure.
	done := make(chan struct{})
	go func() {
		defer close(done)
		_, _ = system.Info(cached.ctx, &system.InfoOptions{})
	}()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("a failing call blocked on r.mu: invalidation must not run inline on net/http's read loop")
	}
}

// TestRealClient_ExecVerifiesOnThePrimaryConnection pins where the version
// probe runs. ContainerExec has to verify before it takes the exec lock — an
// unverified host must fail fast rather than after queueing — but the exec
// connection is the one thing that must stay untouched by anything except the
// exec holding the lock: podman's newUpgradeRequest writes conn.Client.Transport
// unsynchronised for the duration of an exec, so a probe issued over that same
// connection both races that write and can park its keep-alive socket in
// podman's temporary transport, which restoreTransport then orphans. The
// version is a host property the primary connection already establishes — which
// is exactly why invalidateExecConn leaves r.verified alone — so it is the
// connection to probe over.
func TestRealClient_ExecVerifiesOnThePrimaryConnection(t *testing.T) {
	sock, _ := fakeLibpodServer(t)
	c, err := NewReal([]config.Host{{ID: "h1", Addr: "unix", Socket: sock}})
	require.NoError(t, err)

	probed := make(chan context.Context, 4)
	c.versionProbe = func(ctx context.Context) (string, error) {
		probed <- ctx
		return "5.8.2", nil
	}

	// The fake server cannot complete an attach, so the exec fails — what
	// matters is which connection the verification probe ran on.
	_, _ = c.ContainerExec(context.Background(), "h1", "some-container", []string{"/bin/true"})

	c.mu.Lock()
	primary := c.ctx["h1"]
	execConn := c.execCtx["h1"]
	c.mu.Unlock()
	require.NotNil(t, execConn, "exec must dial its own connection")

	require.Len(t, probed, 1, "exec must verify the host exactly once")
	got := <-probed
	// Compared by the bindings client the context carries, not by context
	// identity: since #275 the probe runs on a probeTimeout-bounded context
	// *derived* from the connection's, so it is no longer the connection
	// context itself. The client is what "which connection" actually means.
	gotClient, err := bindings.GetClient(got)
	require.NoError(t, err)
	execClient, err := bindings.GetClient(execConn.ctx)
	require.NoError(t, err)
	primaryClient, err := bindings.GetClient(primary.ctx)
	require.NoError(t, err)
	assert.False(t, gotClient == execClient, "the version probe ran on the exec connection, which only the exec holding the lock may touch")
	assert.True(t, gotClient == primaryClient, "the version probe must run on the primary connection")

	// And it must be bounded now, not riding the connection's undeadlined
	// context the way it did before #275.
	_, hasDeadline := got.Deadline()
	assert.True(t, hasDeadline, "the version probe must carry its own deadline (#275)")
}

// TestRealClient_QueuedExecHonoursCallerCancellation pins the cost of
// serialising exec per host. sync.Mutex.Lock is not context-aware, and by the
// "clock after the lock" ordering the wait for the lock carries no deadline of
// its own — so with k execs queued behind a pre_backup database dump legitimately
// running the full callTimeout, a caller waits up to k*callTimeout with no way
// out: neither its own cancelled request context nor daemon shutdown unblocks
// it, and the goroutine stays parked. ContainerExec took no lock at all before
// #273, so this wait is new and must be cancellable.
func TestRealClient_QueuedExecHonoursCallerCancellation(t *testing.T) {
	sock, _ := fakeLibpodServer(t)
	c, err := NewReal([]config.Host{{ID: "h1", Addr: "unix", Socket: sock}})
	require.NoError(t, err)

	c.mu.Lock()
	c.verified["h1"] = true
	c.mu.Unlock()

	// Stand in for an exec already running on this host, holding the turnstile
	// for longer than this test is willing to wait.
	held, err := c.execGateFor("h1")
	require.NoError(t, err)
	held <- struct{}{}
	defer func() { <-held }()

	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() {
		_, err := c.ContainerExec(ctx, "h1", "some-container", []string{"/bin/true"})
		errCh <- err
	}()
	time.Sleep(50 * time.Millisecond)
	cancel()

	select {
	case err := <-errCh:
		assert.ErrorIs(t, err, context.Canceled,
			"a queued exec must return its caller's cancellation, not swallow it")
	case <-time.After(5 * time.Second):
		t.Fatal("a cancelled caller stayed parked waiting its turn")
	}
}

// TestRealClient_SetHostsClosesTheHookedTransport is
// EvictionClosesTheHookedTransport for the other path that drops connections. A
// config reload landing while an exec is in flight finds podman's replacement in
// conn.Client.Transport, so closing whatever the field holds closes podman's
// throwaway and strands the hooked transport's pooled sockets — and their
// per-socket goroutines — for the process lifetime. Reading the field there is
// also an unsynchronised read racing newUpgradeRequest's write.
func TestRealClient_SetHostsClosesTheHookedTransport(t *testing.T) {
	sock, ln := fakeLibpodServerListener(t, nil)
	c, err := NewReal([]config.Host{{ID: "h1", Addr: "unix", Socket: sock}})
	require.NoError(t, err)

	require.NoError(t, c.Ping(context.Background(), "h1")) // leaves a pooled socket in the hooked transport
	c.mu.Lock()
	cached := c.ctx["h1"]
	c.mu.Unlock()
	require.NotNil(t, cached)

	conn, err := bindings.GetClient(cached.ctx)
	require.NoError(t, err)
	// Stand in for podman's takeover mid-exec.
	conn.Client.Transport = &http.Transport{}

	closedBefore := ln.closes.Load()
	c.SetHosts(nil)

	deadline := time.Now().Add(2 * time.Second)
	for ln.closes.Load() <= closedBefore && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	assert.Greater(t, ln.closes.Load(), closedBefore,
		"a reload closed the transport the connection happens to hold, not the one it hooked")
}

// TestRealClient_QueuedExecResolvesConnectionAfterTheGate pins that the exec
// connection is resolved after the turnstile is won, not before it. The wait is
// unbounded by design, and an entry captured ahead of it can be dropped during
// it — by invalidateExecConn, or by a SetHosts that repointed the host. A
// queued exec running on the dropped entry would talk to the old endpoint.
func TestRealClient_QueuedExecResolvesConnectionAfterTheGate(t *testing.T) {
	sock, _ := fakeLibpodServer(t)
	c, err := NewReal([]config.Host{{ID: "h1", Addr: "unix", Socket: sock}})
	require.NoError(t, err)

	ctx := context.Background()
	c.mu.Lock()
	c.verified["h1"] = true
	c.mu.Unlock()

	// Warm the exec connection, then stand in for an exec already holding the
	// turnstile so the one below queues behind it.
	_, _ = c.ContainerExec(ctx, "h1", "some-container", []string{"/bin/true"})
	c.mu.Lock()
	stale := c.execCtx["h1"]
	c.mu.Unlock()
	require.NotNil(t, stale)

	gate, err := c.execGateFor("h1")
	require.NoError(t, err)
	gate <- struct{}{}

	done := make(chan struct{})
	go func() {
		defer close(done)
		_, _ = c.ContainerExec(ctx, "h1", "some-container", []string{"/bin/true"})
	}()

	// Drop the entry while the exec is queued, exactly as invalidateExecConn or
	// SetHosts would, then let it run.
	time.Sleep(50 * time.Millisecond)
	c.mu.Lock()
	delete(c.execCtx, "h1")
	c.mu.Unlock()
	<-gate
	<-done

	c.mu.Lock()
	fresh := c.execCtx["h1"]
	c.mu.Unlock()
	require.NotNil(t, fresh, "the queued exec must redial, not run on the entry it saw before queueing")
	assert.NotSame(t, stale, fresh,
		"the queued exec ran on the entry captured before the turnstile, which may point at the old endpoint")
}

// TestRealClient_ExecGateNotMintedForUnknownHost pins that execGateFor re-checks
// the host under r.mu. Its caller validated the host and released the lock; if
// SetHosts removed it in that window, minting a gate here would leak a map
// entry SetHosts never iterates over again.
func TestRealClient_ExecGateNotMintedForUnknownHost(t *testing.T) {
	sock, _ := fakeLibpodServer(t)
	c, err := NewReal([]config.Host{{ID: "h1", Addr: "unix", Socket: sock}})
	require.NoError(t, err)

	_, err = c.execGateFor("gone")
	require.Error(t, err)

	c.mu.Lock()
	_, minted := c.execGate["gone"]
	c.mu.Unlock()
	assert.False(t, minted, "execGateFor minted a gate for an id SetHosts will never clean up")
}

// TestRealClient_TransferDeadlineDoesNotEvict pins that the data-volume budgets
// (VolumeExport/VolumeImport, ImagePull) carry no deadline-eviction. Those
// deadlines are sized to bytes, not to hang-detection, so a large-but-healthy
// transfer overrunning one says nothing about the connection's health.
func TestRealClient_TransferDeadlineDoesNotEvict(t *testing.T) {
	sock, _ := fakeLibpodServer(t)
	c, err := NewReal([]config.Host{{ID: "h1", Addr: "unix", Socket: sock}})
	require.NoError(t, err)
	require.NoError(t, c.Ping(context.Background(), "h1"))

	c.mu.Lock()
	c.verified["h1"] = true
	before := c.ctx["h1"]
	c.mu.Unlock()
	require.NotNil(t, before)

	ctx, cancel, err := c.opCtxForTimeout(context.Background(), "h1", time.Millisecond)
	require.NoError(t, err)
	<-ctx.Done()
	require.ErrorIs(t, ctx.Err(), context.DeadlineExceeded)
	cancel()

	c.mu.Lock()
	after, stillCached := c.ctx["h1"]
	verified := c.verified["h1"]
	c.mu.Unlock()
	require.True(t, stillCached, "a transfer budget expiring evicted the host's connection")
	assert.Same(t, before, after)
	assert.True(t, verified, "a transfer budget expiring cleared the host's verified flag")
}

// TestRealClient_ExecOnUnverifiedHostDialsNoExecConnection pins that the exec
// connection is dialed only once the call has been admitted. ContainerExec used
// to resolve it up front as its unknown-host check, which cost a second SSH
// handshake plus a ping under r.mu before anything had been decided — and, when
// the host then failed the MinPodmanVersion gate, left that connection cached
// in r.execCtx forever: nothing ever sends on it, and eviction rides a
// transport error, so one idle connection leaked per too-old host for the
// process lifetime. entryFor and execGateFor both reject an unknown host on
// their own, so the early dial bought nothing.
func TestRealClient_ExecOnUnverifiedHostDialsNoExecConnection(t *testing.T) {
	sock, _ := fakeLibpodServer(t)
	c, err := NewReal([]config.Host{{ID: "h1", Addr: "unix", Socket: sock}})
	require.NoError(t, err)

	c.versionProbe = func(context.Context) (string, error) { return "5.0.0", nil }

	_, err = c.ContainerExec(context.Background(), "h1", "some-container", []string{"/bin/true"})
	require.ErrorIs(t, err, ErrHostVersionUnsupported)

	c.mu.Lock()
	_, dialed := c.execCtx["h1"]
	c.mu.Unlock()
	assert.False(t, dialed, "a host that never got past the version gate must not leave an exec connection behind")
}

// TestRealClient_RestoreTransportPutsBackTheHookedOne pins which transport
// restoreTransport restores. It used to snapshot conn.Client.Transport by
// reading the field back, which is the pattern connEntry and closeIdleConns
// deliberately avoid: the field is not reliably ours to read (podman's
// newUpgradeRequest writes it unsynchronised, and holds its own throwaway there
// for the duration of an exec), and restoring a value read from it cements
// podman's throwaway — IdleConnTimeout: 0, DisableCompression dropped — as the
// connection's permanent transport while orphaning the hooked one, pooled
// sockets and their per-socket goroutines included.
func TestRealClient_RestoreTransportPutsBackTheHookedOne(t *testing.T) {
	sock, _ := fakeLibpodServer(t)
	c, err := NewReal([]config.Host{{ID: "h1", Addr: "unix", Socket: sock}})
	require.NoError(t, err)

	e, err := c.entryFor(context.Background(), "h1")
	require.NoError(t, err)
	require.NotNil(t, e.hooked)

	conn, err := bindings.GetClient(e.ctx)
	require.NoError(t, err)

	// Whatever the field happens to hold when restoreTransport is called is not
	// the transport that belongs on the connection.
	notOurs := &http.Transport{}
	conn.Client.Transport = notOurs

	restore := c.restoreTransport(e)
	conn.Client.Transport = &http.Transport{} // podman's next throwaway
	restore()

	assert.Same(t, e.hooked, conn.Client.Transport,
		"restore must put back the hooked transport, not whatever the connection happened to hold")
	assert.NotSame(t, notOurs, conn.Client.Transport)
}

// respondingThenEOFConn answers every complete request written to it with a
// canned HTTP/1.1 response and then, once it has served one and nothing is
// outstanding, returns io.EOF from the next read — which is exactly what a
// keep-alive connection the server closed while idle looks like to net/http's
// readLoop. It is the counterpart to scriptedConn(io.EOF), which EOFs a
// request that was never answered at all.
//
// Reads BLOCK while nothing is pending and nothing has been served yet,
// because a real socket does: net/http's readLoop Peeks the connection before
// writeLoop has written the request, and a fake that EOFs that peek models a
// server that hung up instantly, not an idle close.
type respondingThenEOFConn struct {
	mu      sync.Mutex
	cond    *sync.Cond
	pending []byte
	served  bool
	closed  bool
}

func newRespondingThenEOFConn() *respondingThenEOFConn {
	c := &respondingThenEOFConn{}
	c.cond = sync.NewCond(&c.mu)
	return c
}

func (c *respondingThenEOFConn) Read(b []byte) (int, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for len(c.pending) == 0 && !c.served && !c.closed {
		c.cond.Wait()
	}
	if c.closed {
		return 0, net.ErrClosed
	}
	if len(c.pending) == 0 {
		// Served a response and nothing is outstanding: the idle close.
		return 0, io.EOF
	}
	n := copy(b, c.pending)
	c.pending = c.pending[n:]
	return n, nil
}

func (c *respondingThenEOFConn) Write(b []byte) (int, error) {
	if strings.Contains(string(b), "\r\n\r\n") {
		c.mu.Lock()
		c.pending = append(c.pending,
			[]byte("HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: 2\r\n\r\n{}")...)
		c.served = true
		c.cond.Broadcast()
		c.mu.Unlock()
	}
	return len(b), nil
}

func (c *respondingThenEOFConn) Close() error {
	c.mu.Lock()
	c.closed = true
	c.cond.Broadcast()
	c.mu.Unlock()
	return nil
}

func (c *respondingThenEOFConn) LocalAddr() net.Addr              { return fakeAddr{} }
func (c *respondingThenEOFConn) RemoteAddr() net.Addr             { return fakeAddr{} }
func (c *respondingThenEOFConn) SetDeadline(time.Time) error      { return nil }
func (c *respondingThenEOFConn) SetReadDeadline(time.Time) error  { return nil }
func (c *respondingThenEOFConn) SetWriteDeadline(time.Time) error { return nil }

// breakConnWith is breakConn with the scripted connection supplied by the
// caller, for tests that need a shape scriptedConn does not model.
func breakConnWith(t *testing.T, r *Real, id string, cached *connEntry, dial func() net.Conn) {
	t.Helper()
	conn, err := bindings.GetClient(cached.ctx)
	require.NoError(t, err)
	conn.Client.Transport = &http.Transport{
		DialContext: func(context.Context, string, string) (net.Conn, error) {
			return dial(), nil
		},
	}
	r.wireInvalidation(cached, id, r.invalidateConn)
}

// TestRealClient_EOFOnOutstandingRequestEvicts covers #276. report suppressed
// io.EOF unconditionally, so a host that keeps *dialing* fine but EOFs every
// response read — a podman service restarting in a loop, a socket that
// accepts then closes, an SSH channel that opens on a half-dead client and
// immediately EOFs — was never evicted: r.ctx[id] and r.verified[id] stayed
// cached and #252's replay-forever shape came back for that case. An EOF on a
// connection with a write outstanding is not an idle close, and must evict.
func TestRealClient_EOFOnOutstandingRequestEvicts(t *testing.T) {
	sock, accepts := fakeLibpodServer(t)
	c, err := NewReal([]config.Host{{ID: "h1", Addr: "unix", Socket: sock}})
	require.NoError(t, err)

	ctx := context.Background()
	require.NoError(t, c.Ping(ctx, "h1"))
	warmAccepts := accepts.Load()

	c.mu.Lock()
	cached := c.ctx["h1"]
	c.verified["h1"] = true
	c.mu.Unlock()
	require.NotNil(t, cached)

	// Dials keep succeeding; every response read EOFs with the request still
	// outstanding.
	breakConn(t, c, "h1", cached, io.EOF)

	require.Error(t, c.Ping(ctx, "h1"))

	after := waitEvicted(t, c, "h1", cached)
	require.False(t, cached == after, "an EOF with a request outstanding must evict, not be mistaken for an idle close")

	c.mu.Lock()
	stillVerified := c.verified["h1"]
	c.mu.Unlock()
	assert.False(t, stillVerified, "eviction must drop the version pass recorded against the dead connection")

	assert.NoError(t, c.Ping(ctx, "h1"), "a call after eviction must redial and succeed")
	assert.Greater(t, accepts.Load(), warmAccepts, "eviction must cause a real redial, not just a state flip")
}

// TestRealClient_IdleCloseEOFDoesNotEvict is the other half of #276: the case
// the io.EOF exclusion was written for must keep working. A keep-alive
// connection the server closes while idle surfaces as a zero-byte EOF read
// with nothing outstanding; http.Transport handles that transparently by
// dialing a fresh one, and evicting there would drop a healthy connection on a
// routine event.
func TestRealClient_IdleCloseEOFDoesNotEvict(t *testing.T) {
	sock, _ := fakeLibpodServer(t)
	c, err := NewReal([]config.Host{{ID: "h1", Addr: "unix", Socket: sock}})
	require.NoError(t, err)

	ctx := context.Background()
	require.NoError(t, c.Ping(ctx, "h1"))

	c.mu.Lock()
	cached := c.ctx["h1"]
	c.verified["h1"] = true
	c.mu.Unlock()
	require.NotNil(t, cached)

	breakConnWith(t, c, "h1", cached, func() net.Conn { return newRespondingThenEOFConn() })

	// The request itself is answered; the EOF arrives afterwards, on the idle
	// connection, which is precisely the excluded case.
	require.NoError(t, c.Ping(ctx, "h1"))

	// waitEvicted returns as soon as the cache changes; here nothing should
	// change for its whole window.
	after := waitEvicted(t, c, "h1", cached)
	assert.True(t, cached == after, "an idle-close EOF must not evict the cached connection")

	c.mu.Lock()
	stillVerified := c.verified["h1"]
	c.mu.Unlock()
	assert.True(t, stillVerified, "an idle-close EOF must not drop the host's version pass")
}

// requestScriptedConn answers the first `answers` complete requests written to
// it with a canned HTTP/1.1 response (answers < 0 means every request), and
// EOFs the next one — with that request still outstanding, which is the shape
// respondingThenEOFConn deliberately cannot produce.
//
// The difference is where the EOF lands. respondingThenEOFConn EOFs the idle
// read that follows a completed response; this EOFs a read whose request has
// just been written. Both are zero-byte io.EOF reads on a connection that has
// already served a response, so `served` alone cannot separate them — only
// writePending can, which is the point of the test below.
//
// Reads block while nothing is pending, the way a real pooled socket does, so
// the connection sits in net/http's idle pool between requests instead of
// being torn down by an EOF nobody asked for.
type requestScriptedConn struct {
	mu       sync.Mutex
	cond     *sync.Cond
	pending  []byte
	answers  int
	requests int
	eof      bool
	closed   bool
}

func newRequestScriptedConn(answers int) *requestScriptedConn {
	c := &requestScriptedConn{answers: answers}
	c.cond = sync.NewCond(&c.mu)
	return c
}

func (c *requestScriptedConn) Read(b []byte) (int, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for len(c.pending) == 0 && !c.eof && !c.closed {
		c.cond.Wait()
	}
	if c.closed {
		return 0, net.ErrClosed
	}
	if len(c.pending) == 0 {
		return 0, io.EOF
	}
	n := copy(b, c.pending)
	c.pending = c.pending[n:]
	return n, nil
}

func (c *requestScriptedConn) Write(b []byte) (int, error) {
	if strings.Contains(string(b), "\r\n\r\n") {
		c.mu.Lock()
		c.requests++
		if c.answers != 0 {
			if c.answers > 0 {
				c.answers--
			}
			c.pending = append(c.pending,
				[]byte("HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: 2\r\n\r\n{}")...)
		} else {
			c.eof = true
		}
		c.cond.Broadcast()
		c.mu.Unlock()
	}
	return len(b), nil
}

func (c *requestScriptedConn) requestCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.requests
}

func (c *requestScriptedConn) Close() error {
	c.mu.Lock()
	c.closed = true
	c.cond.Broadcast()
	c.mu.Unlock()
	return nil
}

func (c *requestScriptedConn) LocalAddr() net.Addr              { return fakeAddr{} }
func (c *requestScriptedConn) RemoteAddr() net.Addr             { return fakeAddr{} }
func (c *requestScriptedConn) SetDeadline(time.Time) error      { return nil }
func (c *requestScriptedConn) SetReadDeadline(time.Time) error  { return nil }
func (c *requestScriptedConn) SetWriteDeadline(time.Time) error { return nil }

// TestRealClient_EOFOnPooledConnectionWithWriteOutstandingEvicts pins the
// writePending half of reportRead's condition — the shape #276 names first: a
// podman service that restarts between two calls, so the connection sitting in
// net/http's idle pool has already carried a response, gets the next request
// written to it, and only then EOFs.
//
// The two other EOF tests are both decided by `served` alone (one EOFs a
// freshly dialed connection, the other EOFs an idle one), so deleting
// `!c.writePending.Load()` from reportRead leaves them passing. This one
// fails without it: `served` is true here, and writePending is the only thing
// separating this EOF from the idle close that must stay suppressed.
func TestRealClient_EOFOnPooledConnectionWithWriteOutstandingEvicts(t *testing.T) {
	sock, _ := fakeLibpodServer(t)
	c, err := NewReal([]config.Host{{ID: "h1", Addr: "unix", Socket: sock}})
	require.NoError(t, err)

	ctx := context.Background()
	require.NoError(t, c.Ping(ctx, "h1"))

	c.mu.Lock()
	cached := c.ctx["h1"]
	c.verified["h1"] = true
	c.mu.Unlock()
	require.NotNil(t, cached)

	// The first dial gets the connection that answers one request and then
	// EOFs the next. Later dials get a healthy one: net/http retries an
	// idempotent request when a *reused* connection fails before any response
	// byte, and that retry must not be able to manufacture an eviction of its
	// own — a fresh connection has served == false, so it would evict even
	// with writePending deleted, masking exactly what this test pins.
	pooled := newRequestScriptedConn(1)
	var dials atomic.Int64
	breakConnWith(t, c, "h1", cached, func() net.Conn {
		if dials.Add(1) == 1 {
			return pooled
		}
		return newRequestScriptedConn(-1)
	})

	// Request one: answered, so the connection is served and goes back to the
	// idle pool with writePending cleared.
	require.NoError(t, c.Ping(ctx, "h1"))
	require.Equal(t, 1, pooled.requestCount(), "the first call must go to the pooled connection")

	// Request two: reuses that pooled connection, writes, and EOFs. Whether
	// the retry then succeeds is net/http's business and not what is being
	// asserted — the eviction is.
	_ = c.Ping(ctx, "h1")
	require.Equal(t, 2, pooled.requestCount(),
		"the second call must have been written to the already-served pooled connection, or this test is not exercising the shape it claims")

	after := waitEvicted(t, c, "h1", cached)
	assert.False(t, cached == after,
		"an EOF on a pooled connection with the request still outstanding must evict: it is a restarted daemon, not an idle close")

	c.mu.Lock()
	stillVerified := c.verified["h1"]
	c.mu.Unlock()
	assert.False(t, stillVerified, "eviction must drop the version pass recorded against the dead connection")
}

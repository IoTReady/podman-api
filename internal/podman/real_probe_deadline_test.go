package podman

import (
	"context"
	"net"
	"net/http"
	"testing"
	"time"

	"github.com/containers/podman/v5/pkg/bindings"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/iotready/podman-api/internal/config"
)

// These tests cover #275: the diagnostics (Ping, Version, HostInfo) and
// ensureVerified's version probe ran their libpod request on the raw cached
// connection context — rooted at context.Background() — so against a wedged
// host (the socket accepts writes, the OS never surfaces a read error) they
// never returned and the caller's context could not stop them. GET /hosts
// calls all three serially per host, so one wedged host hung the whole request
// forever and leaked the handler goroutine.
//
// Every assertion here is bounded: a regression must fail the test, never hang
// the suite.

// wedgedProbeHost stands up the fake libpod server, warms the cached
// connection, then wedges it — the state every test below starts from.
func wedgedProbeHost(t *testing.T) *Real {
	t.Helper()
	sock, _ := fakeLibpodServer(t)
	c, err := NewReal([]config.Host{{ID: "h1", Addr: "unix", Socket: sock}})
	require.NoError(t, err)

	// Warm the cache against the real, healthy server.
	require.NoError(t, c.Ping(context.Background(), "h1"))

	c.mu.Lock()
	cached := c.ctx["h1"]
	c.mu.Unlock()
	require.NotNil(t, cached, "warm-up must have cached a connection")

	// Reads never return: the call surfaces nothing but its own deadline.
	breakConn(t, c, "h1", cached, nil)
	return c
}

// shrinkProbeTimeout makes the diagnostic budget small enough to assert on.
// It is the same package var production derives the probe deadline from; the
// test only makes it small, not different.
func shrinkProbeTimeout(t *testing.T, d time.Duration) {
	t.Helper()
	orig := probeTimeout
	probeTimeout = d
	t.Cleanup(func() { probeTimeout = orig })
}

// mustReturnWithin runs f and fails — rather than hanging the suite — if it
// does not return inside limit. It returns f's error.
func mustReturnWithin(t *testing.T, limit time.Duration, f func() error) error {
	t.Helper()
	done := make(chan error, 1)
	go func() { done <- f() }()
	select {
	case err := <-done:
		return err
	case <-time.After(limit):
		t.Fatalf("call did not return within %s against a wedged connection", limit)
		return nil
	}
}

func TestRealClient_PingIsBoundedAgainstWedgedConn(t *testing.T) {
	c := wedgedProbeHost(t)
	shrinkProbeTimeout(t, 20*time.Millisecond)

	err := mustReturnWithin(t, 5*time.Second, func() error {
		return c.Ping(context.Background(), "h1")
	})
	assert.Error(t, err, "Ping against a wedged connection must fail, not succeed")
}

func TestRealClient_VersionIsBoundedAgainstWedgedConn(t *testing.T) {
	c := wedgedProbeHost(t)
	shrinkProbeTimeout(t, 20*time.Millisecond)

	err := mustReturnWithin(t, 5*time.Second, func() error {
		_, err := c.Version(context.Background(), "h1")
		return err
	})
	assert.Error(t, err)
}

func TestRealClient_HostInfoIsBoundedAgainstWedgedConn(t *testing.T) {
	c := wedgedProbeHost(t)
	shrinkProbeTimeout(t, 20*time.Millisecond)

	err := mustReturnWithin(t, 5*time.Second, func() error {
		_, err := c.HostInfo(context.Background(), "h1")
		return err
	})
	assert.Error(t, err)
}

// The caller's cancellation must unblock a diagnostic promptly, independently
// of the probe budget: GET /hosts' handler goroutine has to die when the
// client disconnects. probeTimeout is left long here so that only the
// AfterFunc bridge can be what ends the call.
func TestRealClient_DiagnosticsUnblockOnCallerCancel(t *testing.T) {
	for _, tc := range []struct {
		name string
		call func(*Real, context.Context) error
	}{
		{"Ping", func(c *Real, ctx context.Context) error { return c.Ping(ctx, "h1") }},
		{"Version", func(c *Real, ctx context.Context) error { _, err := c.Version(ctx, "h1"); return err }},
		{"HostInfo", func(c *Real, ctx context.Context) error { _, err := c.HostInfo(ctx, "h1"); return err }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := wedgedProbeHost(t)
			shrinkProbeTimeout(t, time.Hour)

			ctx, cancel := context.WithCancel(context.Background())
			time.AfterFunc(20*time.Millisecond, cancel)
			defer cancel()

			err := mustReturnWithin(t, 5*time.Second, func() error { return tc.call(c, ctx) })
			assert.Error(t, err, "a cancelled caller must unblock the diagnostic")
		})
	}
}

// ensureVerified runs on every first-use operation against a host, and it ran
// its probe on the raw connection context — so even a properly deadlined
// operation hung there indefinitely. callTimeout is left at its production
// value: only the probe's own budget can end this call.
func TestRealClient_EnsureVerifiedProbeIsBoundedAgainstWedgedConn(t *testing.T) {
	c := wedgedProbeHost(t)
	shrinkVerifyTimeout(t, 20*time.Millisecond)

	c.mu.Lock()
	verified := c.verified["h1"]
	c.mu.Unlock()
	require.False(t, verified, "the host must still be unverified: Ping never verifies")

	err := mustReturnWithin(t, 5*time.Second, func() error {
		_, err := c.PodList(context.Background(), "h1", nil)
		return err
	})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "verify host", "the failure must be the version probe's, not a later call's")
}

// And a cancelled caller must unblock the probe too, with no budget in sight.
func TestRealClient_EnsureVerifiedProbeUnblocksOnCallerCancel(t *testing.T) {
	c := wedgedProbeHost(t)
	shrinkVerifyTimeout(t, time.Hour)

	ctx, cancel := context.WithCancel(context.Background())
	time.AfterFunc(20*time.Millisecond, cancel)
	defer cancel()

	err := mustReturnWithin(t, 5*time.Second, func() error {
		_, err := c.PodList(ctx, "h1", nil)
		return err
	})
	assert.Error(t, err)
}

// A SINGLE diagnostic hitting its own short budget must NOT evict the cached
// connection. This is the deliberate decision the issue asks for: probeTimeout
// is a caller-facing reachability budget (10s), not evidence that the
// connection is dead — the ssh_pool.go reading. callTimeout's 10 minutes is
// what is sized so that reaching it means the call hung; eviction stays with
// it (#252/#273).
func TestRealClient_ProbeTimeoutDoesNotEvictConnection(t *testing.T) {
	c := wedgedProbeHost(t)
	shrinkProbeTimeout(t, 20*time.Millisecond)

	c.mu.Lock()
	before := c.ctx["h1"]
	c.mu.Unlock()

	err := mustReturnWithin(t, 5*time.Second, func() error {
		return c.Ping(context.Background(), "h1")
	})
	require.Error(t, err)

	// Give any (unwanted) asynchronous eviction the same window waitEvicted
	// would have allowed it.
	time.Sleep(100 * time.Millisecond)

	c.mu.Lock()
	after := c.ctx["h1"]
	c.mu.Unlock()
	assert.True(t, before == after, "one probe hitting its own short budget must not evict the cached connection")
}

// healConn is breakConn's inverse: it points the cached connection's transport
// back at a real, answering unix socket, so a test can show a probe succeeding
// between two timeouts.
func healConn(t *testing.T, r *Real, id string, cached *connEntry, sock string) {
	t.Helper()
	conn, err := bindings.GetClient(cached.ctx)
	require.NoError(t, err)
	conn.Client.Transport = &http.Transport{
		DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
			var d net.Dialer
			return d.DialContext(ctx, "unix", sock)
		},
	}
	r.wireInvalidation(cached, id, r.invalidateConn)
}

// wedgedProbeHostSock is wedgedProbeHost plus the socket path, for tests that
// need to heal the connection again.
func wedgedProbeHostSock(t *testing.T) (*Real, string, *connEntry) {
	t.Helper()
	sock, _ := fakeLibpodServer(t)
	c, err := NewReal([]config.Host{{ID: "h1", Addr: "unix", Socket: sock}})
	require.NoError(t, err)
	require.NoError(t, c.Ping(context.Background(), "h1"))
	c.mu.Lock()
	cached := c.ctx["h1"]
	c.mu.Unlock()
	require.NotNil(t, cached)
	breakConn(t, c, "h1", cached, nil)
	return c, sock, cached
}

func shrinkVerifyTimeout(t *testing.T, d time.Duration) {
	t.Helper()
	orig := verifyTimeout
	verifyTimeout = d
	t.Cleanup(func() { verifyTimeout = orig })
}

// ensureVerified's failure is HARD — opCtx returns the error and the operation
// fails, and nothing is cached on failure so the next operation can fail
// identically — unlike preflightHost's, which is soft. It therefore gets its
// own, distinctly larger budget than the diagnostic probe, so that a loaded
// host whose `info` is merely slow keeps working. This pins that the two
// budgets are actually separate: with the diagnostic budget at 1ms and the
// verify budget at 400ms, the verify path must spend the latter.
func TestRealClient_EnsureVerifiedHasItsOwnLargerBudget(t *testing.T) {
	c := wedgedProbeHost(t)
	shrinkProbeTimeout(t, time.Millisecond)
	shrinkVerifyTimeout(t, 400*time.Millisecond)

	start := time.Now()
	err := mustReturnWithin(t, 5*time.Second, func() error {
		_, err := c.PodList(context.Background(), "h1", nil)
		return err
	})
	elapsed := time.Since(start)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "verify host")
	assert.Greater(t, elapsed, 200*time.Millisecond,
		"the version probe must spend verifyTimeout, not the much smaller diagnostic probeTimeout")
}

// The escape hatch for a host that receives only diagnostics (drained, spare,
// no instances): nothing there ever calls opCtxFor, so without this a wedged
// connection is never evicted and the host reports unreachable forever — past
// podman on it recovering. K consecutive timeouts, not one, so the "a single
// slow answer must not cost a connection" property survives.
func TestRealClient_RepeatedProbeTimeoutsEvictWedgedConnection(t *testing.T) {
	c := wedgedProbeHost(t)
	shrinkProbeTimeout(t, 20*time.Millisecond)

	c.mu.Lock()
	before := c.ctx["h1"]
	c.mu.Unlock()

	for i := 1; i < probeEvictThreshold; i++ {
		require.Error(t, mustReturnWithin(t, 5*time.Second, func() error {
			return c.Ping(context.Background(), "h1")
		}))
		c.mu.Lock()
		cur := c.ctx["h1"]
		c.mu.Unlock()
		require.True(t, before == cur, "probe timeout %d of %d must not evict yet", i, probeEvictThreshold)
	}

	require.Error(t, mustReturnWithin(t, 5*time.Second, func() error {
		return c.Ping(context.Background(), "h1")
	}))
	after := waitEvicted(t, c, "h1", before)
	assert.False(t, before == after,
		"a connection that timed out %d consecutive probes must be evicted, or a diagnostics-only host never recovers", probeEvictThreshold)
}

// ...and a probe that answers inside its budget clears the streak, so a merely
// slow host — one that sometimes answers — is never evicted no matter how long
// it runs. That is the discriminator between "slow" and "wedged".
func TestRealClient_ProbeSuccessResetsTimeoutStreak(t *testing.T) {
	c, sock, cached := wedgedProbeHostSock(t)
	shrinkProbeTimeout(t, 20*time.Millisecond)

	for i := 0; i < 3; i++ {
		// One timeout short of the threshold, then a success, forever.
		for j := 1; j < probeEvictThreshold; j++ {
			breakConn(t, c, "h1", cached, nil)
			require.Error(t, mustReturnWithin(t, 5*time.Second, func() error {
				return c.Ping(context.Background(), "h1")
			}))
		}
		healConn(t, c, "h1", cached, sock)
		shrinkProbeTimeout(t, 5*time.Second) // room for a real round trip
		require.NoError(t, mustReturnWithin(t, 10*time.Second, func() error {
			return c.Ping(context.Background(), "h1")
		}))
		shrinkProbeTimeout(t, 20*time.Millisecond)

		c.mu.Lock()
		cur := c.ctx["h1"]
		c.mu.Unlock()
		require.True(t, cached == cur, "a host that keeps answering must never be evicted (round %d)", i)
	}
}

// A caller giving up must not count toward the eviction streak: a cancelled
// request says nothing about the connection's health, which is the same
// reading callCtx already takes for callTimeout.
func TestRealClient_CallerCancelDoesNotCountTowardProbeEviction(t *testing.T) {
	c := wedgedProbeHost(t)
	shrinkProbeTimeout(t, time.Hour)

	c.mu.Lock()
	before := c.ctx["h1"]
	c.mu.Unlock()

	for i := 0; i < probeEvictThreshold*2; i++ {
		ctx, cancel := context.WithCancel(context.Background())
		time.AfterFunc(10*time.Millisecond, cancel)
		require.Error(t, mustReturnWithin(t, 5*time.Second, func() error {
			return c.Ping(ctx, "h1")
		}))
		cancel()
	}

	time.Sleep(100 * time.Millisecond)
	c.mu.Lock()
	after := c.ctx["h1"]
	c.mu.Unlock()
	assert.True(t, before == after, "cancelled callers must not evict a connection")
}

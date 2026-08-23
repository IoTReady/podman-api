package podman

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/iotready/podman-api/internal/config"
)

// These tests cover #282: callCtx used to evict only when the deadline that
// fired was the call's OWN and the parent was still live, which made the one
// caller that structurally touches every registered host — the inventory
// poller — permanently unable to evict anything. The poller wraps each host in
// its own budget (-inventory-refresh-timeout, 20s by default), necessarily far
// below callTimeout (10 minutes), so against a wedged host the parent always
// expired first: the AfterFunc bridge recorded Canceled, parent.Err() was
// non-nil, and both halves of the old condition failed. Every tick, forever.
//
// The fix narrows the exclusion to genuine cancellation. A parent whose own
// DEADLINE expired is evidence — nobody walked away, the host did not answer
// in the time the caller had budgeted — while a parent that was CANCELLED
// (a client disconnecting, a shutdown) still evicts nothing.

// wedgedVerifiedHost stands up the fake libpod server, warms and verifies the
// cached connection, then wedges it: writes are accepted, reads never return,
// so a call surfaces nothing but a context error. It returns the client and
// the entry that must (or must not) still be cached afterwards.
func wedgedVerifiedHost(t *testing.T) (*Real, *connEntry) {
	t.Helper()
	sock, _ := fakeLibpodServer(t)
	c, err := NewReal([]config.Host{{ID: "h1", Addr: "unix", Socket: sock}})
	require.NoError(t, err)

	require.NoError(t, c.Ping(context.Background(), "h1"))

	// In production ensureVerified sets this on the first successful
	// opCtxFor call; Ping deliberately bypasses it. Without it PodList below
	// would fail in the version probe (verifyCtx, a different budget) rather
	// than at the deadline this test means to exercise.
	c.mu.Lock()
	c.verified["h1"] = true
	cached := c.ctx["h1"]
	c.mu.Unlock()
	require.NotNil(t, cached, "warm-up must have cached a connection")

	breakConn(t, c, "h1", cached, nil)
	return c, cached
}

func cachedEntry(t *testing.T, c *Real, id string) *connEntry {
	t.Helper()
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.ctx[id]
}

// TestRealClient_PollerShapedDeadlineEvictsWedgedConnection is #282's
// reproduction: an operation issued under a caller budget far SHORTER than
// callTimeout — exactly the shape of the inventory poller's per-host
// context.WithTimeout(ctx, p.Timeout) wrap around RefreshHost -> PodList —
// must evict a wedged connection. Before the fix it could not: the parent
// expired first and callCtx read that as "the caller walked away".
func TestRealClient_PollerShapedDeadlineEvictsWedgedConnection(t *testing.T) {
	c, cached := wedgedVerifiedHost(t)

	// callTimeout is left at its production value on purpose: the whole point
	// is that the poller's budget is the one that fires, not the call's own.
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()

	_, err := c.PodList(ctx, "h1", nil)
	require.Error(t, err)

	assert.False(t, cached == cachedEntry(t, c, "h1"),
		"a host that did not answer inside the poller's per-host budget must be evicted, or the periodic sweep can never heal a wedged host")
}

// The other direction, unchanged by #282: a caller that is CANCELLED — a UI
// client disconnecting mid-request, a shutdown — says nothing about the
// connection's health and must never cost a healthy host its cached
// connection and its verified flag.
func TestRealClient_ParentCancellationStillDoesNotEvict(t *testing.T) {
	c, cached := wedgedVerifiedHost(t)

	ctx, cancel := context.WithCancel(context.Background())
	time.AfterFunc(20*time.Millisecond, cancel)
	defer cancel()

	_, err := c.PodList(ctx, "h1", nil)
	require.Error(t, err)

	assert.True(t, cached == cachedEntry(t, c, "h1"),
		"a caller walking away must not evict a connection")
}

// A cancelled ancestor reaches the call as Canceled through every intervening
// deadline, so a deadlined wrapper around a disconnecting client is still not
// evidence: Go propagates the ancestor's error, not the wrapper's kind.
func TestRealClient_CancelledAncestorUnderADeadlineDoesNotEvict(t *testing.T) {
	c, cached := wedgedVerifiedHost(t)

	outer, cancelOuter := context.WithCancel(context.Background())
	ctx, cancel := context.WithTimeout(outer, time.Hour)
	defer cancel()
	time.AfterFunc(20*time.Millisecond, cancelOuter)
	defer cancelOuter()

	_, err := c.PodList(ctx, "h1", nil)
	require.Error(t, err)

	assert.True(t, cached == cachedEntry(t, c, "h1"),
		"a client disconnect under a deadlined wrapper is still a disconnect")
}

// WaitForPodCompletion keeps its carve-out. Its per-poll parent is not a
// caller's budget at all but podman's own wait budget, sized to how long the
// POD may legitimately run (30 minutes for a blob GC). Its expiry means the
// pod is still running — the polls themselves may have answered promptly — so
// it must not count as evidence about the connection, whichever way the race
// between it and the poll in flight falls.
//
// This pins the boundary deterministically: the connection is wedged and the
// wait budget is the only thing that can end the poll, because callTimeout is
// left at ten minutes.
func TestRealClient_WaitBudgetExpiryMidPollDoesNotEvict(t *testing.T) {
	sock := waitPodFakeServer(t)
	c, err := NewReal([]config.Host{{ID: "h1", Addr: "unix", Socket: sock}})
	require.NoError(t, err)

	require.NoError(t, c.Ping(context.Background(), "h1"))
	c.mu.Lock()
	c.verified["h1"] = true
	cached := c.ctx["h1"]
	c.mu.Unlock()
	require.NotNil(t, cached)

	breakConn(t, c, "h1", cached, nil)

	_, err = c.WaitForPodCompletion(context.Background(), "h1", "somepod", 50*time.Millisecond)
	require.Error(t, err)

	assert.True(t, cached == cachedEntry(t, c, "h1"),
		"the wait budget expiring is a statement about the pod, not the connection")
}

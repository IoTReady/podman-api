package inventory

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

type fakeRefresher struct {
	mu     sync.Mutex
	calls  map[string]int
	failOn map[string]bool
	delay  time.Duration // simulated refresh cost, for deadline-derivation tests
}

func newFakeRefresher() *fakeRefresher {
	return &fakeRefresher{calls: map[string]int{}, failOn: map[string]bool{}}
}

func (f *fakeRefresher) RefreshHost(ctx context.Context, host string) error {
	if f.delay > 0 {
		time.Sleep(f.delay)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls[host]++
	if f.failOn[host] {
		return errors.New("unreachable")
	}
	return nil
}

func (f *fakeRefresher) count(host string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.calls[host]
}

// statsRec records RefreshHostStats calls (and each call's remaining context
// budget, to pin which context the poller derives the stats timeout from) and
// returns a fixed error.
type statsRec struct {
	mu        sync.Mutex
	hosts     []string
	remaining []time.Duration
	dropped   []string
	err       error
}

func (s *statsRec) DropHostStats(host string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.dropped = append(s.dropped, host)
}

func (s *statsRec) wasDropped(host string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, h := range s.dropped {
		if h == host {
			return true
		}
	}
	return false
}

func (s *statsRec) RefreshHostStats(ctx context.Context, host string) error {
	var left time.Duration
	if dl, ok := ctx.Deadline(); ok {
		left = time.Until(dl)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.hosts = append(s.hosts, host)
	s.remaining = append(s.remaining, left)
	return s.err
}

func (s *statsRec) calls() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.hosts)
}

func (s *statsRec) called(host string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, h := range s.hosts {
		if h == host {
			return true
		}
	}
	return false
}

func (s *statsRec) firstRemaining() time.Duration {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.remaining) == 0 {
		return 0
	}
	return s.remaining[0]
}

// usageRec records RefreshHostVolumeUsage calls and returns a fixed error.
type usageRec struct {
	mu    sync.Mutex
	hosts []string
	err   error
}

func (u *usageRec) RefreshHostVolumeUsage(_ context.Context, host string) error {
	u.mu.Lock()
	defer u.mu.Unlock()
	u.hosts = append(u.hosts, host)
	return u.err
}

func (u *usageRec) calls() int {
	u.mu.Lock()
	defer u.mu.Unlock()
	return len(u.hosts)
}

// waitFor polls cond until it holds or ~2s elapse, failing the test on timeout.
func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

func TestPollerRefreshesStatsOnTick(t *testing.T) {
	f := newFakeRefresher()
	st := &statsRec{}
	p := &Poller{Svc: f, Stats: st, Interval: time.Hour, Timeout: time.Second}
	ctx, cancel := context.WithCancel(context.Background())
	p.Start(ctx, func() []string { return []string{"h1", "h2"} })
	waitFor(t, "both hosts sampled", func() bool { return st.calls() == 2 })
	cancel()
	p.Wait()
}

// The four Grafana alert rules gate on podman_api_host_reachable. A stats
// failure must never look like an unreachable host.
func TestPollerStatsErrorDoesNotAffectReachability(t *testing.T) {
	f := newFakeRefresher()
	st := &statsRec{err: errors.New("stats boom")}
	p := &Poller{Svc: f, Stats: st, Interval: time.Hour, Timeout: time.Second}
	ctx, cancel := context.WithCancel(context.Background())
	p.Start(ctx, func() []string { return []string{"h1"} })
	waitFor(t, "h1 sampled", func() bool { return st.calls() == 1 })
	cancel()
	p.Wait()

	// Reachability is the inventory refresh's verdict alone: the fake inventory
	// refresher succeeded, so the host must still read as reachable.
	p.mu.Lock()
	reachable, seen := p.state["h1"]
	statsOK, statsSeen := p.statsState["h1"]
	p.mu.Unlock()
	if !seen || !reachable {
		t.Fatalf("stats failure leaked into reachability state: seen=%v reachable=%v", seen, reachable)
	}
	if !statsSeen || statsOK {
		t.Fatalf("stats failure not recorded in statsState: seen=%v ok=%v", statsSeen, statsOK)
	}
}

// Skipping the sample must still retire the cached one. An unreachable host is
// still in the configured host list the collector enumerates, and the inventory
// still holds its last-known containers to supply the join keys, so otherwise
// the stale sample is re-emitted on every scrape for as long as the host stays
// down — freezing cumulative counters instead of letting the series go absent.
func TestPollerDropsStatsForHostWhoseRefreshFailed(t *testing.T) {
	f := newFakeRefresher()
	f.failOn["dead"] = true
	st := &statsRec{}
	p := &Poller{Svc: f, Stats: st, Interval: time.Hour, Timeout: time.Second}
	ctx, cancel := context.WithCancel(context.Background())
	p.Start(ctx, func() []string { return []string{"live", "dead"} })
	waitFor(t, "dead host's stats dropped", func() bool { return st.wasDropped("dead") })
	cancel()
	p.Wait()

	if st.called("dead") {
		t.Fatal("the dropped host was sampled after all: the tick pays two timeouts for it")
	}
	if st.wasDropped("live") {
		t.Fatal("a healthy host's samples were dropped")
	}
}

// A host whose inventory refresh failed has no stats to hand over: its hctx is
// most likely already expired, and calling anyway would only log a second
// failure for something reachability has already reported.
func TestPollerSkipsStatsWhenRefreshFailed(t *testing.T) {
	f := newFakeRefresher()
	f.failOn["dead"] = true
	st := &statsRec{}
	p := &Poller{Svc: f, Stats: st, Interval: time.Hour, Timeout: time.Second}
	ctx, cancel := context.WithCancel(context.Background())
	p.Start(ctx, func() []string { return []string{"live", "dead"} })
	waitFor(t, "live host sampled", func() bool { return st.called("live") })
	cancel()
	p.Wait()

	if st.called("dead") {
		t.Fatal("stats sampled a host whose inventory refresh had just failed")
	}
	// The skip records no outcome: statsState keeps whatever the last real
	// sample said (here: nothing), so recovery logs a true transition.
	p.mu.Lock()
	_, statsSeen := p.statsState["dead"]
	reachable := p.state["dead"]
	p.mu.Unlock()
	if statsSeen {
		t.Fatal("a skipped sample must not write statsState")
	}
	if reachable {
		t.Fatal("the failing host should still be recorded unreachable")
	}
}

// Refresh and stats share ONE per-host budget: the sampler runs under the same
// hctx as the refresh that preceded it, so a host's total per-tick cost stays
// bounded by Timeout. That is the property that matters — tick blocks the
// ticker, so two independent Timeouts would make the bound 2*Timeout (40s at
// the 30s/20s defaults), stretching every other host's cadence and inflating
// podman_api_inventory_age_seconds fleet-wide.
func TestPollerStatsSharesThePerHostBudgetWithRefresh(t *testing.T) {
	f := newFakeRefresher()
	f.delay = 300 * time.Millisecond // a slow-but-successful inventory refresh
	st := &statsRec{}
	p := &Poller{Svc: f, Stats: st, Interval: time.Hour, Timeout: 2 * time.Second}
	ctx, cancel := context.WithCancel(context.Background())
	p.Start(ctx, func() []string { return []string{"h1"} })
	waitFor(t, "h1 sampled", func() bool { return st.calls() == 1 })
	cancel()
	p.Wait()

	// Under hctx the sampler sees ~1.7s, the Timeout minus what the refresh
	// already spent. A fresh Timeout of its own would show ~2s and put the
	// per-host worst case at 2*Timeout.
	left := st.firstRemaining()
	if left > 2*time.Second-f.delay {
		t.Fatalf("stats context has %s left, more than the %s the refresh left "+
			"of the per-host budget: it appears to have been given a fresh "+
			"timeout rather than sharing hctx",
			left, 2*time.Second-f.delay)
	}
	if left <= 0 {
		t.Fatalf("stats context had no budget left (%s); the sampler could never run", left)
	}
}

// A nil Stats field means the sampler is off; the tick must still run.
func TestPollerWithoutStatsRefresher(t *testing.T) {
	f := newFakeRefresher()
	p := &Poller{Svc: f, Interval: time.Hour, Timeout: time.Second}
	ctx, cancel := context.WithCancel(context.Background())
	p.Start(ctx, func() []string { return []string{"h1"} })
	waitFor(t, "h1 refreshed", func() bool { return f.count("h1") >= 1 })
	cancel()
	p.Wait()
}

func TestPollerPrunesStatsStateForRemovedHosts(t *testing.T) {
	f := newFakeRefresher()
	st := &statsRec{}
	p := &Poller{
		Svc: f, Stats: st, Interval: time.Hour, Timeout: time.Second,
		state: map[string]bool{}, statsState: map[string]bool{},
	}
	ctx := context.Background()

	p.tick(ctx, []string{"a", "b"})
	p.mu.Lock()
	n := len(p.statsState)
	p.mu.Unlock()
	if n != 2 {
		t.Fatalf("after first tick statsState should hold both hosts, got %d", n)
	}

	p.tick(ctx, []string{"a"})
	p.mu.Lock()
	_, stillB := p.statsState["b"]
	_, hasA := p.statsState["a"]
	n2 := len(p.statsState)
	p.mu.Unlock()
	if stillB || !hasA || n2 != 1 {
		t.Fatalf("statsState should hold only {a}, got %d entries (hasA=%v stillB=%v)", n2, hasA, stillB)
	}
}

func TestPollerStartVolumeUsageRunsImmediatelyAndStops(t *testing.T) {
	u := &usageRec{}
	p := &Poller{Svc: newFakeRefresher(), Interval: time.Hour, Timeout: time.Second}
	ctx, cancel := context.WithCancel(context.Background())
	p.StartVolumeUsage(ctx, func() []string { return []string{"h1", "h2"} }, u, time.Hour, time.Second)
	waitFor(t, "both hosts sized", func() bool { return u.calls() == 2 })
	cancel()

	done := make(chan struct{})
	go func() { p.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("Wait did not return after cancel")
	}
}

// A failing volume walk must not stop the loop or touch inventory state. The
// state maps are pre-seeded (not left nil) so that a stray write would be
// visible as a changed value rather than panicking on a nil map and being
// swallowed by the loop's own recover().
func TestPollerStartVolumeUsageSurvivesErrors(t *testing.T) {
	u := &usageRec{err: errors.New("df boom")}
	p := &Poller{
		Svc: newFakeRefresher(), Interval: time.Hour, Timeout: time.Second,
		state: map[string]bool{"h1": true}, statsState: map[string]bool{"h1": true},
	}
	ctx, cancel := context.WithCancel(context.Background())
	p.StartVolumeUsage(ctx, func() []string { return []string{"h1"} }, u, 10*time.Millisecond, time.Second)
	waitFor(t, "repeated walks despite errors", func() bool { return u.calls() >= 2 })
	cancel()
	p.Wait()

	p.mu.Lock()
	reachable, n := p.state["h1"], len(p.state)
	statsOK, sn := p.statsState["h1"], len(p.statsState)
	p.mu.Unlock()
	if !reachable || n != 1 {
		t.Fatalf("failing volume walk changed reachability state: h1=%v, %d entries", reachable, n)
	}
	if !statsOK || sn != 1 {
		t.Fatalf("failing volume walk changed stats state: h1=%v, %d entries", statsOK, sn)
	}
}

// blockingUsage blocks until its context is cancelled, so Wait() can only
// return if cancellation propagates into an in-flight walk.
type blockingUsage struct {
	entered chan struct{}
	once    sync.Once
}

func (b *blockingUsage) RefreshHostVolumeUsage(ctx context.Context, _ string) error {
	b.once.Do(func() { close(b.entered) })
	<-ctx.Done()
	return ctx.Err()
}

// Shutdown calls invPoller.Wait() with no deadline of its own, and a volume walk
// carries a 5m per-host timeout by default — so cancelling the run context, not
// that timeout, has to be what ends an in-flight walk.
func TestPollerStartVolumeUsageWaitReturnsOnCancelMidWalk(t *testing.T) {
	b := &blockingUsage{entered: make(chan struct{})}
	p := &Poller{Svc: newFakeRefresher(), Interval: time.Hour, Timeout: time.Second}
	ctx, cancel := context.WithCancel(context.Background())
	// 5 minutes, as in production: if Wait() returns, it is because of cancel.
	p.StartVolumeUsage(ctx, func() []string { return []string{"h1"} }, b, time.Hour, 5*time.Minute)

	select {
	case <-b.entered:
	case <-time.After(2 * time.Second):
		cancel()
		t.Fatal("volume walk never started")
	}

	cancel()
	done := make(chan struct{})
	go func() { p.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Wait did not return after cancel with a walk in flight: " +
			"SIGTERM shutdown would block on the 5m per-host timeout")
	}
}

// Disabled by a nil refresher or a non-positive interval: no goroutine, no calls.
func TestPollerStartVolumeUsageDisabled(t *testing.T) {
	u := &usageRec{}
	p := &Poller{Svc: newFakeRefresher(), Interval: time.Hour, Timeout: time.Second}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	p.StartVolumeUsage(ctx, func() []string { return []string{"h1"} }, u, 0, time.Second)
	p.StartVolumeUsage(ctx, func() []string { return []string{"h1"} }, nil, time.Hour, time.Second)
	p.Wait() // no goroutines were started, so this returns immediately
	if u.calls() != 0 {
		t.Fatalf("disabled volume usage loop still called the refresher %d times", u.calls())
	}
}

func TestPollerImmediateFirstPassRefreshesAllHosts(t *testing.T) {
	f := newFakeRefresher()
	f.failOn["dead"] = true
	p := &Poller{Svc: f, Interval: time.Hour, Timeout: time.Second}
	ctx, cancel := context.WithCancel(context.Background())

	p.Start(ctx, func() []string { return []string{"a", "b", "dead"} })

	// The immediate first pass should refresh every host within a moment; a
	// failing host must not block the others or panic the loop.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if f.count("a") >= 1 && f.count("b") >= 1 && f.count("dead") >= 1 {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if f.count("a") == 0 || f.count("b") == 0 || f.count("dead") == 0 {
		t.Fatalf("not all hosts refreshed: a=%d b=%d dead=%d",
			f.count("a"), f.count("b"), f.count("dead"))
	}

	cancel()
	p.Wait() // must return promptly after cancel
}

func TestPollerStopsOnContextCancel(t *testing.T) {
	f := newFakeRefresher()
	p := &Poller{Svc: f, Interval: 10 * time.Millisecond, Timeout: time.Second}
	ctx, cancel := context.WithCancel(context.Background())
	p.Start(ctx, func() []string { return []string{"a"} })

	time.Sleep(50 * time.Millisecond)
	cancel()

	done := make(chan struct{})
	go func() { p.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("Wait did not return after cancel")
	}
}

func TestPollerPrunesStateForRemovedHosts(t *testing.T) {
	f := newFakeRefresher()
	// state is normally initialised by Start; set it directly since this test
	// drives tick() without the goroutine loop.
	p := &Poller{Svc: f, Interval: time.Hour, Timeout: time.Second, state: map[string]bool{}}
	ctx := context.Background()

	p.tick(ctx, []string{"a", "b"})
	p.mu.Lock()
	_, hasB := p.state["b"]
	n := len(p.state)
	p.mu.Unlock()
	if !hasB || n != 2 {
		t.Fatalf("after first tick, state should hold both hosts, got %d entries (hasB=%v)", n, hasB)
	}

	// "b" leaves the active host set (e.g. removed on SIGHUP).
	p.tick(ctx, []string{"a"})
	p.mu.Lock()
	_, stillB := p.state["b"]
	_, hasA := p.state["a"]
	n2 := len(p.state)
	p.mu.Unlock()
	if stillB || !hasA || n2 != 1 {
		t.Fatalf("after pruning, state should hold only {a}, got %d entries (hasA=%v stillB=%v)", n2, hasA, stillB)
	}
}

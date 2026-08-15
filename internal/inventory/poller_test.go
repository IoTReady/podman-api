package inventory

import (
	"context"
	"errors"
	"log"
	"strings"
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
	delay     time.Duration // simulated sample cost, for concurrency tests
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
	if s.delay > 0 {
		time.Sleep(s.delay)
	}
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

// bootRec is a hand-rolled BootConverger fake: per-host uptime/ok/err are set
// directly by the test, and every ReconcileSpecsOnHost call is recorded so a
// test can assert it fired exactly once (not once per tick).
type bootRec struct {
	mu     sync.Mutex
	uptime map[string]time.Duration
	ok     map[string]bool
	err    map[string]error

	reconciled  []string                 // one entry per ReconcileSpecsOnHost call, in order
	reconcileDL map[string]time.Duration // host -> time left on the ctx passed to ReconcileSpecsOnHost
	reconcileHD map[string]bool          // host -> whether that ctx had a deadline at all

	delay time.Duration // simulated HostUptime cost, for concurrency tests
}

func newBootRec() *bootRec {
	return &bootRec{
		uptime: map[string]time.Duration{}, ok: map[string]bool{}, err: map[string]error{},
		reconcileDL: map[string]time.Duration{}, reconcileHD: map[string]bool{},
	}
}

func (b *bootRec) HostUptime(_ context.Context, host string) (time.Duration, bool, error) {
	if b.delay > 0 {
		time.Sleep(b.delay)
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.uptime[host], b.ok[host], b.err[host]
}

func (b *bootRec) ReconcileSpecsOnHost(ctx context.Context, host string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.reconciled = append(b.reconciled, host)
	if dl, ok := ctx.Deadline(); ok {
		b.reconcileHD[host] = true
		b.reconcileDL[host] = time.Until(dl)
	} else {
		b.reconcileHD[host] = false
	}
}

// reconcileRemaining reports the time left on the context most recently
// passed to ReconcileSpecsOnHost for host, and whether that context had a
// deadline at all.
func (b *bootRec) reconcileRemaining(host string) (time.Duration, bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.reconcileDL[host], b.reconcileHD[host]
}

func (b *bootRec) reconcileCount(host string) int {
	b.mu.Lock()
	defer b.mu.Unlock()
	n := 0
	for _, h := range b.reconciled {
		if h == host {
			n++
		}
	}
	return n
}

func (b *bootRec) setUptime(host string, d time.Duration) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.uptime[host] = d
	b.ok[host] = true
	b.err[host] = nil
}

func (b *bootRec) setUnknown(host string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.ok[host] = false
	b.err[host] = nil
}

func (b *bootRec) setErr(host string, err error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.err[host] = err
}

// freshPoller returns a Poller with every state map pre-initialised, so tests
// can drive tick() directly (bypassing Start's ticker goroutine) without
// nil-map panics.
func freshPoller(svc Refresher, boot BootConverger) *Poller {
	return &Poller{
		Svc: svc, Boot: boot, Interval: time.Hour, Timeout: time.Second,
		state: map[string]bool{}, statsState: map[string]bool{}, bootUptime: map[string]time.Duration{},
	}
}

// A host observed for the first time must not reconcile: the daemon's own
// startup boot-converge (server.go) already covers "podman-api just
// started". Triggering here too on the very first tick would just repeat it
// for every host, every time podman-api boots.
func TestPollerBootWatch_FirstObservationDoesNotReconcile(t *testing.T) {
	f := newFakeRefresher()
	b := newBootRec()
	b.setUptime("h1", 10000*time.Second)
	p := freshPoller(f, b)

	p.tick(context.Background(), []string{"h1"})

	if n := b.reconcileCount("h1"); n != 0 {
		t.Fatalf("first observation reconciled %d times, want 0", n)
	}
}

// Uptime advancing between ticks, however much elapses, must never look like
// a reboot — this is the ordinary "host stayed up" case, by far the most
// common tick outcome in production.
func TestPollerBootWatch_StableUptimeNeverReconciles(t *testing.T) {
	f := newFakeRefresher()
	b := newBootRec()
	p := freshPoller(f, b)

	uptime := 10000 * time.Second
	b.setUptime("h1", uptime)
	for i := 0; i < 5; i++ {
		p.tick(context.Background(), []string{"h1"})
		uptime += 30 * time.Second
		b.setUptime("h1", uptime)
	}
	p.tick(context.Background(), []string{"h1"})

	if n := b.reconcileCount("h1"); n != 0 {
		t.Fatalf("stable uptime reconciled %d times, want 0", n)
	}
}

// The core behavior: a host's uptime dropping between polls (it rebooted)
// must trigger exactly one ReconcileSpecsOnHost for that host — not zero
// (the gap #231 exists to close), and not once per tick after that (which
// would turn ReconcileSpecsOnHost's per-instance tolerant skip-logging into a
// permanent flood).
func TestPollerBootWatch_UptimeResetReconcilesOnce(t *testing.T) {
	f := newFakeRefresher()
	b := newBootRec()
	p := freshPoller(f, b)

	// Tick 1: establish a baseline uptime (no reboot detected — first seen).
	b.setUptime("h1", 10000*time.Second)
	p.tick(context.Background(), []string{"h1"})

	// The host reboots between polls: uptime drops near zero.
	b.setUptime("h1", 5*time.Second)
	p.tick(context.Background(), []string{"h1"})

	if n := b.reconcileCount("h1"); n != 1 {
		t.Fatalf("uptime reset reconciled %d times, want exactly 1", n)
	}

	// Subsequent ticks with uptime advancing normally from the new boot must
	// not repeat the reconcile.
	b.setUptime("h1", 35*time.Second)
	p.tick(context.Background(), []string{"h1"})
	b.setUptime("h1", 65*time.Second)
	p.tick(context.Background(), []string{"h1"})

	if n := b.reconcileCount("h1"); n != 1 {
		t.Fatalf("after the reboot settled, reconciled %d times, want still exactly 1 (no repeat)", n)
	}
}

// #231 review finding #5: detection must depend only on the two raw uptime
// readings, never on the control-plane's own wall clock. A control-plane NTP
// step big enough to have spuriously tripped the old boot-instant-vs-now
// comparison must not affect this at all — checkBoot never calls time.Now (or
// any Poller.Now hook; there no longer is one) to make its decision, so
// simulating "the control plane's clock jumped" between ticks (impossible to
// even express here now — there's no clock parameter left to perturb) can
// only be demonstrated negatively: uptime advancing normally never
// reconciles, however it's paced.
func TestPollerBootWatch_UptimeComparisonIsWallClockIndependent(t *testing.T) {
	f := newFakeRefresher()
	b := newBootRec()
	p := freshPoller(f, b)

	b.setUptime("h1", time.Hour)
	p.tick(context.Background(), []string{"h1"}) // baseline

	// Uptime advances by a huge amount in one tick (e.g. the poller itself
	// stalled, or ticks were coalesced) — still not a reboot, since it only
	// ever increased.
	b.setUptime("h1", 30*24*time.Hour)
	p.tick(context.Background(), []string{"h1"})

	if n := b.reconcileCount("h1"); n != 0 {
		t.Fatalf("uptime advancing by a large amount reconciled %d times, want 0", n)
	}
}

// #231 review finding #1: the ReconcileSpecsOnHost call triggered by a
// detected reboot must run under a context independent of the uptime probe's
// own short BootTimeout budget — otherwise a real, multi-instance reconcile
// sweep gets truncated by a deadline sized for one cheap uptime read.
func TestPollerBootWatch_ReconcileContextIndependentOfBootTimeout(t *testing.T) {
	f := newFakeRefresher()
	b := newBootRec()
	p := freshPoller(f, b)
	p.BootTimeout = 5 * time.Millisecond // deliberately tiny

	b.setUptime("h1", 10000*time.Second)
	p.tick(context.Background(), []string{"h1"}) // baseline

	b.setUptime("h1", 5*time.Second) // reboot
	p.tick(context.Background(), []string{"h1"})

	if n := b.reconcileCount("h1"); n != 1 {
		t.Fatalf("reconciled %d times, want exactly 1", n)
	}
	left, hasDeadline := b.reconcileRemaining("h1")
	if hasDeadline {
		t.Fatalf("ReconcileSpecsOnHost got a context with %s left on the probe's "+
			"5ms BootTimeout deadline; it must run on the tick's own long-lived context instead", left)
	}
}

// A host whose podman cannot report uptime at all (ok=false — an old podman,
// or a parse failure) must never be treated as "just booted": that would
// reconcile every unreachable/too-old host on every tick.
func TestPollerBootWatch_UnknownUptimeNeverReconciles(t *testing.T) {
	f := newFakeRefresher()
	b := newBootRec()
	b.setUnknown("h1")
	p := freshPoller(f, b)

	for i := 0; i < 3; i++ {
		p.tick(context.Background(), []string{"h1"})
	}

	if n := b.reconcileCount("h1"); n != 0 {
		t.Fatalf("unknown uptime reconciled %d times, want 0", n)
	}
}

// An error probing uptime (host unreachable) must be skipped silently, same
// as an unknown uptime — never mistaken for a reboot, and never panic.
func TestPollerBootWatch_ErrorNeverReconciles(t *testing.T) {
	f := newFakeRefresher()
	b := newBootRec()
	b.setErr("h1", errors.New("unreachable"))
	p := freshPoller(f, b)

	for i := 0; i < 3; i++ {
		p.tick(context.Background(), []string{"h1"})
	}

	if n := b.reconcileCount("h1"); n != 0 {
		t.Fatalf("errored uptime probe reconciled %d times, want 0", n)
	}
}

// A one-off failed probe between two successful ones must not manufacture a
// false reboot: the missed tick must leave the last-known uptime alone rather
// than clearing it, so the next successful probe compares against the real
// baseline, not a blank slate.
func TestPollerBootWatch_TransientErrorDoesNotResetBaseline(t *testing.T) {
	f := newFakeRefresher()
	b := newBootRec()
	p := freshPoller(f, b)

	b.setUptime("h1", 10000*time.Second)
	p.tick(context.Background(), []string{"h1"}) // baseline

	b.setErr("h1", errors.New("blip"))
	p.tick(context.Background(), []string{"h1"}) // transient failure, no probe recorded

	b.setErr("h1", nil)
	b.setUptime("h1", 10060*time.Second) // consistent with the original baseline, unrelated to the missed tick
	p.tick(context.Background(), []string{"h1"})

	if n := b.reconcileCount("h1"); n != 0 {
		t.Fatalf("a transient probe error caused a false reboot detection: reconciled %d times, want 0", n)
	}
}

// Boot-reboot detection is opt-in: a nil Boot field must add no calls and no
// panics, mirroring the existing nil-Stats contract.
func TestPollerBootWatch_NilBootDisabled(t *testing.T) {
	f := newFakeRefresher()
	p := freshPoller(f, nil)
	p.tick(context.Background(), []string{"h1"})
	// No assertion beyond "did not panic" — there is nothing to record without
	// a Boot implementation.
}

// bootUptime must be pruned for hosts no longer in the active set, exactly
// like state/statsState, so host churn can't grow it unbounded.
func TestPollerBootWatch_PrunesStateForRemovedHosts(t *testing.T) {
	f := newFakeRefresher()
	b := newBootRec()
	b.setUptime("a", time.Hour)
	b.setUptime("b", time.Hour)
	p := freshPoller(f, b)

	p.tick(context.Background(), []string{"a", "b"})
	p.mu.Lock()
	n := len(p.bootUptime)
	p.mu.Unlock()
	if n != 2 {
		t.Fatalf("after first tick bootUptime should hold both hosts, got %d", n)
	}

	p.tick(context.Background(), []string{"a"})
	p.mu.Lock()
	_, hasB := p.bootUptime["b"]
	n2 := len(p.bootUptime)
	p.mu.Unlock()
	if hasB || n2 != 1 {
		t.Fatalf("bootUptime should hold only {a} after pruning, got %d entries (hasB=%v)", n2, hasB)
	}
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

// The stats sample must get its OWN budget, derived from the tick context, not
// from the refresh's hctx. Sharing hctx starved the sampler permanently on the
// fleet's busiest host: a 29-template sweep consumed nearly all of Timeout, the
// refresh still returned success, and the stats call was cancelled instantly by
// the exhausted parent (#212). Here the refresh eats 700ms of a 1s Timeout, so a
// shared budget would leave ~300ms; an independent StatsTimeout of 5s must show
// up in full.
func TestPollerStatsGetsItsOwnBudgetIndependentOfRefresh(t *testing.T) {
	f := newFakeRefresher()
	f.delay = 700 * time.Millisecond // a slow-but-successful inventory refresh
	st := &statsRec{}
	p := &Poller{
		Svc: f, Stats: st, Interval: time.Hour,
		Timeout: time.Second, StatsTimeout: 5 * time.Second,
	}
	ctx, cancel := context.WithCancel(context.Background())
	p.Start(ctx, func() []string { return []string{"h1"} })
	waitFor(t, "h1 sampled", func() bool { return st.calls() == 1 })
	cancel()
	p.Wait()

	left := st.firstRemaining()
	// Anything at or below the refresh's leftover (~300ms) means the sampler is
	// still sharing hctx. Allow generous slack for scheduling: the only value a
	// shared budget can produce is < Timeout (1s).
	if left <= time.Second {
		t.Fatalf("stats context has only %s left, no more than the %s per-host "+
			"refresh budget: it appears to still share hctx rather than getting "+
			"its own StatsTimeout of %s",
			left, p.Timeout, p.StatsTimeout)
	}
	// ...and it must be its own StatsTimeout, not "no timeout at all".
	if left > p.StatsTimeout {
		t.Fatalf("stats context has %s left, more than StatsTimeout %s", left, p.StatsTimeout)
	}
}

// A caller that leaves StatsTimeout zero must get the default, never an
// unbounded stats call.
func TestPollerStatsTimeoutDefaultsWhenUnset(t *testing.T) {
	f := newFakeRefresher()
	st := &statsRec{}
	p := &Poller{
		Svc: f, Stats: st, Interval: time.Hour, Timeout: time.Hour,
		state: map[string]bool{}, statsState: map[string]bool{},
	}
	p.tick(context.Background(), []string{"h1"})

	left := st.firstRemaining()
	if left <= 0 {
		t.Fatalf("stats context had no deadline (%s); an unset StatsTimeout must not mean unbounded", left)
	}
	if left > defaultStatsTimeout {
		t.Fatalf("stats context has %s left, more than the %s default", left, defaultStatsTimeout)
	}
	if left < defaultStatsTimeout-time.Second {
		t.Fatalf("stats context has %s left, well under the %s default", left, defaultStatsTimeout)
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

// syncBuf is a mutex-guarded log sink: the walk goroutines write to it
// concurrently while the test reads it.
type syncBuf struct {
	mu sync.Mutex
	b  strings.Builder
}

func (s *syncBuf) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.Write(p)
}

func (s *syncBuf) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.String()
}

func captureLog(t *testing.T) *syncBuf {
	t.Helper()
	buf := &syncBuf{}
	prevOut, prevFlags := log.Writer(), log.Flags()
	log.SetOutput(buf)
	log.SetFlags(0)
	t.Cleanup(func() { log.SetOutput(prevOut); log.SetFlags(prevFlags) })
	return buf
}

// A SIGTERM landing inside a volume walk must not make "volume usage walk
// failed: context canceled" — once per host — the last thing in the log. The
// 5m per-host timeout on an hourly cadence, plus the immediate startup walk,
// gives shutdown a real window to land there.
func TestPollerStartVolumeUsageSilentOnShutdown(t *testing.T) {
	buf := captureLog(t)
	b := &blockingUsage{entered: make(chan struct{})}
	p := &Poller{Svc: newFakeRefresher(), Interval: time.Hour, Timeout: time.Second}
	ctx, cancel := context.WithCancel(context.Background())
	// Per-host timeout far longer than the test: only cancel can end these walks,
	// so every host's error is context.Canceled from the run context.
	p.StartVolumeUsage(ctx, func() []string { return []string{"h1", "h2", "h3"} }, b, time.Hour, 5*time.Minute)

	select {
	case <-b.entered:
	case <-time.After(2 * time.Second):
		cancel()
		t.Fatal("volume walk never started")
	}
	cancel()
	p.Wait()

	if got := buf.String(); strings.Contains(got, "volume usage walk failed") {
		t.Fatalf("shutdown logged a walk failure per host; log was:\n%s", got)
	}
}

// The converse: a genuine per-host timeout, with the run context still alive,
// must still be logged. The guard keys on the run context's Err(), not the
// per-host timeout context's, precisely so this case stays visible.
func TestPollerStartVolumeUsageLogsRealTimeout(t *testing.T) {
	buf := captureLog(t)
	b := &blockingUsage{entered: make(chan struct{})}
	p := &Poller{Svc: newFakeRefresher(), Interval: time.Hour, Timeout: time.Second}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	// Tiny per-host timeout, hourly cadence: the immediate walk times out on its
	// own while the run context is untouched.
	p.StartVolumeUsage(ctx, func() []string { return []string{"h1"} }, b, time.Hour, 10*time.Millisecond)

	waitFor(t, "per-host timeout logged", func() bool {
		return strings.Contains(buf.String(), "host h1 volume usage walk failed")
	})
}

// loadRec records RefreshHostLoadAvg calls (and each call's remaining context
// budget, to pin which context the poller derives the loadavg timeout from)
// and returns a fixed error.
type loadRec struct {
	mu        sync.Mutex
	hosts     []string
	remaining []time.Duration
	err       error
	skipped   bool          // report that no sample was taken
	delay     time.Duration // simulated sample cost, for concurrency tests
}

func (l *loadRec) RefreshHostLoadAvg(ctx context.Context, host string) (bool, error) {
	if l.delay > 0 {
		time.Sleep(l.delay)
	}
	var left time.Duration
	if dl, ok := ctx.Deadline(); ok {
		left = time.Until(dl)
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	l.hosts = append(l.hosts, host)
	l.remaining = append(l.remaining, left)
	return !l.skipped, l.err
}

func (l *loadRec) calls() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return len(l.hosts)
}

func (l *loadRec) called(host string) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	for _, h := range l.hosts {
		if h == host {
			return true
		}
	}
	return false
}

// The point of sampling on the poller (#258): every host gets its loadavg read
// on the poller's schedule, so GET /hosts never pays for it inside its own
// per-host budget — where it sat last and was reliably starved.
func TestPollerSamplesLoadAvgOnTick(t *testing.T) {
	f := newFakeRefresher()
	lr := &loadRec{}
	p := &Poller{Svc: f, LoadAvg: lr, Interval: time.Hour, Timeout: time.Second}
	ctx, cancel := context.WithCancel(context.Background())
	p.Start(ctx, func() []string { return []string{"h1", "h2"} })
	waitFor(t, "both hosts sampled", func() bool { return lr.calls() == 2 })
	cancel()
	p.Wait()
}

// A nil LoadAvg means the sampler is off — a daemon can run without it and
// still report loadavg, lazily, from the client's own cache. The tick must not
// panic, mirroring the nil-Stats contract.
func TestPollerNilLoadAvgIsInert(t *testing.T) {
	f := newFakeRefresher()
	p := &Poller{Svc: f, Interval: time.Hour, Timeout: time.Second}
	ctx, cancel := context.WithCancel(context.Background())
	p.Start(ctx, func() []string { return []string{"h1"} })
	waitFor(t, "h1 refreshed", func() bool { return f.count("h1") > 0 })
	cancel()
	p.Wait()
}

// Same contract as the stats sampler: a side-channel read failing says nothing
// about whether the host is reachable, and the alert rules gate on
// podman_api_host_reachable.
func TestPollerLoadAvgErrorDoesNotAffectReachability(t *testing.T) {
	f := newFakeRefresher()
	lr := &loadRec{err: errors.New("loadavg boom")}
	p := &Poller{Svc: f, LoadAvg: lr, Interval: time.Hour, Timeout: time.Second}
	ctx, cancel := context.WithCancel(context.Background())
	p.Start(ctx, func() []string { return []string{"h1"} })
	waitFor(t, "h1 sampled", func() bool { return lr.calls() == 1 })
	cancel()
	p.Wait()

	p.mu.Lock()
	reachable, seen := p.state["h1"]
	loadOK, loadSeen := p.loadState["h1"]
	p.mu.Unlock()
	if !seen || !reachable {
		t.Fatalf("loadavg failure leaked into reachability state: seen=%v reachable=%v", seen, reachable)
	}
	if !loadSeen || loadOK {
		t.Fatalf("loadavg failure not recorded in loadState: seen=%v ok=%v", loadSeen, loadOK)
	}
}

// Deliberately NOT gated on the refresh, unlike the stats sampler: the two use
// different transports and fail independently. A host whose container sweep
// times out — a big store, a slow libpod — can still answer a one-line `cat`
// over SSH instantly, and skipping it there is what let its cache go stale and
// pushed the read back onto the request path (#258).
func TestPollerSamplesLoadAvgEvenWhenTheRefreshFailed(t *testing.T) {
	f := newFakeRefresher()
	f.failOn["libpod-slow"] = true
	lr := &loadRec{}
	p := &Poller{Svc: f, LoadAvg: lr, Interval: time.Hour, Timeout: time.Second}
	ctx, cancel := context.WithCancel(context.Background())
	p.Start(ctx, func() []string { return []string{"libpod-slow"} })
	waitFor(t, "the host to be sampled despite its failed refresh", func() bool {
		return lr.called("libpod-slow")
	})
	cancel()
	p.Wait()
}

// And the loadavg outcome must not leak into reachability, which is the verdict
// the alert rules gate on — the same separation the stats sampler keeps.
func TestPollerLoadAvgOutcomeIsIndependentOfReachability(t *testing.T) {
	f := newFakeRefresher()
	f.failOn["dead"] = true
	lr := &loadRec{}
	p := &Poller{Svc: f, LoadAvg: lr, Interval: time.Hour, Timeout: time.Second}
	ctx, cancel := context.WithCancel(context.Background())
	p.Start(ctx, func() []string { return []string{"dead"} })
	waitFor(t, "the host to be sampled", func() bool { return lr.called("dead") })
	cancel()
	p.Wait()

	p.mu.Lock()
	reachable, seen := p.state["dead"]
	loadOK, loadSeen := p.loadState["dead"]
	p.mu.Unlock()
	if !seen || reachable {
		t.Fatalf("refresh failure not recorded as unreachable: seen=%v reachable=%v", seen, reachable)
	}
	// The fake sampler succeeds, so the loadavg verdict differs from the
	// reachability one — which is the whole point of separate state.
	if !loadSeen || !loadOK {
		t.Fatalf("loadavg outcome not recorded independently: seen=%v ok=%v", loadSeen, loadOK)
	}
}

// #212's lesson, applied to this sampler: its budget must come from the tick's
// own context, not from the refresh's already-spent hctx. A slow-but-successful
// refresh would otherwise cancel the sample the instant it started — which is
// how loadavg went missing on exactly the busiest hosts in the first place.
func TestPollerLoadAvgBudgetIsIndependentOfTheRefresh(t *testing.T) {
	f := newFakeRefresher()
	f.delay = 150 * time.Millisecond
	lr := &loadRec{}
	p := &Poller{
		Svc: f, LoadAvg: lr, Interval: time.Hour,
		Timeout: 200 * time.Millisecond, LoadAvgTimeout: 5 * time.Second,
	}
	ctx, cancel := context.WithCancel(context.Background())
	p.Start(ctx, func() []string { return []string{"h1"} })
	waitFor(t, "h1 sampled", func() bool { return lr.calls() == 1 })
	cancel()
	p.Wait()

	lr.mu.Lock()
	left := lr.remaining[0]
	lr.mu.Unlock()
	// Had it shared the refresh's hctx, ~50ms of the 200ms budget would remain.
	if left < 4*time.Second {
		t.Fatalf("loadavg sample had %s left; it is sharing the refresh's budget, not its own", left)
	}
}

func TestEffectiveLoadAvgTimeout(t *testing.T) {
	tests := []struct {
		name string
		in   time.Duration
		want time.Duration
	}{
		// A bare -0 must not mean "no budget" (an unbounded call hangs the
		// whole poll loop, since tick blocks the ticker) nor "expire
		// instantly" (which would never sample anything at all).
		{"zero means the default", 0, defaultLoadAvgTimeout},
		{"negative means the default", -time.Second, defaultLoadAvgTimeout},
		{"a set value is honoured", 2 * time.Second, 2 * time.Second},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := EffectiveLoadAvgTimeout(tc.in); got != tc.want {
				t.Errorf("EffectiveLoadAvgTimeout(%s) = %s, want %s", tc.in, got, tc.want)
			}
		})
	}
}

// Host churn (SIGHUP reloads) must not grow the sampler's state map forever,
// same as the inventory and stats state it sits alongside.
func TestPollerPrunesLoadAvgState(t *testing.T) {
	f := newFakeRefresher()
	lr := &loadRec{}
	p := &Poller{Svc: f, LoadAvg: lr, Interval: time.Hour, Timeout: time.Second}
	p.state = map[string]bool{}
	p.statsState = map[string]bool{}
	p.loadState = map[string]bool{"gone": true, "stays": true}
	p.bootUptime = map[string]time.Duration{}

	p.pruneState([]string{"stays"})

	p.mu.Lock()
	defer p.mu.Unlock()
	if _, ok := p.loadState["gone"]; ok {
		t.Error("removed host's loadavg state was not pruned")
	}
	if _, ok := p.loadState["stays"]; !ok {
		t.Error("active host's loadavg state was pruned")
	}
}

// A tick where no sample was taken is not an outcome. Recorded as one it would
// flip a host sitting in "failing" to "recovered" and announce it, on a tick
// that read nothing at all — the inverse of the silence #258 was about, and
// just as misleading.
func TestPollerIgnoresASkippedLoadAvgSample(t *testing.T) {
	f := newFakeRefresher()
	lr := &loadRec{err: errors.New("loadavg boom")}
	p := &Poller{Svc: f, LoadAvg: lr, Interval: time.Hour, Timeout: time.Second}
	ctx, cancel := context.WithCancel(context.Background())
	p.Start(ctx, func() []string { return []string{"h1"} })
	waitFor(t, "the failing sample to be recorded", func() bool {
		p.mu.Lock()
		defer p.mu.Unlock()
		ok, seen := p.loadState["h1"]
		return seen && !ok
	})
	cancel()
	p.Wait()

	// Now the host is skipped rather than sampled: the recorded failure must
	// survive, not be overwritten with a recovery nobody observed.
	lr.mu.Lock()
	lr.skipped, lr.err = true, nil
	lr.mu.Unlock()

	ctx2, cancel2 := context.WithCancel(context.Background())
	p.Start(ctx2, func() []string { return []string{"h1"} })
	waitFor(t, "the second tick", func() bool { return lr.calls() >= 2 })
	cancel2()
	p.Wait()

	p.mu.Lock()
	ok, seen := p.loadState["h1"]
	p.mu.Unlock()
	if !seen || ok {
		t.Errorf("loadState = (ok=%v seen=%v); a skipped tick overwrote the last real verdict", ok, seen)
	}
}

// The stats, loadavg and boot-probe sub-samplers must fan out concurrently
// after the refresh, not run one after another (#260). Sequentially, three
// 150ms samplers would push one host's tick past 450ms; concurrently, the
// worst case is one 150ms wait, not their sum.
func TestPollerSubSamplersRunConcurrently(t *testing.T) {
	f := newFakeRefresher()
	st := &statsRec{delay: 150 * time.Millisecond}
	lr := &loadRec{delay: 150 * time.Millisecond}
	b := newBootRec()
	b.delay = 150 * time.Millisecond
	// A known uptime with no prior baseline: HostUptime is still called (and
	// still pays its delay), but this is a first observation so no reconcile
	// runs and the sample cost is exactly the HostUptime delay.
	b.setUptime("h1", 10000*time.Second)
	p := &Poller{
		Svc: f, Stats: st, LoadAvg: lr, Boot: b, Interval: time.Hour,
		Timeout: time.Second, StatsTimeout: 5 * time.Second,
		LoadAvgTimeout: 5 * time.Second, BootTimeout: 5 * time.Second,
		state: map[string]bool{}, statsState: map[string]bool{},
		loadState: map[string]bool{}, bootUptime: map[string]time.Duration{},
	}

	start := time.Now()
	p.tick(context.Background(), []string{"h1"})
	elapsed := time.Since(start)

	// Sequential would be >= 450ms (150ms * 3). Concurrent should land close
	// to a single 150ms wait; generous slack for scheduling noise on a
	// loaded/throttled CI runner.
	if elapsed >= 700*time.Millisecond {
		t.Fatalf("tick took %s; the stats/loadavg/boot sub-samplers appear to "+
			"run sequentially rather than concurrently", elapsed)
	}
	if st.calls() != 1 || lr.calls() != 1 || b.reconcileCount("h1") != 0 {
		t.Fatalf("unexpected sample counts: stats=%d loadavg=%d reconciles=%d",
			st.calls(), lr.calls(), b.reconcileCount("h1"))
	}
}

// The stats sampler's skip-and-drop gating on the refresh outcome must
// survive the fan-out: even running concurrently with loadavg and boot, a
// failed refresh must still skip (and drop) the stats sample for that host,
// while loadavg and boot are unaffected.
func TestPollerConcurrentFanOutPreservesStatsGating(t *testing.T) {
	f := newFakeRefresher()
	f.failOn["h1"] = true
	st := &statsRec{}
	lr := &loadRec{}
	b := newBootRec()
	b.setUptime("h1", 10000*time.Second)
	p := &Poller{
		Svc: f, Stats: st, LoadAvg: lr, Boot: b, Interval: time.Hour,
		Timeout: time.Second, StatsTimeout: 5 * time.Second,
		LoadAvgTimeout: 5 * time.Second, BootTimeout: 5 * time.Second,
		state: map[string]bool{}, statsState: map[string]bool{},
		loadState: map[string]bool{}, bootUptime: map[string]time.Duration{},
	}
	p.tick(context.Background(), []string{"h1"})

	if st.calls() != 0 {
		t.Errorf("stats sampled a host whose refresh failed: %d calls", st.calls())
	}
	if !st.wasDropped("h1") {
		t.Errorf("stats for a failed-refresh host were not dropped")
	}
	if lr.calls() != 1 {
		t.Errorf("loadavg was not sampled despite the refresh failing: %d calls", lr.calls())
	}
}

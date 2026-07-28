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
}

func newFakeRefresher() *fakeRefresher {
	return &fakeRefresher{calls: map[string]int{}, failOn: map[string]bool{}}
}

func (f *fakeRefresher) RefreshHost(ctx context.Context, host string) error {
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

// statsRec records RefreshHostStats calls and returns a fixed error.
type statsRec struct {
	mu    sync.Mutex
	hosts []string
	err   error
}

func (s *statsRec) RefreshHostStats(_ context.Context, host string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.hosts = append(s.hosts, host)
	return s.err
}

func (s *statsRec) calls() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.hosts)
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

// A failing volume walk must not stop the loop or touch inventory state.
func TestPollerStartVolumeUsageSurvivesErrors(t *testing.T) {
	u := &usageRec{err: errors.New("df boom")}
	p := &Poller{Svc: newFakeRefresher(), Interval: time.Hour, Timeout: time.Second}
	ctx, cancel := context.WithCancel(context.Background())
	p.StartVolumeUsage(ctx, func() []string { return []string{"h1"} }, u, 10*time.Millisecond, time.Second)
	waitFor(t, "repeated walks despite errors", func() bool { return u.calls() >= 2 })
	cancel()
	p.Wait()

	p.mu.Lock()
	n := len(p.state)
	p.mu.Unlock()
	if n != 0 {
		t.Fatalf("volume usage walk touched inventory reachability state: %d entries", n)
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

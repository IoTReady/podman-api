package podman

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/iotready/podman-api/internal/config"
)

// fakeClock is a manually advanced clock, so TTL behaviour is tested by moving
// time rather than by sleeping for a minute.
type fakeClock struct {
	mu sync.Mutex
	t  time.Time
}

func (c *fakeClock) now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

func (c *fakeClock) advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.t = c.t.Add(d)
}

func newFakeClock() *fakeClock {
	return &fakeClock{t: time.Date(2026, 8, 14, 12, 0, 0, 0, time.UTC)}
}

// The fix for #258: the request path must not pay for an SSH round trip on
// every read. A cached sample younger than the TTL is served as-is.
func TestHostLoadAvg_ServesFromCacheWithinTTL(t *testing.T) {
	s := newTestSSHServer(t, "0.42 0.37 0.31 1/512 12345\n")
	r, h := sshTestHost(t, s)
	clk := newFakeClock()
	r.hooks = &testHooks{now: clk.now}

	first := r.hostLoadAvg(context.Background(), h.ID)
	if first == nil {
		t.Fatal("first read returned nil")
	}
	if *first != [3]float64{0.42, 0.37, 0.31} {
		t.Fatalf("got %v", *first)
	}

	// Change what the host would report, to prove the second call did not go
	// out and ask.
	s.setStdout("9.99 9.99 9.99 1/1 1\n")
	clk.advance(loadAvgTTL - time.Second)

	second := r.hostLoadAvg(context.Background(), h.ID)
	if second == nil {
		t.Fatal("second read returned nil")
	}
	if *second != [3]float64{0.42, 0.37, 0.31} {
		t.Fatalf("got %v, want the cached sample", *second)
	}
	if got := s.execs.Load(); got != 1 {
		t.Errorf("execs = %d, want 1 (second read should be served from cache)", got)
	}
}

// The cache must expire, or a host that stopped being sampled would report its
// last value forever — the failure mode the poller's stats sampler drops
// samples to avoid.
func TestHostLoadAvg_ReadsThroughAfterTTL(t *testing.T) {
	s := newTestSSHServer(t, "0.42 0.37 0.31 1/512 12345\n")
	r, h := sshTestHost(t, s)
	clk := newFakeClock()
	r.hooks = &testHooks{now: clk.now}

	if la := r.hostLoadAvg(context.Background(), h.ID); la == nil {
		t.Fatal("first read returned nil")
	}
	s.setStdout("9.99 8.88 7.77 1/1 1\n")
	clk.advance(loadAvgTTL)

	la := r.hostLoadAvg(context.Background(), h.ID)
	if la == nil {
		t.Fatal("read after TTL returned nil")
	}
	if *la != [3]float64{9.99, 8.88, 7.77} {
		t.Fatalf("got %v, want the fresh sample", *la)
	}
	if got := s.execs.Load(); got != 2 {
		t.Errorf("execs = %d, want 2", got)
	}
}

// SampleLoadAvg is the poller's entry point, and its job is to refresh
// unconditionally — a sampler that honoured the TTL would never actually keep
// the cache warm, it would just occasionally agree with it.
func TestSampleLoadAvg_IgnoresTTLAndWarmsTheCache(t *testing.T) {
	s := newTestSSHServer(t, "0.10 0.20 0.30 1/1 1\n")
	r, h := sshTestHost(t, s)
	clk := newFakeClock()
	r.hooks = &testHooks{now: clk.now}

	if _, err := r.SampleLoadAvg(context.Background(), h.ID); err != nil {
		t.Fatalf("first sample: %v", err)
	}
	s.setStdout("0.50 0.60 0.70 1/1 1\n")
	if _, err := r.SampleLoadAvg(context.Background(), h.ID); err != nil {
		t.Fatalf("second sample: %v", err)
	}
	if got := s.execs.Load(); got != 2 {
		t.Errorf("execs = %d, want 2 (the sampler must not honour the TTL)", got)
	}

	// The request path now costs nothing and sees the newer sample.
	la := r.hostLoadAvg(context.Background(), h.ID)
	if la == nil || *la != [3]float64{0.50, 0.60, 0.70} {
		t.Fatalf("got %v, want the sampler's value served from cache", la)
	}
	if got := s.execs.Load(); got != 2 {
		t.Errorf("execs = %d, want 2 (the read should have been free)", got)
	}
}

// A failed read must leave the metric absent rather than serve a stale value
// as if it were current, and must return an error the poller can log — the
// silence was half the bug in #258.
func TestHostLoadAvg_FailureIsAbsentNotStale(t *testing.T) {
	s := newTestSSHServer(t, "0.42 0.37 0.31 1/512 12345\n")
	r, h := sshTestHost(t, s)
	clk := newFakeClock()
	r.hooks = &testHooks{now: clk.now}

	if la := r.hostLoadAvg(context.Background(), h.ID); la == nil {
		t.Fatal("first read returned nil")
	}
	s.setExit(1) // the remote `cat` now fails
	clk.advance(loadAvgTTL)

	if la := r.hostLoadAvg(context.Background(), h.ID); la != nil {
		t.Errorf("got %v, want nil: a failed read must not resurrect the expired sample", *la)
	}
	if _, err := r.SampleLoadAvg(context.Background(), h.ID); err == nil {
		t.Error("SampleLoadAvg returned nil error on a failing read")
	}
}

// An unparseable /proc/loadavg is a failure, not a zero load. Caching a parse
// failure as [0,0,0] would report every affected host as idle.
func TestSampleLoadAvg_UnparseableIsAnError(t *testing.T) {
	s := newTestSSHServer(t, "not a loadavg line\n")
	r, h := sshTestHost(t, s)

	if _, err := r.SampleLoadAvg(context.Background(), h.ID); err == nil {
		t.Fatal("want an error for an unparseable /proc/loadavg")
	}
	if _, ok := r.cachedLoadAvg(h.ID); ok {
		t.Error("an unparseable read must not populate the cache")
	}
}

// A host the client does not know is a host that was removed since the poller
// picked it off the list — not an outage. This is reachable on every reload,
// not just in theory: applyHosts updates the client before the service, so the
// service's existence check can pass against a map the client has already
// swept. Returning an error here would log a false "loadavg unavailable" for a
// host that was cleanly removed.
func TestSampleLoadAvg_UnknownHostIsNotAnOutage(t *testing.T) {
	r, err := NewReal([]config.Host{{ID: "h1", Addr: "unix", Socket: "/run/podman.sock"}})
	if err != nil {
		t.Fatalf("NewReal: %v", err)
	}
	if _, err := r.SampleLoadAvg(context.Background(), "nope"); err != nil {
		t.Fatalf("SampleLoadAvg returned %v; a host removed mid-tick is not a fault", err)
	}
	if _, ok := r.cachedLoadAvg("nope"); ok {
		t.Error("an unknown host must not gain a cache entry")
	}
}

// The adoption shortcut must not treat someone else's *failed* read as this
// tick's sample. A failure bumps the sequence too, so a sequence-only check
// reports success for a host whose read just failed — swallowing the outage
// and corrupting the poller's transition state, since logLoadAvgTransition
// gates on err == nil.
//
// The failure has to land while the tick is queued on the gate, which is the
// only interleaving where adoption happens at all.
func TestSampleLoadAvg_DoesNotAdoptAFailedRead(t *testing.T) {
	s := newTestSSHServer(t, "")
	s.setExit(1)                           // the request-path read will fail...
	s.setExecDelay(150 * time.Millisecond) // ...slowly enough for the tick to queue behind it
	r, h := sshTestHost(t, s)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		if la := r.hostLoadAvg(context.Background(), h.ID); la != nil {
			t.Errorf("request-path read returned %v, want nil", *la)
		}
	}()
	waitFor(t, "the request's read to start", func() bool { return s.execs.Load() == 1 })

	sampleErr := make(chan error, 1)
	go func() { _, err := r.SampleLoadAvg(context.Background(), h.ID); sampleErr <- err }()

	wg.Wait()
	if err := <-sampleErr; err == nil {
		t.Error("the tick adopted a failed read as success; the outage would go unlogged")
	}
}

// A host moved to a new address must not be served the old endpoint's sample.
func TestSetHosts_DropsCachedLoadAvgOnParamChange(t *testing.T) {
	s := newTestSSHServer(t, "3.33 3.33 3.33 1/1 1\n")
	r, h := sshTestHost(t, s)

	if la := r.hostLoadAvg(context.Background(), h.ID); la == nil {
		t.Fatal("first read returned nil")
	}
	if _, ok := r.cachedLoadAvg(h.ID); !ok {
		t.Fatal("expected a cached sample after the first read")
	}

	r.SetHosts([]config.Host{{ID: h.ID, Addr: "tester@127.0.0.1:1", SSHKey: h.SSHKey}})
	if _, ok := r.cachedLoadAvg(h.ID); ok {
		t.Error("the cached sample survived a connection-parameter change")
	}
}

// A host whose side channel is permanently broken — the trust-divergence case
// sshReadLoadAvg documents — must not cost every single request a fresh dial
// to rediscover that. Caching only successes would leave the request path
// paying the full failure cost forever, on every scrape.
func TestHostLoadAvg_FailedReadIsCachedToo(t *testing.T) {
	s := newTestSSHServer(t, "")
	s.setExit(1)
	r, h := sshTestHost(t, s)
	clk := newFakeClock()
	r.hooks = &testHooks{now: clk.now}

	for i := 0; i < 3; i++ {
		if la := r.hostLoadAvg(context.Background(), h.ID); la != nil {
			t.Fatalf("read %d returned a value from a failing host", i)
		}
	}
	if got := s.execs.Load(); got != 1 {
		t.Errorf("execs = %d, want 1 (the failure should be cached, not retried per request)", got)
	}

	// It must expire, or a host that recovers would never be noticed on a
	// daemon running without the poller.
	clk.advance(loadAvgFailTTL)
	s.setExit(0)
	s.setStdout("0.11 0.22 0.33 1/1 1\n")
	la := r.hostLoadAvg(context.Background(), h.ID)
	if la == nil || *la != [3]float64{0.11, 0.22, 0.33} {
		t.Fatalf("got %v, want the host's recovery to be picked up", la)
	}
}

// The poller must not be held back by the request path's failure cache: it has
// its own budget and its own cadence, and it is what notices a recovery first
// on a polled daemon.
func TestSampleLoadAvg_IgnoresTheFailureCache(t *testing.T) {
	s := newTestSSHServer(t, "")
	s.setExit(1)
	r, h := sshTestHost(t, s)
	clk := newFakeClock()
	r.hooks = &testHooks{now: clk.now}

	for i := 0; i < 3; i++ {
		if _, err := r.SampleLoadAvg(context.Background(), h.ID); err == nil {
			t.Fatalf("sample %d: want an error", i)
		}
	}
	if got := s.execs.Load(); got != 3 {
		t.Errorf("execs = %d, want 3 (the sampler retries every tick)", got)
	}
}

// Logging is transition-gated, like the poller gates its own: a permanently
// broken host would otherwise emit a line on every read that reaches the wire.
func TestMarkLoadAvgFailure_ReportsOnlyTransitions(t *testing.T) {
	r, err := NewReal([]config.Host{{ID: "h1", Addr: "unix", Socket: "/run/podman.sock"}})
	if err != nil {
		t.Fatalf("NewReal: %v", err)
	}
	clk := newFakeClock()
	r.hooks = &testHooks{now: clk.now}

	h := config.Host{ID: "h1", Addr: "unix", Socket: "/run/podman.sock"}
	if !r.markLoadAvgFailure("h1", h) {
		t.Error("the first failure must report as a transition")
	}
	if r.markLoadAvgFailure("h1", h) {
		t.Error("a repeat failure must not report as a transition")
	}

	// A success in between makes the next failure a transition again.
	r.mu.Lock()
	r.loadavg["h1"] = loadSample{val: [3]float64{1, 1, 1}, at: clk.now(), ok: true}
	r.mu.Unlock()
	if !r.markLoadAvgFailure("h1", h) {
		t.Error("a failure after a success must report as a transition")
	}

	// A read that raced a reload describes an endpoint that is no longer this
	// host's: nothing to record, nothing to log.
	if r.markLoadAvgFailure("h1", config.Host{ID: "h1", Addr: "tester@moved:22"}) {
		t.Error("a failure against a stale endpoint must not report as a transition")
	}
	if r.markLoadAvgFailure("gone", h) {
		t.Error("a failure for an unconfigured host must not report as a transition")
	}
}

// The read happens with r.mu released, so a SIGHUP reload can reconfigure or
// remove the host while it is in flight. Committing anyway would resurrect a
// sample taken against an endpoint the operator has already moved away from —
// and serve it as the new host's for a full TTL — which is the exact invariant
// SetHosts's own comment promises.
func TestSampleLoadAvg_DiscardsASampleForAHostReconfiguredMidRead(t *testing.T) {
	s := newTestSSHServer(t, "7.77 7.77 7.77 1/1 1\n")
	r, h := sshTestHost(t, s)

	// Interleave the reload at the one moment that matters: read done, sample
	// not yet committed.
	r.hooks = &testHooks{afterLoadRead: func() {
		r.SetHosts([]config.Host{{ID: h.ID, Addr: "tester@moved.example:22", SSHKey: h.SSHKey}})
	}}

	// The read itself reports the race...
	_, _, err := r.sampleLoadAvg(context.Background(), h.ID)
	if !errors.Is(err, errHostReconfigured) {
		t.Fatalf("sampleLoadAvg returned %v, want errHostReconfigured", err)
	}
	if _, ok := r.cachedLoadAvg(h.ID); ok {
		t.Error("a sample from the old endpoint was committed for the reconfigured host")
	}
}

// ...but the poller must not see it as a host fault. logLoadAvgTransition gates
// only on err != nil, so surfacing this would log "loadavg unavailable" as an
// outage every time a routine config reload lands mid-tick — the false-positive
// alerting this change exists to remove.
func TestSampleLoadAvg_ReconfigureRaceIsNotReportedAsAHostFault(t *testing.T) {
	s := newTestSSHServer(t, "7.77 7.77 7.77 1/1 1\n")
	r, h := sshTestHost(t, s)
	r.hooks = &testHooks{afterLoadRead: func() {
		r.SetHosts([]config.Host{{ID: h.ID, Addr: "tester@moved.example:22", SSHKey: h.SSHKey}})
	}}

	if _, err := r.SampleLoadAvg(context.Background(), h.ID); err != nil {
		t.Fatalf("SampleLoadAvg returned %v; a config reload racing a tick is not a host fault", err)
	}
	if _, ok := r.cachedLoadAvg(h.ID); ok {
		t.Error("a sample from the old endpoint was committed for the reconfigured host")
	}
}

// Same race with the host removed outright: the cache entry SetHosts just
// deleted must not come back.
func TestSampleLoadAvg_DiscardsASampleForAHostRemovedMidRead(t *testing.T) {
	s := newTestSSHServer(t, "7.77 7.77 7.77 1/1 1\n")
	r, h := sshTestHost(t, s)

	r.hooks = &testHooks{afterLoadRead: func() { r.SetHosts(nil) }}

	if _, err := r.SampleLoadAvg(context.Background(), h.ID); err != nil {
		t.Fatalf("SampleLoadAvg returned %v; a removal racing a tick is not a host fault", err)
	}
	r.mu.Lock()
	_, resurrected := r.loadavg[h.ID]
	r.mu.Unlock()
	if resurrected {
		t.Error("a cache entry was resurrected for a removed host")
	}
}

// loadAvgTTL's doc comment promises one read per host per TTL. Without
// single-flighting, a burst of requests arriving after the TTL lapses each pays
// its own SSH round trip — the cost this change exists to remove, just moved
// from "every request" to "every request in the first burst after an expiry".
func TestHostLoadAvg_SingleFlightsTheReadThrough(t *testing.T) {
	s := newTestSSHServer(t, "0.42 0.37 0.31 1/1 1\n")
	r, h := sshTestHost(t, s)
	s.setExecDelay(100 * time.Millisecond) // wide enough for the others to queue

	const callers = 6
	var wg sync.WaitGroup
	results := make([]*[3]float64, callers)
	for i := 0; i < callers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			results[i] = r.hostLoadAvg(context.Background(), h.ID)
		}(i)
	}
	wg.Wait()

	if got := s.execs.Load(); got != 1 {
		t.Errorf("execs = %d, want 1 (the read-through should be single-flighted)", got)
	}
	// Losing the race must still yield the value, not a nil.
	for i, la := range results {
		if la == nil || *la != [3]float64{0.42, 0.37, 0.31} {
			t.Errorf("caller %d got %v, want the sampled value", i, la)
		}
	}
}

// A queued caller must not wait past its own deadline for someone else's read
// — the same rule the pool's dial gate follows, and for the same reason.
func TestHostLoadAvg_QueuedCallerHonoursItsOwnDeadline(t *testing.T) {
	s := newTestSSHServer(t, "0.42 0.37 0.31 1/1 1\n")
	r, h := sshTestHost(t, s)
	s.setExecDelay(700 * time.Millisecond)

	done := make(chan struct{})
	go func() {
		defer close(done)
		r.hostLoadAvg(context.Background(), h.ID) // holds the gate
	}()
	waitFor(t, "the leader to start its read", func() bool { return s.execs.Load() == 1 })

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	start := time.Now()
	if la := r.hostLoadAvg(ctx, h.ID); la != nil {
		t.Errorf("got %v, want nil once the queued caller's deadline expires", *la)
	}
	if elapsed := time.Since(start); elapsed > 400*time.Millisecond {
		t.Errorf("waited %s; a queued caller must give up at its own deadline", elapsed)
	}
	<-done
}

// The poller ignores the TTL, but it must not ignore a read already in flight:
// a tick landing on top of a request's read-through would otherwise put two SSH
// reads on one host at one moment, which is exactly what the gate exists to
// prevent.
func TestSampleLoadAvg_SharesTheReadThroughGateWithRequests(t *testing.T) {
	s := newTestSSHServer(t, "0.42 0.37 0.31 1/1 1\n")
	r, h := sshTestHost(t, s)
	s.setExecDelay(150 * time.Millisecond)

	var wg sync.WaitGroup
	wg.Add(2)
	go func() { defer wg.Done(); r.hostLoadAvg(context.Background(), h.ID) }()
	// Give the request a head start so the tick genuinely lands mid-read.
	waitFor(t, "the request's read to start", func() bool { return s.execs.Load() == 1 })
	go func() {
		defer wg.Done()
		if _, err := r.SampleLoadAvg(context.Background(), h.ID); err != nil {
			t.Errorf("SampleLoadAvg: %v", err)
		}
	}()
	wg.Wait()

	if got := s.execs.Load(); got != 1 {
		t.Errorf("execs = %d, want 1 (the tick should adopt the read it waited on)", got)
	}
	if _, ok := r.cachedLoadAvg(h.ID); !ok {
		t.Error("no sample cached; the tick must still leave the cache warm")
	}
}

// Sequential ticks must still each do a real read — adopting a concurrent
// read is not the same as honouring the TTL, which the sampler deliberately
// does not do.
func TestSampleLoadAvg_SequentialTicksStillEachRead(t *testing.T) {
	s := newTestSSHServer(t, "0.42 0.37 0.31 1/1 1\n")
	r, h := sshTestHost(t, s)

	for i := 0; i < 3; i++ {
		if _, err := r.SampleLoadAvg(context.Background(), h.ID); err != nil {
			t.Fatalf("sample %d: %v", i, err)
		}
	}
	if got := s.execs.Load(); got != 3 {
		t.Errorf("execs = %d, want 3", got)
	}
}

// loadGateFor must refuse an unconfigured host, or a request racing SetHosts's
// removal loop leaves a gate entry behind that nothing will ever remove.
func TestLoadGateFor_RemovedHostDoesNotLeakAGate(t *testing.T) {
	s := newTestSSHServer(t, "0.42 0.37 0.31 1/1 1\n")
	r, h := sshTestHost(t, s)

	if la := r.hostLoadAvg(context.Background(), h.ID); la == nil {
		t.Fatal("first read returned nil")
	}
	r.SetHosts(nil)

	if _, ok := r.loadGateFor(h.ID); ok {
		t.Error("loadGateFor handed out a gate for an unconfigured host")
	}
	if la := r.hostLoadAvg(context.Background(), h.ID); la != nil {
		t.Errorf("got %v for a removed host, want nil", *la)
	}
	r.mu.Lock()
	n := len(r.loadGate)
	r.mu.Unlock()
	if n != 0 {
		t.Errorf("loadGate holds %d entries after the host was removed, want 0", n)
	}
}

// Three different reload races reach the poller from three different depths,
// and only one of them is caught by the commit-time check. Reporting the other
// two logs an outage for a host that was simply reconfigured — the same
// false-positive class this change exists to remove.
func TestSampleLoadAvg_AllReloadRacesAreBenign(t *testing.T) {
	tests := []struct {
		name  string
		setup func(t *testing.T, r *Real, h config.Host)
	}{
		{
			// Removed between the gate check and the read's own resolution.
			name: "host removed before the read resolves it",
			setup: func(t *testing.T, r *Real, h config.Host) {
				r.hooks = &testHooks{beforeLoadResolve: func() { r.SetHosts(nil) }}
			},
		},
		{
			// Reconfigured after a good read, caught at commit.
			name: "reconfigured after the read, caught at commit",
			setup: func(t *testing.T, r *Real, h config.Host) {
				r.hooks = &testHooks{afterLoadRead: func() {
					r.SetHosts([]config.Host{{ID: h.ID, Addr: "tester@elsewhere.example:22", SSHKey: h.SSHKey}})
				}}
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			s := newTestSSHServer(t, "7.77 7.77 7.77 1/1 1\n")
			r, h := sshTestHost(t, s)
			tc.setup(t, r, h)

			if _, err := r.SampleLoadAvg(context.Background(), h.ID); err != nil {
				t.Errorf("SampleLoadAvg returned %v; a reload race is not a host fault", err)
			}
		})
	}

	// The converse, and the reason this cannot simply swallow everything: a
	// read that genuinely fails against the host's *current* config is a real
	// outage and must still be reported.
	t.Run("a real failure against the current config is still reported", func(t *testing.T) {
		s := newTestSSHServer(t, "7.77 7.77 7.77 1/1 1\n")
		r, h := sshTestHost(t, s)
		r.hooks = &testHooks{beforeLoadResolve: func() {
			r.SetHosts([]config.Host{{ID: h.ID, Addr: "tester@no-such-host.invalid:22", SSHKey: h.SSHKey}})
		}}
		if _, err := r.SampleLoadAvg(context.Background(), h.ID); err == nil {
			t.Error("a failing read against the current config was swallowed as a reload race")
		}
	})
}

// The third race — the pool entry retired between the read resolving its host
// and the dial starting — has no seam to interleave from the outside without a
// hook inside sshRun itself, which is not worth carrying. The classification is
// what the fix actually is, so that is what is pinned here.
func TestIsReloadRace(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{"nil is not a race", nil, false},
		{"discarded at commit", fmt.Errorf("h1: %w", errHostReconfigured), true},
		{"pool entry retired mid-dial", fmt.Errorf("dial: %w", errRetiredHost), true},
		{"host gone when the read resolved it", fmt.Errorf("%q: %w", "h1", errUnknownHost), true},
		{"a genuine read failure is not a race", errors.New("connection refused"), false},
		{"a deadline is not a race", context.DeadlineExceeded, false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := isReloadRace(tc.err); got != tc.want {
				t.Errorf("isReloadRace(%v) = %v, want %v", tc.err, got, tc.want)
			}
		})
	}
}

// The read resolves the host itself, so it can run against a newer config than
// the caller's snapshot. Judging the sample by the snapshot would discard a
// perfectly current reading and make the host wait another cycle to go warm.
func TestSampleLoadAvg_KeepsASampleReadAgainstTheNewerConfig(t *testing.T) {
	s := newTestSSHServer(t, "1.23 1.23 1.23 1/1 1\n")
	r, h := sshTestHost(t, s)

	// Re-point the host at a second, equally live server before the read
	// resolves it: the read runs against the new address, which is current.
	s2 := newTestSSHServer(t, "4.56 4.56 4.56 1/1 1\n")
	appendKnownHost(t, s2)
	moved := config.Host{ID: h.ID, Addr: "tester@" + s2.addr(), SSHKey: h.SSHKey}
	r.hooks = &testHooks{beforeLoadResolve: func() { r.SetHosts([]config.Host{moved}) }}

	if _, err := r.SampleLoadAvg(context.Background(), h.ID); err != nil {
		t.Fatalf("SampleLoadAvg: %v", err)
	}
	la, ok := r.cachedLoadAvg(h.ID)
	if !ok {
		t.Fatal("a sample read against the current config was discarded")
	}
	if la != [3]float64{4.56, 4.56, 4.56} {
		t.Errorf("cached %v, want the new endpoint's reading", la)
	}
}

// A request-path caller can hold the gate longer than a tick's whole budget —
// getHost passes a context with no deadline at all. The tick giving up on that
// wait is not an outage: a read is in flight and the cache is about to be warm.
func TestSampleLoadAvg_LosingTheGateWaitIsNotAnOutage(t *testing.T) {
	s := newTestSSHServer(t, "0.42 0.37 0.31 1/1 1\n")
	r, h := sshTestHost(t, s)
	s.setExecDelay(400 * time.Millisecond)

	done := make(chan struct{})
	go func() {
		defer close(done)
		r.hostLoadAvg(context.Background(), h.ID) // holds the gate, no deadline
	}()
	waitFor(t, "the request's read to start", func() bool { return s.execs.Load() == 1 })

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	if _, err := r.SampleLoadAvg(ctx, h.ID); err != nil {
		t.Errorf("SampleLoadAvg returned %v; losing the gate wait is not a host fault", err)
	}
	<-done
}

// A read that fails must be recorded against the host it actually ran against,
// not the snapshot taken before it. Judging a failure by the stale snapshot
// finds a mismatch, concludes the reload invalidated it, and drops a real
// ongoing outage — neither cached nor logged. That is the silence #258 set out
// to fix, reintroduced through the guard added to prevent a different bug.
func TestSampleLoadAvg_RecordsAFailureAgainstTheHostItActuallyRead(t *testing.T) {
	s := newTestSSHServer(t, "")
	s.setExit(1) // the read will fail...
	r, h := sshTestHost(t, s)

	// ...against a second, equally configured endpoint the host is moved to
	// just before the read resolves it. The failure is real and current.
	s2 := newTestSSHServer(t, "")
	s2.setExit(1)
	appendKnownHost(t, s2)
	moved := config.Host{ID: h.ID, Addr: "tester@" + s2.addr(), SSHKey: h.SSHKey}
	r.hooks = &testHooks{beforeLoadResolve: func() { r.SetHosts([]config.Host{moved}) }}

	sampled, err := r.SampleLoadAvg(context.Background(), h.ID)
	if err == nil {
		t.Fatal("want an error: the read genuinely failed against the current config")
	}
	if !sampled {
		t.Error("a real failure against the current config was classified as a reload race")
	}
	if !r.recentLoadAvgFailure(h.ID) {
		t.Error("the failure was not cached; every later request pays a fresh dial to rediscover it")
	}
}

// A host switched from SSH to a local unix socket mid-read comes back from the
// read reporting the *new* config — so it equals r.hosts[id], sails past
// markLoadAvgFailure's "is this host still current" guard, and would be
// recorded as failing on a read that was never attempted. That poisons the
// cache for a host whose local /proc read succeeds instantly, and logs an
// outage for it.
func TestSampleLoadAvg_SSHToUnixRaceRecordsNoFailure(t *testing.T) {
	s := newTestSSHServer(t, "0.42 0.37 0.31 1/1 1\n")
	r, h := sshTestHost(t, s)

	local := config.Host{ID: h.ID, Addr: "unix", Socket: "/run/podman.sock"}
	r.hooks = &testHooks{beforeLoadResolve: func() { r.SetHosts([]config.Host{local}) }}

	sampled, err := r.SampleLoadAvg(context.Background(), h.ID)
	if err != nil {
		t.Fatalf("SampleLoadAvg returned %v; a reconfigure is not a host fault", err)
	}
	if sampled {
		t.Error("reported as an outcome; no read was attempted against the new config")
	}
	if r.recentLoadAvgFailure(h.ID) {
		t.Error("a failure was cached for a host that was never read; its next local read is suppressed for a full TTL")
	}
}

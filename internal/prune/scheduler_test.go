package prune

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/iotready/podman-api/internal/podman"
	"github.com/iotready/podman-api/internal/podman/fake"
	"github.com/iotready/podman-api/internal/store"
)

func hp(id string, p Policy) HostPolicy { return HostPolicy{Host: id, Policy: p} }

func enabledDanglingPolicy() Policy {
	return Policy{Enabled: true, Interval: time.Hour, DiskThreshold: 80, Scope: []string{ScopeDangling}}
}

func decodePayload(t *testing.T, j store.Job) Payload {
	t.Helper()
	var p Payload
	if err := json.Unmarshal(j.Args, &p); err != nil {
		t.Fatal(err)
	}
	return p
}

func TestTickIntervalZeroDisablesIntervalTrigger(t *testing.T) {
	mem := store.NewMemory()
	f := fake.New()
	f.HostInfoVal = podman.HostInfo{Disk: podman.DiskUsage{Total: 100, Used: 10}} // under threshold
	now := time.Date(2026, 6, 4, 12, 0, 0, 0, time.UTC)
	pol := enabledDanglingPolicy()
	pol.Interval = 0 // interval trigger disabled
	// Host was pruned a year ago — with a >0 interval this would be long overdue,
	// but Interval==0 must make the interval trigger inert.
	s := &Scheduler{Store: mem, Client: f, Now: func() time.Time { return now },
		lastOverride: map[string]time.Time{"h1": now.Add(-365 * 24 * time.Hour)}}
	s.tick(context.Background(), []HostPolicy{hp("h1", pol)})
	jobs, _ := mem.ListJobs(context.Background(), store.JobFilter{Kind: "prune", State: store.JobQueued})
	if len(jobs) != 0 {
		t.Fatalf("Interval==0 must not trigger by interval, got %d", len(jobs))
	}
}

// TestTickReadsLastPruneFromStore exercises the real store-derived last-prune
// path (no lastOverride seam): a recently-finished succeeded prune for the host
// must suppress a fresh enqueue when the interval hasn't elapsed.
func TestTickReadsLastPruneFromStore(t *testing.T) {
	mem := store.NewMemory()
	f := fake.New()
	f.HostInfoVal = podman.HostInfo{Disk: podman.DiskUsage{Total: 100, Used: 10}} // under threshold
	ctx := context.Background()

	// Seed a succeeded prune for h1 and let Finish stamp its Finished time.
	args, _ := json.Marshal(Payload{Host: "h1"})
	j, _ := mem.Enqueue(ctx, "prune", args, "")
	mem.ClaimNext(ctx)
	if err := mem.Finish(ctx, j.ID, store.JobSucceeded, ""); err != nil {
		t.Fatal(err)
	}
	got, _ := mem.GetJob(ctx, j.ID)

	// "now" is 5 minutes after that finish — well within the 1h interval.
	now := got.Finished.Add(5 * time.Minute)
	s := &Scheduler{Store: mem, Client: f, Now: func() time.Time { return now }}
	s.tick(ctx, []HostPolicy{hp("h1", enabledDanglingPolicy())})

	jobs, _ := mem.ListJobs(ctx, store.JobFilter{Kind: "prune", State: store.JobQueued})
	if len(jobs) != 0 {
		t.Fatalf("store-derived last-prune should suppress enqueue within interval, got %d queued", len(jobs))
	}
}

func TestTickSkipsUnknownHost(t *testing.T) {
	mem := store.NewMemory()
	f := fake.New()
	f.Unknown = map[string]bool{"h1": true} // added via reload, client can't reach it
	now := time.Date(2026, 6, 4, 12, 0, 0, 0, time.UTC)
	s := &Scheduler{Store: mem, Client: f, Now: func() time.Time { return now }}
	s.tick(context.Background(), []HostPolicy{hp("h1", enabledDanglingPolicy())})
	jobs, _ := mem.ListJobs(context.Background(), store.JobFilter{Kind: "prune", State: store.JobQueued})
	if len(jobs) != 0 {
		t.Fatalf("unknown host must not enqueue, got %d", len(jobs))
	}
}

func TestTickBacksOffAfterRecentFailure(t *testing.T) {
	mem := store.NewMemory()
	f := fake.New()
	ctx := context.Background()
	// Seed a recently-failed prune for h1.
	args, _ := json.Marshal(Payload{Host: "h1"})
	j, _ := mem.Enqueue(ctx, "prune", args, "")
	mem.ClaimNext(ctx)
	mem.Finish(ctx, j.ID, store.JobFailed, "boom")
	got, _ := mem.GetJob(ctx, j.ID)

	// 5 minutes after the failure — within failureBackoff (1h).
	now := got.Finished.Add(5 * time.Minute)
	s := &Scheduler{Store: mem, Client: f, Now: func() time.Time { return now }}
	s.tick(ctx, []HostPolicy{hp("h1", enabledDanglingPolicy())})
	jobs, _ := mem.ListJobs(ctx, store.JobFilter{Kind: "prune", State: store.JobQueued})
	if len(jobs) != 0 {
		t.Fatalf("must back off within failureBackoff after a failure, got %d queued", len(jobs))
	}

	// Past the backoff window — should retry.
	now2 := got.Finished.Add(failureBackoff + time.Minute)
	s2 := &Scheduler{Store: mem, Client: f, Now: func() time.Time { return now2 }}
	s2.tick(ctx, []HostPolicy{hp("h1", enabledDanglingPolicy())})
	jobs2, _ := mem.ListJobs(ctx, store.JobFilter{Kind: "prune", State: store.JobQueued})
	if len(jobs2) != 1 {
		t.Fatalf("should retry after backoff elapses, got %d queued", len(jobs2))
	}
}

func TestTickThrottlesThresholdProbe(t *testing.T) {
	mem := store.NewMemory()
	f := fake.New()
	f.HostInfoVal = podman.HostInfo{Disk: podman.DiskUsage{Total: 100, Used: 10}} // under threshold
	now := time.Date(2026, 6, 4, 12, 0, 0, 0, time.UTC)
	// Not due by interval (recent success via override), so each tick would probe.
	s := &Scheduler{Store: mem, Client: f, Now: func() time.Time { return now },
		lastOverride: map[string]time.Time{"h1": now.Add(-1 * time.Minute)}}
	hps := []HostPolicy{hp("h1", enabledDanglingPolicy())}
	s.tick(context.Background(), hps) // probes once
	s.tick(context.Background(), hps) // within thresholdProbeInterval → must NOT probe again
	if f.HostInfoCalls != 1 {
		t.Fatalf("HostInfo probe not throttled: got %d calls, want 1", f.HostInfoCalls)
	}
}

func TestTickEnqueuesWhenIntervalElapsed(t *testing.T) {
	mem := store.NewMemory()
	f := fake.New()
	now := time.Date(2026, 6, 4, 12, 0, 0, 0, time.UTC)
	s := &Scheduler{Store: mem, Client: f, Now: func() time.Time { return now }}

	s.tick(context.Background(), []HostPolicy{hp("h1", enabledDanglingPolicy())})

	jobs, _ := mem.ListJobs(context.Background(), store.JobFilter{Kind: "prune"})
	if len(jobs) != 1 {
		t.Fatalf("expected 1 prune job, got %d", len(jobs))
	}
	if decodePayload(t, jobs[0]).Host != "h1" {
		t.Fatalf("wrong host: %+v", jobs[0])
	}
}

func TestTickSkipsDisabledHost(t *testing.T) {
	mem := store.NewMemory()
	f := fake.New()
	s := &Scheduler{Store: mem, Client: f, Now: time.Now}
	pol := enabledDanglingPolicy()
	pol.Enabled = false
	s.tick(context.Background(), []HostPolicy{hp("h1", pol)})
	jobs, _ := mem.ListJobs(context.Background(), store.JobFilter{Kind: "prune"})
	if len(jobs) != 0 {
		t.Fatalf("disabled host must not enqueue, got %d", len(jobs))
	}
}

func TestTickSkipsWhenNotYetDueAndUnderThreshold(t *testing.T) {
	mem := store.NewMemory()
	f := fake.New()
	f.HostInfoVal = podman.HostInfo{Disk: podman.DiskUsage{Total: 100, Used: 10}} // 10% < 80
	now := time.Date(2026, 6, 4, 12, 0, 0, 0, time.UTC)
	s := &Scheduler{Store: mem, Client: f, Now: func() time.Time { return now },
		lastOverride: map[string]time.Time{"h1": now.Add(-30 * time.Minute)}}

	s.tick(context.Background(), []HostPolicy{hp("h1", enabledDanglingPolicy())})
	jobs, _ := mem.ListJobs(context.Background(), store.JobFilter{Kind: "prune", State: store.JobQueued})
	if len(jobs) != 0 {
		t.Fatalf("expected no new enqueue, got %d queued", len(jobs))
	}
}

func TestTickEnqueuesOnThresholdBeforeInterval(t *testing.T) {
	mem := store.NewMemory()
	f := fake.New()
	f.HostInfoVal = podman.HostInfo{Disk: podman.DiskUsage{Total: 100, Used: 90}} // 90% >= 80
	now := time.Date(2026, 6, 4, 12, 0, 0, 0, time.UTC)
	s := &Scheduler{Store: mem, Client: f, Now: func() time.Time { return now },
		lastOverride: map[string]time.Time{"h1": now.Add(-1 * time.Minute)}}

	s.tick(context.Background(), []HostPolicy{hp("h1", enabledDanglingPolicy())})
	jobs, _ := mem.ListJobs(context.Background(), store.JobFilter{Kind: "prune", State: store.JobQueued})
	if len(jobs) != 1 {
		t.Fatalf("threshold trigger should enqueue, got %d", len(jobs))
	}
}

func TestTickThresholdZeroDisablesThresholdTrigger(t *testing.T) {
	mem := store.NewMemory()
	f := fake.New()
	f.HostInfoVal = podman.HostInfo{Disk: podman.DiskUsage{Total: 100, Used: 99}}
	now := time.Date(2026, 6, 4, 12, 0, 0, 0, time.UTC)
	pol := enabledDanglingPolicy()
	pol.DiskThreshold = 0
	s := &Scheduler{Store: mem, Client: f, Now: func() time.Time { return now },
		lastOverride: map[string]time.Time{"h1": now.Add(-1 * time.Minute)}}
	s.tick(context.Background(), []HostPolicy{hp("h1", pol)})
	jobs, _ := mem.ListJobs(context.Background(), store.JobFilter{Kind: "prune", State: store.JobQueued})
	if len(jobs) != 0 {
		t.Fatalf("threshold=0 must not trigger, got %d", len(jobs))
	}
}

func TestTickSkipsWhenPruneInFlight(t *testing.T) {
	mem := store.NewMemory()
	f := fake.New()
	now := time.Date(2026, 6, 4, 12, 0, 0, 0, time.UTC)
	args, _ := json.Marshal(Payload{Host: "h1"})
	mem.Enqueue(context.Background(), "prune", args, "") // a queued prune for h1 already exists
	s := &Scheduler{Store: mem, Client: f, Now: func() time.Time { return now }}
	s.tick(context.Background(), []HostPolicy{hp("h1", enabledDanglingPolicy())})
	jobs, _ := mem.ListJobs(context.Background(), store.JobFilter{Kind: "prune", State: store.JobQueued})
	if len(jobs) != 1 {
		t.Fatalf("must not double-enqueue while prune in flight, got %d", len(jobs))
	}
}

func TestTickDropsVolumesScopeWhenMigrateActive(t *testing.T) {
	mem := store.NewMemory()
	f := fake.New()
	now := time.Date(2026, 6, 4, 12, 0, 0, 0, time.UTC)
	mem.Enqueue(context.Background(), "migrate", json.RawMessage(`{}`), "") // active migrate
	s := &Scheduler{Store: mem, Client: f, Now: func() time.Time { return now }}
	pol := enabledDanglingPolicy()
	pol.Scope = []string{ScopeDangling, ScopeVolumes}
	s.tick(context.Background(), []HostPolicy{hp("h1", pol)})
	jobs, _ := mem.ListJobs(context.Background(), store.JobFilter{Kind: "prune", State: store.JobQueued})
	if len(jobs) != 1 {
		t.Fatalf("expected 1 prune job, got %d", len(jobs))
	}
	p := decodePayload(t, jobs[0])
	for _, sc := range p.Policy.Scope {
		if sc == ScopeVolumes {
			t.Fatal("volumes scope must be dropped while migrate active")
		}
	}
}

func TestTickDropsVolumesScopeWhenMigrateReconciling(t *testing.T) {
	mem := store.NewMemory()
	f := fake.New()
	ctx := context.Background()
	now := time.Date(2026, 6, 4, 12, 0, 0, 0, time.UTC)

	// Seed a migrate job and move it to the reconciling state.
	migArgs, _ := json.Marshal(map[string]string{})
	_, _ = mem.Enqueue(ctx, "migrate", migArgs, "")
	mem.ClaimNext(ctx)
	mem.MarkReconciling(ctx, []string{"migrate"})

	s := &Scheduler{Store: mem, Client: f, Now: func() time.Time { return now }}
	pol := enabledDanglingPolicy()
	pol.Scope = []string{ScopeDangling, ScopeVolumes}
	s.tick(ctx, []HostPolicy{hp("h1", pol)})

	jobs, _ := mem.ListJobs(ctx, store.JobFilter{Kind: "prune", State: store.JobQueued})
	if len(jobs) != 1 {
		t.Fatalf("expected 1 prune job, got %d", len(jobs))
	}
	p := decodePayload(t, jobs[0])
	for _, sc := range p.Policy.Scope {
		if sc == ScopeVolumes {
			t.Fatal("volumes scope must be dropped while migrate is reconciling")
		}
	}
}

func TestTickSkipsHostWhenInfoErrorsButThresholdNeeded(t *testing.T) {
	mem := store.NewMemory()
	f := fake.New()
	f.HostInfoErr = context.DeadlineExceeded
	now := time.Date(2026, 6, 4, 12, 0, 0, 0, time.UTC)
	s := &Scheduler{Store: mem, Client: f, Now: func() time.Time { return now },
		lastOverride: map[string]time.Time{"h1": now.Add(-1 * time.Minute)}}
	s.tick(context.Background(), []HostPolicy{hp("h1", enabledDanglingPolicy())})
	jobs, _ := mem.ListJobs(context.Background(), store.JobFilter{Kind: "prune", State: store.JobQueued})
	if len(jobs) != 0 {
		t.Fatalf("host with info error and not-due must be skipped, got %d", len(jobs))
	}
}

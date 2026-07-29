package instance

import (
	"context"
	"errors"
	"testing"

	"github.com/iotready/podman-api/internal/config"
	"github.com/iotready/podman-api/internal/podman"
	"github.com/iotready/podman-api/internal/podman/fake"
)

func statsService(f *fake.Fake) *Service {
	return NewService(f, []config.Host{{ID: "h1"}})
}

func TestRefreshHostStatsPopulatesSnapshot(t *testing.T) {
	f := fake.New()
	f.ContainerStatsVal = map[string][]podman.ContainerStats{
		"h1": {{Name: "engine-valvo-engine", CPUNano: 5, MemUsageBytes: 9}},
	}
	svc := statsService(f)
	if err := svc.RefreshHostStats(context.Background(), "h1"); err != nil {
		t.Fatalf("RefreshHostStats: %v", err)
	}
	snap := svc.StatsSnapshot()
	got, ok := snap["h1"].Containers["engine-valvo-engine"]
	if !ok {
		t.Fatalf("container absent from snapshot: %+v", snap)
	}
	if got.CPUNano != 5 || got.MemUsageBytes != 9 {
		t.Fatalf("stats = %+v", got)
	}
	if snap["h1"].FetchedAt.IsZero() {
		t.Fatal("FetchedAt not stamped")
	}
}

// A failed stats refresh must DROP the host's samples, not keep them. A frozen
// cumulative counter reads as "container went idle", which is a lie; an absent
// series is honest.
func TestRefreshHostStatsDropsOnError(t *testing.T) {
	f := fake.New()
	f.ContainerStatsVal = map[string][]podman.ContainerStats{
		"h1": {{Name: "c", CPUNano: 5}},
	}
	svc := statsService(f)
	if err := svc.RefreshHostStats(context.Background(), "h1"); err != nil {
		t.Fatalf("first refresh: %v", err)
	}
	f.ContainerStatsErr = errors.New("boom")
	if err := svc.RefreshHostStats(context.Background(), "h1"); err == nil {
		t.Fatal("want error")
	}
	if _, ok := svc.StatsSnapshot()["h1"]; ok {
		t.Fatal("stale stats retained after a failed refresh")
	}
}

// The poller skips sampling a host whose inventory refresh just failed, so the
// "called and errored" path above is never reached for it. DropHostStats is how
// that host's samples are still retired instead of freezing at their last value.
func TestDropHostStatsRetiresSamplesWithoutSampling(t *testing.T) {
	f := fake.New()
	f.ContainerStatsVal = map[string][]podman.ContainerStats{
		"h1": {{Name: "c", CPUNano: 5}},
		"h2": {{Name: "d", CPUNano: 7}},
	}
	svc := statsService(f)
	for _, h := range []string{"h1", "h2"} {
		if err := svc.RefreshHostStats(context.Background(), h); err != nil {
			t.Fatalf("seed refresh %s: %v", h, err)
		}
	}

	svc.DropHostStats("h1")

	snap := svc.StatsSnapshot()
	if _, ok := snap["h1"]; ok {
		t.Fatal("DropHostStats left the host's samples in the cache")
	}
	if _, ok := snap["h2"]; !ok {
		t.Fatal("DropHostStats discarded another host's samples")
	}
	// Dropping an unknown host is a no-op, not a panic: the poller may skip a
	// host that has never been sampled.
	svc.DropHostStats("never-seen")
}

// Volume usage is the opposite: sizes change slowly and the walk is hourly, so
// a single failure keeps the last value and lets the age metric report the gap.
func TestRefreshHostVolumeUsageKeepsLastOnError(t *testing.T) {
	f := fake.New()
	f.VolumeUsageVal = map[string]map[string]int64{"h1": {"engine-valvo-data": 4096}}
	svc := statsService(f)
	if err := svc.RefreshHostVolumeUsage(context.Background(), "h1"); err != nil {
		t.Fatalf("first refresh: %v", err)
	}
	f.VolumeUsageErr = errors.New("boom")
	if err := svc.RefreshHostVolumeUsage(context.Background(), "h1"); err == nil {
		t.Fatal("want error")
	}
	if got := svc.VolumeUsageSnapshot()["h1"].Sizes["engine-valvo-data"]; got != 4096 {
		t.Fatalf("size = %d, want the retained 4096", got)
	}
}

func TestSnapshotsAreCopies(t *testing.T) {
	f := fake.New()
	f.ContainerStatsVal = map[string][]podman.ContainerStats{"h1": {{Name: "c"}}}
	svc := statsService(f)
	if err := svc.RefreshHostStats(context.Background(), "h1"); err != nil {
		t.Fatalf("refresh: %v", err)
	}
	snap := svc.StatsSnapshot()
	delete(snap, "h1")
	if _, ok := svc.StatsSnapshot()["h1"]; !ok {
		t.Fatal("mutating a snapshot mutated the cache")
	}
}

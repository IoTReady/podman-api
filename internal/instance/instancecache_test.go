package instance

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/iotready/podman-api/internal/config"
	"github.com/iotready/podman-api/internal/podman"
	"github.com/iotready/podman-api/internal/podman/fake"
	"github.com/iotready/podman-api/internal/render"
	"github.com/iotready/podman-api/internal/store"
)

func TestInstanceCache_HitWithinTTL(t *testing.T) {
	c := newInstanceCache(time.Minute)
	var calls int32
	fetch := func() ([]Observed, error) {
		atomic.AddInt32(&calls, 1)
		return []Observed{{Slug: "a"}}, nil
	}
	if _, err := c.get("h1", fetch); err != nil {
		t.Fatal(err)
	}
	if _, err := c.get("h1", fetch); err != nil {
		t.Fatal(err)
	}
	if got := atomic.LoadInt32(&calls); got != 1 {
		t.Errorf("fetch called %d times, want 1 (second read should hit cache)", got)
	}
}

func TestInstanceCache_ExpiresAfterTTL(t *testing.T) {
	c := newInstanceCache(10 * time.Second)
	now := time.Unix(0, 0)
	c.now = func() time.Time { return now }
	var calls int32
	fetch := func() ([]Observed, error) { atomic.AddInt32(&calls, 1); return nil, nil }
	_, _ = c.get("h1", fetch)
	now = now.Add(11 * time.Second)
	_, _ = c.get("h1", fetch)
	if got := atomic.LoadInt32(&calls); got != 2 {
		t.Errorf("fetch called %d times, want 2 (entry should expire)", got)
	}
}

func TestInstanceCache_Invalidate(t *testing.T) {
	c := newInstanceCache(time.Minute)
	var calls int32
	fetch := func() ([]Observed, error) { atomic.AddInt32(&calls, 1); return nil, nil }
	_, _ = c.get("h1", fetch)
	c.invalidate("h1")
	_, _ = c.get("h1", fetch)
	if got := atomic.LoadInt32(&calls); got != 2 {
		t.Errorf("fetch called %d times, want 2 (invalidate should force refetch)", got)
	}
}

func TestInstanceCache_ErrorsNotCached(t *testing.T) {
	c := newInstanceCache(time.Minute)
	var calls int32
	fetch := func() ([]Observed, error) {
		atomic.AddInt32(&calls, 1)
		return nil, errors.New("boom")
	}
	_, _ = c.get("h1", fetch)
	_, _ = c.get("h1", fetch)
	if got := atomic.LoadInt32(&calls); got != 2 {
		t.Errorf("fetch called %d times, want 2 (errors must not be cached)", got)
	}
}

func TestInstanceCache_DisabledWhenTTLZero(t *testing.T) {
	c := newInstanceCache(0)
	var calls int32
	fetch := func() ([]Observed, error) { atomic.AddInt32(&calls, 1); return nil, nil }
	_, _ = c.get("h1", fetch)
	_, _ = c.get("h1", fetch)
	if got := atomic.LoadInt32(&calls); got != 2 {
		t.Errorf("fetch called %d times, want 2 (ttl=0 disables caching)", got)
	}
}

func TestInstanceCache_CollapsesConcurrentFetches(t *testing.T) {
	c := newInstanceCache(time.Minute)
	var calls int32
	release := make(chan struct{})
	fetch := func() ([]Observed, error) {
		atomic.AddInt32(&calls, 1)
		<-release // hold all callers inside fetch until released
		return nil, nil
	}
	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); _, _ = c.get("h1", fetch) }()
	}
	time.Sleep(50 * time.Millisecond) // let goroutines arrive at fetch
	close(release)
	wg.Wait()
	if got := atomic.LoadInt32(&calls); got != 1 {
		t.Errorf("fetch called %d times, want 1 (concurrent calls must collapse)", got)
	}
}

// TestInstanceCache_InvalidateDuringInFlightFetchIsNotOverwritten drives the
// race directly: a get() is in the middle of fetch() (started, not yet
// returned) when invalidate() runs concurrently. The fetch's result — which
// is stale relative to the invalidate — must NOT be stored: a subsequent
// get() must trigger a fresh fetch rather than serving the stale value.
func TestInstanceCache_InvalidateDuringInFlightFetchIsNotOverwritten(t *testing.T) {
	c := newInstanceCache(time.Minute)
	var calls int32
	started := make(chan struct{})
	release := make(chan struct{})
	fetch := func() ([]Observed, error) {
		n := atomic.AddInt32(&calls, 1)
		if n == 1 {
			close(started)
			<-release
		}
		return []Observed{{Slug: "stale"}}, nil
	}

	done := make(chan struct{})
	go func() {
		defer close(done)
		_, _ = c.get("h1", fetch)
	}()

	<-started          // fetch #1 is in flight
	c.invalidate("h1") // invalidate races the in-flight fetch
	close(release)     // let fetch #1 finish and attempt to store
	<-done

	// The stale result from fetch #1 must not have been cached: a subsequent
	// get() must re-fetch rather than returning the stale "stale" value.
	if _, err := c.get("h1", fetch); err != nil {
		t.Fatal(err)
	}
	if got := atomic.LoadInt32(&calls); got != 2 {
		t.Errorf("fetch called %d times, want 2 (invalidate-during-fetch must not poison the cache)", got)
	}
}

// TestListAllInstances_CachesAcrossCalls drives ListAllInstances through the
// real per-host cache using the package's existing podman fake (newSvc, see
// service_test.go): a second call within the TTL must not re-sweep PodList,
// and invalidateInstances must force the next call to re-sweep.
func TestListAllInstances_CachesAcrossCalls(t *testing.T) {
	svc, f := newSvc(t)
	svc.SetInstanceCacheTTL(time.Minute)

	ctx := context.Background()
	if _, err := svc.ListAllInstances(ctx, "h1"); err != nil {
		t.Fatal(err)
	}
	before := f.PodListCallCount()
	if _, err := svc.ListAllInstances(ctx, "h1"); err != nil {
		t.Fatal(err)
	}
	if after := f.PodListCallCount(); after != before {
		t.Errorf("PodList called again (%d -> %d); second list should hit cache", before, after)
	}

	svc.invalidateInstances("h1")
	if _, err := svc.ListAllInstances(ctx, "h1"); err != nil {
		t.Fatal(err)
	}
	if after := f.PodListCallCount(); after == before {
		t.Errorf("PodList not called after invalidate; expected a refetch")
	}
}

// newTestServiceWithFakeTemplates builds a Service backed by the package's
// podman fake, seeded with n distinct templates ("tmpl-0".."tmpl-(n-1)"),
// each with exactly one pod on host h1 carrying a distinct slug. Used by the
// parallel-sweep tests below.
func newTestServiceWithFakeTemplates(t *testing.T, n int) (*Service, *fake.Fake) {
	t.Helper()
	hosts := []config.Host{{ID: "h1", Addr: "unix", Socket: "/x"}}
	f := fake.New()
	tmpls := make([]store.Template, 0, n)
	for i := 0; i < n; i++ {
		id := fmt.Sprintf("tmpl-%d", i)
		tmpls = append(tmpls, store.Template{
			Meta: render.Meta{ID: id},
			Body: `apiVersion: v1
kind: Pod
metadata:
  name: ` + id + `
`,
			Origin: "seed",
		})
	}
	svc, _ := newSvcWith(t, f, hosts, tmpls...)
	for i, tm := range tmpls {
		slug := fmt.Sprintf("slug-%d", i)
		f.AddPod("h1", podman.Pod{
			ID:   tm.Meta.ID + "-" + slug,
			Name: tm.Meta.ID + "-" + slug,
			Labels: map[string]string{
				"podman-api/template": tm.Meta.ID,
				"podman-api/slug":     slug,
			},
			Status: "Running",
		})
	}
	return svc, f
}

func TestListAllInstances_ParallelSweepMatchesSerial(t *testing.T) {
	// A fake with N templates, each returning a distinct pod. Result set must be
	// complete and independent of ordering.
	svc, _ := newTestServiceWithFakeTemplates(t, 5) // 5 templates, 1 pod each
	svc.SetInstanceCacheTTL(0)                      // force a live sweep

	obs, err := svc.ListAllInstances(context.Background(), "h1")
	if err != nil {
		t.Fatal(err)
	}
	if len(obs) != 5 {
		t.Fatalf("got %d observed, want 5", len(obs))
	}
	slugs := map[string]bool{}
	for _, o := range obs {
		slugs[o.Slug] = true
	}
	if len(slugs) != 5 {
		t.Errorf("expected 5 distinct slugs, got %d (%v)", len(slugs), slugs)
	}
}

// TestApply_InvalidatesInstanceCache proves the applyLocked funnel drops the
// per-host cache entry so the next ListAllInstances re-sweeps instead of
// serving a stale (pre-Apply) result.
func TestApply_InvalidatesInstanceCache(t *testing.T) {
	svc, f := newSvc(t)
	svc.SetInstanceCacheTTL(time.Minute)
	ctx := context.Background()

	// Prime the cache.
	if _, err := svc.ListAllInstances(ctx, "h1"); err != nil {
		t.Fatal(err)
	}
	before := f.PodListCallCount()

	// A mutation via Apply must invalidate, so the next list re-sweeps.
	if err := svc.Apply(ctx, "h1", pgApply("demo"), ApplyOptions{Replace: true}); err != nil {
		t.Fatalf("apply: %v", err)
	}
	if _, err := svc.ListAllInstances(ctx, "h1"); err != nil {
		t.Fatal(err)
	}
	if f.PodListCallCount() == before {
		t.Error("ListAllInstances hit stale cache after Apply; expected invalidation + refetch")
	}
}

// TestDelete_InvalidatesInstanceCache proves the Delete funnel drops the
// per-host cache entry so the next ListAllInstances re-sweeps instead of
// serving a stale (pre-Delete) result.
func TestDelete_InvalidatesInstanceCache(t *testing.T) {
	svc, f := newSvc(t)
	svc.SetInstanceCacheTTL(time.Minute)
	ctx := context.Background()

	if err := svc.Apply(ctx, "h1", pgApply("demo"), ApplyOptions{Replace: true}); err != nil {
		t.Fatalf("apply: %v", err)
	}

	// Prime the cache post-Apply (Apply itself already invalidates).
	if _, err := svc.ListAllInstances(ctx, "h1"); err != nil {
		t.Fatal(err)
	}
	before := f.PodListCallCount()

	if err := svc.Delete(ctx, "h1", "postgres", "demo", DeleteOptions{}); err != nil {
		t.Fatalf("delete: %v", err)
	}
	if _, err := svc.ListAllInstances(ctx, "h1"); err != nil {
		t.Fatal(err)
	}
	if f.PodListCallCount() == before {
		t.Error("ListAllInstances hit stale cache after Delete; expected invalidation + refetch")
	}
}

// TestRestart_InvalidatesInstanceCache proves the shared lifecycle funnel
// (used by Start/Stop/Restart) drops the per-host cache entry so the next
// ListAllInstances re-sweeps instead of serving stale running-state that
// predates the mutation.
func TestRestart_InvalidatesInstanceCache(t *testing.T) {
	svc, f := newSvc(t)
	svc.SetInstanceCacheTTL(time.Minute)
	ctx := context.Background()

	if err := svc.Apply(ctx, "h1", pgApply("demo"), ApplyOptions{Replace: true}); err != nil {
		t.Fatalf("apply: %v", err)
	}

	// Prime the cache post-Apply (Apply itself already invalidates).
	if _, err := svc.ListAllInstances(ctx, "h1"); err != nil {
		t.Fatal(err)
	}
	before := f.PodListCallCount()

	if err := svc.Restart(ctx, "h1", "postgres", "demo"); err != nil {
		t.Fatalf("restart: %v", err)
	}
	if _, err := svc.ListAllInstances(ctx, "h1"); err != nil {
		t.Fatal(err)
	}
	if f.PodListCallCount() == before {
		t.Error("ListAllInstances hit stale cache after Restart; expected invalidation + refetch")
	}
}

func TestListAllInstances_PropagatesTemplateError(t *testing.T) {
	svc, f := newTestServiceWithFakeTemplates(t, 3)
	svc.SetInstanceCacheTTL(0)
	f.FailPodListFor("tmpl-2", errors.New("podlist boom"))
	if _, err := svc.ListAllInstances(context.Background(), "h1"); err == nil {
		t.Fatal("expected error from failing template, got nil")
	}
}

func TestCacheWarmServesWithoutFetchAndIgnoresTTL(t *testing.T) {
	c := newInstanceCache(3 * time.Second)
	c.setWarm(true)
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	c.now = func() time.Time { return base }

	// Poller populates the entry.
	gen := c.beginRefresh("h")
	c.put("h", gen, []Observed{{Template: "t", Slug: "a"}}, base)

	// A much later read (well past ttl) still serves the warm entry with no fetch.
	c.now = func() time.Time { return base.Add(time.Hour) }
	fetched := false
	obs, fresh, err := c.getWithMeta("h", func() ([]Observed, error) {
		fetched = true
		return nil, nil
	})
	if err != nil {
		t.Fatalf("err = %v", err)
	}
	if fetched {
		t.Fatal("warm hit must not fetch")
	}
	if len(obs) != 1 || obs[0].Slug != "a" {
		t.Fatalf("obs = %+v", obs)
	}
	if !fresh.Reachable || !fresh.HasData || !fresh.FetchedAt.Equal(base) {
		t.Fatalf("fresh = %+v", fresh)
	}
}

func TestCacheColdMissFetchesAndPopulates(t *testing.T) {
	c := newInstanceCache(3 * time.Second)
	c.setWarm(true)
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	c.now = func() time.Time { return base }

	calls := 0
	obs, fresh, err := c.getWithMeta("h", func() ([]Observed, error) {
		calls++
		return []Observed{{Slug: "x"}}, nil
	})
	if err != nil || calls != 1 || len(obs) != 1 {
		t.Fatalf("cold miss: obs=%+v calls=%d err=%v", obs, calls, err)
	}
	if !fresh.HasData || !fresh.Reachable {
		t.Fatalf("fresh = %+v", fresh)
	}
	// Second read is now a warm hit (no fetch).
	_, _, _ = c.getWithMeta("h", func() ([]Observed, error) {
		calls++
		return nil, nil
	})
	if calls != 1 {
		t.Fatalf("expected warm hit, calls=%d", calls)
	}
}

func TestCacheMarkUnreachableKeepsLastKnownGood(t *testing.T) {
	c := newInstanceCache(3 * time.Second)
	c.setWarm(true)
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	c.now = func() time.Time { return base }
	gen := c.beginRefresh("h")
	c.put("h", gen, []Observed{{Slug: "a"}}, base)

	// A later refresh fails.
	gen2 := c.beginRefresh("h")
	c.markUnreachable("h", gen2)

	obs, fresh, err := c.getWithMeta("h", func() ([]Observed, error) {
		t.Fatal("must not fetch when last-known-good exists")
		return nil, nil
	})
	if err != nil {
		t.Fatalf("err = %v", err)
	}
	if len(obs) != 1 || obs[0].Slug != "a" {
		t.Fatalf("obs = %+v (should keep last-known-good)", obs)
	}
	if fresh.Reachable {
		t.Fatal("fresh.Reachable should be false after markUnreachable")
	}
	if !fresh.HasData {
		t.Fatal("fresh.HasData should stay true (we still have data)")
	}
}

func TestCacheColdFetchErrorReturnsErrNoData(t *testing.T) {
	c := newInstanceCache(3 * time.Second)
	c.setWarm(true)
	boom := errors.New("connection refused")
	obs, fresh, err := c.getWithMeta("h", func() ([]Observed, error) {
		return nil, boom
	})
	if !errors.Is(err, boom) {
		t.Fatalf("err = %v, want boom", err)
	}
	if obs != nil || fresh.HasData || fresh.Reachable {
		t.Fatalf("cold error should be empty+unreachable: obs=%+v fresh=%+v", obs, fresh)
	}
}

func TestCachePutSkippedAfterInvalidate(t *testing.T) {
	c := newInstanceCache(3 * time.Second)
	c.setWarm(true)
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	c.now = func() time.Time { return base }

	// Refresh begins, captures gen.
	gen := c.beginRefresh("h")
	// A mutation invalidates before the refresh stores its (now stale) result.
	c.invalidate("h")
	c.put("h", gen, []Observed{{Slug: "stale"}}, base)

	// Entry must be cold (the stale put was dropped), so the next read fetches.
	fetched := false
	c.getWithMeta("h", func() ([]Observed, error) {
		fetched = true
		return []Observed{{Slug: "fresh"}}, nil
	})
	if !fetched {
		t.Fatal("stale put should have been dropped, forcing a cold fetch")
	}
}

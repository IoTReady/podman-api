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

func TestListAllInstances_PropagatesTemplateError(t *testing.T) {
	svc, f := newTestServiceWithFakeTemplates(t, 3)
	svc.SetInstanceCacheTTL(0)
	f.FailPodListFor("tmpl-2", errors.New("podlist boom"))
	if _, err := svc.ListAllInstances(context.Background(), "h1"); err == nil {
		t.Fatal("expected error from failing template, got nil")
	}
}

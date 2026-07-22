package instance

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
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

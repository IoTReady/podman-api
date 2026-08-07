package imgregistry

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// countingClient is a fake inner Client that counts Tags() calls and either
// returns a fixed result or a fixed error, so tests can observe exactly how
// many times the decorator reaches through to it.
type countingClient struct {
	tagsCalls   atomic.Int32
	deleteCalls atomic.Int32
	groups      []TagGroup
	err         error
	deleteErr   error
}

func (c *countingClient) Catalog(ctx context.Context) ([]string, error) { return nil, nil }
func (c *countingClient) TagCount(ctx context.Context, repo string) (int, error) {
	return 0, nil
}
func (c *countingClient) Manifest(ctx context.Context, repo, ref string) (Manifest, error) {
	return Manifest{}, nil
}
func (c *countingClient) Tags(ctx context.Context, repo string) ([]TagGroup, error) {
	c.tagsCalls.Add(1)
	if c.err != nil {
		return nil, c.err
	}
	return c.groups, nil
}
func (c *countingClient) Delete(ctx context.Context, repo, digest string) error {
	c.deleteCalls.Add(1)
	return c.deleteErr
}

func TestCachingClient_Tags_SecondCallHitsCache(t *testing.T) {
	inner := &countingClient{groups: []TagGroup{{Digest: "sha256:a", Tags: []string{"v1"}}}}
	cc := NewCachingClient(inner, time.Minute)

	if _, err := cc.Tags(context.Background(), "engine"); err != nil {
		t.Fatalf("first Tags: %v", err)
	}
	if _, err := cc.Tags(context.Background(), "engine"); err != nil {
		t.Fatalf("second Tags: %v", err)
	}
	if n := inner.tagsCalls.Load(); n != 1 {
		t.Fatalf("inner Tags called %d times, want 1 (second call should be served from cache)", n)
	}
}

func TestCachingClient_Tags_ExpiredEntryRefetches(t *testing.T) {
	inner := &countingClient{groups: []TagGroup{{Digest: "sha256:a"}}}
	cc := NewCachingClient(inner, time.Millisecond)

	if _, err := cc.Tags(context.Background(), "engine"); err != nil {
		t.Fatalf("first Tags: %v", err)
	}
	time.Sleep(5 * time.Millisecond)
	if _, err := cc.Tags(context.Background(), "engine"); err != nil {
		t.Fatalf("second Tags: %v", err)
	}
	if n := inner.tagsCalls.Load(); n != 2 {
		t.Fatalf("inner Tags called %d times, want 2 (ttl should have expired)", n)
	}
}

func TestCachingClient_Tags_ErrorNotCached(t *testing.T) {
	inner := &countingClient{err: fmt.Errorf("registry blip: %w", ErrUnreachable)}
	cc := NewCachingClient(inner, time.Minute)

	if _, err := cc.Tags(context.Background(), "engine"); !errors.Is(err, ErrUnreachable) {
		t.Fatalf("first Tags: got err %v, want ErrUnreachable", err)
	}
	if _, err := cc.Tags(context.Background(), "engine"); !errors.Is(err, ErrUnreachable) {
		t.Fatalf("second Tags: got err %v, want ErrUnreachable", err)
	}
	if n := inner.tagsCalls.Load(); n != 2 {
		t.Fatalf("inner Tags called %d times, want 2 (an error must never be cached)", n)
	}
}

func TestCachingClient_Tags_ConcurrentAccessIsRaceClean(t *testing.T) {
	inner := &countingClient{groups: []TagGroup{{Digest: "sha256:a", Tags: []string{"v1"}}}}
	cc := NewCachingClient(inner, 10*time.Millisecond)

	var wg sync.WaitGroup
	repos := []string{"engine", "otp", "qabazaar"}
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			repo := repos[i%len(repos)]
			if _, err := cc.Tags(context.Background(), repo); err != nil {
				t.Errorf("Tags(%s): %v", repo, err)
			}
		}(i)
	}
	wg.Wait()
}

func TestCachingClient_PassesThroughOtherMethods(t *testing.T) {
	inner := &countingClient{}
	cc := NewCachingClient(inner, time.Minute)
	if _, err := cc.Catalog(context.Background()); err != nil {
		t.Fatalf("Catalog: %v", err)
	}
	if _, err := cc.TagCount(context.Background(), "engine"); err != nil {
		t.Fatalf("TagCount: %v", err)
	}
	if _, err := cc.Manifest(context.Background(), "engine", "sha256:a"); err != nil {
		t.Fatalf("Manifest: %v", err)
	}
}

// TestCachingClient_Delete_DelegatesToInner asserts Delete is a pure
// passthrough to the inner client — the decorator must not swallow or alter
// the inner call's own error handling (ErrNotFound vs ErrUnreachable).
func TestCachingClient_Delete_DelegatesToInner(t *testing.T) {
	inner := &countingClient{}
	cc := NewCachingClient(inner, time.Minute)
	if err := cc.Delete(context.Background(), "engine", "sha256:a"); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if n := inner.deleteCalls.Load(); n != 1 {
		t.Fatalf("inner Delete called %d times, want 1", n)
	}
}

func TestCachingClient_Delete_ErrorPropagatesAndSkipsInvalidation(t *testing.T) {
	inner := &countingClient{
		groups:    []TagGroup{{Digest: "sha256:a", Tags: []string{"v1"}}},
		deleteErr: fmt.Errorf("boom: %w", ErrUnreachable),
	}
	cc := NewCachingClient(inner, time.Minute)

	// Populate the cache first.
	if _, err := cc.Tags(context.Background(), "engine"); err != nil {
		t.Fatalf("Tags: %v", err)
	}

	err := cc.Delete(context.Background(), "engine", "sha256:a")
	if !errors.Is(err, ErrUnreachable) {
		t.Fatalf("expected errors.Is(err, ErrUnreachable), got %v", err)
	}

	// A failed delete must not invalidate the cache — nothing was actually
	// removed at the registry.
	if _, err := cc.Tags(context.Background(), "engine"); err != nil {
		t.Fatalf("Tags after failed Delete: %v", err)
	}
	if n := inner.tagsCalls.Load(); n != 1 {
		t.Fatalf("inner Tags called %d times, want 1 (cache should still be warm after a failed Delete)", n)
	}
}

// TestCachingClient_Delete_InvalidatesTagsCache is the requirement this
// exists for: a stale cached listing after a successful delete would make
// the next run reason from a tag that no longer exists on the registry.
func TestCachingClient_Delete_InvalidatesTagsCache(t *testing.T) {
	inner := &countingClient{groups: []TagGroup{{Digest: "sha256:a", Tags: []string{"v1"}}}}
	cc := NewCachingClient(inner, time.Minute)

	// Populate the cache.
	if _, err := cc.Tags(context.Background(), "engine"); err != nil {
		t.Fatalf("Tags (populate): %v", err)
	}
	if n := inner.tagsCalls.Load(); n != 1 {
		t.Fatalf("inner Tags called %d times, want 1 before Delete", n)
	}

	if err := cc.Delete(context.Background(), "engine", "sha256:a"); err != nil {
		t.Fatalf("Delete: %v", err)
	}

	// The next Tags() must re-fetch, not serve the pre-delete cached entry.
	if _, err := cc.Tags(context.Background(), "engine"); err != nil {
		t.Fatalf("Tags (after delete): %v", err)
	}
	if n := inner.tagsCalls.Load(); n != 2 {
		t.Fatalf("inner Tags called %d times, want 2 (Delete must invalidate the cached Tags entry)", n)
	}
}

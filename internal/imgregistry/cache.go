package imgregistry

import (
	"context"
	"sync"
	"time"
)

// TagsCacheTTL bounds how long a CachingClient serves a repo's Tags() result
// before re-resolving it. Registry contents only change on push (a human or
// CI action), never spontaneously, so a few minutes of staleness is harmless
// — an operator who just pushed a new tag and immediately checks the UI sees
// it within this window, not never. 5 minutes is picked to make the common
// case (browsing the list, clicking into a couple of repos, going back) free
// after the first view, while still bounding how long a since-deleted tag
// would linger in the UI.
const TagsCacheTTL = 5 * time.Minute

// tagsCacheEntry is one repo's cached listing. It stores the whole
// TagListing, drop evidence included: caching only the groups would serve a
// degraded listing for the rest of the TTL as if it were complete, which is
// exactly the silent partial view TagListing.Dropped exists to prevent. Tags
// and ResolveTags share this one entry, so a repo warmed by the UI cannot
// then hand a pruning caller a listing with its evidence stripped.
type tagsCacheEntry struct {
	listing TagListing
	expires time.Time
}

// cloneListing deep-copies the slices so nothing outside this type can mutate
// a cached listing through an alias.
func cloneListing(l TagListing) TagListing {
	return TagListing{
		Groups:  append([]TagGroup(nil), l.Groups...),
		Dropped: append([]DroppedTag(nil), l.Dropped...),
	}
}

// CachingClient wraps a Client and caches its Tags() results for ttl, since
// Tags is the one call expensive enough to matter — for the fleet's "engine"
// repo, ~1000 manifest GETs plus 737 config-blob GETs, ~21s on a cold cache.
// The list page (internal/ui/handlers_registry.go) fires this once per
// catalog repo, concurrently, on every single page view — without a cache
// that's the full per-repo cost paid ~26 times on every visit. With it, only
// the first visit (or the first visit after ttl) pays that cost; every
// revisit inside the window is instant.
//
// It is a decorator, not a change to HTTPClient itself: HTTPClient stays a
// pure client with no caching semantics of its own, the cache is
// independently testable against a fake inner Client, and a caller that
// wants uncached behavior (e.g. a future admin "force refresh" action, or a
// test) can simply hold the inner Client instead.
//
// Catalog, TagCount, and Manifest are passed straight through uncached:
// Catalog and TagCount are already cheap (a single, unresolved tags/catalog
// list call), and Manifest is one lookup for one digest — caching it would
// buy little while adding another thing that could go stale.
//
// Only a successful Tags() result is cached (see Tags below) — a transient
// registry blip must not poison every viewer's list page for the rest of the
// TTL.
type CachingClient struct {
	inner Client
	ttl   time.Duration

	mu    sync.RWMutex
	cache map[string]tagsCacheEntry
}

// NewCachingClient wraps inner in a Tags()-caching decorator with the given
// ttl (TagsCacheTTL in production; tests pass a short one to observe
// expiry).
func NewCachingClient(inner Client, ttl time.Duration) *CachingClient {
	return &CachingClient{
		inner: inner,
		ttl:   ttl,
		cache: map[string]tagsCacheEntry{},
	}
}

func (c *CachingClient) Catalog(ctx context.Context) ([]string, error) {
	return c.inner.Catalog(ctx)
}

func (c *CachingClient) TagCount(ctx context.Context, repo string) (int, error) {
	return c.inner.TagCount(ctx, repo)
}

func (c *CachingClient) Manifest(ctx context.Context, repo, ref string) (Manifest, error) {
	return c.inner.Manifest(ctx, repo, ref)
}

// Delete delegates to the inner Client and, only on success, invalidates
// repo's cached Tags() entry — a stale listing after a delete would let a
// subsequent run reason from a tag that no longer exists on the registry. A
// failed delete leaves the cache untouched, since nothing was actually
// removed.
func (c *CachingClient) Delete(ctx context.Context, repo, digest string) error {
	if err := c.inner.Delete(ctx, repo, digest); err != nil {
		return err
	}
	c.mu.Lock()
	delete(c.cache, repo)
	c.mu.Unlock()
	return nil
}

// Tags returns repo's cached TagGroups if present and not yet expired,
// otherwise resolves them via the inner Client and, only on success, stores
// the result. Safe for concurrent use — the list page fans this out across
// ~26 goroutines on a single page load.
func (c *CachingClient) Tags(ctx context.Context, repo string) ([]TagGroup, error) {
	listing, err := c.ResolveTags(ctx, repo)
	if err != nil {
		return nil, err
	}
	return listing.Groups, nil
}

// ResolveTags returns repo's cached listing if present and not yet expired,
// otherwise resolves it via the inner Client and, only on success, stores the
// result. Safe for concurrent use — the list page fans this out across ~26
// goroutines on a single page load.
func (c *CachingClient) ResolveTags(ctx context.Context, repo string) (TagListing, error) {
	c.mu.RLock()
	entry, ok := c.cache[repo]
	c.mu.RUnlock()
	if ok && time.Now().Before(entry.expires) {
		return cloneListing(entry.listing), nil
	}

	listing, err := c.inner.ResolveTags(ctx, repo)
	if err != nil {
		// Never cache an error: a transient registry blip must not wedge
		// every viewer's page at "unreachable" for the rest of the TTL.
		return TagListing{}, err
	}

	stored := cloneListing(listing)
	c.mu.Lock()
	c.cache[repo] = tagsCacheEntry{listing: stored, expires: time.Now().Add(c.ttl)}
	c.mu.Unlock()
	return cloneListing(stored), nil
}

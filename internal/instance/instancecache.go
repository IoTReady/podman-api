package instance

import (
	"sync"
	"time"
)

// instanceCache is a per-host cache over the (expensive) live podman sweep done
// by ListAllInstances. Two modes:
//
//   - lazy (default): entries expire after ttl; a stale read re-fetches. This is
//     the behaviour when no inventory poller runs (ttl>0) or caching is off
//     (ttl==0).
//   - warm (setWarm(true)): entries never expire on age — a background poller
//     keeps them fresh via put(); reads always serve the entry when present. A
//     failed refresh marks the entry unreachable but keeps its last-known-good
//     data. Cold misses still fetch synchronously (deduped) so the very first
//     read per host, or a read right after invalidate(), returns real data.
//
// Concurrent misses for the same host collapse into a single fetch (in-flight
// de-dup). Cached slices are treated read-only by callers.
type instanceCache struct {
	ttl  time.Duration
	warm bool
	now  func() time.Time

	mu       sync.Mutex
	data     map[string]instEntry
	inflight map[string]*instCall
	gen      map[string]uint64
}

type instEntry struct {
	obs       []Observed
	fetchedAt time.Time
	reachable bool
	hasData   bool
}

type instCall struct {
	wg    sync.WaitGroup
	obs   []Observed
	fresh Freshness
	err   error
}

// Freshness describes how current a returned inventory snapshot is. HasData is
// false only on a cold, never-populated (or failed cold) host; Reachable is
// false when the most recent refresh failed but stale data is still being
// served.
type Freshness struct {
	FetchedAt time.Time
	Reachable bool
	HasData   bool
}

// Age is how long ago the served snapshot was captured. Zero when there is no
// data.
func (f Freshness) Age() time.Duration {
	if f.FetchedAt.IsZero() {
		return 0
	}
	return time.Since(f.FetchedAt)
}

func newInstanceCache(ttl time.Duration) *instanceCache {
	return &instanceCache{
		ttl:      ttl,
		now:      time.Now,
		data:     make(map[string]instEntry),
		inflight: make(map[string]*instCall),
		gen:      make(map[string]uint64),
	}
}

// setWarm switches the cache into warm mode (see type doc). Called once at
// startup when the inventory poller is enabled, before traffic is served.
func (c *instanceCache) setWarm(v bool) {
	c.mu.Lock()
	c.warm = v
	c.mu.Unlock()
}

// get is the legacy read path (backward compatibility for old tests). It calls
// getWithMeta and discards the freshness metadata.
func (c *instanceCache) get(host string, fetch func() ([]Observed, error)) ([]Observed, error) {
	obs, _, err := c.getWithMeta(host, fetch)
	return obs, err
}

// getWithMeta is the cache-always read path. A present entry is returned
// directly when warm, or when still within ttl (lazy fallback). On a cold miss
// it performs the blocking fetch, de-duplicating concurrent callers, stores a
// successful result, and returns freshness metadata.
func (c *instanceCache) getWithMeta(host string, fetch func() ([]Observed, error)) ([]Observed, Freshness, error) {
	c.mu.Lock()
	if e, ok := c.data[host]; ok && (c.warm || (c.ttl > 0 && c.now().Sub(e.fetchedAt) < c.ttl)) {
		c.mu.Unlock()
		return e.obs, Freshness{FetchedAt: e.fetchedAt, Reachable: e.reachable, HasData: e.hasData}, nil
	}
	if call, ok := c.inflight[host]; ok {
		c.mu.Unlock()
		call.wg.Wait()
		return call.obs, call.fresh, call.err
	}
	call := &instCall{}
	call.wg.Add(1)
	c.inflight[host] = call
	gen := c.gen[host]
	c.mu.Unlock()

	obs, err := fetch()

	c.mu.Lock()
	delete(c.inflight, host)
	now := c.now()
	if err == nil && c.gen[host] == gen {
		c.data[host] = instEntry{obs: obs, fetchedAt: now, reachable: true, hasData: true}
	}
	call.obs = obs
	call.err = err
	if err == nil {
		call.fresh = Freshness{FetchedAt: now, Reachable: true, HasData: true}
	} else {
		call.fresh = Freshness{Reachable: false, HasData: false}
	}
	c.mu.Unlock()
	call.wg.Done()
	return call.obs, call.fresh, call.err
}

// beginRefresh captures the current generation for a host so a following put or
// markUnreachable can detect an invalidate() that raced with the live fetch.
func (c *instanceCache) beginRefresh(host string) uint64 {
	c.mu.Lock()
	g := c.gen[host]
	c.mu.Unlock()
	return g
}

// put stores a successful background refresh, guarded by gen so a refresh that
// raced with an invalidate() is dropped (read-your-writes for the mutator).
func (c *instanceCache) put(host string, gen uint64, obs []Observed, at time.Time) {
	c.mu.Lock()
	if c.gen[host] == gen {
		c.data[host] = instEntry{obs: obs, fetchedAt: at, reachable: true, hasData: true}
	}
	c.mu.Unlock()
}

// markUnreachable flags the host's last-known-good entry as stale after a failed
// refresh, preserving its data so reads still serve it. No-op when the entry is
// gone (cold — nothing to serve) or a mutation intervened (gen changed).
func (c *instanceCache) markUnreachable(host string, gen uint64) {
	c.mu.Lock()
	if e, ok := c.data[host]; ok && c.gen[host] == gen {
		e.reachable = false
		c.data[host] = e
	}
	c.mu.Unlock()
}

// invalidate drops the cached instance list for a host so the next read
// re-sweeps, and bumps the generation so an in-flight refresh's put is dropped.
func (c *instanceCache) invalidate(host string) {
	c.mu.Lock()
	delete(c.data, host)
	c.gen[host]++
	c.mu.Unlock()
}

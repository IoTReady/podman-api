package instance

import (
	"sync"
	"time"
)

// instanceCache is a per-host, short-TTL cache over the (expensive) live podman
// sweep done by ListAllInstances. Concurrent misses for the same host collapse
// into a single fetch (in-flight de-dup). Errors are never cached. A ttl of 0
// disables caching entirely (every get calls fetch), while still collapsing
// concurrent in-flight calls. Cached slices are treated read-only by callers.
type instanceCache struct {
	ttl time.Duration
	now func() time.Time

	mu       sync.Mutex
	data     map[string]instEntry
	inflight map[string]*instCall
	gen      map[string]uint64
}

type instEntry struct {
	obs       []Observed
	fetchedAt time.Time
}

type instCall struct {
	wg  sync.WaitGroup
	obs []Observed
	err error
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

func (c *instanceCache) get(host string, fetch func() ([]Observed, error)) ([]Observed, error) {
	c.mu.Lock()
	// Fresh cache hit.
	if c.ttl > 0 {
		if e, ok := c.data[host]; ok && c.now().Sub(e.fetchedAt) < c.ttl {
			c.mu.Unlock()
			return e.obs, nil
		}
	}
	// Join an in-flight fetch for the same host.
	if call, ok := c.inflight[host]; ok {
		c.mu.Unlock()
		call.wg.Wait()
		return call.obs, call.err
	}
	// Become the fetcher. Capture the current generation for host before
	// releasing the lock: if invalidate(host) runs while fetch() is in
	// flight, it bumps the generation, and the store below (which re-checks
	// the generation under the lock) will skip writing our now-stale result.
	call := &instCall{}
	call.wg.Add(1)
	c.inflight[host] = call
	gen := c.gen[host]
	c.mu.Unlock()

	call.obs, call.err = fetch()

	c.mu.Lock()
	delete(c.inflight, host)
	if call.err == nil && c.ttl > 0 && c.gen[host] == gen {
		c.data[host] = instEntry{obs: call.obs, fetchedAt: c.now()}
	}
	c.mu.Unlock()
	call.wg.Done()
	return call.obs, call.err
}

func (c *instanceCache) invalidate(host string) {
	c.mu.Lock()
	delete(c.data, host)
	c.gen[host]++
	c.mu.Unlock()
}

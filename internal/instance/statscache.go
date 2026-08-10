package instance

import (
	"sync"
	"time"

	"github.com/iotready/podman-api/internal/podman"
)

// HostStats is one host's most recent container resource sample, keyed by
// container name. Read by the metrics collector at scrape time.
type HostStats struct {
	Containers map[string]podman.ContainerStats
	FetchedAt  time.Time
}

// HostVolumeUsage is one host's most recent volume sizing, keyed by volume name.
type HostVolumeUsage struct {
	Sizes     map[string]int64
	FetchedAt time.Time
}

// statsCache holds the latest container sample per host.
//
// A failed refresh DROPS the host's entry rather than keeping the last one.
// These are cumulative counters: a frozen value reads as a container that went
// idle, which is worse than an absent series. Volume usage takes the opposite
// choice — see volumeUsageCache.
type statsCache struct {
	mu   sync.Mutex
	data map[string]HostStats
}

func newStatsCache() *statsCache {
	return &statsCache{data: map[string]HostStats{}}
}

func (c *statsCache) put(host string, s HostStats) {
	c.mu.Lock()
	c.data[host] = s
	c.mu.Unlock()
}

func (c *statsCache) drop(host string) {
	c.mu.Lock()
	delete(c.data, host)
	c.mu.Unlock()
}

func (c *statsCache) snapshot() map[string]HostStats {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make(map[string]HostStats, len(c.data))
	for h, s := range c.data {
		out[h] = s
	}
	return out
}

// volumeUsageCache holds the latest volume sizing per host.
//
// Unlike statsCache it KEEPS the last value through a failed walk: sizes change
// slowly, the walk is hourly, and podman_api_volume_usage_age_seconds already
// reports how stale the figure is. Dropping would punch hour-wide holes in a
// gauge for one transient error.
type volumeUsageCache struct {
	mu   sync.Mutex
	data map[string]HostVolumeUsage
}

func newVolumeUsageCache() *volumeUsageCache {
	return &volumeUsageCache{data: map[string]HostVolumeUsage{}}
}

func (c *volumeUsageCache) put(host string, u HostVolumeUsage) {
	c.mu.Lock()
	c.data[host] = u
	c.mu.Unlock()
}

// drop evicts one host's sizing outright. Unlike statsCache.drop this is NOT
// used on a failed walk (see the type comment — a transient error must not
// punch an hour-wide hole in the gauge); it exists for the case where the host
// id itself has gone away, e.g. a rename, where keeping the entry would export
// a series under an id that no longer exists forever.
func (c *volumeUsageCache) drop(host string) {
	c.mu.Lock()
	delete(c.data, host)
	c.mu.Unlock()
}

func (c *volumeUsageCache) snapshot() map[string]HostVolumeUsage {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make(map[string]HostVolumeUsage, len(c.data))
	for h, u := range c.data {
		out[h] = u
	}
	return out
}

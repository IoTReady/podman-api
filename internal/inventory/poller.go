// Package inventory keeps each host's cached instance inventory warm by
// refreshing it on a schedule, so UI/API reads are served without a live podman
// sweep and an unreachable host never stalls a request.
package inventory

import (
	"context"
	"log"
	"sync"
	"time"
)

// Refresher refreshes one host's cached inventory. Implemented by
// *instance.Service.RefreshHost.
type Refresher interface {
	RefreshHost(ctx context.Context, host string) error
}

// StatsRefresher samples one host's container resource usage.
// Implemented by *instance.Service.RefreshHostStats.
type StatsRefresher interface {
	RefreshHostStats(ctx context.Context, host string) error
}

// VolumeUsageRefresher sizes one host's volumes.
// Implemented by *instance.Service.RefreshHostVolumeUsage.
type VolumeUsageRefresher interface {
	RefreshHostVolumeUsage(ctx context.Context, host string) error
}

// Poller periodically refreshes every host's inventory into the service cache.
// It mirrors internal/prune.Scheduler: an immediate first pass so a fresh start
// warms within one cycle, per-tick panic recovery, a per-host timeout so one
// hung host can't bleed into the next cycle, and Wait() for clean shutdown.
type Poller struct {
	Svc      Refresher
	Interval time.Duration
	Timeout  time.Duration

	// Stats, when non-nil, samples container resource usage on every tick,
	// alongside the inventory refresh. Its failures are logged and otherwise
	// ignored: reachability is the inventory refresh's to decide, and the
	// Grafana alert rules all gate on podman_api_host_reachable.
	Stats StatsRefresher

	mu         sync.Mutex
	state      map[string]bool // host -> last-known reachable, for transition logging
	statsState map[string]bool // host -> last-known stats-sampler outcome (never reachability)
	wg         sync.WaitGroup
}

// Start launches the ticker loop until ctx is cancelled. hostsFn returns the
// current host ids on each tick (so SIGHUP host reloads are picked up).
func (p *Poller) Start(ctx context.Context, hostsFn func() []string) {
	p.mu.Lock()
	if p.state == nil {
		p.state = map[string]bool{}
	}
	if p.statsState == nil {
		p.statsState = map[string]bool{}
	}
	p.mu.Unlock()

	p.wg.Add(1)
	go func() {
		defer p.wg.Done()
		t := time.NewTicker(p.Interval)
		defer t.Stop()
		runTick := func() {
			defer func() {
				if r := recover(); r != nil {
					log.Printf("inventory: poller tick panicked: %v", r)
				}
			}()
			p.tick(ctx, hostsFn())
		}
		runTick() // prompt first pass so a fresh start doesn't wait a full tick
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				runTick()
			}
		}
	}()
}

// Wait blocks until the poller goroutine has exited after ctx cancellation.
func (p *Poller) Wait() { p.wg.Wait() }

// tick refreshes every host concurrently, each under its own timeout.
func (p *Poller) tick(ctx context.Context, hosts []string) {
	var wg sync.WaitGroup
	for _, h := range hosts {
		wg.Add(1)
		go func(host string) {
			defer wg.Done()
			hctx, cancel := context.WithTimeout(ctx, p.Timeout)
			defer cancel()
			err := p.Svc.RefreshHost(hctx, host)
			p.logTransition(host, err)
			// Sampled under its own timeout and its own state map: whatever it
			// does, the reachability verdict above is already final.
			//
			// Skipped when the refresh itself failed: a host that just timed out
			// has no stats to hand over, and calling anyway would spend a second
			// full Timeout on it — doubling a hung host's cost per tick, which
			// (since tick blocks the ticker) stretches every healthy host's
			// cadence and inflates podman_api_inventory_age_seconds fleet-wide.
			// statsState is deliberately left as-is across the skip: no sample
			// was attempted, so there is no outcome to record, and keeping the
			// last real one means recovery logs the true transition instead of
			// a spurious one.
			if p.Stats != nil && err == nil {
				sctx, scancel := context.WithTimeout(ctx, p.Timeout)
				p.logStatsTransition(host, p.Stats.RefreshHostStats(sctx, host))
				scancel()
			}
		}(h)
	}
	wg.Wait()
	p.pruneState(hosts)
}

// pruneState drops transition-log state for hosts no longer in the active set
// (e.g. removed via SIGHUP), so the map can't grow unbounded over host churn.
func (p *Poller) pruneState(hosts []string) {
	keep := make(map[string]bool, len(hosts))
	for _, h := range hosts {
		keep[h] = true
	}
	p.mu.Lock()
	for h := range p.state {
		if !keep[h] {
			delete(p.state, h)
		}
	}
	for h := range p.statsState {
		if !keep[h] {
			delete(p.statsState, h)
		}
	}
	p.mu.Unlock()
}

// logTransition logs only when a host's reachability changes (or is first seen
// unreachable), so a persistently-down host doesn't flood the log every tick.
func (p *Poller) logTransition(host string, err error) {
	reachable := err == nil
	p.mu.Lock()
	prev, seen := p.state[host]
	p.state[host] = reachable
	p.mu.Unlock()

	switch {
	case !seen && !reachable:
		log.Printf("inventory: host %s unreachable: %v", host, err)
	case seen && prev && !reachable:
		log.Printf("inventory: host %s unreachable: %v", host, err)
	case seen && !prev && reachable:
		log.Printf("inventory: host %s reachable again", host)
	}
}

// logStatsTransition logs stats-sampler failures only when the outcome changes,
// mirroring logTransition. Kept separate from the inventory state so a stats
// failure can never be mistaken for — or influence — host reachability.
func (p *Poller) logStatsTransition(host string, err error) {
	ok := err == nil
	p.mu.Lock()
	prev, seen := p.statsState[host]
	p.statsState[host] = ok
	p.mu.Unlock()

	switch {
	case !seen && !ok:
		log.Printf("inventory: host %s container stats unavailable: %v", host, err)
	case seen && prev && !ok:
		log.Printf("inventory: host %s container stats unavailable: %v", host, err)
	case seen && !prev && ok:
		log.Printf("inventory: host %s container stats available again", host)
	}
}

// StartVolumeUsage runs a separate, much slower loop that sizes each host's
// volumes. It is deliberately NOT part of tick(): podman's system df walks the
// whole store and can take minutes, so it must never delay an inventory
// refresh. Failures leave the previous sizing in place; staleness surfaces as
// podman_api_volume_usage_age_seconds rather than as a gap. It touches no
// reachability state, for the same reason the stats sampler doesn't.
func (p *Poller) StartVolumeUsage(ctx context.Context, hostsFn func() []string, r VolumeUsageRefresher, interval, timeout time.Duration) {
	if r == nil || interval <= 0 {
		return
	}
	p.wg.Add(1)
	go func() {
		defer p.wg.Done()
		t := time.NewTicker(interval)
		defer t.Stop()
		run := func() {
			defer func() {
				if rec := recover(); rec != nil {
					log.Printf("inventory: volume usage tick panicked: %v", rec)
				}
			}()
			var wg sync.WaitGroup
			for _, h := range hostsFn() {
				wg.Add(1)
				go func(host string) {
					defer wg.Done()
					hctx, cancel := context.WithTimeout(ctx, timeout)
					defer cancel()
					if err := r.RefreshHostVolumeUsage(hctx, host); err != nil {
						log.Printf("inventory: host %s volume usage walk failed: %v", host, err)
					}
				}(h)
			}
			wg.Wait()
		}
		run() // prompt first walk so the metric isn't absent for a full interval
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				run()
			}
		}
	}()
}

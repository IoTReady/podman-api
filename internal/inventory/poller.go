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

// StatsRefresher samples one host's container resource usage, or retires the
// samples it already holds. Implemented by *instance.Service.
//
// Both methods are required, not optional: a sampler that can be skipped but
// not retired freezes a down host's series at their last value. Keeping them on
// one interface makes that a compile error rather than a silent metrics bug.
type StatsRefresher interface {
	RefreshHostStats(ctx context.Context, host string) error
	// DropHostStats discards the host's cached samples. Called when the host is
	// not going to be sampled at all. No context: it does no I/O.
	DropHostStats(host string)
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

	// StatsTimeout bounds one host's stats sample, independently of Timeout.
	// Zero means defaultStatsTimeout — never "no timeout", so a caller that
	// forgets to set it cannot hand the sampler an unbounded call. See tick for
	// why this is a separate budget rather than a share of Timeout (#212).
	StatsTimeout time.Duration

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
			// Sampled under its own state map — whatever it does, the
			// reachability verdict above is already final — and under its own
			// StatsTimeout derived from ctx, NOT from the refresh's hctx.
			//
			// It used to share hctx, to keep the per-host bound at exactly
			// Timeout rather than Timeout+StatsTimeout, since tick blocks the
			// ticker and a host's total per-tick cost overrunning Interval
			// stretches every other host's cadence and inflates
			// podman_api_inventory_age_seconds fleet-wide. In production that
			// traded a bounded cadence for no metrics at all: on the busiest
			// host a 29-template sweep consumed nearly all of the 20s budget,
			// the refresh still returned success so the guard below passed, and
			// the stats call — 92ms of work — was cancelled the instant it
			// started by the already-exhausted parent. Not a sawtooth:
			// permanent starvation, 115 of ~190 containers with no resource
			// series at all (#212).
			//
			// So the sampler gets its own small budget, and the cadence
			// invariant it used to guarantee structurally is now checked and
			// warned about at startup instead (server.statsBudgetWarning):
			// -inventory-refresh-timeout + -container-stats-timeout must stay
			// under -inventory-refresh-interval. At the defaults that is
			// 20s + 5s < 30s.
			//
			// A host that just failed its refresh is not sampled at all: it is
			// unreachable, so the sample would fail too — now spending a fresh
			// StatsTimeout to find that out — and would only log a second
			// failure for something reachability has already reported.
			//
			// But skipping is not the same as not caring: the cached samples
			// must still be retired. A host that is merely unreachable is still in
			// the configured host list, which is what the collector enumerates,
			// and the inventory still holds its last-known containers to supply
			// the join keys — so without the drop every scrape would re-emit that
			// last sample for as long as the host stays down. Cumulative counters
			// that stop advancing read as an idle container; absent is the honest
			// answer.
			//
			// statsState is deliberately left as-is either way: no sample was
			// attempted, so there is no outcome to record, and keeping the last
			// real one means recovery logs the true transition, not a spurious
			// one.
			if p.Stats != nil {
				if err != nil {
					p.Stats.DropHostStats(host)
				} else {
					sctx, scancel := context.WithTimeout(ctx, p.statsTimeout())
					p.logStatsTransition(host, p.Stats.RefreshHostStats(sctx, host))
					scancel()
				}
			}
		}(h)
	}
	wg.Wait()
	p.pruneState(hosts)
}

// defaultStatsTimeout bounds one stats sample when StatsTimeout is unset. The
// call itself measures ~50-100ms even on a 115-container host, so this is
// generous headroom rather than a tuned value; the server's
// -container-stats-timeout flag carries the same default.
const defaultStatsTimeout = 5 * time.Second

// EffectiveStatsTimeout maps a configured stats timeout to the value the poller
// will actually spend: anything <= 0 means defaultStatsTimeout, never "no
// timeout" (tick blocks the ticker, so an unbounded stats call would hang the
// whole poll loop) and never "expire instantly".
//
// Exported because the zero-defaulting is not an internal detail: anything
// reasoning about the per-host budget from the outside — server's startup check
// that Timeout+StatsTimeout stays under Interval — has to reason about the same
// effective value, or a bare -container-stats-timeout=0 passes a check the
// poller then violates by 5s.
func EffectiveStatsTimeout(d time.Duration) time.Duration {
	if d <= 0 {
		return defaultStatsTimeout
	}
	return d
}

// statsTimeout is StatsTimeout normalised through EffectiveStatsTimeout.
func (p *Poller) statsTimeout() time.Duration { return EffectiveStatsTimeout(p.StatsTimeout) }

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
//
// Sampling has its own budget (StatsTimeout, see tick), so these messages no
// longer track the inventory refresh's leftover time — until #212 they did, and
// a host whose refresh chronically ate most of Timeout would flip its outcome
// tick to tick, or on the worst host never sample at all. A failure now means
// the stats call itself exceeded StatsTimeout or the host errored, and the lever
// is -container-stats-timeout, not -inventory-refresh-timeout. The metrics stay
// honest either way: a missed sample renders absent, never stale.
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
					// ctx.Err(), NOT hctx.Err(): a genuine per-host timeout must
					// still log. Only shutdown is silent — a SIGTERM landing
					// inside a walk (a 5m timeout on an hourly cadence, plus the
					// immediate startup walk, gives it a real window) would
					// otherwise make "volume usage walk failed: context
					// canceled", once per host, the last thing in the log.
					if err := r.RefreshHostVolumeUsage(hctx, host); err != nil && ctx.Err() == nil {
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

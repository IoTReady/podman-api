// Package inventory keeps each host's cached instance inventory warm by
// refreshing it on a schedule, so UI/API reads are served without a live podman
// sweep and an unreachable host never stalls a request.
package inventory

import (
	"context"
	"fmt"
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

// LoadAvgRefresher samples one host's /proc/loadavg into the podman client's
// cache. Implemented by *instance.Service.RefreshHostLoadAvg.
//
// This exists because the read is an SSH round trip on a remote host, and
// paying for it at the tail of a per-host request budget meant it was reliably
// the thing that got starved — silently, since the metric renders by omission
// (#258). There is nothing to retire on failure, unlike the stats sampler: the
// cache entry expires on age, so a host that stops being sampled stops
// reporting loadavg rather than repeating its last value forever.
type LoadAvgRefresher interface {
	// sampled is false when no read was attempted against the host as it is
	// currently configured — it was removed or re-addressed mid-read, or
	// another caller held the per-host read lock and this call ran out of
	// budget waiting. That is not an outcome, and recording it as one would
	// announce a recovery (or an outage) that never happened, so the tick
	// leaves the last real verdict standing.
	RefreshHostLoadAvg(ctx context.Context, host string) (sampled bool, err error)
}

// VolumeUsageRefresher sizes one host's volumes.
// Implemented by *instance.Service.RefreshHostVolumeUsage.
type VolumeUsageRefresher interface {
	RefreshHostVolumeUsage(ctx context.Context, host string) error
}

// BootConverger detects a host reboot from its kernel uptime and re-converges
// that host's stored specs when one is detected. Implemented by
// *instance.Service: HostUptime delegates to podman.Client.HostUptime, and
// ReconcileSpecsOnHost is the same method server.go already runs once at
// daemon startup — this just gives it a second trigger, for the case that
// one-shot boot converge doesn't cover: the host reboots while podman-api
// keeps running (#231).
type BootConverger interface {
	// HostUptime returns hostID's current kernel uptime. ok is false when the
	// host's podman cannot report it (e.g. too old, or a format this client
	// doesn't parse) — callers must treat that as "unknown", never as "just
	// booted".
	HostUptime(ctx context.Context, hostID string) (uptime time.Duration, ok bool, err error)
	// ReconcileSpecsOnHost re-creates any stored spec whose pod is missing.
	// Deliberately tolerant (see the method's own doc comment): it logs and
	// continues per-instance and never propagates an error, so calling it
	// speculatively here is safe.
	ReconcileSpecsOnHost(ctx context.Context, hostID string)
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

	// LoadAvg, when non-nil, samples each host's load averages on every tick,
	// keeping GET /hosts's load.loadavg served from cache instead of from a
	// live SSH read inside the request's own budget (#258). nil disables it
	// entirely: the read then happens lazily on the request path, which still
	// works — the podman client caches either way — it just pays for a read
	// once per host per cache TTL.
	LoadAvg LoadAvgRefresher

	// LoadAvgTimeout bounds one host's loadavg sample, independently of
	// Timeout — mirrors StatsTimeout/BootTimeout, for the same #212 reason.
	// Zero means defaultLoadAvgTimeout.
	LoadAvgTimeout time.Duration

	// Boot, when non-nil, probes each host's uptime on every tick and
	// re-converges stored specs for a host whose uptime resets — i.e. it
	// rebooted while podman-api kept running (#231). nil disables the check
	// entirely: no extra calls, same as a nil Stats.
	Boot BootConverger

	// BootTimeout bounds one host's uptime probe when Boot is set,
	// independently of Timeout — mirrors StatsTimeout, and for the same
	// #212 reason: a slow-but-successful refresh must not starve this probe
	// of its own budget by sharing the refresh's hctx. Zero means
	// defaultBootTimeout.
	//
	// This bounds ONLY the lightweight HostUptime probe. The
	// ReconcileSpecsOnHost call triggered by a detected reboot deliberately
	// does NOT share this budget (#231 review finding #1): it is a full sweep
	// of every stored spec on the host (PodInspect/GetSpec/GetTemplate/render/
	// PlayKube per instance, plus an ingress reconcile), and a host with more
	// than a handful of instances would have that recreation loop silently
	// truncated by a 5s deadline meant for one cheap uptime read. It runs
	// instead under the tick's own long-lived context (cancelled only on
	// poller shutdown), the same convention server.go's one-shot startup boot
	// converge already uses for "let a full per-host reconcile actually
	// finish".
	BootTimeout time.Duration

	mu         sync.Mutex
	state      map[string]bool          // host -> last-known reachable, for transition logging
	statsState map[string]bool          // host -> last-known stats-sampler outcome (never reachability)
	loadState  map[string]bool          // host -> last-known loadavg-sampler outcome (never reachability)
	bootUptime map[string]time.Duration // host -> last-observed kernel uptime, for reboot (uptime reset) detection
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
	if p.loadState == nil {
		p.loadState = map[string]bool{}
	}
	if p.bootUptime == nil {
		p.bootUptime = map[string]time.Duration{}
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
			// Own budget derived from ctx, own state map — and, like the boot
			// probe below and unlike the stats sampler, run regardless of the
			// refresh's outcome.
			//
			// The stats sampler skips a failed host because it asks libpod the
			// same question the refresh just failed to answer. This does not:
			// it reads /proc over SSH, a different transport entirely, and the
			// two fail independently. A host whose container sweep times out
			// (a big store, a slow libpod) can answer a one-line `cat`
			// instantly — and gating on the sweep meant that host's cache went
			// stale and its loadavg fell back to a live read inside a request's
			// own budget, which is the arrangement this whole change exists to
			// get rid of.
			//
			// Nothing is dropped when a sample fails, unlike Stats: a loadavg
			// entry expires on age, so an unsampled host's value falls out on
			// its own. There is no equivalent of a cumulative counter frozen at
			// its last value.
			if p.LoadAvg != nil {
				lctx, lcancel := context.WithTimeout(ctx, p.loadAvgTimeout())
				sampled, lerr := p.LoadAvg.RefreshHostLoadAvg(lctx, host)
				lcancel()
				if sampled {
					p.logLoadAvgTransition(host, lerr)
				}
			}
			// Independent of the refresh's own outcome (err above): a reboot
			// probe is a different, lighter libpod call, and a host that just
			// failed its container sweep may still answer it (or vice versa).
			// Own budget for the same #212 reason the stats sampler has one —
			// but that budget bounds ONLY the uptime probe. If a reboot is
			// detected, the resulting ReconcileSpecsOnHost call is passed ctx
			// itself (the tick's own long-lived context, cancelled only on
			// poller shutdown), never bctx: see BootTimeout's doc comment.
			if p.Boot != nil {
				bctx, bcancel := context.WithTimeout(ctx, p.bootTimeout())
				p.checkBoot(bctx, ctx, host)
				bcancel()
			}
		}(h)
	}
	wg.Wait()
	p.pruneState(hosts)
}

// defaultBootTimeout bounds one host's uptime probe when BootTimeout is
// unset. It is a single libpod `info` call — the same cost as one HostInfo
// sub-call — so this mirrors defaultStatsTimeout rather than being separately
// tuned.
const defaultBootTimeout = 5 * time.Second

// EffectiveBootTimeout maps a configured boot-probe timeout to the value the
// poller will actually spend: anything <= 0 means defaultBootTimeout, never
// "no timeout" and never "expire instantly". Exported for the same reason
// EffectiveStatsTimeout is: server's startup budget check
// (statsBudgetWarning) has to reason about the same effective value the
// poller uses, not the raw (possibly zero) configured one.
func EffectiveBootTimeout(d time.Duration) time.Duration {
	return effectiveTimeout(d, defaultBootTimeout)
}

// effectiveTimeout maps a configured per-host sub-budget to what the poller
// will actually spend. Anything <= 0 means the default — never "no timeout"
// (tick blocks the ticker, so an unbounded call hangs the whole poll loop) and
// never "expire instantly" (which would sample nothing, ever).
//
// One implementation for all three sub-budgets: the rule is identical, and
// three copies meant a fix to it could land in one and be missed in the others.
func effectiveTimeout(configured, def time.Duration) time.Duration {
	if configured <= 0 {
		return def
	}
	return configured
}

// bootTimeout is BootTimeout normalised through EffectiveBootTimeout.
func (p *Poller) bootTimeout() time.Duration { return EffectiveBootTimeout(p.BootTimeout) }

// bootJitterTolerance separates ordinary measurement jitter between
// consecutive uptime probes from a genuine reboot. It is a fixed constant,
// deliberately independent of -inventory-refresh-interval: uptime advances in
// lockstep with wall-clock time between polls (jitter is sub-second), while a
// real reboot resets uptime near zero, so a rebooted host's newly-observed
// uptime comes in *lower* than the last one by roughly its entire pre-reboot
// uptime — at minimum tens of seconds in any realistic case, and usually far
// more.
const bootJitterTolerance = 20 * time.Second

// checkBoot probes host's uptime and, if it can be determined, compares it
// directly against the last uptime observed for this host — never via an
// absolute wall-clock-derived "boot instant" (#231 review finding #5): a
// derived boot instant moves with the control-plane's own clock, so an NTP
// step on podman-api itself would shift every tracked host's computed boot
// instant by the same delta in the same tick, flagging every host as
// rebooted simultaneously. Comparing raw uptime durations has no such
// dependency: a real reboot is visible as uptime dropping, full stop.
//
// A drop beyond bootJitterTolerance means the host rebooted since the last
// successful probe, and triggers exactly one ReconcileSpecsOnHost for this
// tick — the check then records the new uptime as the baseline, so a stable
// follow-up tick does not repeat the reconcile.
//
// probeCtx bounds only the HostUptime call (see BootTimeout's doc comment).
// reconcileCtx is used only for the ReconcileSpecsOnHost call, so a real
// reboot recovery is never truncated by the probe's own short budget.
func (p *Poller) checkBoot(probeCtx, reconcileCtx context.Context, host string) {
	uptime, ok, err := p.Boot.HostUptime(probeCtx, host)
	if err != nil || !ok {
		// Unreachable, or a podman that can't report uptime: leave the last
		// known uptime untouched. Clearing it here would forget a real
		// baseline on one flaky poll and manufacture a false "reboot"
		// detection out of the next successful one.
		return
	}

	p.mu.Lock()
	prev, seen := p.bootUptime[host]
	p.mu.Unlock()

	if !seen {
		// First observation for this host: record the baseline only. The
		// daemon's own startup boot-converge (server.go) already covers
		// "podman-api just started" — triggering here too would just repeat
		// it for every host on every daemon start.
		p.mu.Lock()
		p.bootUptime[host] = uptime
		p.mu.Unlock()
		return
	}

	if uptime >= prev-bootJitterTolerance {
		// The ordinary case: uptime held steady or advanced since the last
		// probe. No reconcile call happens on this path, so the baseline can
		// be committed immediately.
		p.mu.Lock()
		p.bootUptime[host] = uptime
		p.mu.Unlock()
		return
	}

	log.Printf("inventory: host %s rebooted (uptime reset) — re-converging stored specs", host)
	p.Boot.ReconcileSpecsOnHost(reconcileCtx, host)
	// Committed only AFTER the reconcile call returns (#231 review finding
	// #1): if this attempt were cut short, leaving the stale (pre-reboot)
	// baseline in place means the NEXT tick recomputes the same "uptime
	// dropped relative to baseline" condition — uptime keeps climbing from
	// its new, post-reboot value while the stale baseline stays put — and
	// retries, rather than the truncated attempt being silently accepted as
	// done. Combined with reconcileCtx no longer sharing the probe's short
	// budget, a real reconcile now gets the room to finish in the first
	// place.
	p.mu.Lock()
	p.bootUptime[host] = uptime
	p.mu.Unlock()
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
	return effectiveTimeout(d, defaultStatsTimeout)
}

// statsTimeout is StatsTimeout normalised through EffectiveStatsTimeout.
func (p *Poller) statsTimeout() time.Duration { return EffectiveStatsTimeout(p.StatsTimeout) }

// defaultLoadAvgTimeout bounds one host's loadavg sample when LoadAvgTimeout is
// unset. Over a pooled SSH connection the read is a single short exec — the
// ~2.9s handshake it used to pay is gone (see podman/ssh_pool.go) — so this is
// headroom for a cold reconnect, not a tuned value.
const defaultLoadAvgTimeout = 5 * time.Second

// EffectiveLoadAvgTimeout maps a configured loadavg timeout to the value the
// poller will actually spend: anything <= 0 means defaultLoadAvgTimeout, never
// "no timeout" and never "expire instantly". Exported for the same reason
// EffectiveStatsTimeout is — server's startup budget check has to reason about
// the effective value, not the raw configured one.
func EffectiveLoadAvgTimeout(d time.Duration) time.Duration {
	return effectiveTimeout(d, defaultLoadAvgTimeout)
}

// loadAvgTimeout is LoadAvgTimeout normalised through EffectiveLoadAvgTimeout.
func (p *Poller) loadAvgTimeout() time.Duration { return EffectiveLoadAvgTimeout(p.LoadAvgTimeout) }

// pruneState drops transition-log state for hosts no longer in the active set
// (e.g. removed via SIGHUP), so the map can't grow unbounded over host churn.
//
// This deliberately still includes bootUptime: pruning a host's own baseline
// as soon as it drops out (rather than trying to detect a reboot that
// happened during its absence from here) is the correct call once server.go's
// SIGHUP host-reload path treats "reappeared after being removed" the same as
// "genuinely new" (#231 review findings #3/#4, one consistent mechanism) —
// server.go now reconciles any host id it hasn't seen in its OWN tracked set
// on the same reload that adds it back, independent of what this poller
// still remembers. Two independent "is this host new to me" trackers (this
// one keyed on probe success, server.go's on the host list) would otherwise
// have to be kept in permanent agreement for no benefit: whichever fires the
// reconcile, the gap is closed exactly once.
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
	for h := range p.loadState {
		if !keep[h] {
			delete(p.loadState, h)
		}
	}
	for h := range p.bootUptime {
		if !keep[h] {
			delete(p.bootUptime, h)
		}
	}
	p.mu.Unlock()
}

// logStateTransition logs only when a host's outcome for one concern changes,
// so a persistently-failing host doesn't flood the log every tick. bad is the
// message logged on a failure (it takes the host and the error), good the one
// logged on recovery (it takes the host).
//
// state is the caller's own map, and keeping them separate is the point: a
// side-channel sampler failing says nothing about whether the host is
// reachable, and must never be able to influence — or be mistaken for — that
// verdict.
func (p *Poller) logStateTransition(state map[string]bool, host string, err error, bad func(string, error) string, good func(string) string) {
	ok := err == nil
	p.mu.Lock()
	prev, seen := state[host]
	state[host] = ok
	p.mu.Unlock()

	switch {
	case !seen && !ok:
		log.Print(bad(host, err))
	case seen && prev && !ok:
		log.Print(bad(host, err))
	case seen && !prev && ok:
		log.Print(good(host))
	}
}

// logTransition logs only when a host's reachability changes (or is first seen
// unreachable). This is the verdict the four Grafana alert rules gate on, via
// podman_api_host_reachable — the sampler transitions below deliberately do not
// feed it.
func (p *Poller) logTransition(host string, err error) {
	p.logStateTransition(p.state, host, err,
		func(h string, e error) string { return fmt.Sprintf("inventory: host %s unreachable: %v", h, e) },
		func(h string) string { return fmt.Sprintf("inventory: host %s reachable again", h) })
}

// logStatsTransition logs stats-sampler failures only when the outcome changes.
//
// Sampling has its own budget (StatsTimeout, see tick), so these messages no
// longer track the inventory refresh's leftover time — until #212 they did, and
// a host whose refresh chronically ate most of Timeout would flip its outcome
// tick to tick, or on the worst host never sample at all. A failure now means
// the stats call itself exceeded StatsTimeout or the host errored, and the lever
// is -container-stats-timeout, not -inventory-refresh-timeout. The metrics stay
// honest either way: a missed sample renders absent, never stale.
func (p *Poller) logStatsTransition(host string, err error) {
	p.logStateTransition(p.statsState, host, err,
		func(h string, e error) string {
			return fmt.Sprintf("inventory: host %s container stats unavailable: %v", h, e)
		},
		func(h string) string { return fmt.Sprintf("inventory: host %s container stats available again", h) })
}

// logLoadAvgTransition logs loadavg-sampler failures only when the outcome
// changes.
//
// It logs at all because the silence was the bug. A nil loadavg used to be
// indistinguishable from a host that simply had no loadavg to give, and
// diagnosing it took a hand-built table of per-host latencies (#258). It logs
// only on a transition because the other half of that bug was a permanently
// broken host emitting a line on every tick, which is its own kind of silence.
func (p *Poller) logLoadAvgTransition(host string, err error) {
	p.logStateTransition(p.loadState, host, err,
		func(h string, e error) string { return fmt.Sprintf("inventory: host %s loadavg unavailable: %v", h, e) },
		func(h string) string { return fmt.Sprintf("inventory: host %s loadavg available again", h) })
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

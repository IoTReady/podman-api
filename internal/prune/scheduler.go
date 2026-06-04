package prune

import (
	"context"
	"encoding/json"
	"log"
	"sync"
	"time"

	"github.com/iotready/podman-api/internal/podman"
	"github.com/iotready/podman-api/internal/store"
)

// TickInterval is how often the scheduler re-evaluates every host. It is the
// granularity of both the interval and threshold gates.
const TickInterval = time.Minute

// activeScanLimit bounds how many recent jobs we scan to find in-flight work and
// last-prune times. Active (queued/running) jobs are always among the newest, so
// this is ample for any realistic queue depth.
const activeScanLimit = 500

// HostPolicy pairs a host id with its resolved policy. The caller (main) builds
// this slice once at startup and again on SIGHUP reload.
type HostPolicy struct {
	Host   string
	Policy Policy
}

// Scheduler enqueues prune jobs on a schedule. Store/Client/Now are injected so
// the tick logic is unit-testable without real time or a real podman host.
type Scheduler struct {
	Store  store.JobStore
	Client podman.Client
	Now    func() time.Time

	// lastOverride, when set for a host, replaces the store-derived last-prune
	// time. Test seam only; nil in production.
	lastOverride map[string]time.Time

	wg sync.WaitGroup
}

// Start launches the ticker loop until ctx is cancelled. hostsFn returns the
// current host policies on each tick (so SIGHUP reloads are picked up). An
// immediate first pass runs before the ticker so a host already over threshold
// (or past its interval) at boot is handled without waiting a full tick. Use
// Wait to block until the loop has exited after cancellation.
func (s *Scheduler) Start(ctx context.Context, hostsFn func() []HostPolicy) {
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		t := time.NewTicker(TickInterval)
		defer t.Stop()
		runTick := func() {
			defer func() {
				if r := recover(); r != nil {
					log.Printf("prune: scheduler tick panicked: %v", r)
				}
			}()
			s.tick(ctx, hostsFn())
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

// Wait blocks until the scheduler goroutine has exited (after its ctx is
// cancelled). Mirrors jobs.Runner so callers can drain cleanly on shutdown.
func (s *Scheduler) Wait() { s.wg.Wait() }

// tick evaluates every host once.
func (s *Scheduler) tick(ctx context.Context, hosts []HostPolicy) {
	now := s.Now()
	inflight, lastPrune := s.scanPruneJobs(ctx)
	migrateActive := s.migrateOrEvacuateActive(ctx)

	for _, hp := range hosts {
		if !hp.Policy.Enabled {
			continue
		}
		if inflight[hp.Host] {
			continue // dedup: a prune for this host is queued/running
		}

		due := false
		if last, ok := lastPrune[hp.Host]; !ok {
			due = true // never pruned
		} else if hp.Policy.Interval > 0 && now.Sub(last) >= hp.Policy.Interval {
			// Interval==0 disables the interval trigger (see policy.go); such a
			// host only prunes when it crosses the disk threshold.
			due = true
		}

		overThreshold := false
		if !due && hp.Policy.DiskThreshold > 0 {
			info, err := s.Client.HostInfo(ctx, hp.Host)
			if err != nil {
				log.Printf("prune: host %s info failed, skipping this tick: %v", hp.Host, err)
				continue
			}
			if info.Disk.Total > 0 {
				usedPct := float64(info.Disk.Used) / float64(info.Disk.Total) * 100
				if usedPct >= float64(hp.Policy.DiskThreshold) {
					overThreshold = true
				}
			}
		}

		if !due && !overThreshold {
			continue
		}

		pol := hp.Policy
		if migrateActive && pol.HasScope(ScopeVolumes) {
			pol.Scope = withoutScope(pol.Scope, ScopeVolumes)
			log.Printf("prune: migrate/evacuate active, dropping volumes scope for host %s this run", hp.Host)
		}

		args, err := json.Marshal(Payload{Host: hp.Host, Policy: pol})
		if err != nil {
			log.Printf("prune: marshal payload for %s: %v", hp.Host, err)
			continue
		}
		if _, err := s.Store.Enqueue(ctx, "prune", args, ""); err != nil {
			log.Printf("prune: enqueue for %s failed: %v", hp.Host, err)
			continue
		}
		log.Printf("prune: enqueued cleanup for host %s (scopes=%v)", hp.Host, pol.Scope)
	}
}

// scanPruneJobs returns, from the most recent prune jobs, the set of hosts with a
// queued/running prune (in-flight) and the last successful prune time per host.
func (s *Scheduler) scanPruneJobs(ctx context.Context) (inflight map[string]bool, last map[string]time.Time) {
	inflight = map[string]bool{}
	last = map[string]time.Time{}
	if s.lastOverride != nil {
		for h, t := range s.lastOverride {
			last[h] = t
		}
	}
	jobs, err := s.Store.ListJobs(ctx, store.JobFilter{Kind: "prune", Limit: activeScanLimit})
	if err != nil {
		log.Printf("prune: list prune jobs failed: %v", err)
		return
	}
	for _, j := range jobs {
		var p Payload
		if err := json.Unmarshal(j.Args, &p); err != nil || p.Host == "" {
			continue
		}
		switch j.State {
		case store.JobQueued, store.JobRunning:
			inflight[p.Host] = true
		case store.JobSucceeded:
			if s.lastOverride != nil {
				continue // test seam wins
			}
			if cur, ok := last[p.Host]; !ok || j.Finished.After(cur) {
				last[p.Host] = j.Finished
			}
		}
	}
	return
}

// migrateOrEvacuateActive reports whether any migrate/evacuate job is queued or
// running. Coarse-grained (host-agnostic) on purpose: the only dangerous overlap
// is volume reaping, which we suppress entirely while any such job is active.
func (s *Scheduler) migrateOrEvacuateActive(ctx context.Context) bool {
	for _, kind := range []string{"migrate", "evacuate"} {
		jobs, err := s.Store.ListJobs(ctx, store.JobFilter{Kind: kind, Limit: activeScanLimit})
		if err != nil {
			log.Printf("prune: list %s jobs failed (assuming active for safety): %v", kind, err)
			return true
		}
		for _, j := range jobs {
			if j.State == store.JobQueued || j.State == store.JobRunning {
				return true
			}
		}
	}
	return false
}

func withoutScope(scopes []string, drop string) []string {
	out := make([]string, 0, len(scopes))
	for _, s := range scopes {
		if s != drop {
			out = append(out, s)
		}
	}
	return out
}

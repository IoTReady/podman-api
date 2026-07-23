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

// Poller periodically refreshes every host's inventory into the service cache.
// It mirrors internal/prune.Scheduler: an immediate first pass so a fresh start
// warms within one cycle, per-tick panic recovery, a per-host timeout so one
// hung host can't bleed into the next cycle, and Wait() for clean shutdown.
type Poller struct {
	Svc      Refresher
	Interval time.Duration
	Timeout  time.Duration

	mu    sync.Mutex
	state map[string]bool // host -> last-known reachable, for transition logging
	wg    sync.WaitGroup
}

// Start launches the ticker loop until ctx is cancelled. hostsFn returns the
// current host ids on each tick (so SIGHUP host reloads are picked up).
func (p *Poller) Start(ctx context.Context, hostsFn func() []string) {
	p.mu.Lock()
	if p.state == nil {
		p.state = map[string]bool{}
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

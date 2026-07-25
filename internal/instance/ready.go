package instance

import (
	"context"
	"errors"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/iotready/podman-api/internal/podman"
)

var errReadyTimeout = errors.New("readiness timeout")

// errStillStarting additionally wraps a timeout where the pod was not ready
// *only* because a container was still inside its healthcheck start period —
// i.e. nothing has failed yet. Callers that treat a timeout as advisory use it
// to say "still initialising" rather than implying a fault (#196).
var errStillStarting = errors.New("healthcheck still in start period")

// readyOpts configures a readiness wait. The two call paths want materially
// different behaviour, so the difference is named here rather than passed as a
// bare boolean at the call site.
type readyOpts struct {
	// timeout is the base budget. Zero disables the wait entirely.
	timeout time.Duration
	// stableCount is how many consecutive ready observations are required.
	stableCount int
	// grantStartPeriod extends the deadline to cover the healthcheck start
	// period each container was actually promised, so a container cannot be
	// failed while it is still legitimately starting up. Set on paths where a
	// timeout is fatal (migrate/evacuate) and correctness outweighs latency.
	//
	// Left false on the deploy path, where the operator is waiting on a
	// synchronous response: there, a timeout is only advisory, and blocking an
	// apply for a 5-minute start period to report a warning would be worse than
	// returning promptly with an accurate "still starting" note.
	grantStartPeriod bool
}

// deployVerifyTimeout is the readiness wait applied after Apply and Start.
// Vars (not consts) so same-package tests can shorten them via setVerifyKnobs.
var deployVerifyTimeout = 30 * time.Second

// SetDeployVerifyTimeout configures the readiness wait applied after Apply and
// Start. No-op for d <= 0. Called at startup via -deploy-verify-timeout flag.
func SetDeployVerifyTimeout(d time.Duration) {
	if d > 0 {
		deployVerifyTimeout = d
	}
}

// verifyStableCount is the number of consecutive polls that must observe the
// pod as ready before waitReady returns success (for the migrate path). This
// prevents false positives from pods that briefly report Running but then
// restart (e.g. apps with slow startup that exit between the healthcheck start
// period and the first healthy heartbeat). (#143)
var verifyStableCount = 3

// SetVerifyStableCount overrides the number of consecutive ready polls needed
// for waitReady to succeed in the migrate path. No-op for n <= 0. Called at
// startup via -migrate-verify-stable-count flag.
func SetVerifyStableCount(n int) {
	if n > 0 {
		verifyStableCount = n
	}
}

// deployVerifyStableCount is the same mechanism for the deploy path (Apply,
// Start). Defaults to 1 so a single ready observation is sufficient — the
// deploy path has a tighter timeout budget (deployVerifyTimeout=30s) and
// the app has just been freshly applied, so crash-restart cycles are less
// likely than during migration where the app was already running elsewhere.
var deployVerifyStableCount = 1

// SetDeployVerifyStableCount overrides the number of consecutive ready polls
// needed for waitReady to succeed in the deploy path. No-op for n <= 0.
func SetDeployVerifyStableCount(n int) {
	if n > 0 {
		deployVerifyStableCount = n
	}
}

// waitReady polls until podReady returns true, or the deadline elapses, or ctx
// is cancelled. Returns nil on success, errReadyTimeout on timeout, ctx.Err() on
// cancellation. o.timeout==0 disables the wait and returns nil immediately.
//
// o.stableCount is the required number of observations where PodInspect succeeds
// AND podReady returns true. Transient PodInspect errors (e.g. SSH blips) do
// NOT reset the counter, avoiding false negatives when the destination host has
// intermittent reachability (#145). Only a successful PodInspect that reports
// the pod as not-ready resets progress, preserving the anti-flap mechanism (#143).
//
// With o.grantStartPeriod, the deadline is extended to cover the healthcheck
// start period the containers actually declare, so a still-starting container is
// never failed for being slower than a fixed constant (#196). A timeout that
// occurred while a container was still starting additionally wraps
// errStillStarting, letting callers distinguish "not finished yet" from "faulty".
func (s *Service) waitReady(ctx context.Context, host, tmpl, slug string, o readyOpts) error {
	if o.timeout == 0 {
		return nil
	}
	deadline := time.Now().Add(o.timeout)
	extended := false
	starting := false
	ticker := time.NewTicker(verifyInterval)
	defer ticker.Stop()
	stable := 0
	for {
		name := podName(tmpl, slug)
		p, err := s.client.PodInspect(ctx, host, name)
		switch {
		case err == nil && podReady(p):
			stable++
			log.Printf("pod %s ready (stable=%d/%d)", name, stable, o.stableCount)
			if stable >= o.stableCount {
				return nil
			}
		case err == nil:
			stable = 0
			starting = anyStarting(p)
			// The declared start period is only knowable from a successful
			// inspect, so the deadline is extended on the first one rather than
			// up front. Done once: a container that restarts mid-wait must not
			// keep pushing the deadline outwards indefinitely.
			if o.grantStartPeriod && !extended {
				extended = true
				if g := startupDeadline(p); g.After(deadline) {
					log.Printf("pod %s: extending readiness deadline to %s to honour declared healthcheck start period", name, g.Format(time.RFC3339))
					deadline = g
				}
			}
			log.Printf("pod %s not ready (status=%q, stable reset to 0)", name, p.Status)
			for _, c := range p.Containers {
				log.Printf("  container %s: status=%q health=%q", c.Name, c.Status, c.Health)
			}
		default:
			log.Printf("pod %s inspect error: %v (stable=%d/%d, not reset)", name, err, stable, o.stableCount)
		}
		if time.Now().After(deadline) {
			if starting {
				return fmt.Errorf("%w: %w", errReadyTimeout, errStillStarting)
			}
			return errReadyTimeout
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

// readinessWarning renders the operator-facing note for a readiness wait that
// did not succeed, or "" when there is nothing to report.
//
// A container still inside its start period is reported as initialising rather
// than as a timeout. The deploy path deliberately does not wait out a long start
// period, so on a template that declares one this outcome is routine — wording it
// as a failure would make the warning fire on nearly every deploy and drown the
// case where something is actually wrong (#196).
func readinessWarning(err error) string {
	switch {
	case err == nil:
		return ""
	case errors.Is(err, errStillStarting):
		return fmt.Sprintf(
			"not yet serving after %s: still inside its healthcheck start period — expected for a slow-starting app, and it will be restarted automatically if the check ultimately fails",
			deployVerifyTimeout,
		)
	case errors.Is(err, errReadyTimeout):
		return fmt.Sprintf(
			"readiness timeout: healthcheck did not pass within %s — the app may still be initialising",
			deployVerifyTimeout,
		)
	}
	return ""
}

// anyStarting reports whether some container is inside its healthcheck start
// period. Distinct from "unhealthy": nothing has failed yet.
func anyStarting(p podman.Pod) bool {
	for _, c := range p.Containers {
		if strings.EqualFold(c.Health, "starting") {
			return true
		}
	}
	return false
}

// startupDeadline returns the latest moment any container in the pod could
// still legitimately turn healthy for the first time: its start period, plus one
// check interval for the check that decides the outcome, measured from when the
// container actually started.
//
// Podman reports "starting" until the first *passing* check, and that check
// cannot run before one interval has elapsed. A fixed readiness budget shorter
// than that interval therefore always expires while the container is still
// starting, and a budget shorter than the start period fails apps that were
// explicitly granted a longer runway (#196). Deriving the bound from the spec
// keeps the two in agreement by construction.
//
// Returns the zero time when no container declares a healthcheck, so callers
// keep their configured budget unchanged.
func startupDeadline(p podman.Pod) time.Time {
	var latest time.Time
	for _, c := range p.Containers {
		if c.HealthStartPeriod <= 0 {
			continue
		}
		from := c.StartedAt
		if from.IsZero() {
			// No observed start time: measure from now, which is the
			// conservative direction (waits longer rather than failing early).
			from = time.Now()
		}
		if end := from.Add(c.HealthStartPeriod + c.HealthInterval); end.After(latest) {
			latest = end
		}
	}
	return latest
}

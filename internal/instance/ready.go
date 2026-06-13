package instance

import (
	"context"
	"errors"
	"log"
	"time"
)

var errReadyTimeout = errors.New("readiness timeout")

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

// waitReady polls until podReady returns true, or timeout elapses, or ctx is
// cancelled. Returns nil on success, errReadyTimeout on timeout, ctx.Err() on
// cancellation. timeout==0 disables the wait and returns nil immediately.
// stableCount is the required number of observations where PodInspect succeeds
// AND podReady returns true. Transient PodInspect errors (e.g. SSH blips) do
// NOT reset the counter, avoiding false negatives when the destination host has
// intermittent reachability (#145). Only a successful PodInspect that reports
// the pod as not-ready resets progress, preserving the anti-flap mechanism (#143).
func (s *Service) waitReady(ctx context.Context, host, tmpl, slug string, timeout time.Duration, stableCount int) error {
	if timeout == 0 {
		return nil
	}
	deadline := time.Now().Add(timeout)
	ticker := time.NewTicker(verifyInterval)
	defer ticker.Stop()
	stable := 0
	for {
		p, err := s.client.PodInspect(ctx, host, podName(tmpl, slug))
		switch {
		case err == nil && podReady(p):
			stable++
			log.Printf("pod %s ready (stable=%d/%d)", podName(tmpl, slug), stable, stableCount)
			if stable >= stableCount {
				return nil
			}
		case err == nil:
			stable = 0
			log.Printf("pod %s not ready (status=%q, stable reset to 0)", podName(tmpl, slug), p.Status)
		default:
			log.Printf("pod %s inspect error: %v (stable=%d/%d, not reset)", podName(tmpl, slug), err, stable, stableCount)
		}
		if time.Now().After(deadline) {
			return errReadyTimeout
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

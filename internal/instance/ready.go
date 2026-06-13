package instance

import (
	"context"
	"errors"
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
// pod as ready before waitReady returns success. This prevents false positives
// from pods that briefly report Running but then restart (e.g. apps with slow
// startup that exit between the healthcheck start period and the first healthy
// heartbeat). (#143)
var verifyStableCount = 3

// SetVerifyStableCount overrides the number of consecutive ready polls needed
// for waitReady to succeed. No-op for n <= 0. Exported for test use.
func SetVerifyStableCount(n int) {
	if n > 0 {
		verifyStableCount = n
	}
}

// waitReady polls until podReady returns true, or timeout elapses, or ctx is
// cancelled. Returns nil on success, errReadyTimeout on timeout, ctx.Err() on
// cancellation. timeout==0 disables the wait and returns nil immediately.
func (s *Service) waitReady(ctx context.Context, host, tmpl, slug string, timeout time.Duration) error {
	if timeout == 0 {
		return nil
	}
	deadline := time.Now().Add(timeout)
	ticker := time.NewTicker(verifyInterval)
	defer ticker.Stop()
	stable := 0
	for {
		p, err := s.client.PodInspect(ctx, host, podName(tmpl, slug))
		if err == nil && podReady(p) {
			stable++
			if stable >= verifyStableCount {
				return nil
			}
		} else {
			stable = 0
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

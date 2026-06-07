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
	for {
		p, err := s.client.PodInspect(ctx, host, podName(tmpl, slug))
		if err == nil && podReady(p) {
			return nil
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

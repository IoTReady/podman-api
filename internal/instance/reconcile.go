package instance

import (
	"context"
	"errors"

	"github.com/iotready/podman-api/internal/podman"
)

// destStatus is the destination's reconcile-relevant state.
type destStatus int

const (
	destHealthy destStatus = iota
	destUnhealthy
	destAbsent
	destUnreachable
)

// ReconcileMigrate drives a migrate that was interrupted by a daemon restart to a
// consistent state, inspecting the real host state rather than trusting any
// persisted progress. It returns:
//
//	resolved=false  inconclusive (a host was unreachable) — caller retries later
//	resolved=true, succeeded=true   rolled forward (or the commit had finished)
//	resolved=true, succeeded=false  rolled back, or the dest is an orphan left in place
//
// step is a best-effort progress callback (may be nil). It reuses the same
// primitives as Migrate (waitRunning/Start/Delete) and takes migrateLock so it
// cannot race a re-issued migrate of the same instance.
func (s *Service) ReconcileMigrate(ctx context.Context, req MigrateRequest, step func(step, detail string)) (resolved, succeeded bool, err error) {
	if step == nil {
		step = func(string, string) {}
	}
	lk := s.migrateLock(req.Template, req.Slug)
	lk.Lock()
	defer lk.Unlock()

	ds := s.destState(ctx, req.ToHost, req.Template, req.Slug)
	if ds == destUnreachable {
		step("reconcile-inconclusive", req.ToHost+" unreachable")
		return false, false, nil
	}

	srcPresent, srcReachable := s.sourcePresent(ctx, req.FromHost, req.Template, req.Slug)
	if !srcReachable {
		step("reconcile-inconclusive", req.FromHost+" unreachable")
		return false, false, nil
	}

	// Mutations run on a detached context so a sweep/shutdown cancellation cannot
	// strand a half-finished compensation, mirroring Migrate's rollback/commit.
	mctx := context.WithoutCancel(ctx)

	if ds == destHealthy {
		// Roll forward: dest is truth. Reap the source if it still exists.
		if srcPresent {
			if derr := s.Delete(mctx, req.FromHost, req.Template, req.Slug, DeleteOptions{PruneVolumes: true, PruneSecrets: true}); derr != nil {
				step("reconcile-inconclusive", "reap source: "+derr.Error())
				return false, false, nil
			}
		}
		step("reconcile-roll-forward", req.ToHost)
		return true, true, nil
	}

	// dest absent or unhealthy.
	if srcPresent {
		// Roll back: restore source, reap any partial dest.
		if rerr := s.Start(mctx, req.FromHost, req.Template, req.Slug); rerr != nil {
			step("reconcile-inconclusive", "restore source: "+rerr.Error())
			return false, false, nil
		}
		if derr := s.Delete(mctx, req.ToHost, req.Template, req.Slug, DeleteOptions{PruneVolumes: true, PruneSecrets: true}); derr != nil {
			step("reconcile-inconclusive", "reap dest: "+derr.Error())
			return false, false, nil
		}
		step("reconcile-roll-back", req.FromHost)
		return true, false, nil
	}

	// Source gone and dest not healthy: never destroy the only copy. Leave the
	// dest in place for the operator and record a needs-attention failure.
	step("reconcile-orphan-dest", req.ToHost+" left in place; source already removed")
	return true, false, nil
}

// destState classifies the destination, distinguishing absent from unreachable
// and giving a present-but-not-yet-ready dest the verify window to become healthy.
func (s *Service) destState(ctx context.Context, host, tmpl, slug string) destStatus {
	if _, err := s.client.PodInspect(ctx, host, podName(tmpl, slug)); err != nil {
		if errors.Is(err, podman.ErrNotFound) {
			return destAbsent
		}
		return destUnreachable
	}
	if err := s.waitRunning(ctx, host, tmpl, slug); err == nil {
		return destHealthy
	}
	// Not healthy within the window. Distinguish a genuine unhealthy dest from a
	// host that dropped mid-wait (which must be inconclusive, not a false roll-back).
	if _, err := s.client.PodInspect(ctx, host, podName(tmpl, slug)); err != nil && !errors.Is(err, podman.ErrNotFound) {
		return destUnreachable
	}
	return destUnhealthy
}

// sourcePresent reports whether the source instance exists and whether the host
// was reachable for the check.
func (s *Service) sourcePresent(ctx context.Context, host, tmpl, slug string) (present, reachable bool) {
	_, err := s.client.PodInspect(ctx, host, podName(tmpl, slug))
	if err == nil {
		return true, true
	}
	if errors.Is(err, podman.ErrNotFound) {
		return false, true
	}
	return false, false
}

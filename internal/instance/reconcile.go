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
// message is an operator-facing summary recorded in the job's error field for
// terminal failed outcomes; it is empty for success and for inconclusive results.
//
// step is a best-effort progress callback (may be nil). It reuses the same
// primitives as Migrate (waitRunning/Start/Delete) and takes migrateLock so it
// cannot race a re-issued migrate of the same instance.
func (s *Service) ReconcileMigrate(ctx context.Context, req MigrateRequest, step func(step, detail string)) (resolved, succeeded bool, message string, err error) {
	if step == nil {
		step = func(string, string) {}
	}
	lk := s.migrateLock(req.Template, req.Slug)
	lk.Lock()
	defer lk.Unlock()

	// Terminate immediately if either host has been removed from config — a
	// missing host makes PodInspect fail with an opaque "unknown host" error, which
	// sourcePresent/destState classify as unreachable, causing an infinite retry
	// loop that can never be resolved automatically.
	if _, ok := s.host(req.FromHost); !ok {
		return true, false, "source host " + req.FromHost + " is no longer configured; manual cleanup required", nil
	}
	if _, ok := s.host(req.ToHost); !ok {
		return true, false, "destination host " + req.ToHost + " is no longer configured; manual cleanup required", nil
	}

	// Check source reachability first: it is a single cheap inspect, whereas
	// destState may poll for up to verifyTimeout. This avoids burning the verify
	// window when the source host is unreachable.
	srcPresent, srcReachable := s.sourcePresent(ctx, req.FromHost, req.Template, req.Slug)
	if !srcReachable {
		step("reconcile-inconclusive", req.FromHost+" unreachable")
		return false, false, "", nil
	}

	ds := s.destState(ctx, req.ToHost, req.Template, req.Slug)
	if ds == destUnreachable {
		step("reconcile-inconclusive", req.ToHost+" unreachable")
		return false, false, "", nil
	}

	// Mutations run on a detached context so a sweep/shutdown cancellation cannot
	// strand a half-finished compensation, mirroring Migrate's rollback/commit.
	mctx := context.WithoutCancel(ctx)

	if ds == destHealthy && s.destSpecPersisted(ctx, req.ToHost, req.Template, req.Slug) {
		// Dest is a complete, committed replacement. Repair its ingress routes
		// (idempotent; covers a crash between PutSpec and ingress.Reconcile),
		// then reap the source.
		if s.ingressEnabled() {
			if rerr := s.ingress.Reconcile(mctx, req.ToHost); rerr != nil {
				step("reconcile-inconclusive", "dest ingress reconcile: "+rerr.Error())
				return false, false, "", nil
			}
		}
		if srcPresent {
			if derr := s.Delete(mctx, req.FromHost, req.Template, req.Slug, DeleteOptions{PruneVolumes: true, PruneSecrets: true}); derr != nil {
				step("reconcile-inconclusive", "reap source: "+derr.Error())
				return false, false, "", nil
			}
		}
		step("reconcile-roll-forward", req.ToHost)
		return true, true, "", nil
	}

	// dest absent or unhealthy, OR dest healthy but spec not persisted (Apply
	// interrupted before commit — dest must not be treated as source of truth).
	if srcPresent {
		// Roll back: restore source, reap any partial dest.
		if rerr := s.Start(mctx, req.FromHost, req.Template, req.Slug); rerr != nil {
			step("reconcile-inconclusive", "restore source: "+rerr.Error())
			return false, false, "", nil
		}
		if derr := s.Delete(mctx, req.ToHost, req.Template, req.Slug, DeleteOptions{PruneVolumes: true, PruneSecrets: true}); derr != nil {
			step("reconcile-inconclusive", "reap dest: "+derr.Error())
			return false, false, "", nil
		}
		step("reconcile-roll-back", req.FromHost)
		return true, false, "rolled back: destination unverified, source restored", nil
	}

	// Source gone and dest not committable.
	if ds == destAbsent {
		step("reconcile-not-found", "instance absent on both hosts")
		return true, false, "instance not found on either host", nil
	}
	step("reconcile-orphan-dest", req.ToHost+" left in place; source already removed")
	return true, false, "destination left in place; source already removed — manual cleanup required", nil
}

// destSpecPersisted reports whether the destination's desired-state spec was
// stored — the last durable step of a migrate's Apply (PlayKube → PutSpec →
// ingress). A healthy dest pod whose spec is missing means Apply was interrupted
// before it committed; that dest must NOT be treated as the source of truth.
func (s *Service) destSpecPersisted(ctx context.Context, host, tmpl, slug string) bool {
	if s.store == nil {
		return true // no persistence layer; pod liveness is all there is
	}
	_, err := s.store.GetSpec(ctx, host, tmpl, slug)
	return err == nil
}

// destState classifies the destination, distinguishing absent from unreachable
// and giving a present-but-not-yet-ready dest the verify window to become healthy.
func (s *Service) destState(ctx context.Context, host, tmpl, slug string) destStatus {
	// A pre-waitRunning inspect distinguishes destAbsent (ErrNotFound) from
	// destUnreachable (any other error) before paying the verifyTimeout cost.
	// If the pod exists, fall through to let it race the verify window.
	if _, err := s.client.PodInspect(ctx, host, podName(tmpl, slug)); err != nil {
		if errors.Is(err, podman.ErrNotFound) {
			return destAbsent
		}
		return destUnreachable
	}
	if err := s.waitRunning(ctx, host, tmpl, slug); err == nil {
		return destHealthy
	}
	// Not healthy within the window. Re-inspect to distinguish a pod that is
	// genuinely unhealthy from a host that dropped mid-wait:
	//   - ErrNotFound: pod vanished during the wait — the host is still reachable
	//     but the pod is gone; treat as destUnhealthy (reachable, destabilised).
	//   - any other error: host became unreachable mid-wait → destUnreachable
	//     (must be inconclusive, not a false roll-back).
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

package instance

import (
	"context"
	"errors"

	"github.com/iotready/podman-api/internal/podman"
	"github.com/iotready/podman-api/internal/store"
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
	// Same for the template: Start/Delete begin with lookup(host, tmpl), so a
	// template removed from config would otherwise fail inside the compensation
	// phase and be swallowed as inconclusive — the same infinite loop the host
	// guards above prevent. (Templates load once at startup, so removal coincides
	// with the restart that produced these reconciling jobs.)
	if !s.hasTemplate(req.Template) {
		return true, false, "template " + req.Template + " is no longer configured; manual cleanup required", nil
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

	// An operator cancel (or shutdown) arriving before the compensation phase
	// aborts without touching the hosts, honoring the cancel "left as-is"
	// contract. A compensation already begun (below) runs to completion on the
	// detached context.
	if ctx.Err() != nil {
		step("reconcile-canceled", "interrupted before compensation")
		return false, false, "", nil
	}

	// Mutations run on a detached context so a sweep/shutdown cancellation cannot
	// strand a half-finished compensation, mirroring Migrate's rollback/commit.
	mctx := context.WithoutCancel(ctx)

	if ds == destHealthy {
		persisted, ok := s.destSpecState(ctx, req.ToHost, req.Template, req.Slug)
		if !ok {
			// The store could not be consulted (transient: cancellation, BUSY,
			// decrypt). Treat as inconclusive — NOT as "not persisted", which would
			// wrongly roll back and delete a committed destination.
			step("reconcile-inconclusive", "destination spec lookup failed")
			return false, false, "", nil
		}
		if persisted {
			// Dest is a complete, committed replacement. Repair its ingress routes
			// (idempotent; covers a crash between PutSpec and ingress.Reconcile),
			// then reap the source.
			if s.ingressEnabled() {
				if rerr := s.ingress.Reconcile(mctx, req.ToHost); rerr != nil {
					step("reconcile-inconclusive", "dest ingress reconcile: "+rerr.Error())
					return false, false, "", nil
				}
			}
			// Reap the source. If its pod is present, fully delete it (pod +
			// volumes + secrets + spec + ingress); a failure there is genuinely
			// inconclusive — we must not leave a duplicate of the committed dest.
			//
			// If the pod is already gone (fully committed, or a commit interrupted
			// between PodRemove and DeleteSpec), only persisted state may linger:
			// clear the spec row directly with a best-effort ingress refresh. Using
			// the full Delete here would couple adoption of the healthy dest to the
			// *source* host's proxy health — Delete's final ingress.Reconcile
			// propagates errors — stranding a committed migrate in `reconciling`
			// whenever the source caddy is wedged while still serving other apps.
			if srcPresent {
				if derr := s.Delete(mctx, req.FromHost, req.Template, req.Slug, DeleteOptions{PruneVolumes: true, PruneSecrets: true}); derr != nil {
					step("reconcile-inconclusive", "reap source: "+derr.Error())
					return false, false, "", nil
				}
			} else if s.store != nil {
				// A store write is local and reliable; a failure is worth retrying so
				// the orphan row is cleaned. The source ingress refresh is best-effort
				// — if it fails, the periodic ingress loop reconciles from the now
				// row-less state.
				if derr := s.store.DeleteSpec(mctx, req.FromHost, req.Template, req.Slug); derr != nil && !errors.Is(derr, store.ErrNotFound) {
					step("reconcile-inconclusive", "clean source spec: "+derr.Error())
					return false, false, "", nil
				}
				if s.ingressEnabled() {
					if rerr := s.ingress.Reconcile(mctx, req.FromHost); rerr != nil {
						step("reconcile-source-ingress-cleanup-failed", rerr.Error())
					}
				}
			}
			step("reconcile-roll-forward", req.ToHost)
			return true, true, "", nil
		}
		// dest healthy but spec not persisted (Apply interrupted before commit) —
		// fall through; the dest must not be treated as the source of truth.
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

// destSpecState reports whether the destination's desired-state spec was stored
// — the last durable step of a migrate's Apply (PlayKube → PutSpec → ingress). A
// healthy dest pod whose spec is missing means Apply was interrupted before it
// committed; that dest must NOT be treated as the source of truth.
//
// ok=false means the store could not be consulted (a transient error: context
// cancellation, SQLITE_BUSY, a decrypt failure). The caller must treat that as
// inconclusive and retry — never as "not persisted", which would wrongly roll
// back (and delete) a committed destination. Only store.ErrNotFound is a
// definitive "not persisted".
//
// TODO(#54): this re-derives Apply's commit point from "a spec row exists", and
// the ingress repair in the roll-forward branch is evidence the equivalence
// leaks (a durable step runs after PutSpec). A cleaner design records one
// explicit commit marker as Apply's final durable action and gates roll-forward
// on that fact.
func (s *Service) destSpecState(ctx context.Context, host, tmpl, slug string) (persisted, ok bool) {
	if s.store == nil {
		return true, true // no persistence layer; pod liveness is all there is
	}
	_, err := s.store.GetSpec(ctx, host, tmpl, slug)
	if err == nil {
		return true, true
	}
	if errors.Is(err, store.ErrNotFound) {
		return false, true
	}
	return false, false // transient/unknown — caller retries
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

package prune

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/iotready/podman-api/internal/jobs"
	"github.com/iotready/podman-api/internal/podman"
	"github.com/iotready/podman-api/internal/store"
)

// ProtectLabel marks a volume that must never be reaped by the volumes scope.
// The volume prune passes a "label!" filter so volumes carrying it are excluded.
const ProtectLabel = "podman-api.protect"

// Payload is the job-args shape the scheduler enqueues and the handler reads.
// It carries a snapshot of the resolved policy so a mid-flight config reload
// cannot change a running job's behavior.
type Payload struct {
	Host   string `json:"host"`
	Policy Policy `json:"policy"`
}

// Metrics records prune outcomes. nil-safe via Handler.metric().
type Metrics interface {
	RunDone(host, result string)
	Reclaimed(host, scope string, bytes int64)
}

// Handler implements jobs.Handler for the "prune" kind.
type Handler struct {
	Client podman.Client
	// Jobs, when set, enables a run-time safety re-check: before running the
	// volumes scope the handler re-queries for an active migrate/evacuate and
	// skips volumes if one is in flight. The scheduler also drops the volumes
	// scope at enqueue time, but a job can sit queued while a move starts, so the
	// guarantee belongs here too.
	Jobs    store.JobStore // optional
	Metrics Metrics        // optional
}

var _ jobs.Handler = (*Handler)(nil)

func (h *Handler) metric() Metrics {
	if h.Metrics == nil {
		return noopMetrics{}
	}
	return h.Metrics
}

// Run executes the policy's enabled scopes in a fixed safe order.
func (h *Handler) Run(ctx context.Context, job store.Job, jc *jobs.JobContext) error {
	var p Payload
	if err := json.Unmarshal(job.Args, &p); err != nil {
		return fmt.Errorf("decode prune args: %w", err)
	}

	if p.Policy.DryRun {
		if err := ctx.Err(); err != nil {
			return err
		}
		info, err := h.Client.HostInfo(ctx, p.Host)
		if err != nil {
			h.metric().RunDone(p.Host, "error")
			return fmt.Errorf("dry-run host info: %w", err)
		}
		// `system df` only reports a reclaimable figure for volumes; images and
		// build cache have no dry-run size in the libpod bindings. So only quote a
		// number when the volumes scope is enabled, and label it as such rather
		// than implying it covers the whole run.
		detail := fmt.Sprintf("scopes=%s (nothing removed)", strings.Join(p.Policy.Scope, ","))
		if p.Policy.HasScope(ScopeVolumes) {
			detail = fmt.Sprintf("scopes=%s volume-reclaimable=%d bytes (nothing removed; image/build-cache sizes unavailable in dry-run)",
				strings.Join(p.Policy.Scope, ","), info.Disk.Reclaimable)
		}
		jc.Step("dry-run", detail)
		h.metric().RunDone(p.Host, "dry-run")
		return nil
	}

	type step struct {
		scope string
		// guard, when set, is evaluated immediately before the step runs; a true
		// return skips the step (recording the reason) without it counting as a
		// failure. Used by the volumes scope to re-check for an active
		// migrate/evacuate right before it executes — see below.
		guard func() (skip bool, reason string)
		run   func() (podman.PruneReport, error)
	}
	var steps []step
	// Image scopes collapse onto a single images.Prune call: all-images (all=true)
	// is a superset of dangling (all=false), so when both are enabled we run once
	// and attribute the bytes to a single scope rather than double-counting.
	switch {
	case p.Policy.HasScope(ScopeAllImages):
		steps = append(steps, step{scope: ScopeAllImages, run: func() (podman.PruneReport, error) { return h.Client.ImagePrune(ctx, p.Host, true) }})
	case p.Policy.HasScope(ScopeDangling):
		steps = append(steps, step{scope: ScopeDangling, run: func() (podman.PruneReport, error) { return h.Client.ImagePrune(ctx, p.Host, false) }})
	}
	if p.Policy.HasScope(ScopeContainers) {
		steps = append(steps, step{scope: ScopeContainers, run: func() (podman.PruneReport, error) { return h.Client.ContainerPrune(ctx, p.Host) }})
	}
	if p.Policy.HasScope(ScopeBuildCache) {
		steps = append(steps, step{scope: ScopeBuildCache, run: func() (podman.PruneReport, error) { return h.Client.BuildCachePrune(ctx, p.Host) }})
	}
	if p.Policy.HasScope(ScopeVolumes) {
		steps = append(steps, step{
			scope: ScopeVolumes,
			// Re-check right before VolumePrune (volumes runs last, after the other
			// scopes): a migrate/evacuate may have started while this job was queued
			// or while the earlier scopes ran. Skip volumes if so, so we can't reap a
			// migration's transiently-detached volume.
			guard: func() (bool, string) {
				if h.Jobs != nil && migrateOrEvacuateActive(ctx, h.Jobs) {
					return true, "migrate/evacuate active"
				}
				return false, ""
			},
			run: func() (podman.PruneReport, error) {
				return h.Client.VolumePrune(ctx, p.Host, map[string][]string{"label!": {ProtectLabel + "=true"}})
			},
		})
	}

	var firstErr error
	for _, s := range steps {
		if err := ctx.Err(); err != nil {
			return err
		}
		if s.guard != nil {
			if skip, reason := s.guard(); skip {
				jc.Step("prune:"+s.scope, "skipped: "+reason)
				continue
			}
		}
		rep, err := s.run()
		if err != nil {
			jc.Step("prune:"+s.scope, "FAILED: "+err.Error())
			h.metric().Reclaimed(p.Host, s.scope, 0)
			if firstErr == nil {
				firstErr = fmt.Errorf("prune %s: %w", s.scope, err)
			}
			continue
		}
		jc.Step("prune:"+s.scope, fmt.Sprintf("removed %d item(s), reclaimed %d bytes", len(rep.Items), rep.Reclaimed))
		h.metric().Reclaimed(p.Host, s.scope, rep.Reclaimed)
	}
	if firstErr != nil {
		h.metric().RunDone(p.Host, "failed")
		return firstErr
	}
	h.metric().RunDone(p.Host, "succeeded")
	return nil
}

type noopMetrics struct{}

func (noopMetrics) RunDone(string, string)          {}
func (noopMetrics) Reclaimed(string, string, int64) {}

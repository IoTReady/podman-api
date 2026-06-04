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
	Client  podman.Client
	Metrics Metrics // optional
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
		run   func() (podman.PruneReport, error)
	}
	steps := []step{
		{ScopeDangling, func() (podman.PruneReport, error) { return h.Client.ImagePrune(ctx, p.Host, false) }},
		{ScopeAllImages, func() (podman.PruneReport, error) { return h.Client.ImagePrune(ctx, p.Host, true) }},
		{ScopeContainers, func() (podman.PruneReport, error) { return h.Client.ContainerPrune(ctx, p.Host) }},
		{ScopeBuildCache, func() (podman.PruneReport, error) { return h.Client.BuildCachePrune(ctx, p.Host) }},
		{ScopeVolumes, func() (podman.PruneReport, error) {
			return h.Client.VolumePrune(ctx, p.Host, map[string][]string{"label!": {ProtectLabel + "=true"}})
		}},
	}

	var firstErr error
	for _, s := range steps {
		if !p.Policy.HasScope(s.scope) {
			continue
		}
		if err := ctx.Err(); err != nil {
			return err
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

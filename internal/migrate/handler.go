// Package migrate adapts the instance migrate algorithm to the jobs runner.
package migrate

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/iotready/podman-api/internal/instance"
	"github.com/iotready/podman-api/internal/jobs"
	"github.com/iotready/podman-api/internal/store"
)

// Handler runs "migrate" jobs by delegating to instance.Service.Migrate.
type Handler struct {
	Svc     *instance.Service
	Metrics jobs.Metrics // optional; nil-safe
}

// Run unmarshals the job args into a MigrateRequest and performs the migration,
// reporting progress through the job context. If the migration rolls back, the
// rollback is recorded in Metrics.
func (h *Handler) Run(ctx context.Context, job store.Job, jc *jobs.JobContext) error {
	var req instance.MigrateRequest
	if err := json.Unmarshal(job.Args, &req); err != nil {
		return fmt.Errorf("decode migrate args: %w", err)
	}

	// rollback detection relies on the step name contract with
	// instance.Service.Migrate, which emits "rollback"/"rollback-restore-failed"/
	// "rollback-reap-failed" during compensation. HasPrefix captures all variants
	// and counts one Rollback call per migrate attempt regardless of how many
	// compensation steps fire.
	var rolledBack bool
	wrappedStep := func(step, detail string) {
		if strings.HasPrefix(step, "rollback") {
			rolledBack = true
		}
		jc.Step(step, detail)
	}

	err := h.Svc.Migrate(ctx, req, wrappedStep)
	if rolledBack && h.Metrics != nil {
		h.Metrics.Rollback("migrate")
	}
	return err
}

// Ensure Handler satisfies the runner contract.
var _ jobs.Handler = (*Handler)(nil)

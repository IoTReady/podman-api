// Package migrate adapts the instance migrate algorithm to the jobs runner.
package migrate

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/iotready/podman-api/internal/instance"
	"github.com/iotready/podman-api/internal/jobs"
	"github.com/iotready/podman-api/internal/store"
)

// Handler runs "migrate" jobs by delegating to instance.Service.Migrate.
type Handler struct {
	Svc *instance.Service
}

// Run unmarshals the job args into a MigrateRequest and performs the migration,
// reporting progress through the job context.
func (h *Handler) Run(ctx context.Context, job store.Job, jc *jobs.JobContext) error {
	var req instance.MigrateRequest
	if err := json.Unmarshal(job.Args, &req); err != nil {
		return fmt.Errorf("decode migrate args: %w", err)
	}
	return h.Svc.Migrate(ctx, req, jc.Step)
}

// Ensure Handler satisfies the runner contract.
var _ jobs.Handler = (*Handler)(nil)

package backup

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/iotready/podman-api/internal/instance"
	"github.com/iotready/podman-api/internal/jobs"
	"github.com/iotready/podman-api/internal/store"
)

// Handler runs "backup" jobs by delegating to instance.Service.Backup.
type Handler struct {
	Svc *instance.Service
}

// Run unmarshals the job args into a BackupRequest and performs the backup,
// reporting progress through the job context.
func (h *Handler) Run(ctx context.Context, job store.Job, jc *jobs.JobContext) error {
	var req instance.BackupRequest
	if err := json.Unmarshal(job.Args, &req); err != nil {
		return fmt.Errorf("decode backup args: %w", err)
	}
	return h.Svc.Backup(ctx, req, jc.Step)
}

// RestoreHandler runs "restore" jobs by delegating to instance.Service.Restore.
//
// restore has no reconciler on purpose: an interrupted restore is resolved by
// boot recovery failing the job, and the operator re-running the restore —
// which is idempotent from the blob (it re-does teardown + import + verify).
type RestoreHandler struct {
	Svc *instance.Service
}

// Run unmarshals the job args into a RestoreRequest and performs the restore,
// reporting progress through the job context.
func (h *RestoreHandler) Run(ctx context.Context, job store.Job, jc *jobs.JobContext) error {
	var req instance.RestoreRequest
	if err := json.Unmarshal(job.Args, &req); err != nil {
		return fmt.Errorf("decode restore args: %w", err)
	}
	return h.Svc.Restore(ctx, req, jc.Step)
}

// Ensure both adapters satisfy the runner contract.
var (
	_ jobs.Handler = (*Handler)(nil)
	_ jobs.Handler = (*RestoreHandler)(nil)
)

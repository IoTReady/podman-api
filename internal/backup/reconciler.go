package backup

import (
	"context"
	"encoding/json"

	"github.com/iotready/podman-api/internal/instance"
	"github.com/iotready/podman-api/internal/jobs"
	"github.com/iotready/podman-api/internal/store"
)

// Reconciler recovers "backup" jobs interrupted by a daemon restart: the
// backup row is failed (unless the work actually completed), partial blobs
// are deleted, and the instance is restarted.
type Reconciler struct {
	Svc *instance.Service
}

// Reconcile decodes the job args and drives the interrupted backup to a
// terminal state. Unparseable args are a permanent failure; an inconclusive
// host check (unreachable host) leaves the job reconciling for retry.
func (r *Reconciler) Reconcile(ctx context.Context, job store.Job, jc *jobs.JobContext) (store.JobState, string, bool, error) {
	var req instance.BackupRequest
	if err := json.Unmarshal(job.Args, &req); err != nil {
		jc.Step("reconcile-bad-args", err.Error())
		return store.JobFailed, "interrupted backup could not be decoded; manual cleanup may be required", true, nil
	}
	resolved, ok, message, err := r.Svc.ReconcileBackup(ctx, req, jc.Step)
	if err != nil {
		return "", "", false, err
	}
	if !resolved {
		return "", "", false, nil
	}
	if ok {
		return store.JobSucceeded, message, true, nil
	}
	return store.JobFailed, message, true, nil
}

// Ensure Reconciler satisfies the runner contract.
var _ jobs.Reconciler = (*Reconciler)(nil)

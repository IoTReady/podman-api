package migrate

import (
	"context"
	"encoding/json"

	"github.com/iotready/podman-api/internal/instance"
	"github.com/iotready/podman-api/internal/jobs"
	"github.com/iotready/podman-api/internal/store"
)

// Reconciler adapts instance.Service.ReconcileMigrate to the jobs runner's
// Reconciler contract, recovering migrate jobs interrupted by a daemon restart.
type Reconciler struct {
	Svc *instance.Service
}

// Reconcile decodes the job args and drives the interrupted migrate to a
// consistent state. Unparseable args are a permanent failure (the job cannot be
// acted on); an inconclusive host check leaves the job reconciling for retry.
func (r *Reconciler) Reconcile(ctx context.Context, job store.Job, jc *jobs.JobContext) (store.JobState, bool, error) {
	var req instance.MigrateRequest
	if err := json.Unmarshal(job.Args, &req); err != nil {
		jc.Step("reconcile-bad-args", err.Error())
		return store.JobFailed, true, nil
	}
	resolved, ok, err := r.Svc.ReconcileMigrate(ctx, req, jc.Step)
	if err != nil {
		return "", false, err
	}
	if !resolved {
		return "", false, nil
	}
	if ok {
		return store.JobSucceeded, true, nil
	}
	return store.JobFailed, true, nil
}

// Ensure Reconciler satisfies the runner contract.
var _ jobs.Reconciler = (*Reconciler)(nil)

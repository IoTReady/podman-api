package api

import (
	"context"
	"errors"
	"net/http"

	"github.com/iotready/podman-api/internal/registryprune"
	"github.com/iotready/podman-api/internal/store"
)

// RegistryPruner enqueues an on-demand registry prune run. Satisfied by
// *registryprune.Scheduler.
//
// It is deliberately the SCHEDULER and not the job store: the scheduler's
// in-flight check is the only thing preventing two runs from classifying the
// same catalog while each mutates it, and a route that wrote to the store
// directly would walk straight past it. The route exposes the guard; it does
// not bypass it.
type RegistryPruner interface {
	EnqueueNow(ctx context.Context) (store.Job, error)
}

// enqueueRegistryPrune serves POST /registry/prune.
//
// The trigger exists because the interval gate is backed by persisted job
// history, so it survives a restart: without this route, a second run inside
// the configured interval can only be had by editing -registry-prune-interval
// and restarting. #220's staged rollout — dry run, read the steps, Stage-A-only
// real run, verify, then Stage B — would otherwise cost a day per step.
//
// It takes no body. Everything about the run (dry-run, policy, whether Stage B
// is enabled) comes from the server's own flags, so a caller cannot, for
// example, turn a dry-run deployment into a deleting one over HTTP.
func (h *handlers) enqueueRegistryPrune(w http.ResponseWriter, r *http.Request) {
	if h.pruner == nil {
		WriteJSON(w, http.StatusNotFound, ErrorBody{
			Code:    "not_found",
			Message: "registry prune is not configured (-registry-prune-enabled)",
		})
		return
	}
	job, err := h.pruner.EnqueueNow(r.Context())
	if err != nil {
		if errors.Is(err, registryprune.ErrRunInFlight) {
			// 409, not 202-with-the-existing-job: "your run started" and
			// "someone else's run is still going" are different answers, and
			// collapsing them would let an operator believe a fresh run began.
			WriteJSON(w, http.StatusConflict, ErrorBody{
				Code:    "conflict",
				Message: err.Error(),
			})
			return
		}
		WriteJSON(w, http.StatusInternalServerError, ErrorBody{
			Code:    "internal",
			Message: err.Error(),
		})
		return
	}
	WriteJSON(w, http.StatusAccepted, map[string]string{"job_id": job.ID})
}

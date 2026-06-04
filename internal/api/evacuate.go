package api

import (
	"encoding/json"
	"net/http"

	"github.com/iotready/podman-api/internal/instance"
)

func (h *handlers) evacuate(w http.ResponseWriter, r *http.Request) {
	if h.jobs == nil { // store disabled → evacuate unavailable
		WriteError(w, errJobsDisabled)
		return
	}
	var req instance.EvacuateRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		WriteJSON(w, http.StatusBadRequest, ErrorBody{Code: "invalid_body", Message: err.Error()})
		return
	}
	if _, err := h.svc.ResolveEvacuation(r.Context(), req); err != nil {
		WriteError(w, err)
		return
	}
	args, err := json.Marshal(req)
	if err != nil {
		WriteJSON(w, http.StatusInternalServerError, ErrorBody{Code: "internal", Message: err.Error()})
		return
	}
	job, err := h.jobs.Enqueue(r.Context(), "evacuate", args, "")
	if err != nil {
		WriteError(w, err)
		return
	}
	WriteJSON(w, http.StatusAccepted, map[string]string{"job_id": job.ID})
}

// evacuatePlan previews an evacuation: it resolves the map and runs the live
// destination preflight checks per instance, returning a per-move report. It is
// read-only — no job is enqueued and nothing is mutated. Map-level validation
// errors return the same 4xx the real POST /evacuate would.
func (h *handlers) evacuatePlan(w http.ResponseWriter, r *http.Request) {
	if h.jobs == nil { // store disabled → evacuate unavailable
		WriteError(w, errJobsDisabled)
		return
	}
	var req instance.EvacuateRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		WriteJSON(w, http.StatusBadRequest, ErrorBody{Code: "invalid_body", Message: err.Error()})
		return
	}
	plan, err := h.svc.PlanEvacuation(r.Context(), req)
	if err != nil {
		WriteError(w, err)
		return
	}
	WriteJSON(w, http.StatusOK, plan)
}

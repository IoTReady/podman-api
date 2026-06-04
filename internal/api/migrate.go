package api

import (
	"encoding/json"
	"net/http"

	"github.com/iotready/podman-api/internal/instance"
)

func (h *handlers) migrate(w http.ResponseWriter, r *http.Request) {
	if h.jobs == nil { // store disabled → migrate unavailable
		WriteError(w, errJobsDisabled)
		return
	}
	var req instance.MigrateRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		WriteJSON(w, http.StatusBadRequest, ErrorBody{Code: "invalid_body", Message: err.Error()})
		return
	}
	if err := h.svc.CheckMigratable(r.Context(), req); err != nil {
		WriteError(w, err)
		return
	}
	args, err := json.Marshal(req)
	if err != nil {
		WriteJSON(w, http.StatusInternalServerError, ErrorBody{Code: "internal", Message: err.Error()})
		return
	}
	job, err := h.jobs.Enqueue(r.Context(), "migrate", args, "")
	if err != nil {
		WriteError(w, err)
		return
	}
	WriteJSON(w, http.StatusAccepted, map[string]string{"job_id": job.ID})
}

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
	if !decodeBody(w, r, &req) {
		return
	}
	// An absent/empty body decodes to FromHost==ToHost=="", which
	// CheckMigratable's first check reports as ErrSameHost — "source and
	// destination host are the same", which is misleading for a request that
	// named no hosts at all. Check the required fields here so a missing
	// body still 400s for the right reason (#254 review finding 2).
	if req.FromHost == "" || req.ToHost == "" || req.Template == "" || req.Slug == "" {
		WriteJSON(w, http.StatusBadRequest, ErrorBody{Code: "invalid_body", Message: "from_host, to_host, template, and slug are required"})
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

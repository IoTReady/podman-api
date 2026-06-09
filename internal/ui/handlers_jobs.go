package ui

import (
	"net/http"

	"github.com/iotready/podman-api/internal/store"
)

func (u *UI) jobsList(w http.ResponseWriter, r *http.Request) {
	if u.cfg.Jobs == nil {
		u.render(w, r, http.StatusOK, "jobs", u.pageData(map[string]any{"ActivePage": "jobs", "Disabled": true}))
		return
	}
	jobs, err := u.cfg.Jobs.ListJobs(r.Context(), store.JobFilter{Limit: 50})
	if err != nil {
		u.renderError(w, r, err)
		return
	}
	u.render(w, r, http.StatusOK, "jobs", u.pageData(map[string]any{"ActivePage": "jobs", "Jobs": jobs}))
}

func (u *UI) jobDetail(w http.ResponseWriter, r *http.Request) {
	if u.cfg.Jobs == nil {
		// Render through renderError (404 + layout chrome) rather than a bare
		// http.NotFound, for consistency with the rest of the UI.
		u.renderError(w, r, store.ErrNotFound)
		return
	}
	j, err := u.cfg.Jobs.GetJob(r.Context(), r.PathValue("id"))
	if err != nil {
		u.renderError(w, r, err)
		return
	}
	u.render(w, r, http.StatusOK, "job-detail", u.pageData(map[string]any{"Job": j}))
}

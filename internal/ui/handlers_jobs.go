package ui

import (
	"net/http"

	"github.com/iotready/podman-api/internal/store"
)

func (u *UI) jobsList(w http.ResponseWriter, r *http.Request) {
	if u.cfg.Jobs == nil {
		u.render(w, r, http.StatusOK, "jobs", u.pageData(map[string]any{"Disabled": true}))
		return
	}
	jobs, err := u.cfg.Jobs.ListJobs(r.Context(), store.JobFilter{})
	if err != nil {
		u.renderError(w, r, err)
		return
	}
	u.render(w, r, http.StatusOK, "jobs", u.pageData(map[string]any{"Jobs": jobs}))
}

func (u *UI) jobDetail(w http.ResponseWriter, r *http.Request) {
	if u.cfg.Jobs == nil {
		http.NotFound(w, r)
		return
	}
	j, err := u.cfg.Jobs.GetJob(r.Context(), r.PathValue("id"))
	if err != nil {
		u.renderError(w, r, err)
		return
	}
	u.render(w, r, http.StatusOK, "job-detail", u.pageData(map[string]any{"Job": j}))
}

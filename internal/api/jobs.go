package api

import (
	"encoding/json"
	"errors"
	"net/http"
	"time"

	"github.com/iotready/podman-api/internal/store"
)

// errJobsDisabled is returned by the jobs endpoints when no JobStore is wired
// (the -state-db store is off). It classifies to 501 Not Implemented.
var errJobsDisabled = errors.New("jobs store not enabled (set -state-db)")

type stepView struct {
	TS     string `json:"ts"`
	Step   string `json:"step"`
	Detail string `json:"detail,omitempty"`
}

type jobView struct {
	ID       string          `json:"id"`
	Kind     string          `json:"kind"`
	State    string          `json:"state"`
	Args     json.RawMessage `json:"args"`
	Steps    []stepView      `json:"steps"`
	ParentID string          `json:"parent_id,omitempty"`
	Error    string          `json:"error,omitempty"`
	Created  string          `json:"created"`
	Started  string          `json:"started,omitempty"`
	Finished string          `json:"finished,omitempty"`
}

func toJobView(j store.Job) jobView {
	v := jobView{
		ID: j.ID, Kind: j.Kind, State: string(j.State), Args: j.Args,
		Steps: []stepView{}, ParentID: j.ParentID, Error: j.Error,
		Created: j.Created.UTC().Format(time.RFC3339),
	}
	if v.Args == nil {
		v.Args = json.RawMessage("null")
	}
	for _, s := range j.Steps {
		v.Steps = append(v.Steps, stepView{
			TS: s.TS.UTC().Format(time.RFC3339), Step: s.Step, Detail: s.Detail,
		})
	}
	if !j.Started.IsZero() {
		v.Started = j.Started.UTC().Format(time.RFC3339)
	}
	if !j.Finished.IsZero() {
		v.Finished = j.Finished.UTC().Format(time.RFC3339)
	}
	return v
}

func (h *handlers) listJobs(w http.ResponseWriter, r *http.Request) {
	if h.jobs == nil {
		WriteError(w, errJobsDisabled)
		return
	}
	// Empty state/kind query params become zero-value filter fields, which the
	// store treats as "match all".
	f := store.JobFilter{
		State: store.JobState(r.URL.Query().Get("state")),
		Kind:  r.URL.Query().Get("kind"),
	}
	jobs, err := h.jobs.ListJobs(r.Context(), f)
	if err != nil {
		WriteError(w, err)
		return
	}
	out := make([]jobView, 0, len(jobs))
	for _, j := range jobs {
		out = append(out, toJobView(j))
	}
	WriteJSON(w, http.StatusOK, out)
}

func (h *handlers) getJob(w http.ResponseWriter, r *http.Request) {
	if h.jobs == nil {
		WriteError(w, errJobsDisabled)
		return
	}
	j, err := h.jobs.GetJob(r.Context(), r.PathValue("id"))
	if err != nil {
		WriteError(w, err)
		return
	}
	WriteJSON(w, http.StatusOK, toJobView(j))
}

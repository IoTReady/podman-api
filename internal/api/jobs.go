package api

import (
	"encoding/json"
	"errors"
	"net/http"
	"strconv"
	"time"

	"github.com/iotready/podman-api/internal/store"
)

// errJobsDisabled is returned by the jobs endpoints when no JobStore is wired
// (the -state-db store is off). It classifies to 501 Not Implemented.
var errJobsDisabled = errors.New("jobs store not enabled (set -state-db)")

// JobCanceller cancels an in-flight (running) job. Implemented by *jobs.Runner.
// Nil when the job runner is not wired (no -state-db), in which case the cancel
// endpoint is unreachable anyway (the jobs-disabled guard precedes it).
type JobCanceller interface {
	Cancel(id string) bool
}

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
	// Empty state/kind/parent_id query params become zero-value filter fields,
	// which the store treats as "match all".
	f := store.JobFilter{
		State:    store.JobState(r.URL.Query().Get("state")),
		Kind:     r.URL.Query().Get("kind"),
		ParentID: r.URL.Query().Get("parent_id"),
		Before:   r.URL.Query().Get("before"),
	}
	if s := r.URL.Query().Get("limit"); s != "" {
		n, err := strconv.Atoi(s)
		if err != nil {
			WriteJSON(w, http.StatusBadRequest, ErrorBody{Code: "invalid_query", Message: "limit must be an integer"})
			return
		}
		f.Limit = n // store clamps to [1, MaxJobLimit]
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

func (h *handlers) cancelJob(w http.ResponseWriter, r *http.Request) {
	if h.jobs == nil {
		WriteError(w, errJobsDisabled)
		return
	}
	id := r.PathValue("id")
	j, err := h.jobs.GetJob(r.Context(), id)
	if err != nil {
		WriteError(w, err) // store.ErrNotFound -> 404
		return
	}

	switch j.State {
	case store.JobSucceeded, store.JobFailed, store.JobCanceled:
		WriteJSON(w, http.StatusConflict,
			ErrorBody{Code: "job_terminal", Message: "job is already in a terminal state"})
		return

	case store.JobRunning:
		if h.canceller != nil && h.canceller.Cancel(id) {
			h.writeAcceptedJob(w, r, id)
			return
		}
		// Cancel returned false: the job either just finished, or was claimed but
		// not yet registered in the runner's in-flight map (a brief claim→register
		// window). Re-read to report the accurate reason.
		h.writeCancelConflict(w, r, id)
		return

	case store.JobReconciling:
		ok, err := h.jobs.CancelReconciling(r.Context(), id)
		if err != nil {
			WriteError(w, err)
			return
		}
		if ok {
			if h.canceller != nil {
				h.canceller.Cancel(id) // best-effort: interrupt an in-flight reconcile pass
			}
			h.writeAcceptedJob(w, r, id)
			return
		}
		// Lost the CAS to the reconcile loop — it just resolved. Report accurately.
		h.writeCancelConflict(w, r, id)
		return

	default: // queued
		ok, err := h.jobs.CancelQueued(r.Context(), id)
		if err != nil {
			WriteError(w, err)
			return
		}
		if ok {
			h.writeAcceptedJob(w, r, id)
			return
		}
		// Not queued any more — it raced into running. Try the in-flight registry;
		// if that misses too, re-read to report the accurate reason.
		if h.canceller != nil && h.canceller.Cancel(id) {
			h.writeAcceptedJob(w, r, id)
			return
		}
		h.writeCancelConflict(w, r, id)
	}
}

// writeCancelConflict re-reads the job and returns a 409 that distinguishes a
// genuinely terminal job from one that is merely not-yet-cancelable (a transient
// claim→register or queued→running race the operator can retry).
func (h *handlers) writeCancelConflict(w http.ResponseWriter, r *http.Request, id string) {
	j, err := h.jobs.GetJob(r.Context(), id)
	if err != nil {
		WriteError(w, err)
		return
	}
	switch j.State {
	case store.JobSucceeded, store.JobFailed, store.JobCanceled:
		WriteJSON(w, http.StatusConflict,
			ErrorBody{Code: "job_terminal", Message: "job is already in a terminal state"})
	default:
		WriteJSON(w, http.StatusConflict,
			ErrorBody{Code: "job_not_cancelable", Message: "job is not yet cancelable; retry shortly"})
	}
}

// writeAcceptedJob re-reads the job and returns it with 202. Running cancels are
// asynchronous, so the returned state may still be "running".
func (h *handlers) writeAcceptedJob(w http.ResponseWriter, r *http.Request, id string) {
	j, err := h.jobs.GetJob(r.Context(), id)
	if err != nil {
		WriteError(w, err)
		return
	}
	WriteJSON(w, http.StatusAccepted, toJobView(j))
}

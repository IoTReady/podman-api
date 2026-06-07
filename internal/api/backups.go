package api

import (
	"encoding/json"
	"net/http"
	"strconv"
	"time"

	"github.com/iotready/podman-api/internal/instance"
	"github.com/iotready/podman-api/internal/store"
)

// postBackup enqueues a backup job for an instance. The backup id is
// generated here so the response can carry it before the job runs.
func (h *handlers) postBackup(w http.ResponseWriter, r *http.Request) {
	if h.jobs == nil {
		WriteError(w, errJobsDisabled)
		return
	}
	host, tmpl, slug := r.PathValue("host"), r.PathValue("template"), r.PathValue("slug")
	if err := h.svc.CheckBackupable(r.Context(), host, tmpl, slug); err != nil {
		WriteError(w, err)
		return
	}
	req := instance.BackupRequest{BackupID: store.NewBackupID(), Host: host, Template: tmpl, Slug: slug}
	args, err := json.Marshal(req)
	if err != nil {
		WriteJSON(w, http.StatusInternalServerError, ErrorBody{Code: "internal", Message: err.Error()})
		return
	}
	job, err := h.jobs.Enqueue(r.Context(), "backup", args, "")
	if err != nil {
		WriteError(w, err)
		return
	}
	WriteJSON(w, http.StatusAccepted, map[string]string{"job_id": job.ID, "backup_id": req.BackupID})
}

// listBackups returns an instance's backups, newest first. ?limit= clamps to
// [1, 1000]; absent or <=0 uses the default of 100. The response envelope is
// {"backups":[...]} (deliberately wrapped for future extensibility).
func (h *handlers) listBackups(w http.ResponseWriter, r *http.Request) {
	host, tmpl, slug := r.PathValue("host"), r.PathValue("template"), r.PathValue("slug")
	limit := 0
	if v := r.URL.Query().Get("limit"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil {
			WriteJSON(w, http.StatusBadRequest, ErrorBody{Code: "invalid_request", Message: "limit must be an integer"})
			return
		}
		limit = n
	}
	backups, err := h.svc.ListBackups(r.Context(), host, tmpl, slug, limit)
	if err != nil {
		WriteError(w, err)
		return
	}
	WriteJSON(w, http.StatusOK, map[string]any{"backups": toBackupViews(backups)})
}

// BackupView is the JSON shape of one backup. Manifests are internal
// verification metadata and are not exposed; per-volume name+size are.
type BackupView struct {
	ID       string             `json:"id"`
	Host     string             `json:"host"`
	Template string             `json:"template"`
	Slug     string             `json:"slug"`
	State    string             `json:"state"`
	Image    string             `json:"image,omitempty"`
	Volumes  []BackupVolumeView `json:"volumes,omitempty"`
	Created  string             `json:"created"`
	Finished string             `json:"finished,omitempty"`
}

// BackupVolumeView is one exported volume's public metadata: name + tar size.
type BackupVolumeView struct {
	Name      string `json:"name"`
	SizeBytes int64  `json:"size_bytes"`
}

// toBackupViews maps store rows to their public JSON shape. Times are
// formatted as UTC RFC3339 (same format as the jobs view). Finished is empty
// while zero. Always returns a non-nil slice so the list field is [] not null.
func toBackupViews(bs []store.Backup) []BackupView {
	out := make([]BackupView, 0, len(bs))
	for _, b := range bs {
		v := BackupView{
			ID: b.ID, Host: b.Host, Template: b.Template, Slug: b.Slug,
			State: string(b.State), Image: b.Image,
			Created: b.Created.UTC().Format(time.RFC3339),
		}
		for _, vol := range b.Volumes {
			v.Volumes = append(v.Volumes, BackupVolumeView{Name: vol.Name, SizeBytes: vol.SizeBytes})
		}
		if !b.Finished.IsZero() {
			v.Finished = b.Finished.UTC().Format(time.RFC3339)
		}
		out = append(out, v)
	}
	return out
}

// postRestore enqueues a restore job for a backup.
func (h *handlers) postRestore(w http.ResponseWriter, r *http.Request) {
	if h.jobs == nil {
		WriteError(w, errJobsDisabled)
		return
	}
	id := r.PathValue("id")
	if _, err := h.svc.CheckRestorable(r.Context(), id); err != nil {
		WriteError(w, err)
		return
	}
	args, err := json.Marshal(instance.RestoreRequest{BackupID: id})
	if err != nil {
		WriteJSON(w, http.StatusInternalServerError, ErrorBody{Code: "internal", Message: err.Error()})
		return
	}
	job, err := h.jobs.Enqueue(r.Context(), "restore", args, "")
	if err != nil {
		WriteError(w, err)
		return
	}
	WriteJSON(w, http.StatusAccepted, map[string]string{"job_id": job.ID})
}

// deleteBackup synchronously removes a backup's blobs and row; refused with
// 409 while a backup or restore of it is in flight.
func (h *handlers) deleteBackup(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if err := instance.BackupDeletable(r.Context(), h.jobs, id); err != nil {
		WriteError(w, err)
		return
	}
	if err := h.svc.DeleteBackup(r.Context(), id); err != nil {
		WriteError(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

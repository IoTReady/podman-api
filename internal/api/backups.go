package api

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
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

	// The body is optional: every client predating volume scoping sends none,
	// and an ENTIRELY ABSENT body must keep meaning "every backupable volume".
	// That wire compatibility is load-bearing. io.EOF is therefore the ordinary
	// case, not an error.
	//
	// `volumes` is a *[]string so absent and `[]` are distinguishable. They are
	// NOT the same request: `{"volumes":[]}` (a UI where every box was
	// deselected) previously decoded to an empty slice, which the handler read
	// as "unscoped" and answered by stopping the pod and exporting every volume
	// the caller explicitly did not ask for, behind a 202. An explicit empty
	// array is rejected as an invalid scope instead — the safer reading, and the
	// same direction as extension/backup.go's promise that a scope "never
	// silently degrades to a smaller backup".
	//
	// DisallowUnknownFields catches the other half of that failure:
	// `{"volume":["data"]}` (singular typo) decoded cleanly to an empty slice
	// and was read as "everything" too.
	var body struct {
		Volumes *[]string `json:"volumes"`
	}
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	if err := dec.Decode(&body); err != nil && !errors.Is(err, io.EOF) {
		WriteJSON(w, http.StatusBadRequest, ErrorBody{Code: "invalid_request", Message: "body must be a JSON object with an optional \"volumes\" array: " + err.Error()})
		return
	}
	var volumes []string
	if body.Volumes != nil {
		if len(*body.Volumes) == 0 {
			WriteError(w, fmt.Errorf("%w: \"volumes\" is present but empty; omit the body entirely to back up every volume not marked `backup: none`", instance.ErrInvalidBackupScope))
			return
		}
		volumes = *body.Volumes
	}

	if err := h.svc.CheckBackupable(r.Context(), host, tmpl, slug, volumes); err != nil {
		WriteError(w, err)
		return
	}
	req := instance.BackupRequest{
		BackupID: store.NewBackupID(), Host: host, Template: tmpl, Slug: slug,
		Volumes: volumes,
	}
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

// ExcludedView reports the backup exclude patterns applied to one volume and
// what they removed. Absent when the volume was exported in full. (#248)
type ExcludedView struct {
	Patterns []string `json:"patterns"`
	Entries  int      `json:"entries"`
	Bytes    int64    `json:"bytes"`
}

// BackupVolumeView is one exported volume's public metadata: name, tar size,
// and — when the template declared exclude patterns — what they dropped.
type BackupVolumeView struct {
	Name      string        `json:"name"`
	SizeBytes int64         `json:"size_bytes"`
	Excluded  *ExcludedView `json:"excluded,omitempty"`
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
			vv := BackupVolumeView{Name: vol.Name, SizeBytes: vol.SizeBytes}
			if vol.Excluded != nil {
				vv.Excluded = &ExcludedView{
					Patterns: vol.Excluded.Patterns,
					Entries:  vol.Excluded.Entries,
					Bytes:    vol.Excluded.Bytes,
				}
			}
			v.Volumes = append(v.Volumes, vv)
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

// postPITRRestore enqueues a point-in-time restore job for an instance. The body
// carries the target timestamp (an opaque selector interpreted by the injector —
// the Litestream injector uses RFC3339) and an optional subset of volumes to
// restore; an empty timestamp is rejected. This is distinct from the
// /backups/{id}/restore tarball restore: PITR rolls a live instance back to a
// chosen instant via the injected restore initContainer.
func (h *handlers) postPITRRestore(w http.ResponseWriter, r *http.Request) {
	if h.jobs == nil {
		WriteError(w, errJobsDisabled)
		return
	}
	host, tmpl, slug := r.PathValue("host"), r.PathValue("template"), r.PathValue("slug")
	var body struct {
		Timestamp string   `json:"timestamp"`
		Volumes   []string `json:"volumes,omitempty"`
	}
	if !decodeBody(w, r, &body) {
		return
	}
	if strings.TrimSpace(body.Timestamp) == "" {
		WriteJSON(w, http.StatusBadRequest, ErrorBody{Code: "invalid_request", Message: "timestamp is required"})
		return
	}
	// PITR restores from the Litestream S3 replica via the injected initContainer,
	// not the tarball blob store — so the precondition is "instance exists", not
	// CheckBackupable (which would 501 a Litestream-only deployment).
	if err := h.svc.CheckInstanceExists(r.Context(), host, tmpl, slug); err != nil {
		WriteError(w, err)
		return
	}
	req := instance.PITRRestoreRequest{
		Host: host, Template: tmpl, Slug: slug,
		Timestamp: body.Timestamp, Volumes: body.Volumes,
	}
	args, err := json.Marshal(req)
	if err != nil {
		WriteJSON(w, http.StatusInternalServerError, ErrorBody{Code: "internal", Message: err.Error()})
		return
	}
	job, err := h.jobs.Enqueue(r.Context(), "pitr-restore", args, "")
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

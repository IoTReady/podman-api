package ui

import (
	"context"
	"encoding/json"
	"net/http"

	"github.com/iotready/podman-api/internal/instance"
	"github.com/iotready/podman-api/internal/store"
)

// renderInstanceNotice re-renders the instance detail with a notice banner.
func (u *UI) renderInstanceNotice(w http.ResponseWriter, r *http.Request, host, tmpl, slug, notice string) {
	obs, err := u.cfg.Svc.Get(r.Context(), host, tmpl, slug)
	if err != nil {
		u.renderError(w, r, err)
		return
	}
	data := u.instanceView(r.Context(), host, obs)
	data["Notice"] = notice
	u.render(w, r, http.StatusOK, "instance-detail", u.pageData(data))
}

// renderInstanceActionError re-renders the instance detail with an error
// banner (mirrors lifecycle's failure path).
func (u *UI) renderInstanceActionError(w http.ResponseWriter, r *http.Request, host, tmpl, slug string, actionErr error) {
	obs, gerr := u.cfg.Svc.Get(r.Context(), host, tmpl, slug)
	if gerr != nil {
		u.renderError(w, r, actionErr)
		return
	}
	data := u.instanceView(r.Context(), host, obs)
	data["ActionError"] = actionErr.Error()
	u.render(w, r, errorStatus(actionErr), "instance-detail", u.pageData(data))
}

// backupNow enqueues a backup job and re-renders the detail with a notice.
func (u *UI) backupNow(w http.ResponseWriter, r *http.Request) {
	host, tmpl, slug := r.PathValue("host"), r.PathValue("template"), r.PathValue("slug")
	ctx := r.Context()
	if err := u.cfg.Svc.CheckBackupable(ctx, host, tmpl, slug); err != nil {
		u.renderInstanceActionError(w, r, host, tmpl, slug, err)
		return
	}
	req := instance.BackupRequest{BackupID: store.NewBackupID(), Host: host, Template: tmpl, Slug: slug}
	args, err := json.Marshal(req)
	if err != nil {
		u.renderError(w, r, err)
		return
	}
	job, err := u.cfg.Jobs.Enqueue(ctx, "backup", args, "")
	if err != nil {
		u.renderInstanceActionError(w, r, host, tmpl, slug, err)
		return
	}
	u.renderInstanceNotice(w, r, host, tmpl, slug, "Backup started — job "+job.ID)
}

// restoreBackup enqueues a restore job for a backup id.
func (u *UI) restoreBackup(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	ctx := r.Context()
	b, err := u.cfg.Svc.CheckRestorable(ctx, id)
	if err != nil {
		u.renderError(w, r, err)
		return
	}
	args, merr := json.Marshal(instance.RestoreRequest{BackupID: id})
	if merr != nil {
		u.renderError(w, r, merr)
		return
	}
	job, err := u.cfg.Jobs.Enqueue(ctx, "restore", args, "")
	if err != nil {
		u.renderInstanceActionError(w, r, b.Host, b.Template, b.Slug, err)
		return
	}
	u.renderInstanceNotice(w, r, b.Host, b.Template, b.Slug, "Restore started — job "+job.ID)
}

// deleteBackup removes a backup (blobs + row); refused mid-backup/restore.
func (u *UI) deleteBackup(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	ctx := r.Context()
	b, err := u.cfg.Svc.GetBackup(ctx, id)
	if err != nil {
		u.renderError(w, r, err)
		return
	}
	if u.cfg.Jobs != nil {
		for _, inFlight := range []func(context.Context, store.JobStore, string) (bool, error){instance.RestoreInFlight, instance.BackupInFlight} {
			busy, berr := inFlight(ctx, u.cfg.Jobs, id)
			if berr != nil {
				u.renderError(w, r, berr)
				return
			}
			if busy {
				u.renderInstanceActionError(w, r, b.Host, b.Template, b.Slug, instance.ErrBackupBusy)
				return
			}
		}
	}
	if err := u.cfg.Svc.DeleteBackup(ctx, id); err != nil {
		u.renderInstanceActionError(w, r, b.Host, b.Template, b.Slug, err)
		return
	}
	u.renderInstanceNotice(w, r, b.Host, b.Template, b.Slug, "Backup "+id+" deleted")
}

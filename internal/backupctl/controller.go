package backupctl

import (
	"context"
	"encoding/json"
	"log"
	"time"

	"github.com/iotready/podman-api/extension"
	"github.com/iotready/podman-api/internal/config"
	"github.com/iotready/podman-api/internal/instance"
	"github.com/iotready/podman-api/internal/store"
)

// Service is the slice of *instance.Service the controller needs. Declaring it
// as an interface keeps the controller unit-testable with a fake.
type Service interface {
	Hosts() []config.Host
	ListAllInstances(ctx context.Context, host string) ([]instance.Observed, error)
	GetTemplate(ctx context.Context, id string) (store.Template, error)
	ListBackups(ctx context.Context, host, template, slug string, limit int) ([]store.Backup, error)
	CheckBackupable(ctx context.Context, host, template, slug string) error
}

// Controller is the core-side implementation of extension.BackupController. It
// projects backup-eligible instances, reports last-success times, and enqueues
// backup jobs over the same path as the HTTP POST /backups handler.
type Controller struct {
	Svc  Service
	Jobs store.JobStore
}

// ListBackupInstances walks every known host, lists its live instances, and
// returns those whose template declares at least one backup-marked volume. A
// per-host listing failure is logged and skipped rather than aborting the whole
// sweep, so one unreachable host doesn't starve backups on the others.
func (c *Controller) ListBackupInstances(ctx context.Context) ([]extension.BackupInstance, error) {
	markersByTmpl := map[string][]extension.BackupVolumeMarker{}
	var out []extension.BackupInstance
	for _, h := range c.Svc.Hosts() {
		obs, err := c.Svc.ListAllInstances(ctx, h.ID)
		if err != nil {
			log.Printf("backupctl: list instances on host %s: %v (skipping host)", h.ID, err)
			continue
		}
		for _, o := range obs {
			markers, ok := markersByTmpl[o.Template]
			if !ok {
				markers = c.backupMarkers(ctx, o.Template)
				markersByTmpl[o.Template] = markers
			}
			if len(markers) == 0 {
				continue
			}
			out = append(out, extension.BackupInstance{
				Host:     h.ID,
				Template: o.Template,
				Slug:     o.Slug,
				Volumes:  markers,
			})
		}
	}
	return out, nil
}

// backupMarkers projects a template's backup-marked volumes. An unknown template
// (or one with no marked volumes) yields an empty slice.
func (c *Controller) backupMarkers(ctx context.Context, template string) []extension.BackupVolumeMarker {
	t, err := c.Svc.GetTemplate(ctx, template)
	if err != nil {
		log.Printf("backupctl: template %s: %v (skipping)", template, err)
		return nil
	}
	var markers []extension.BackupVolumeMarker
	for _, v := range t.Meta.Volumes {
		if v.Backup != "" {
			markers = append(markers, extension.BackupVolumeMarker{Name: v.Name, Backup: v.Backup})
		}
	}
	return markers
}

// LastBackupAt returns the finish time of the newest complete backup for an
// instance, or the zero time if none exists. ListBackups is newest-first, so the
// first complete row wins.
func (c *Controller) LastBackupAt(ctx context.Context, host, template, slug string) (time.Time, error) {
	backups, err := c.Svc.ListBackups(ctx, host, template, slug, 0)
	if err != nil {
		return time.Time{}, err
	}
	for _, b := range backups {
		if b.State == store.BackupComplete {
			return b.Finished, nil
		}
	}
	return time.Time{}, nil
}

// EnqueueBackup enqueues a backup job for one instance, deduping against any
// backup already queued/running/reconciling for the same instance.
func (c *Controller) EnqueueBackup(ctx context.Context, host, template, slug string) (string, error) {
	inFlight, err := c.backupInFlight(ctx, host, template, slug)
	if err != nil {
		return "", err
	}
	if inFlight {
		return "", nil
	}
	if err := c.Svc.CheckBackupable(ctx, host, template, slug); err != nil {
		return "", err
	}
	req := instance.BackupRequest{BackupID: store.NewBackupID(), Host: host, Template: template, Slug: slug}
	args, err := json.Marshal(req)
	if err != nil {
		return "", err
	}
	job, err := c.Jobs.Enqueue(ctx, "backup", args, "")
	if err != nil {
		return "", err
	}
	return job.ID, nil
}

// backupInFlight reports whether a backup job targeting this instance is in a
// non-terminal state. Mirrors instance.BackupInFlight but matches on the
// host/template/slug triple instead of a specific backup id.
func (c *Controller) backupInFlight(ctx context.Context, host, template, slug string) (bool, error) {
	for _, st := range []store.JobState{store.JobQueued, store.JobRunning, store.JobReconciling} {
		jobs, err := c.Jobs.ListJobs(ctx, store.JobFilter{State: st, Kind: "backup", Limit: store.MaxJobLimit})
		if err != nil {
			return false, err
		}
		for _, j := range jobs {
			var req instance.BackupRequest
			if err := json.Unmarshal(j.Args, &req); err != nil {
				continue
			}
			if req.Host == host && req.Template == template && req.Slug == slug {
				return true, nil
			}
		}
	}
	return false, nil
}

package backupctl

import (
	"context"
	"encoding/json"
	"fmt"
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
	CheckBackupable(ctx context.Context, host, template, slug string, volumes []string) error
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
		// `none` is the one marker the core interprets: the volume is never
		// backed up, so projecting it would only make a scheduler re-derive the
		// same veto — and an instance whose only marked volume is `none` is not
		// backup-eligible at all.
		if v.Backup == "" || instance.IsBackupMarkerNone(v.Backup) {
			continue
		}
		markers = append(markers, extension.BackupVolumeMarker{Name: v.Name, Backup: v.Backup})
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

// EnqueueBackup enqueues a backup job for one instance, deduping against a
// backup already queued/running/reconciling for the same instance WHOSE SCOPE
// COVERS the requested one.
//
// The scope is validated BEFORE the dedupe check, deliberately. The other order
// answers a misconfigured scheduler — a typo'd volume name, or one newly marked
// `backup: none` — with the documented "already in flight, nothing to do"
// (`"", nil`), which a scheduler records as a handled tick. The misconfiguration
// then stays invisible for as long as backups keep overlapping, while the volume
// the scheduler believes it is protecting is never captured. A bad scope is a
// bad scope whether or not a run happens to be in flight.
//
// Coverage, not mere per-instance presence, is what the dedupe keys on. Per
// instance was safe while every enqueue for an instance requested the same
// work; with scopes it is not — a `{Volumes:["wal"]}` tick landing during an
// unscoped nightly run would get `("", nil)`, "already handled", for work that
// run does do, but a `{Volumes:["wal"]}` tick landing during a
// `{Volumes:["data"]}` run would get the same answer for work nobody is doing.
// That window's snapshot is then never taken and never retried, silently.
func (c *Controller) EnqueueBackup(ctx context.Context, host, template, slug string, opts extension.BackupOptions) (string, error) {
	// nil means unscoped; an explicitly EMPTY scope is an error rather than an
	// escalation to a full-instance backup (see extension.BackupOptions).
	var volumes []string
	if opts.Volumes != nil {
		if len(*opts.Volumes) == 0 {
			return "", fmt.Errorf("%w: an explicitly empty volume scope requests nothing; pass a nil scope for a full-instance backup", instance.ErrInvalidBackupScope)
		}
		volumes = *opts.Volumes
	}
	if err := c.Svc.CheckBackupable(ctx, host, template, slug, volumes); err != nil {
		return "", err
	}
	// Reconciling is not COVERAGE (below) but it IS CONCURRENCY: the instance is
	// mid-recovery from a crashed backup and ReconcileBackup is about to restart
	// it. Enqueueing anything now — whatever its scope — starts a second backup
	// against an instance the crash left STOPPED, which records wasRunning=false
	// and so never restarts the pod afterwards: two rows for one window and a
	// green backup on an instance that is not serving. Defer the tick instead;
	// the next one lands after the reconcile has settled.
	//
	// The deferral is reported as extension.ErrBackupDeferred, NOT as the
	// `("", nil)` a covered tick returns (review-5 finding 6). Those two answers
	// mean opposite things — "the window is handled" vs "nothing captured it and
	// nothing will" — and a scheduler that re-arms its interval gate on `nil`
	// would record every window a multi-sweep reconcile spans as taken. That is
	// the exact silent drop the coverage check exists to prevent, relocated to
	// the deferral path.
	reconciling, err := c.backupReconciling(ctx, host, template, slug)
	if err != nil {
		return "", err
	}
	if reconciling {
		return "", fmt.Errorf("%w: %s/%s/%s", extension.ErrBackupDeferred, host, template, slug)
	}
	covered, err := c.backupInFlightCovering(ctx, host, template, slug, volumes)
	if err != nil {
		return "", err
	}
	if covered {
		return "", nil
	}
	req := instance.BackupRequest{
		BackupID: store.NewBackupID(), Host: host, Template: template, Slug: slug,
		Volumes: volumes,
	}
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

// scopeCovers reports whether an in-flight job's scope covers a requested one.
// An unscoped in-flight job (empty inFlight) captures every volume not vetoed,
// so it covers everything, including an unscoped request. A scoped in-flight
// job covers only a request whose volumes are all in its own set — and never an
// unscoped request, which asks for more than it will capture.
func scopeCovers(inFlight, want []string) bool {
	if len(inFlight) == 0 {
		return true
	}
	if len(want) == 0 {
		return false
	}
	have := make(map[string]bool, len(inFlight))
	for _, v := range inFlight {
		have[v] = true
	}
	for _, v := range want {
		if !have[v] {
			return false
		}
	}
	return true
}

// backupInFlightCovering reports whether a backup job targeting this instance
// is in a non-terminal state AND its scope covers the requested volumes. The
// in-flight job's own scope is read back out of its persisted args, which is
// where the enqueue wrote it.
//
// JobReconciling is deliberately NOT counted as coverage (round-3 finding 9).
// It is non-terminal, but ReconcileBackup only fails the row, reaps partial
// blobs and restarts the instance — it never exports anything. Counting it
// would answer a tick that landed on a crashed job with "already handled", and
// that window's snapshot would then never be taken and never retried: exactly
// the silent drop this coverage check exists to prevent, on the one job state
// guaranteed to produce no backup. It is handled by backupReconciling instead,
// which DEFERS the tick rather than satisfying it — reconciling is concurrency,
// not coverage (review-4 finding 6).
func (c *Controller) backupInFlightCovering(ctx context.Context, host, template, slug string, want []string) (bool, error) {
	for _, st := range []store.JobState{store.JobQueued, store.JobRunning} {
		jobs, err := c.Jobs.ListJobs(ctx, store.JobFilter{State: st, Kind: "backup", Limit: store.MaxJobLimit})
		if err != nil {
			return false, err
		}
		for _, j := range jobs {
			var req instance.BackupRequest
			if err := json.Unmarshal(j.Args, &req); err != nil {
				continue
			}
			if req.Host == host && req.Template == template && req.Slug == slug &&
				scopeCovers(req.Volumes, want) {
				return true, nil
			}
		}
	}
	return false, nil
}

// backupReconciling reports whether a backup job for this instance is sitting
// in JobReconciling — a crashed run whose ReconcileBackup sweep has not yet
// failed the row and restarted the instance.
//
// Scope is deliberately NOT consulted. A reconciling job covers nothing (it
// exports no volume, whatever its scope), so this is not a coverage question at
// all: the instance is mid-recovery and, right now, very likely STOPPED. Any
// backup started against it captures a stopped instance and, recording
// wasRunning=false, leaves it stopped afterwards. So a reconciling job of ANY
// scope defers a tick of ANY scope.
func (c *Controller) backupReconciling(ctx context.Context, host, template, slug string) (bool, error) {
	jobs, err := c.Jobs.ListJobs(ctx, store.JobFilter{State: store.JobReconciling, Kind: "backup", Limit: store.MaxJobLimit})
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
	return false, nil
}

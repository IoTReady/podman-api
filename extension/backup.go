package extension

import (
	"context"
	"time"
)

// BackupInstance is one live instance that has at least one backup-marked
// volume, projected for a commercial BackupScheduler to act on. Volumes carries
// only the backup-marked volumes, each with its raw marker string. The marker
// grammar (e.g. cadence, mode) is owned by the commercial layer — the core
// projects the string verbatim and ascribes no meaning to it beyond
// "non-empty == marked for backup".
type BackupInstance struct {
	Host     string
	Template string
	Slug     string
	Volumes  []BackupVolumeMarker
}

// BackupVolumeMarker pairs a volume name with its raw backup marker.
type BackupVolumeMarker struct {
	Name   string
	Backup string // raw marker, e.g. "s3; interval=6h"; never empty here
}

// BackupController is handed to a registered BackupScheduler so it can drive
// scheduled backups without reaching into internal/ packages. It exposes only
// the three capabilities a scheduler needs: discover backup-eligible instances,
// learn when each last succeeded, and enqueue a backup job.
type BackupController interface {
	// ListBackupInstances returns every live instance (across all known hosts)
	// that has at least one backup-marked volume, with those markers attached.
	ListBackupInstances(ctx context.Context) ([]BackupInstance, error)

	// LastBackupAt returns the finish time of the newest successful (complete)
	// backup for an instance, or the zero time if none exists. A scheduler uses
	// this for its interval gate.
	LastBackupAt(ctx context.Context, host, template, slug string) (time.Time, error)

	// EnqueueBackup enqueues a backup job for one instance (snapshotting all of
	// its backup-marked volumes) over the same path the HTTP POST /backups
	// handler uses, returning the new job id.
	//
	// It is authoritative for in-flight dedupe: if a backup job for this
	// instance is already queued, running, or reconciling, it enqueues nothing
	// and returns an empty jobID with a nil error. This lets a scheduler re-tick
	// freely without flooding the job store while a backup is still in progress
	// (a queued backup does not update LastBackupAt until it finishes).
	EnqueueBackup(ctx context.Context, host, template, slug string) (jobID string, err error)
}

// BackupScheduler is the commercial hook for scheduled volume backups. When one
// is registered via server.WithBackupScheduler, the server starts it after
// wiring and runs it until the server's run context is cancelled, passing a
// BackupController. The implementation owns all timing, interval, and
// marker-grammar policy; the core ships no scheduling behavior of its own.
type BackupScheduler interface {
	Run(ctx context.Context, c BackupController) error
}

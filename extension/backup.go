package extension

import (
	"context"
	"strings"
	"time"
)

// BackupMarkerNone is the one `backup:` marker literal the core interprets: a
// volume declaring it is never exported by a backup, on any path. Every other
// marker string stays opaque and belongs to the commercial marker grammar.
//
// It is exported here — the module's public seam — so a commercial consumer
// links against the literal at compile time instead of hardcoding "none" and
// silently drifting from the core. This is the CANONICAL definition; the
// internal render and instance packages alias it rather than declaring copies.
const BackupMarkerNone = "none"

// IsBackupMarkerNone reports whether a raw `backup:` marker is the veto.
//
// The comparison fails CLOSED: a marker equal to "none" after trimming
// surrounding whitespace and case-folding (`None`, `NONE`, `"none "`) vetoes
// the volume. Registration rejects those spellings outright — an author who
// writes `None` is told rather than guessed at — but registration only runs on
// the write path, so a template row persisted before that validator existed
// still carries the defect. For a marker whose entire job is "never capture
// this", the wrong direction to fail on a near-miss is open: the volume would
// be exported into every blob against the operator's explicit veto, with no
// error and no warning. Every comparison site goes through this function so
// they cannot drift.
func IsBackupMarkerNone(marker string) bool {
	return strings.EqualFold(strings.TrimSpace(marker), BackupMarkerNone)
}

// BackupInstance is one live instance that has at least one backup-marked
// volume, projected for a commercial BackupScheduler to act on. Volumes carries
// only the backup-marked volumes, each with its raw marker string. The core
// interprets exactly one literal — `none`, meaning never back this volume up,
// which is filtered out before projection so a scheduler never sees one. Every
// other non-empty marker value is opaque and belongs to the commercial marker
// grammar (e.g. cadence, mode).
type BackupInstance struct {
	Host     string
	Template string
	Slug     string
	Volumes  []BackupVolumeMarker
}

// BackupVolumeMarker pairs a volume name with its raw backup marker. The core
// interprets exactly one literal — `none`, meaning never back this volume up,
// which is filtered out before projection so it never reaches a scheduler.
// Every other value is opaque and belongs to the commercial marker grammar.
type BackupVolumeMarker struct {
	Name   string
	Backup string // raw marker, e.g. "s3; interval=6h"; never empty here
}

// BackupOptions narrows what a backup job captures. It is a struct rather than
// a bare parameter because further knobs are planned (a per-volume mode, an
// opaque instance id): growing a struct is additive, growing a parameter list
// breaks the interface again each time.
type BackupOptions struct {
	// Volumes lists the template's declared (short) volume names to snapshot,
	// e.g. ["sites"].
	//
	// It is a POINTER so absent and empty are distinguishable, exactly as the
	// HTTP layer distinguishes an absent `volumes` key from `"volumes": []`:
	//
	//	nil          -> unscoped: every declared volume not marked `none`
	//	&[]string{}  -> an error; it never escalates to a full-instance backup
	//	&[]string{…} -> exactly those volumes
	//
	// A scheduler that builds its scope by filtering (every tracked volume just
	// re-marked `none`, a config reload that emptied the list) would otherwise
	// pass an empty slice, receive a job id, and record a narrow request as
	// handled — while the job it actually got stopped the pod and captured
	// everything.
	//
	// Naming a volume the template does not declare, or one marked `none`,
	// fails the call — it never silently degrades to a smaller backup.
	Volumes *[]string
}

// BackupController is handed to a registered BackupScheduler so it can drive
// scheduled backups without reaching into internal/ packages. It exposes only
// the three capabilities a scheduler needs: discover backup-eligible instances,
// learn when each last succeeded, and enqueue a backup job.
type BackupController interface {
	// ListBackupInstances returns every live instance (across all known hosts)
	// that has at least one volume whose marker is not `none`, with those
	// markers attached.
	ListBackupInstances(ctx context.Context) ([]BackupInstance, error)

	// LastBackupAt returns the finish time of the newest successful (complete)
	// backup for an instance, or the zero time if none exists. A scheduler uses
	// this for its interval gate.
	LastBackupAt(ctx context.Context, host, template, slug string) (time.Time, error)

	// EnqueueBackup enqueues a backup job for one instance over the same path
	// the HTTP POST .../backup handler uses, returning the new job id.
	// opts.Volumes narrows what is captured; a nil scope means every declared
	// volume not marked `none`, and an explicitly EMPTY one is an error.
	//
	// It is authoritative for in-flight dedupe: if a backup job for this
	// instance is already in flight (queued, running, or reconciling) AND its
	// scope COVERS the requested one, it enqueues nothing and returns an empty
	// jobID with a nil error. An in-flight unscoped job covers every request; a
	// scoped one covers only requests whose volumes are a subset of its own.
	// A request the in-flight job does not cover is enqueued normally — it asks
	// for work that run will not do, and swallowing it would drop that window's
	// snapshot with no error and no retry.
	EnqueueBackup(ctx context.Context, host, template, slug string, opts BackupOptions) (jobID string, err error)
}

// BackupScheduler is the commercial hook for scheduled volume backups. When one
// is registered via server.WithBackupScheduler, the server starts it after
// wiring and runs it until the server's run context is cancelled, passing a
// BackupController. The implementation owns all timing, interval, and
// marker-grammar policy; the core ships no scheduling behavior of its own.
type BackupScheduler interface {
	Run(ctx context.Context, c BackupController) error
}

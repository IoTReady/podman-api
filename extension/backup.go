package extension

import (
	"context"
	"errors"
	"strings"
	"time"
)

// ErrBackupDeferred is returned by BackupController.EnqueueBackup when the
// instance is temporarily unable to accept a backup and the tick was NOT
// serviced by anything else. It is distinct from the "already covered by an
// in-flight run" answer (`"", nil`) precisely because that one means the window
// IS handled and this one means it is not: a scheduler that re-arms its interval
// gate on a deferral silently drops every window the deferral spans.
//
// The correct response is to retry on the next sweep without treating the
// window as satisfied. It is not a misconfiguration and should not be alerted on
// as a failure — the condition (a crashed backup mid-reconcile) clears by
// itself.
var ErrBackupDeferred = errors.New("backup deferred: the instance is recovering from an interrupted backup")

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

// BackupInstance is one live instance that has at least one backup-ELIGIBLE
// volume, projected for a commercial BackupScheduler to act on. Volumes
// carries every eligible volume — including an unmarked one (empty
// `Backup`), on the same footing as an explicitly marked one, since #255 —
// each with its raw marker string. The core interprets exactly one literal —
// `none`, meaning never back this volume up, which is filtered out before
// projection so a scheduler never sees one. Every other marker value,
// including empty, is opaque and belongs to the commercial marker grammar
// (e.g. cadence, mode).
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
	Name string
	// Backup is the raw marker, e.g. "s3; interval=6h" — or the empty string
	// for a declared volume that carries no `backup:` marker at all. Since
	// #255, unmarked volumes are projected here on the same footing as marked
	// ones (only an explicit `none` is filtered out beforehand), so a consumer
	// MUST treat "" as "eligible, no marker" rather than assume it can't occur.
	Backup string
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

	// Mode selects the backup mechanism ("" / "snapshot" = the existing
	// stop/export/restart flow; "live" = exec-based, never stops the pod —
	// see the core's Service.liveBackup). A commercial scheduler sets this
	// from its own opaque `mode=` marker grammar; the core does not interpret
	// the marker string itself, only this already-decided value.
	Mode string
}

// Backup is one backup row, projected for a retention policy. It carries
// state, timing, and total size only — deliberately WITHOUT the per-file
// sha256 manifest that store.Backup.Volumes carries, which is what makes a
// bulk listing expensive: a single Frappe sites-tree manifest alone is
// ~92MB, and a fleet-wide retention pass listing hundreds of backups must not
// deserialize that into memory just to decide what to prune.
type Backup struct {
	ID       string
	Host     string
	Template string
	Slug     string
	// State is the backup's lifecycle state — "creating", "complete", or
	// "failed" (the string form of the core's store.BackupState; kept as a
	// plain string here since this package does not import internal/store).
	// Only "complete" and "failed" are terminal; DeleteBackup refuses any
	// other state.
	State    string
	Created  time.Time
	Finished time.Time // zero until complete/failed
	// SizeBytes is the sum of every exported volume's tar size, zero for a
	// backup that never completed.
	SizeBytes int64
}

// BackupController is handed to a registered BackupScheduler so it can drive
// scheduled backups without reaching into internal/ packages. It exposes
// discovery of backup-eligible instances, learning when each last succeeded,
// enqueuing a backup job, listing an instance's backups, and deleting one
// completely — so a downstream retention policy can prune from the SAME
// inventory the API lists, rather than re-deriving one from the blob store's
// key space (see IoTReadyNext/podman-api#292).
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
	// It is authoritative for in-flight dedupe, and distinguishes two answers a
	// scheduler must NOT conflate:
	//
	//   - COVERED — a backup job for this instance is already queued or running
	//     AND its scope COVERS the requested one. It enqueues nothing and
	//     returns an empty jobID with a NIL error: the window is handled, and a
	//     scheduler may re-arm its interval gate on it. An in-flight unscoped
	//     job covers every request; a scoped one covers only requests whose
	//     volumes are a subset of its own. A request the in-flight job does not
	//     cover is enqueued normally — it asks for work that run will not do,
	//     and swallowing it would drop that window's snapshot with no error and
	//     no retry.
	//
	//   - DEFERRED — a backup job for this instance is RECONCILING, i.e. a
	//     crashed run whose recovery sweep has not yet failed the row and
	//     restarted the (currently stopped) pod. Reconciling exports nothing, so
	//     it is not coverage at ANY scope; but starting a second backup now
	//     would snapshot a stopped instance and leave it stopped. The call
	//     returns an empty jobID and an error wrapping ErrBackupDeferred.
	//     Nothing was captured and nothing is in flight that will capture it —
	//     retry on the next sweep, and do NOT record the window as satisfied.
	//
	// COST OF DISTINCT SCOPES. Because dedupe is by coverage rather than by
	// instance, two ticks for the same instance with scopes neither of which
	// covers the other BOTH enqueue. Each resulting job takes the instance lock,
	// stops the pod, exports, and restarts it — so a scheduler emitting per-
	// volume ticks buys one stop/start cycle PER SCOPE, serialized, not one for
	// the window. That is the honest cost of not silently dropping a snapshot
	// nobody else is taking, but it is a real outage multiplier: a scheduler
	// that wants one outage per window should coalesce its volumes into a single
	// scoped call rather than issuing one call per volume.
	EnqueueBackup(ctx context.Context, host, template, slug string, opts BackupOptions) (jobID string, err error)

	// ListBackups returns the instance's backups newest-first, so a retention
	// policy can select what to prune from the same inventory
	// GET .../backups exposes. limit <= 0 uses the core's default page size.
	ListBackups(ctx context.Context, host, template, slug string, limit int) ([]Backup, error)

	// DeleteBackup removes one backup completely: its blobs AND its row, over
	// the same path DELETE /backups/{id} uses — so a retention policy's
	// listing and the blob store can never diverge the way they do when a
	// pruner deletes objects without deleting the row.
	//
	// Deleting an absent backup is a no-op (nil error): a retention pass that
	// is retried, or that races another deleter, must not treat "already
	// gone" as a failure. It refuses (returns an error and deletes nothing) a
	// backup that has a backup or restore job actively in flight for it —
	// i.e. one that is not yet in a terminal state — so retention can never
	// delete a run that is still being written or restored from.
	DeleteBackup(ctx context.Context, id string) error
}

// BackupScheduler is the commercial hook for scheduled volume backups. When one
// is registered via server.WithBackupScheduler, the server starts it after
// wiring and runs it until the server's run context is cancelled, passing a
// BackupController. The implementation owns all timing, interval, and
// marker-grammar policy; the core ships no scheduling behavior of its own.
type BackupScheduler interface {
	Run(ctx context.Context, c BackupController) error
}

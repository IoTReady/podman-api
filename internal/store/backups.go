package store

import (
	"context"
	"encoding/json"
	"time"
)

// BackupState is the lifecycle state of a backup row.
type BackupState string

const (
	BackupCreating BackupState = "creating" // job in flight; not restorable
	BackupComplete BackupState = "complete" // all volumes exported + manifests recorded
	BackupFailed   BackupState = "failed"   // job failed or was interrupted; not restorable
)

// ExcludedPaths records what a volume's backup exclude patterns removed. It
// is nil for a volume that declared no patterns, so an unfiltered backup is
// byte-identical in the row as well as on the wire. Entries == 0 with a
// non-empty Patterns means the patterns matched nothing — usually a stale or
// misspelled pattern. (#248)
type ExcludedPaths struct {
	Patterns []string `json:"patterns"`
	Entries  int      `json:"entries"`
	Bytes    int64    `json:"bytes"`
}

// BackupVolume records one exported volume: its full name
// (<template>-<slug>-<vol>), the tar's byte size, and the sha256 per-file
// manifest (the instance package's Manifest, serialized). Manifests live in
// the row — not the blob store — so restore verifies the artifact against
// metadata it does not have to trust. Excluded is set only when the template
// declared exclude patterns for this volume (#248).
type BackupVolume struct {
	Name      string          `json:"name"`
	SizeBytes int64           `json:"size_bytes"`
	Manifest  json.RawMessage `json:"manifest"`
	Excluded  *ExcludedPaths  `json:"excluded,omitempty"`
}

// Backup is one row of the backups table.
type Backup struct {
	ID       string
	Host     string
	Template string
	Slug     string
	State    BackupState
	Volumes  []BackupVolume
	Image    string // image ref at backup time; informational hint only
	Created  time.Time
	Finished time.Time // zero until complete/failed
}

// BackupStore persists backup metadata. Implemented by *SQLite and *Memory.
type BackupStore interface {
	// CreateBackup inserts a new row in state creating, stamping Created.
	// The caller supplies the ID (NewBackupID).
	CreateBackup(ctx context.Context, b Backup) error
	// CompleteBackup transitions creating → complete, recording the exported
	// volumes and Finished. CAS: returns false (no error) if the row is not
	// currently creating.
	CompleteBackup(ctx context.Context, id string, vols []BackupVolume) (bool, error)
	// FailBackup transitions creating → failed, setting Finished. CAS like
	// CompleteBackup.
	FailBackup(ctx context.Context, id string) (bool, error)
	GetBackup(ctx context.Context, id string) (Backup, error) // ErrNotFound when absent
	// ListBackups returns the instance's backups newest-first. limit <= 0 uses
	// DefaultJobLimit; clamped at MaxJobLimit.
	ListBackups(ctx context.Context, host, template, slug string, limit int) ([]Backup, error)
	// DeleteBackup removes the row; ErrNotFound when absent. Blob deletion is
	// the caller's job (instance.Service.DeleteBackup) — the store only holds
	// metadata.
	DeleteBackup(ctx context.Context, id string) error
}

// NewBackupID returns a sortable backup id: "bk_" + the jobs id scheme
// (time-prefixed hex + random suffix).
func NewBackupID() string { return "bk_" + newJobID() }

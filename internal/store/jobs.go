package store

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"fmt"
	"io"
	"time"
)

// JobState is the lifecycle state of a job row.
type JobState string

const (
	JobQueued      JobState = "queued"
	JobRunning     JobState = "running"
	JobReconciling JobState = "reconciling"
	JobSucceeded   JobState = "succeeded"
	JobFailed      JobState = "failed"
	JobCanceled    JobState = "canceled"
)

// JobStep is one progress entry recorded by a handler.
type JobStep struct {
	TS     time.Time `json:"ts"`
	Step   string    `json:"step"`
	Detail string    `json:"detail,omitempty"`
}

// Job is one row of the jobs table.
type Job struct {
	ID       string
	Kind     string
	Args     json.RawMessage // opaque to the store; handlers unmarshal their own shape
	State    JobState
	Steps    []JobStep
	ParentID string // "" if none
	Error    string
	Created  time.Time
	Started  time.Time // zero until claimed
	Finished time.Time // zero until done
}

// JobFilter narrows ListJobs. Empty fields match anything. Limit/Before paginate
// the result (ordered newest-first); Before is the id of the previous page's
// last row (a cursor), returning only rows older than it.
type JobFilter struct {
	State    JobState
	Kind     string
	ParentID string
	Limit    int    // <=0 → DefaultJobLimit; values above MaxJobLimit are clamped
	Before   string // cursor: return jobs with id < Before
}

// Job listing page-size bounds.
const (
	DefaultJobLimit = 100
	MaxJobLimit     = 1000
)

// clampJobLimit applies the default/maximum page-size policy.
func clampJobLimit(n int) int {
	if n <= 0 {
		return DefaultJobLimit
	}
	if n > MaxJobLimit {
		return MaxJobLimit
	}
	return n
}

// JobStore persists and dispenses jobs. Implemented by *SQLite and *Memory.
type JobStore interface {
	// Enqueue inserts a new queued job, generating its ID. parentID is "" for
	// top-level jobs.
	Enqueue(ctx context.Context, kind string, args json.RawMessage, parentID string) (Job, error)
	// StartChild inserts a child job already in the running state (never queued),
	// owned by parentID. Because it is not queued, ClaimNext never claims it — the
	// caller (a parent job handler) drives it directly.
	StartChild(ctx context.Context, kind string, args json.RawMessage, parentID string) (Job, error)
	GetJob(ctx context.Context, id string) (Job, error) // ErrNotFound when absent
	ListJobs(ctx context.Context, f JobFilter) ([]Job, error)
	// ClaimNext atomically transitions the oldest queued job to running and
	// returns it. ok=false when there is nothing to claim.
	ClaimNext(ctx context.Context) (job Job, ok bool, err error)
	AppendStep(ctx context.Context, id string, step JobStep) error
	// Finish sets the terminal state, finished timestamp, and error (empty for
	// success). state must be JobSucceeded or JobFailed; passing any other value
	// is a programming error.
	Finish(ctx context.Context, id string, state JobState, errMsg string) error
	// FailRunning marks every job still in running as failed with reason; returns
	// the count. Called once at startup to reap crash-interrupted jobs.
	FailRunning(ctx context.Context, reason string) (int, error)
	// MarkReconciling moves every running job whose kind is in kinds to the
	// reconciling state (non-terminal); returns the count moved. Called once at
	// startup, before FailRunning, so reconcilable kinds are recovered rather than
	// failed. An empty kinds slice is a no-op returning 0.
	MarkReconciling(ctx context.Context, kinds []string) (int, error)
	// CancelQueued atomically transitions a still-queued job to canceled (setting
	// finished). Returns true if it transitioned; false if the job was not in the
	// queued state (already claimed, terminal, or absent).
	CancelQueued(ctx context.Context, id string) (bool, error)
	// PruneJobs deletes terminal (succeeded/failed) jobs finished before
	// olderThan, preserving parent/child integrity: a parent row is deleted only
	// when it has no surviving child. Returns the number of rows deleted.
	PruneJobs(ctx context.Context, olderThan time.Time) (int, error)
}

// DB is the full backend: spec store + job store + closer. main holds one of
// these; instance.Service takes the Store view, the runner takes the JobStore view.
type DB interface {
	Store
	JobStore
	io.Closer
}

// newJobID returns a sortable, time-prefixed unique id: 16 hex digits of the
// creation time in unix-nanoseconds, a dash, then 6 random bytes (12 hex).
func newJobID() string {
	var b [6]byte
	_, _ = io.ReadFull(rand.Reader, b[:])
	return fmt.Sprintf("%016x-%x", uint64(time.Now().UnixNano()), b)
}

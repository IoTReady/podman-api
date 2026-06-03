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
	JobQueued    JobState = "queued"
	JobRunning   JobState = "running"
	JobSucceeded JobState = "succeeded"
	JobFailed    JobState = "failed"
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

// JobFilter narrows ListJobs. Empty fields match anything.
type JobFilter struct {
	State JobState
	Kind  string
}

// JobStore persists and dispenses jobs. Implemented by *SQLite and *Memory.
type JobStore interface {
	// Enqueue inserts a new queued job, generating its ID. parentID is "" for
	// top-level jobs.
	Enqueue(ctx context.Context, kind string, args json.RawMessage, parentID string) (Job, error)
	GetJob(ctx context.Context, id string) (Job, error) // ErrNotFound when absent
	ListJobs(ctx context.Context, f JobFilter) ([]Job, error)
	// ClaimNext atomically transitions the oldest queued job to running and
	// returns it. ok=false when there is nothing to claim.
	ClaimNext(ctx context.Context) (job Job, ok bool, err error)
	AppendStep(ctx context.Context, id string, step JobStep) error
	// Finish sets the terminal state, finished timestamp, and error (empty for success).
	Finish(ctx context.Context, id string, state JobState, errMsg string) error
	// FailRunning marks every job still in running as failed with reason; returns
	// the count. Called once at startup to reap crash-interrupted jobs.
	FailRunning(ctx context.Context, reason string) (int, error)
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

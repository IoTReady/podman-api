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

// Active reports whether the job is in a non-terminal state — queued, running,
// or reconciling — i.e. work that may still mutate hosts. Guards that must not
// run concurrently with a migrate-class job should use this so a new
// non-terminal state cannot silently slip past them.
func (s JobState) Active() bool {
	return s == JobQueued || s == JobRunning || s == JobReconciling
}

// Terminal reports whether the job has reached a final state (succeeded, failed,
// or canceled).
func (s JobState) Terminal() bool { return !s.Active() }

// JobStep is one progress entry recorded by a handler.
type JobStep struct {
	TS     time.Time `json:"ts"`
	Step   string    `json:"step"`
	Detail string    `json:"detail,omitempty"`
	// Count is the total number of consecutive identical occurrences of this
	// step, materialized only when coalesced (>1). 0/omitted ⇒ a single
	// occurrence. AppendStep collapses consecutive identical (Step, Detail)
	// rows so a long-looping reconcile can't grow the array unboundedly. (#117)
	Count int `json:"count,omitempty"`
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
	// ResolveReconciling transitions a reconciling job to a terminal state
	// (succeeded or failed), setting finished + error. Compare-and-swap: it
	// affects only a row currently in reconciling, so it no-ops (returns false) if
	// an operator cancel already moved it. Passing any non-terminal state is a
	// programming error.
	ResolveReconciling(ctx context.Context, id string, state JobState, errMsg string) (bool, error)
	// CancelReconciling transitions a reconciling job to canceled, setting
	// finished. Compare-and-swap: affects only a row currently in reconciling,
	// returning false otherwise. Used by the cancel endpoint as the escape hatch.
	CancelReconciling(ctx context.Context, id string) (bool, error)
	// CancelQueued atomically transitions a still-queued job to canceled (setting
	// finished). Returns true if it transitioned; false if the job was not in the
	// queued state (already claimed, terminal, or absent).
	CancelQueued(ctx context.Context, id string) (bool, error)
	// PruneJobs deletes terminal (succeeded/failed) jobs finished before
	// olderThan, preserving parent/child integrity: a parent row is deleted only
	// when it has no surviving child. Returns the number of rows deleted.
	PruneJobs(ctx context.Context, olderThan time.Time) (int, error)
}

// DB is the full backend: spec store + job store + template store + backup store + closer.
// main holds one of these; instance.Service takes the Store view, the runner
// takes the JobStore view.
type DB interface {
	Store
	JobStore
	TemplateStore
	BackupStore
	io.Closer
}

// newJobID returns a sortable, time-prefixed unique id: 16 hex digits of the
// creation time in unix-nanoseconds, a dash, then 6 random bytes (12 hex).
func newJobID() string {
	var b [6]byte
	_, _ = io.ReadFull(rand.Reader, b[:])
	return fmt.Sprintf("%016x-%x", uint64(time.Now().UnixNano()), b)
}

// coalesceStep appends step to steps, collapsing a consecutive identical
// (Step, Detail) into an occurrence-count bump + timestamp refresh on the
// last element instead of growing the slice. Count holds total occurrences,
// materialized only when >1. (#117)
func coalesceStep(steps []JobStep, step JobStep) []JobStep {
	if n := len(steps); n > 0 && steps[n-1].Step == step.Step && steps[n-1].Detail == step.Detail {
		if steps[n-1].Count == 0 {
			steps[n-1].Count = 1
		}
		steps[n-1].Count++
		steps[n-1].TS = step.TS
		return steps
	}
	return append(steps, step)
}

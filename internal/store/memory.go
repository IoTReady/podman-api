package store

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"
)

// Memory is an in-memory Store for tests. Secrets are kept in plaintext and
// timestamps are NOT stamped (unlike the SQLite store) — it is a test double,
// not a production backend. PutErr/DeleteErr, when non-nil, make the
// corresponding call fail, to exercise callers' fatal-failure paths.
// It also implements JobStore with an in-memory []Job slice.
type Memory struct {
	mu    sync.Mutex
	specs map[string]Spec
	jobs  []Job // insertion order; newest last

	PutErr    error
	DeleteErr error
}

// NewMemory returns an empty in-memory store.
func NewMemory() *Memory {
	return &Memory{specs: map[string]Spec{}}
}

func memKey(host, template, slug string) string {
	return host + "|" + template + "|" + slug
}

// PutSpec inserts or replaces (upserts) the spec for (host, template, slug).
func (m *Memory) PutSpec(_ context.Context, s Spec) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.PutErr != nil {
		return m.PutErr
	}
	m.specs[memKey(s.Host, s.Template, s.Slug)] = s
	return nil
}

func (m *Memory) GetSpec(_ context.Context, host, template, slug string) (Spec, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	s, ok := m.specs[memKey(host, template, slug)]
	if !ok {
		return Spec{}, ErrNotFound
	}
	return s, nil
}

func (m *Memory) DeleteSpec(_ context.Context, host, template, slug string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.DeleteErr != nil {
		return m.DeleteErr
	}
	k := memKey(host, template, slug)
	if _, ok := m.specs[k]; !ok {
		return ErrNotFound
	}
	delete(m.specs, k)
	return nil
}

func (m *Memory) Enqueue(_ context.Context, kind string, args json.RawMessage, parentID string) (Job, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if len(args) == 0 {
		args = json.RawMessage("null")
	}
	j := Job{
		ID: newJobID(), Kind: kind, Args: args, State: JobQueued,
		Steps: []JobStep{}, ParentID: parentID, Created: time.Now(),
	}
	m.jobs = append(m.jobs, j)
	return j, nil
}

func (m *Memory) GetJob(_ context.Context, id string) (Job, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, j := range m.jobs {
		if j.ID == id {
			return cloneJob(j), nil
		}
	}
	return Job{}, ErrNotFound
}

func (m *Memory) ListJobs(_ context.Context, f JobFilter) ([]Job, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := []Job{}
	for i := len(m.jobs) - 1; i >= 0; i-- { // newest first
		j := m.jobs[i]
		if f.State != "" && j.State != f.State {
			continue
		}
		if f.Kind != "" && j.Kind != f.Kind {
			continue
		}
		out = append(out, cloneJob(j))
	}
	return out, nil
}

func (m *Memory) ClaimNext(_ context.Context) (Job, bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for i := range m.jobs { // oldest first
		if m.jobs[i].State == JobQueued {
			m.jobs[i].State = JobRunning
			m.jobs[i].Started = time.Now()
			return cloneJob(m.jobs[i]), true, nil
		}
	}
	return Job{}, false, nil
}

func (m *Memory) AppendStep(_ context.Context, id string, step JobStep) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	for i := range m.jobs {
		if m.jobs[i].ID == id {
			m.jobs[i].Steps = append(m.jobs[i].Steps, step)
			return nil
		}
	}
	return ErrNotFound
}

func (m *Memory) Finish(_ context.Context, id string, state JobState, errMsg string) error {
	if state != JobSucceeded && state != JobFailed {
		return fmt.Errorf("store: Finish: invalid terminal state %q", state)
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	for i := range m.jobs {
		if m.jobs[i].ID == id {
			m.jobs[i].State = state
			m.jobs[i].Error = errMsg
			m.jobs[i].Finished = time.Now()
			return nil
		}
	}
	return ErrNotFound
}

func (m *Memory) FailRunning(_ context.Context, reason string) (int, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	n := 0
	for i := range m.jobs {
		if m.jobs[i].State == JobRunning {
			m.jobs[i].State = JobFailed
			m.jobs[i].Error = reason
			m.jobs[i].Finished = time.Now()
			n++
		}
	}
	return n, nil
}

// cloneJob returns a copy whose Steps and Args do not share backing arrays with
// the stored job — matching SQLite, which deserializes fresh on every read, so a
// caller mutating a returned job cannot corrupt the in-memory store.
func cloneJob(j Job) Job {
	if j.Steps != nil {
		steps := make([]JobStep, len(j.Steps))
		copy(steps, j.Steps)
		j.Steps = steps
	}
	if j.Args != nil {
		j.Args = append(json.RawMessage(nil), j.Args...)
	}
	return j
}

var _ JobStore = (*Memory)(nil)

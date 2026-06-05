package store

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"sync"
	"time"
)

// Memory is an in-memory Store for tests. Secrets are kept in plaintext and
// timestamps are NOT stamped (unlike the SQLite store) — it is a test double,
// not a production backend. PutErr/DeleteErr, when non-nil, make the
// corresponding call fail, to exercise callers' fatal-failure paths.
// It also implements JobStore with an in-memory []Job slice and TemplateStore
// with an in-memory map.
type Memory struct {
	mu          sync.Mutex
	specs       map[string]Spec
	jobs        []Job             // insertion order; newest last
	hostSecrets map[string][]byte // "host\x00name" -> value
	templates   map[string]Template

	PutErr    error
	DeleteErr error
}

// NewMemory returns an empty in-memory store.
func NewMemory() *Memory {
	return &Memory{
		specs:       map[string]Spec{},
		hostSecrets: map[string][]byte{},
		templates:   map[string]Template{},
	}
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

func (m *Memory) ListSpecKeys(_ context.Context, host string) ([]SpecKey, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := []SpecKey{}
	for _, s := range m.specs {
		if s.Host == host {
			out = append(out, SpecKey{Template: s.Template, Slug: s.Slug})
		}
	}
	return out, nil
}

func (m *Memory) PutHostSecret(_ context.Context, host, name string, value []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.PutErr != nil {
		return m.PutErr
	}
	cp := append([]byte(nil), value...)
	m.hostSecrets[host+"\x00"+name] = cp
	return nil
}

func (m *Memory) GetHostSecret(_ context.Context, host, name string) ([]byte, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	v, ok := m.hostSecrets[host+"\x00"+name]
	if !ok {
		return nil, ErrNotFound
	}
	return append([]byte(nil), v...), nil
}

func (m *Memory) DeleteHostSecret(_ context.Context, host, name string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.DeleteErr != nil {
		return m.DeleteErr
	}
	delete(m.hostSecrets, host+"\x00"+name)
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

func (m *Memory) StartChild(_ context.Context, kind string, args json.RawMessage, parentID string) (Job, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if len(args) == 0 {
		args = json.RawMessage("null")
	}
	now := time.Now()
	j := Job{
		ID: newJobID(), Kind: kind, Args: args, State: JobRunning,
		Steps: []JobStep{}, ParentID: parentID, Created: now, Started: now,
	}
	m.jobs = append(m.jobs, j)
	return cloneJob(j), nil
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
	limit := clampJobLimit(f.Limit)
	var matched []Job
	for _, j := range m.jobs {
		if f.State != "" && j.State != f.State {
			continue
		}
		if f.Kind != "" && j.Kind != f.Kind {
			continue
		}
		if f.ParentID != "" && j.ParentID != f.ParentID {
			continue
		}
		if f.Before != "" && j.ID >= f.Before {
			continue
		}
		matched = append(matched, j)
	}
	// Sort by id descending (newest first), the same total order SQLite uses, so
	// the limit truncates the right rows and the Before cursor stays consistent —
	// append order can diverge from id order under concurrent enqueues.
	sort.Slice(matched, func(i, k int) bool { return matched[i].ID > matched[k].ID })
	out := []Job{}
	for i := 0; i < len(matched) && i < limit; i++ {
		out = append(out, cloneJob(matched[i]))
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
	if state != JobSucceeded && state != JobFailed && state != JobCanceled {
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

func (m *Memory) MarkReconciling(_ context.Context, kinds []string) (int, error) {
	if len(kinds) == 0 {
		return 0, nil
	}
	want := make(map[string]bool, len(kinds))
	for _, k := range kinds {
		want[k] = true
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	n := 0
	for i := range m.jobs {
		if m.jobs[i].State == JobRunning && want[m.jobs[i].Kind] {
			m.jobs[i].State = JobReconciling
			n++
		}
	}
	return n, nil
}

func (m *Memory) ResolveReconciling(_ context.Context, id string, state JobState, errMsg string) (bool, error) {
	if state != JobSucceeded && state != JobFailed {
		return false, fmt.Errorf("store: ResolveReconciling: invalid terminal state %q", state)
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	for i := range m.jobs {
		if m.jobs[i].ID == id && m.jobs[i].State == JobReconciling {
			m.jobs[i].State = state
			m.jobs[i].Error = errMsg
			m.jobs[i].Finished = time.Now()
			return true, nil
		}
	}
	return false, nil
}

func (m *Memory) CancelReconciling(_ context.Context, id string) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for i := range m.jobs {
		if m.jobs[i].ID == id && m.jobs[i].State == JobReconciling {
			m.jobs[i].State = JobCanceled
			m.jobs[i].Finished = time.Now()
			return true, nil
		}
	}
	return false, nil
}

func (m *Memory) CancelQueued(_ context.Context, id string) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for i := range m.jobs {
		if m.jobs[i].ID == id && m.jobs[i].State == JobQueued {
			m.jobs[i].State = JobCanceled
			m.jobs[i].Finished = time.Now()
			return true, nil
		}
	}
	return false, nil
}

func (m *Memory) PruneJobs(_ context.Context, olderThan time.Time) (int, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	terminal := func(j Job) bool {
		return j.State.Terminal()
	}
	isOld := func(j Job) bool {
		return !j.Finished.IsZero() && j.Finished.Before(olderThan)
	}

	// Pass 1: delete old terminal children.
	kept := m.jobs[:0:0]
	deleted := 0
	for _, j := range m.jobs {
		if j.ParentID != "" && terminal(j) && isOld(j) {
			deleted++
			continue
		}
		kept = append(kept, j)
	}
	m.jobs = kept

	// Pass 2: delete old terminal jobs not referenced as a parent by survivors.
	referenced := map[string]bool{}
	for _, j := range m.jobs {
		if j.ParentID != "" {
			referenced[j.ParentID] = true
		}
	}
	kept = m.jobs[:0:0]
	for _, j := range m.jobs {
		if terminal(j) && isOld(j) && !referenced[j.ID] {
			deleted++
			continue
		}
		kept = append(kept, j)
	}
	m.jobs = kept
	return deleted, nil
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

// ---------------------------------------------------------------------------
// TemplateStore implementation
// ---------------------------------------------------------------------------

func (m *Memory) ListTemplates(_ context.Context) ([]Template, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]Template, 0, len(m.templates))
	for _, t := range m.templates {
		out = append(out, t)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Meta.ID < out[j].Meta.ID })
	return out, nil
}

func (m *Memory) GetTemplate(_ context.Context, id string) (Template, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	t, ok := m.templates[id]
	if !ok {
		return Template{}, ErrNotFound
	}
	return t, nil
}

func (m *Memory) PutTemplate(_ context.Context, t Template) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	now := time.Now()
	existing, exists := m.templates[t.Meta.ID]
	if exists {
		t.Created = existing.Created
	} else {
		t.Created = now
	}
	t.Updated = now
	m.templates[t.Meta.ID] = t
	return nil
}

func (m *Memory) DeleteTemplate(_ context.Context, id string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.templates, id)
	return nil
}

func (m *Memory) CountTemplates(_ context.Context) (int, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.templates), nil
}

var _ TemplateStore = (*Memory)(nil)

package store

import (
	"context"
	"sync"
)

// Memory is an in-memory Store for tests. Secrets are kept in plaintext and
// timestamps are NOT stamped (unlike the SQLite store) — it is a test double,
// not a production backend. PutErr/DeleteErr, when non-nil, make the
// corresponding call fail, to exercise callers' fatal-failure paths.
type Memory struct {
	mu    sync.Mutex
	specs map[string]Spec

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

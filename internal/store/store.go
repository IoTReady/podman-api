// Package store is the daemon's durable desired-state record: one encrypted
// row per instance, written on Apply and removed on Delete. It is opt-in; when
// no store is wired the daemon is a stateless proxy as before.
package store

import (
	"context"
	"errors"
	"time"
)

// ErrNotFound is returned when no row matches a lookup (specs or jobs).
var ErrNotFound = errors.New("store: not found")

// Spec is the desired state of one instance.
type Spec struct {
	Host     string
	Template string
	Slug     string
	// Parameters is the instance's render parameters. NOTE: SQLite-backed
	// storage round-trips this through JSON, so numbers come back as float64
	// (e.g. an int 5432 becomes float64(5432)). Callers that re-render via
	// text/template are unaffected; callers must not type-assert .(int).
	Parameters map[string]any
	Secrets    map[string]string
	Created    time.Time
	Updated    time.Time
}

// Store persists instance specs. Implementations encrypt Secrets at rest and
// stamp Created (first write) and Updated (every write); the in-memory test
// double does neither.
type Store interface {
	// PutSpec inserts or replaces the spec for (s.Host, s.Template, s.Slug).
	PutSpec(ctx context.Context, s Spec) error
	GetSpec(ctx context.Context, host, template, slug string) (Spec, error)
	DeleteSpec(ctx context.Context, host, template, slug string) error
}

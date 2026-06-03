// Package store is the daemon's durable desired-state record: one encrypted
// row per instance, written on Apply and removed on Delete. It is opt-in; when
// no store is wired the daemon is a stateless proxy as before.
package store

import (
	"context"
	"errors"
	"time"
)

// ErrNotFound is returned by GetSpec/DeleteSpec when no row matches.
var ErrNotFound = errors.New("store: spec not found")

// Spec is the desired state of one instance.
type Spec struct {
	Host       string
	Template   string
	Slug       string
	Parameters map[string]any
	Secrets    map[string]string
	Created    time.Time
	Updated    time.Time
}

// Store persists instance specs. Implementations encrypt Secrets at rest.
type Store interface {
	PutSpec(ctx context.Context, s Spec) error
	GetSpec(ctx context.Context, host, template, slug string) (Spec, error)
	DeleteSpec(ctx context.Context, host, template, slug string) error
}

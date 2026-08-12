package store

import (
	"context"
	"time"

	"github.com/iotready/podman-api/internal/render"
)

// Template is an authored contract (render.Meta) plus its renderable body and
// provenance. The template id is Meta.ID.
type Template struct {
	Meta render.Meta
	Body string
	// Origin is "seed" (shipped in the bundled catalog), "user" (created
	// through the API/UI), or "seed-migrated" — a shipped row a boot-time seed
	// migration has rewritten. The last is also the migration's applied-once
	// marker: only "seed" rows are ever considered, so a migrated row is never
	// revisited. See server.migrateSeededTemplates.
	Origin  string
	Created time.Time
	Updated time.Time
}

// TemplateStore persists deployable templates.
type TemplateStore interface {
	ListTemplates(ctx context.Context) ([]Template, error)
	GetTemplate(ctx context.Context, id string) (Template, error)
	PutTemplate(ctx context.Context, t Template) error
	DeleteTemplate(ctx context.Context, id string) error
	CountTemplates(ctx context.Context) (int, error)
}

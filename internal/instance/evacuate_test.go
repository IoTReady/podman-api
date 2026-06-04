package instance

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/iotready/podman-api/internal/config"
	"github.com/iotready/podman-api/internal/podman/fake"
	"github.com/iotready/podman-api/internal/store"
)

func TestResolveEvacuation(t *testing.T) {
	ctx := context.Background()

	t.Run("happy path sorted by slug", func(t *testing.T) {
		svc, _, mem := newMigrateSvc(t)
		seedSpec(t, mem, "h1", "postgres", "db2", map[string]any{})
		seedSpec(t, mem, "h1", "postgres", "db1", map[string]any{})

		moves, err := svc.ResolveEvacuation(ctx, EvacuateRequest{
			FromHost: "h1",
			Map:      map[string]string{"db1": "h2", "db2": "draining"},
		})
		require.NoError(t, err)
		require.Len(t, moves, 2)
		assert.Equal(t, MigrateRequest{FromHost: "h1", ToHost: "h2", Template: "postgres", Slug: "db1"}, moves[0])
		assert.Equal(t, MigrateRequest{FromHost: "h1", ToHost: "draining", Template: "postgres", Slug: "db2"}, moves[1])
	})

	t.Run("unmapped instance", func(t *testing.T) {
		svc, _, mem := newMigrateSvc(t)
		seedSpec(t, mem, "h1", "postgres", "db1", map[string]any{})
		seedSpec(t, mem, "h1", "postgres", "db2", map[string]any{})

		_, err := svc.ResolveEvacuation(ctx, EvacuateRequest{
			FromHost: "h1",
			Map:      map[string]string{"db1": "h2"},
		})
		assert.ErrorIs(t, err, ErrInvalidEvacuation)
	})

	t.Run("extra map key", func(t *testing.T) {
		svc, _, mem := newMigrateSvc(t)
		seedSpec(t, mem, "h1", "postgres", "db1", map[string]any{})

		_, err := svc.ResolveEvacuation(ctx, EvacuateRequest{
			FromHost: "h1",
			Map:      map[string]string{"db1": "h2", "ghost": "h2"},
		})
		assert.ErrorIs(t, err, ErrInvalidEvacuation)
	})

	t.Run("dest equals from_host", func(t *testing.T) {
		svc, _, mem := newMigrateSvc(t)
		seedSpec(t, mem, "h1", "postgres", "db1", map[string]any{})

		_, err := svc.ResolveEvacuation(ctx, EvacuateRequest{
			FromHost: "h1",
			Map:      map[string]string{"db1": "h1"},
		})
		assert.ErrorIs(t, err, ErrSameHost)
	})

	t.Run("unknown dest host", func(t *testing.T) {
		svc, _, mem := newMigrateSvc(t)
		seedSpec(t, mem, "h1", "postgres", "db1", map[string]any{})

		_, err := svc.ResolveEvacuation(ctx, EvacuateRequest{
			FromHost: "h1",
			Map:      map[string]string{"db1": "nope"},
		})
		// An unknown destination named in the map is bad map content, not a
		// missing top-level resource: it surfaces as ErrInvalidEvacuation (400),
		// consistent with the other map-content errors above.
		assert.ErrorIs(t, err, ErrInvalidEvacuation)
	})

	t.Run("unknown from_host", func(t *testing.T) {
		svc, _, _ := newMigrateSvc(t)

		_, err := svc.ResolveEvacuation(ctx, EvacuateRequest{
			FromHost: "nope",
			Map:      map[string]string{"db1": "h2"},
		})
		assert.ErrorIs(t, err, ErrUnknownHost)
	})

	t.Run("empty host and map is a no-op", func(t *testing.T) {
		svc, _, _ := newMigrateSvc(t)

		moves, err := svc.ResolveEvacuation(ctx, EvacuateRequest{
			FromHost: "h1",
			Map:      map[string]string{},
		})
		require.NoError(t, err)
		assert.Empty(t, moves)
	})

	t.Run("slug ambiguous across templates", func(t *testing.T) {
		svc, _, mem := newMigrateSvc(t)
		require.NoError(t, mem.PutSpec(ctx, store.Spec{
			Host: "h1", Template: "postgres", Slug: "dup",
			Parameters: map[string]any{}, Secrets: map[string]string{},
		}))
		require.NoError(t, mem.PutSpec(ctx, store.Spec{
			Host: "h1", Template: "redis", Slug: "dup",
			Parameters: map[string]any{}, Secrets: map[string]string{},
		}))

		_, err := svc.ResolveEvacuation(ctx, EvacuateRequest{
			FromHost: "h1",
			Map:      map[string]string{"dup": "h2"},
		})
		assert.ErrorIs(t, err, ErrInvalidEvacuation)
	})

	t.Run("store disabled", func(t *testing.T) {
		hosts := []config.Host{
			{ID: "h1", Addr: "unix", Socket: "/x"},
			{ID: "h2", Addr: "unix", Socket: "/y"},
		}
		svc := NewService(fake.New(), hosts, []config.Template{pgTemplate()})
		// No SetStore: s.store stays nil.

		_, err := svc.ResolveEvacuation(ctx, EvacuateRequest{
			FromHost: "h1",
			Map:      map[string]string{"db1": "h2"},
		})
		assert.ErrorIs(t, err, ErrStoreDisabled)
	})
}

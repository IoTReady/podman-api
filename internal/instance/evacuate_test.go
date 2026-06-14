package instance

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/iotready/podman-api/internal/store"
)

func TestResolveEvacuation(t *testing.T) {
	ctx := context.Background()

	t.Run("happy path sorted by slug (legacy map)", func(t *testing.T) {
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

	t.Run("happy path (moves array)", func(t *testing.T) {
		svc, _, mem := newMigrateSvc(t)
		seedSpec(t, mem, "h1", "postgres", "db2", map[string]any{})
		seedSpec(t, mem, "h1", "postgres", "db1", map[string]any{})

		moves, err := svc.ResolveEvacuation(ctx, EvacuateRequest{
			FromHost: "h1",
			Moves: []Move{
				{Template: "postgres", Slug: "db1", ToHost: "h2"},
				{Template: "postgres", Slug: "db2", ToHost: "draining"},
			},
		})
		require.NoError(t, err)
		require.Len(t, moves, 2)
		assert.Equal(t, MigrateRequest{FromHost: "h1", ToHost: "h2", Template: "postgres", Slug: "db1"}, moves[0])
		assert.Equal(t, MigrateRequest{FromHost: "h1", ToHost: "draining", Template: "postgres", Slug: "db2"}, moves[1])
	})

	t.Run("shared slug across templates with moves array", func(t *testing.T) {
		svc, _, mem := newMigrateSvc(t)
		require.NoError(t, mem.PutSpec(ctx, store.Spec{
			Host: "h1", Template: "postgres", Slug: "dup",
			Parameters: map[string]any{}, Secrets: map[string]string{},
		}))
		require.NoError(t, mem.PutSpec(ctx, store.Spec{
			Host: "h1", Template: "redis", Slug: "dup",
			Parameters: map[string]any{}, Secrets: map[string]string{},
		}))

		moves, err := svc.ResolveEvacuation(ctx, EvacuateRequest{
			FromHost: "h1",
			Moves: []Move{
				{Template: "postgres", Slug: "dup", ToHost: "h2"},
				{Template: "redis", Slug: "dup", ToHost: "draining"},
			},
		})
		require.NoError(t, err)
		require.Len(t, moves, 2)
		assert.Equal(t, MigrateRequest{FromHost: "h1", ToHost: "h2", Template: "postgres", Slug: "dup"}, moves[0])
		assert.Equal(t, MigrateRequest{FromHost: "h1", ToHost: "draining", Template: "redis", Slug: "dup"}, moves[1])
	})

	t.Run("unmapped instance (legacy map)", func(t *testing.T) {
		svc, _, mem := newMigrateSvc(t)
		seedSpec(t, mem, "h1", "postgres", "db1", map[string]any{})
		seedSpec(t, mem, "h1", "postgres", "db2", map[string]any{})

		_, err := svc.ResolveEvacuation(ctx, EvacuateRequest{
			FromHost: "h1",
			Map:      map[string]string{"db1": "h2"},
		})
		assert.ErrorIs(t, err, ErrInvalidEvacuation)
	})

	t.Run("unmapped instance (moves array)", func(t *testing.T) {
		svc, _, mem := newMigrateSvc(t)
		seedSpec(t, mem, "h1", "postgres", "db1", map[string]any{})
		seedSpec(t, mem, "h1", "postgres", "db2", map[string]any{})

		_, err := svc.ResolveEvacuation(ctx, EvacuateRequest{
			FromHost: "h1",
			Moves: []Move{
				{Template: "postgres", Slug: "db1", ToHost: "h2"},
			},
		})
		assert.ErrorIs(t, err, ErrInvalidEvacuation)
	})

	t.Run("extra map key (legacy map)", func(t *testing.T) {
		svc, _, mem := newMigrateSvc(t)
		seedSpec(t, mem, "h1", "postgres", "db1", map[string]any{})

		_, err := svc.ResolveEvacuation(ctx, EvacuateRequest{
			FromHost: "h1",
			Map:      map[string]string{"db1": "h2", "ghost": "h2"},
		})
		assert.ErrorIs(t, err, ErrInvalidEvacuation)
	})

	t.Run("extra move (moves array)", func(t *testing.T) {
		svc, _, mem := newMigrateSvc(t)
		seedSpec(t, mem, "h1", "postgres", "db1", map[string]any{})

		_, err := svc.ResolveEvacuation(ctx, EvacuateRequest{
			FromHost: "h1",
			Moves: []Move{
				{Template: "postgres", Slug: "db1", ToHost: "h2"},
				{Template: "postgres", Slug: "ghost", ToHost: "h2"},
			},
		})
		assert.ErrorIs(t, err, ErrInvalidEvacuation)
	})

	t.Run("dest equals from_host (legacy map)", func(t *testing.T) {
		svc, _, mem := newMigrateSvc(t)
		seedSpec(t, mem, "h1", "postgres", "db1", map[string]any{})

		_, err := svc.ResolveEvacuation(ctx, EvacuateRequest{
			FromHost: "h1",
			Map:      map[string]string{"db1": "h1"},
		})
		assert.ErrorIs(t, err, ErrSameHost)
	})

	t.Run("dest equals from_host (moves array)", func(t *testing.T) {
		svc, _, mem := newMigrateSvc(t)
		seedSpec(t, mem, "h1", "postgres", "db1", map[string]any{})

		_, err := svc.ResolveEvacuation(ctx, EvacuateRequest{
			FromHost: "h1",
			Moves:    []Move{{Template: "postgres", Slug: "db1", ToHost: "h1"}},
		})
		assert.ErrorIs(t, err, ErrSameHost)
	})

	t.Run("unknown dest host (legacy map)", func(t *testing.T) {
		svc, _, mem := newMigrateSvc(t)
		seedSpec(t, mem, "h1", "postgres", "db1", map[string]any{})

		_, err := svc.ResolveEvacuation(ctx, EvacuateRequest{
			FromHost: "h1",
			Map:      map[string]string{"db1": "nope"},
		})
		assert.ErrorIs(t, err, ErrInvalidEvacuation)
	})

	t.Run("unknown dest host (moves array)", func(t *testing.T) {
		svc, _, mem := newMigrateSvc(t)
		seedSpec(t, mem, "h1", "postgres", "db1", map[string]any{})

		_, err := svc.ResolveEvacuation(ctx, EvacuateRequest{
			FromHost: "h1",
			Moves:    []Move{{Template: "postgres", Slug: "db1", ToHost: "nope"}},
		})
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

	t.Run("empty host and moves is a no-op", func(t *testing.T) {
		svc, _, _ := newMigrateSvc(t)

		moves, err := svc.ResolveEvacuation(ctx, EvacuateRequest{
			FromHost: "h1",
			Moves:    []Move{},
		})
		require.NoError(t, err)
		assert.Empty(t, moves)
	})

	t.Run("slug ambiguous across templates (legacy map rejected)", func(t *testing.T) {
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

	t.Run("duplicate move rejected", func(t *testing.T) {
		svc, _, mem := newMigrateSvc(t)
		seedSpec(t, mem, "h1", "postgres", "db1", map[string]any{})

		_, err := svc.ResolveEvacuation(ctx, EvacuateRequest{
			FromHost: "h1",
			Moves: []Move{
				{Template: "postgres", Slug: "db1", ToHost: "h2"},
				{Template: "postgres", Slug: "db1", ToHost: "h3"},
			},
		})
		assert.ErrorIs(t, err, ErrInvalidEvacuation)
	})

	t.Run("moves wins over map when both provided", func(t *testing.T) {
		svc, _, mem := newMigrateSvc(t)
		seedSpec(t, mem, "h1", "postgres", "db1", map[string]any{})

		moves, err := svc.ResolveEvacuation(ctx, EvacuateRequest{
			FromHost: "h1",
			Map:      map[string]string{"db1": "h2"},
			Moves:    []Move{{Template: "postgres", Slug: "db1", ToHost: "draining"}},
		})
		require.NoError(t, err)
		require.Len(t, moves, 1)
		assert.Equal(t, "draining", moves[0].ToHost)
	})
}

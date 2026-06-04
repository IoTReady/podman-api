package instance

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/iotready/podman-api/internal/config"
	"github.com/iotready/podman-api/internal/podman/fake"
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
		assert.ErrorIs(t, err, ErrUnknownHost)
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

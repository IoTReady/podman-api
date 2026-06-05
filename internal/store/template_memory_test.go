package store

import (
	"context"
	"testing"

	"github.com/iotready/podman-api/internal/render"
	"github.com/stretchr/testify/require"
)

func TestMemory_TemplatePutStampsTimes(t *testing.T) {
	ctx := context.Background()
	m := NewMemory()
	require.NoError(t, m.PutTemplate(ctx, Template{Meta: render.Meta{ID: "a"}, Body: "k", Origin: "user"}))
	got, _ := m.GetTemplate(ctx, "a")
	require.False(t, got.Created.IsZero())
	require.False(t, got.Updated.IsZero())
	first := got.Created
	require.NoError(t, m.PutTemplate(ctx, Template{Meta: render.Meta{ID: "a"}, Body: "k2", Origin: "user"}))
	got2, _ := m.GetTemplate(ctx, "a")
	require.Equal(t, first, got2.Created, "Created must be preserved on upsert")
}

func TestMemory_TemplateCRUD(t *testing.T) {
	ctx := context.Background()
	m := NewMemory()
	n, err := m.CountTemplates(ctx)
	require.NoError(t, err)
	require.Zero(t, n)

	tpl := Template{Meta: render.Meta{ID: "web"}, Body: "kind: Pod", Origin: "user"}
	require.NoError(t, m.PutTemplate(ctx, tpl))

	got, err := m.GetTemplate(ctx, "web")
	require.NoError(t, err)
	require.Equal(t, "web", got.Meta.ID)
	require.Equal(t, "kind: Pod", got.Body)

	list, err := m.ListTemplates(ctx)
	require.NoError(t, err)
	require.Len(t, list, 1)

	require.NoError(t, m.DeleteTemplate(ctx, "web"))
	_, err = m.GetTemplate(ctx, "web")
	require.ErrorIs(t, err, ErrNotFound)
}

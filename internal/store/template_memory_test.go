package store

import (
	"context"
	"testing"

	"github.com/iotready/podman-api/internal/render"
	"github.com/stretchr/testify/require"
)

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

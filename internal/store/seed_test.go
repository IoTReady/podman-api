package store

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/iotready/podman-api/internal/render"
	"github.com/iotready/podman-api/templates"
)

func TestParseSeeds_ParsesEmbedded(t *testing.T) {
	seeds, err := ParseSeeds(templates.Files)
	require.NoError(t, err)
	require.NotEmpty(t, seeds)
	for _, s := range seeds {
		require.Equal(t, "seed", s.Origin)
		require.NotEmpty(t, s.Meta.ID)
		require.NotEmpty(t, s.Body)
	}
}

func TestParseSeeds_IncludesBasicWeb(t *testing.T) {
	seeds, err := ParseSeeds(templates.Files)
	require.NoError(t, err)
	var bw *Template
	for i := range seeds {
		if seeds[i].Meta.ID == "basic-web" {
			bw = &seeds[i]
		}
	}
	require.NotNil(t, bw, "basic-web seed must exist")
	require.NotNil(t, bw.Meta.Ingress)
	require.Equal(t, "app", bw.Meta.Ingress.Container)
}

func TestBasicWeb_Renders(t *testing.T) {
	// Read the full source (meta + body) directly from the embed.FS so that
	// render.Render can strip the meta block before template execution.
	// render.RenderBody does not exist yet; render.Render requires the full source.
	raw, err := templates.Files.ReadFile("basic-web.yaml")
	require.NoError(t, err)
	out, err := render.Render(string(raw), map[string]any{"slug": "demo", "image": "nginx:1", "port": 8080})
	require.NoError(t, err)
	require.Contains(t, out, "basic-web-demo")
	require.Contains(t, out, "nginx:1")
}

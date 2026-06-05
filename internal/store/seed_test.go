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
	// Read the full source (meta + body) directly from the embed.FS and split
	// the meta block off with ParseMeta before rendering the body.
	raw, err := templates.Files.ReadFile("basic-web.yaml")
	require.NoError(t, err)
	_, body, err := render.ParseMeta(string(raw))
	require.NoError(t, err)
	out, err := render.RenderBody(body, map[string]any{"slug": "demo", "image": "nginx:1"})
	require.NoError(t, err)
	require.Contains(t, out, "basic-web-demo")
	require.Contains(t, out, "nginx:1")
	require.Contains(t, out, "containerPort: 8080")
}

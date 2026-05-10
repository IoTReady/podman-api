package render

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRender_SubstitutesParams(t *testing.T) {
	src := `# template-meta:
#   id: x
#   parameters:
#     required: [slug]
---
apiVersion: v1
kind: Pod
metadata:
  name: x-{{.slug}}
`
	out, err := Render(src, map[string]any{"slug": "iotready"})
	require.NoError(t, err)
	assert.Contains(t, out, "name: x-iotready")
	assert.NotContains(t, out, "template-meta")
}

func TestRender_MissingParamErrors(t *testing.T) {
	src := `# template-meta:
#   id: x
#   parameters:
#     required: [slug]
---
metadata:
  name: x-{{.slug}}
`
	// Use Go template's missingkey=error mode.
	_, err := Render(src, map[string]any{})
	require.Error(t, err)
}

func TestRender_PreservesMultipleDocs(t *testing.T) {
	src := `# template-meta:
#   id: x
#   parameters:
#     required: [slug]
---
apiVersion: v1
kind: ConfigMap
metadata:
  name: cm-{{.slug}}
---
apiVersion: v1
kind: Pod
metadata:
  name: p-{{.slug}}
`
	out, err := Render(src, map[string]any{"slug": "a"})
	require.NoError(t, err)
	assert.Contains(t, out, "name: cm-a")
	assert.Contains(t, out, "name: p-a")
}

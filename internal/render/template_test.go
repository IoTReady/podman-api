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
#     - name: slug
#       type: string
#       required: true
---
apiVersion: v1
kind: Pod
metadata:
  name: x-{{.slug}}
`
	_, body, err := ParseMeta(src)
	require.NoError(t, err)
	out, err := RenderBody(body, map[string]any{"slug": "iotready"})
	require.NoError(t, err)
	assert.Contains(t, out, "name: x-iotready")
	assert.NotContains(t, out, "template-meta")
}

func TestRender_MissingParamErrors(t *testing.T) {
	src := `# template-meta:
#   id: x
#   parameters:
#     - name: slug
#       type: string
#       required: true
---
metadata:
  name: x-{{.slug}}
`
	// Use Go template's missingkey=error mode.
	_, body, err := ParseMeta(src)
	require.NoError(t, err)
	_, err = RenderBody(body, map[string]any{})
	require.Error(t, err)
}

func TestRenderBody_SubstitutesParams(t *testing.T) {
	out, err := RenderBody("value: {{.x}}", map[string]any{"x": "hello"})
	require.NoError(t, err)
	assert.Equal(t, "value: hello", out)
}

func TestRender_PreservesMultipleDocs(t *testing.T) {
	src := `# template-meta:
#   id: x
#   parameters:
#     - name: slug
#       type: string
#       required: true
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
	_, body, err := ParseMeta(src)
	require.NoError(t, err)
	out, err := RenderBody(body, map[string]any{"slug": "a"})
	require.NoError(t, err)
	assert.Contains(t, out, "name: cm-a")
	assert.Contains(t, out, "name: p-a")
}

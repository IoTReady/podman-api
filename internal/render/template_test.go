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

func TestRenderAndValidate_MultiLineParam_NoIndent_Rejected(t *testing.T) {
	body := `data:
  config: |
    {{.config}}
`
	_, err := RenderAndValidate(body, map[string]any{"config": "line1\nline2"})
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrRenderInvalid)
	assert.Contains(t, err.Error(), "config")
}

func TestRenderAndValidate_MultiLineParam_Indented_OK(t *testing.T) {
	body := `data:
  config: |
    {{.config}}
`
	// The template ref is at 4 spaces. With first line at 0 indent, baseline = 4.
	// Continuation lines need >= 4 spaces to stay inside the same block scalar.
	out, err := RenderAndValidate(body, map[string]any{"config": "line1\n    line2"})
	require.NoError(t, err)
	assert.Contains(t, out, "    line2")
}

func TestRenderAndValidate_MultiLineParam_BaselineAccountsForFirstLineIndent(t *testing.T) {
	body := `data:
  config: |
    {{.config}}
`
	// Template ref at 4 spaces, first line "  a" at 2 spaces → baseline = 6.
	// Continuation "    b" at 4 spaces < 6 → rejected.
	_, err := RenderAndValidate(body, map[string]any{"config": "  a\n    b"})
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrRenderInvalid)
}

func TestRenderAndValidate_MultiLineParam_GreaterIndent_OK(t *testing.T) {
	body := `data:
  config: |
    {{.config}}
`
	// Template ref at 4 spaces, first line "  a" at 2 spaces → baseline = 6.
	// Continuation "      b" at 6 spaces >= 6 → OK.
	out, err := RenderAndValidate(body, map[string]any{"config": "  a\n      b"})
	require.NoError(t, err)
	assert.Contains(t, out, "      b")
}

func TestRenderAndValidate_MultiLineParam_FirstLineIndented_ContinuationOK(t *testing.T) {
	body := `data:
  config: |
    {{.config}}
`
	// Template ref at 4 spaces, first line "    a" at 4 spaces → baseline = 8.
	// Continuation "        b" at 8 spaces >= 8 → OK.
	out, err := RenderAndValidate(body, map[string]any{"config": "    a\n        b"})
	require.NoError(t, err)
	assert.Contains(t, out, "        b")
}

func TestRenderAndValidate_NonMultiLine_Skips(t *testing.T) {
	body := `value: {{.x}}`
	out, err := RenderAndValidate(body, map[string]any{"x": "hello"})
	require.NoError(t, err)
	assert.Equal(t, "value: hello", out)
}

func TestRenderAndValidate_IndentFunc(t *testing.T) {
	body := `data:
  config: |
{{.config | indent 2}}
`
	out, err := RenderAndValidate(body, map[string]any{"config": "line1\nline2"})
	require.NoError(t, err)
	assert.Contains(t, out, "  line1")
	assert.Contains(t, out, "  line2")
}

func TestRenderAndValidate_SpacedDelimiter_Detected(t *testing.T) {
	body := `data:
  config: |
    {{ .config }}
`
	_, err := RenderAndValidate(body, map[string]any{"config": "line1\nline2"})
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrRenderInvalid)
}

func TestRenderAndValidate_SpacedDelimiterCondensed_Detected(t *testing.T) {
	body := `data:
  config: |
    {{.config }}
`
	_, err := RenderAndValidate(body, map[string]any{"config": "line1\nline2"})
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrRenderInvalid)
}

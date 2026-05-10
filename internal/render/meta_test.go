package render

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseMeta_Minimal(t *testing.T) {
	src := `# template-meta:
#   id: example
#   parameters:
#     required: [slug, image]
#     optional: []
#   secrets:
#     per_instance: [password]
#     per_host_referenced: [registry-pull-token]
#   volumes:
#     - name: data
#       backup: none
---
apiVersion: v1
kind: Pod
`
	meta, body, err := ParseMeta(src)
	require.NoError(t, err)

	assert.Equal(t, "example", meta.ID)
	assert.Equal(t, []string{"slug", "image"}, meta.Parameters.Required)
	assert.Empty(t, meta.Parameters.Optional)
	assert.Equal(t, []string{"password"}, meta.Secrets.PerInstance)
	assert.Equal(t, []string{"registry-pull-token"}, meta.Secrets.PerHostReferenced)
	require.Len(t, meta.Volumes, 1)
	assert.Equal(t, "data", meta.Volumes[0].Name)
	assert.Equal(t, "none", meta.Volumes[0].Backup)

	assert.Contains(t, body, "apiVersion: v1")
	assert.NotContains(t, body, "template-meta")
	assert.True(t, strings.HasPrefix(strings.TrimLeft(body, " \t"), "---"),
		"body must start with --- separator, got: %q", body[:min(40, len(body))])
}

func TestParseMeta_MissingMeta(t *testing.T) {
	src := `apiVersion: v1
kind: Pod
`
	_, _, err := ParseMeta(src)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "template-meta")
}

func TestParseMeta_EmptyBody(t *testing.T) {
	src := `# template-meta:
#   id: x
#   parameters:
#     required: []
`
	_, body, err := ParseMeta(src)
	require.NoError(t, err)
	assert.Equal(t, "", body, "body should be empty when meta block runs to EOF")
}

func TestParseMeta_MissingID(t *testing.T) {
	src := `# template-meta:
#   parameters:
#     required: []
---
apiVersion: v1
`
	_, _, err := ParseMeta(src)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "id")
}

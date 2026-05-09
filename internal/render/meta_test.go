package render

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseMeta_Minimal(t *testing.T) {
	src := `# template-meta:
#   id: lite-engine
#   parameters:
#     required: [slug, image]
#     optional: []
#   secrets:
#     per_instance: [auth_secret]
#     per_host_referenced: [s3-access-key-id]
#   volumes:
#     - name: data
#       backup: litestream
---
apiVersion: v1
kind: Pod
`
	meta, body, err := ParseMeta(src)
	require.NoError(t, err)

	assert.Equal(t, "lite-engine", meta.ID)
	assert.Equal(t, []string{"slug", "image"}, meta.Parameters.Required)
	assert.Empty(t, meta.Parameters.Optional)
	assert.Equal(t, []string{"auth_secret"}, meta.Secrets.PerInstance)
	assert.Equal(t, []string{"s3-access-key-id"}, meta.Secrets.PerHostReferenced)
	require.Len(t, meta.Volumes, 1)
	assert.Equal(t, "data", meta.Volumes[0].Name)
	assert.Equal(t, "litestream", meta.Volumes[0].Backup)

	assert.Contains(t, body, "apiVersion: v1")
	assert.NotContains(t, body, "template-meta")
}

func TestParseMeta_MissingMeta(t *testing.T) {
	src := `apiVersion: v1
kind: Pod
`
	_, _, err := ParseMeta(src)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "template-meta")
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

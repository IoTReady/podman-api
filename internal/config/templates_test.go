package config

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/iotready/podman-api/templates"
)

func TestLoadTemplates_FromEmbed(t *testing.T) {
	tmpls, err := LoadTemplates(templates.Files, ".")
	require.NoError(t, err)

	byID := map[string]Template{}
	for _, t := range tmpls {
		byID[t.Meta.ID] = t
	}

	le, ok := byID["lite-engine"]
	require.True(t, ok, "expected lite-engine template to load")
	assert.Contains(t, le.Meta.Parameters.Required, "slug")
	assert.Contains(t, le.Meta.Parameters.Required, "image")
	assert.Contains(t, le.Meta.Secrets.PerInstance, "auth_secret")
	assert.NotEmpty(t, le.Body, "body should be non-empty")
}

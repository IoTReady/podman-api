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
	for _, tmpl := range tmpls {
		byID[tmpl.Meta.ID] = tmpl
	}

	// All three bundled templates must be present.
	require.GreaterOrEqual(t, len(byID), 3, "expected at least 3 templates")
	for _, id := range []string{"lite-engine", "lite-crm", "google-groups"} {
		_, ok := byID[id]
		require.True(t, ok, "expected template %q to load", id)
	}

	le := byID["lite-engine"]
	assert.Contains(t, le.Meta.Parameters.Required, "slug")
	assert.Contains(t, le.Meta.Parameters.Required, "image")
	assert.Contains(t, le.Meta.Secrets.PerInstance, "auth_secret")
	assert.NotEmpty(t, le.Body, "body should be non-empty")

	lc := byID["lite-crm"]
	assert.Contains(t, lc.Meta.Parameters.Required, "slug")
	assert.Contains(t, lc.Meta.Parameters.Required, "image")
	assert.Contains(t, lc.Meta.Secrets.PerInstance, "auth_secret")
	assert.NotEmpty(t, lc.Body, "body should be non-empty")

	gg := byID["google-groups"]
	assert.Contains(t, gg.Meta.Parameters.Required, "slug")
	assert.Contains(t, gg.Meta.Parameters.Required, "image")
	assert.Contains(t, gg.Meta.Parameters.Required, "port")
	assert.NotEmpty(t, gg.Body, "body should be non-empty")
}

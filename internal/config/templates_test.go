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

	pg, ok := byID["postgres"]
	require.True(t, ok, "expected bundled postgres template to load")
	assert.Contains(t, pg.Meta.Parameters.Required, "slug")
	assert.Contains(t, pg.Meta.Parameters.Required, "image")
	assert.Contains(t, pg.Meta.Parameters.Required, "port")
	assert.Contains(t, pg.Meta.Secrets.PerInstance, "password")
	assert.NotEmpty(t, pg.Body, "body should be non-empty")
}

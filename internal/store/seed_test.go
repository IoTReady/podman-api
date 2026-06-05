package store

import (
	"testing"

	"github.com/stretchr/testify/require"

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

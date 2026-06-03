//go:build integration

package podman

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/iotready/podman-api/internal/config"
)

func TestRealHostInfo_LocalSocket(t *testing.T) {
	sock := localSocket(t)
	c, err := NewReal([]config.Host{{ID: "local", Addr: "unix", Socket: sock}})
	require.NoError(t, err)

	info, err := c.HostInfo(context.Background(), "local")
	require.NoError(t, err)

	assert.Greater(t, info.CPUs, 0, "CPUs should be > 0")
	assert.Greater(t, info.MemTotal, int64(0), "MemTotal should be > 0")
	assert.GreaterOrEqual(t, info.MemUsedPct, float64(0), "MemUsedPct should be >= 0")

	// On a unix (local) host, /proc/loadavg is always readable.
	require.NotNil(t, info.LoadAvg, "LoadAvg should not be nil for local unix host")
	assert.GreaterOrEqual(t, info.LoadAvg[0], float64(0), "LoadAvg[0] (1-min) should be >= 0")
}

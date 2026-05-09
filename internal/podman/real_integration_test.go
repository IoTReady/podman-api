//go:build integration

package podman

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/iotready/podman-api/internal/config"
)

func localSocket(t *testing.T) string {
	t.Helper()
	rt := os.Getenv("XDG_RUNTIME_DIR")
	if rt == "" {
		t.Skip("XDG_RUNTIME_DIR unset")
	}
	p := filepath.Join(rt, "podman", "podman.sock")
	if _, err := os.Stat(p); err != nil {
		t.Skip("local podman socket not available: " + err.Error())
	}
	return p
}

func TestRealClient_Ping_LocalOnly(t *testing.T) {
	sock := localSocket(t)
	c, err := NewReal([]config.Host{{ID: "local", Addr: "unix", Socket: sock}})
	require.NoError(t, err)

	require.NoError(t, c.Ping(context.Background(), "local"))

	v, err := c.Version(context.Background(), "local")
	require.NoError(t, err)
	require.NotEmpty(t, v)
}

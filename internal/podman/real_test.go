package podman

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/iotready/podman-api/internal/config"
)

func TestRealClient_RegistersHosts(t *testing.T) {
	hosts := []config.Host{
		{ID: "local", Addr: "unix", Socket: "/run/user/1000/podman/podman.sock"},
		{ID: "remote", Addr: "ubuntu@x", Socket: "/run/user/1000/podman/podman.sock"},
	}
	c, err := NewReal(hosts)
	require.NoError(t, err)
	assert.True(t, c.Knows("local"))
	assert.True(t, c.Knows("remote"))
	assert.False(t, c.Knows("nope"))
}

func TestRealClient_URIFor(t *testing.T) {
	hosts := []config.Host{
		{ID: "local", Addr: "unix", Socket: "/tmp/podman.sock"},
		{ID: "remote", Addr: "ubuntu@x.example", Socket: "/run/user/1000/podman/podman.sock", SSHKey: "/k"},
	}
	c, err := NewReal(hosts)
	require.NoError(t, err)

	uri, err := c.URIFor("local")
	require.NoError(t, err)
	assert.Equal(t, "unix:///tmp/podman.sock", uri)

	uri, err = c.URIFor("remote")
	require.NoError(t, err)
	assert.Equal(t, "ssh://ubuntu@x.example/run/user/1000/podman/podman.sock", uri)
}

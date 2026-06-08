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

func TestRealClient_SetHosts_AddsNew(t *testing.T) {
	c, err := NewReal([]config.Host{
		{ID: "h1", Addr: "unix", Socket: "/s1"},
	})
	require.NoError(t, err)
	assert.True(t, c.Knows("h1"))
	assert.False(t, c.Knows("h2"))

	c.SetHosts([]config.Host{
		{ID: "h1", Addr: "unix", Socket: "/s1"},
		{ID: "h2", Addr: "unix", Socket: "/s2"},
	})
	assert.True(t, c.Knows("h1"))
	assert.True(t, c.Knows("h2"))
}

func TestRealClient_SetHosts_Removes(t *testing.T) {
	c, err := NewReal([]config.Host{
		{ID: "h1", Addr: "unix", Socket: "/s1"},
		{ID: "h2", Addr: "unix", Socket: "/s2"},
	})
	require.NoError(t, err)
	assert.True(t, c.Knows("h1"))

	c.SetHosts([]config.Host{
		{ID: "h2", Addr: "unix", Socket: "/s2"},
	})
	assert.False(t, c.Knows("h1"))
	assert.True(t, c.Knows("h2"))
	// URIFor must also reject the removed host.
	_, err = c.URIFor("h1")
	assert.ErrorContains(t, err, "unknown host")
}

func TestRealClient_SetHosts_ChangesConnectionParams(t *testing.T) {
	c, err := NewReal([]config.Host{
		{ID: "h1", Addr: "unix", Socket: "/old.sock"},
	})
	require.NoError(t, err)
	_, err = c.URIFor("h1")
	require.NoError(t, err)

	// Change the socket path. URIFor reflects the new params.
	c.SetHosts([]config.Host{
		{ID: "h1", Addr: "unix", Socket: "/new.sock"},
	})
	uri, err := c.URIFor("h1")
	require.NoError(t, err)
	assert.Equal(t, "unix:///new.sock", uri)
}

func TestRealClient_SetHosts_NonConnectionFieldsIgnored(t *testing.T) {
	// Drain, Labels, Prune etc. should NOT invalidate the cached connection.
	c, err := NewReal([]config.Host{
		{ID: "h1", Addr: "unix", Socket: "/s1", Drain: false},
	})
	require.NoError(t, err)
	uri, err := c.URIFor("h1")
	require.NoError(t, err)
	assert.Equal(t, "unix:///s1", uri)

	c.SetHosts([]config.Host{
		{ID: "h1", Addr: "unix", Socket: "/s1", Drain: true},
	})
	uri, err = c.URIFor("h1")
	require.NoError(t, err)
	assert.Equal(t, "unix:///s1", uri)
}

// TestRealClient_URIFor_WithSSHKey is a smoke test that ssh-key-bearing hosts
// still produce the canonical libpod URI. The SSH identity is wired in
// ctxFor (via bindings.NewConnectionWithIdentity); end-to-end verification
// requires a real SSH host and is a manual post-merge step.
func TestRealClient_URIFor_WithSSHKey(t *testing.T) {
	hosts := []config.Host{
		{ID: "ssh", Addr: "user@example", Socket: "/run/podman.sock", SSHKey: "/etc/keys/ssh"},
	}
	c, err := NewReal(hosts)
	require.NoError(t, err)
	uri, err := c.URIFor("ssh")
	require.NoError(t, err)
	assert.Equal(t, "ssh://user@example/run/podman.sock", uri)
}

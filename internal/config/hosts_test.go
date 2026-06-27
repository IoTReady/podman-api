package config

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLoadHosts(t *testing.T) {
	hosts, err := LoadHosts("testdata/hosts")
	require.NoError(t, err)
	require.Len(t, hosts, 2)

	byID := map[string]Host{}
	for _, h := range hosts {
		byID[h.ID] = h
	}

	local, ok := byID["local"]
	require.True(t, ok)
	assert.Equal(t, "unix", local.Addr)
	assert.Equal(t, "/run/user/1000/podman/podman.sock", local.Socket)
	assert.Equal(t, "dev", local.Labels["env"])

	prod, ok := byID["prod-1"]
	require.True(t, ok)
	assert.Equal(t, "ubuntu@prod-1", prod.Addr)
	assert.Equal(t, "/etc/podman-api/ssh/prod-1", prod.SSHKey)
}

func TestLoadHosts_MissingDir(t *testing.T) {
	_, err := LoadHosts("testdata/does-not-exist")
	require.Error(t, err)
}

func TestLoadHosts_DuplicateID(t *testing.T) {
	dir := t.TempDir()
	err := writeFile(dir+"/a.yaml", "id: same\naddr: unix\nsocket: /tmp/x\n")
	require.NoError(t, err)
	err = writeFile(dir+"/b.yaml", "id: same\naddr: unix\nsocket: /tmp/y\n")
	require.NoError(t, err)
	_, err = LoadHosts(dir)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "duplicate")
}

func writeFile(path, content string) error {
	return os.WriteFile(path, []byte(content), 0644)
}

func TestLoadHostsCaddyAdminAddr(t *testing.T) {
	dir := t.TempDir()
	writeHost(t, dir, "h1.yaml", "id: h1\naddr: user@engine-1\nsocket: /run/user/1000/podman/podman.sock\ncaddy_admin_addr: \"100.64.1.2:2019\"\n")
	hosts, err := LoadHosts(dir)
	require.NoError(t, err)
	require.Len(t, hosts, 1)
	require.Equal(t, "100.64.1.2:2019", hosts[0].CaddyAdminAddr)
}

func TestLoadHostsCaddyAdminAddrAbsent(t *testing.T) {
	dir := t.TempDir()
	writeHost(t, dir, "h1.yaml", "id: h1\naddr: unix\nsocket: /run/user/1000/podman/podman.sock\n")
	hosts, err := LoadHosts(dir)
	require.NoError(t, err)
	require.Equal(t, "", hosts[0].CaddyAdminAddr)
}

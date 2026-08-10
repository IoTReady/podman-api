package config

import (
	"os"
	"path/filepath"
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

func TestRenameHostFile(t *testing.T) {
	dir := t.TempDir()
	content := "id: h1\naddr: unix\nsocket: /run/podman.sock\n# a comment to preserve\nlabels:\n  env: dev\n"
	require.NoError(t, os.WriteFile(filepath.Join(dir, "h1.yaml"), []byte(content), 0o644))

	path, original, err := RenameHostFile(dir, "h1", "h2")
	require.NoError(t, err)
	require.Equal(t, filepath.Join(dir, "h1.yaml"), path)
	require.Equal(t, []byte(content), original)

	got, err := os.ReadFile(path)
	require.NoError(t, err)
	require.Equal(t, "id: h2\naddr: unix\nsocket: /run/podman.sock\n# a comment to preserve\nlabels:\n  env: dev\n", string(got))

	hosts, err := LoadHosts(dir)
	require.NoError(t, err)
	require.Len(t, hosts, 1)
	require.Equal(t, "h2", hosts[0].ID)
}

func TestRenameHostFileNoMatch(t *testing.T) {
	dir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(dir, "h1.yaml"), []byte("id: h1\naddr: unix\nsocket: /x\n"), 0o644))

	_, _, err := RenameHostFile(dir, "does-not-exist", "h2")
	require.Error(t, err)
}

func TestRenameHostFileUnexpectedIDLineFormat(t *testing.T) {
	dir := t.TempDir()
	// id on the same line as a trailing comment — the exact-line match must
	// fail closed rather than guess.
	require.NoError(t, os.WriteFile(filepath.Join(dir, "h1.yaml"), []byte("id: h1 # primary\naddr: unix\nsocket: /x\n"), 0o644))

	_, _, err := RenameHostFile(dir, "h1", "h2")
	require.Error(t, err)
}

func TestHostsDirRenameHostFile(t *testing.T) {
	dir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(dir, "h1.yaml"), []byte("id: h1\naddr: unix\nsocket: /x\n"), 0o644))

	d := HostsDir(dir)
	path, original, err := d.RenameHostFile("h1", "h2")
	require.NoError(t, err)
	require.NotEmpty(t, path)
	require.Contains(t, string(original), "id: h1")
}

func TestWriteFileAtomicPreservesMode(t *testing.T) {
	dir := t.TempDir()
	p := filepath.Join(dir, "h1.yaml")
	require.NoError(t, os.WriteFile(p, []byte("old\n"), 0o600))

	require.NoError(t, WriteFileAtomic(p, []byte("new\n")))

	got, err := os.ReadFile(p)
	require.NoError(t, err)
	require.Equal(t, "new\n", string(got))
	fi, err := os.Stat(p)
	require.NoError(t, err)
	require.Equal(t, os.FileMode(0o600), fi.Mode().Perm())

	// No temp file left behind.
	entries, err := os.ReadDir(dir)
	require.NoError(t, err)
	require.Len(t, entries, 1)
}

func TestWriteFileAtomicCreatesWithDefaultMode(t *testing.T) {
	p := filepath.Join(t.TempDir(), "new.yaml")
	require.NoError(t, WriteFileAtomic(p, []byte("x\n")))
	fi, err := os.Stat(p)
	require.NoError(t, err)
	require.Equal(t, DefaultConfigFileMode, fi.Mode().Perm())
}

// RenameHostFile's forward write must keep the host file's mode, not flatten
// it to 0644 (final-review finding #9).
func TestRenameHostFilePreservesMode(t *testing.T) {
	dir := t.TempDir()
	p := filepath.Join(dir, "h1.yaml")
	require.NoError(t, os.WriteFile(p, []byte("# a comment\nid: h1\naddr: unix\nsocket: /x\n"), 0o600))

	gotPath, original, err := RenameHostFile(dir, "h1", "h2")
	require.NoError(t, err)
	require.Equal(t, p, gotPath)
	require.Contains(t, string(original), "id: h1")

	fi, err := os.Stat(p)
	require.NoError(t, err)
	require.Equal(t, os.FileMode(0o600), fi.Mode().Perm())

	raw, err := os.ReadFile(p)
	require.NoError(t, err)
	require.Contains(t, string(raw), "id: h2")
	require.Contains(t, string(raw), "# a comment")
}

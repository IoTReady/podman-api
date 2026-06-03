package config

import (
	"strings"
	"testing"
	"testing/fstest"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// --- ParseKeysYAML error paths ----------------------------------------------

func TestParseKeysYAML_Errors(t *testing.T) {
	t.Run("unknown field", func(t *testing.T) {
		_, err := ParseKeysYAML([]byte("keys:\n  - id: k\n    secret_hash: h\n    bogus: x\n"))
		require.Error(t, err)
		assert.Contains(t, err.Error(), "parse keys")
	})
	t.Run("missing id", func(t *testing.T) {
		_, err := ParseKeysYAML([]byte("keys:\n  - secret_hash: h\n"))
		require.Error(t, err)
		assert.Contains(t, err.Error(), "id is required")
	})
	t.Run("missing secret_hash", func(t *testing.T) {
		_, err := ParseKeysYAML([]byte("keys:\n  - id: k\n"))
		require.Error(t, err)
		assert.Contains(t, err.Error(), "secret_hash is required")
	})
}

// --- VerifyToken malformed-encoding paths -----------------------------------

func TestVerifyToken_MalformedEncodings(t *testing.T) {
	// c2FsdA == "salt", aGFzaA == "hash" in RawStdEncoding; both non-empty so
	// they pass the empty-check when a case is meant to reach it.
	cases := []struct {
		name    string
		encoded string
		errSub  string
	}{
		{"wrong part count", "not-a-hash", "not an argon2id hash"},
		{"not argon2id", "$bcrypt$v=19$m=65536,t=3,p=4$c2FsdA$aGFzaA", "not an argon2id hash"},
		{"bad version", "$argon2id$vXX$m=65536,t=3,p=4$c2FsdA$aGFzaA", "bad version"},
		{"version mismatch", "$argon2id$v=99$m=65536,t=3,p=4$c2FsdA$aGFzaA", "version mismatch"},
		{"bad params", "$argon2id$v=19$nope$c2FsdA$aGFzaA", "bad params"},
		{"bad salt b64", "$argon2id$v=19$m=65536,t=3,p=4$!!!$aGFzaA", "illegal base64"},
		{"bad hash b64", "$argon2id$v=19$m=65536,t=3,p=4$c2FsdA$!!!", "illegal base64"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			ok, err := VerifyToken("anything", c.encoded)
			require.Error(t, err)
			assert.False(t, ok)
			assert.Contains(t, err.Error(), c.errSub)
		})
	}
}

// --- LoadTemplates error paths (in-memory fs) -------------------------------

const validMeta = "# template-meta:\n#   id: %s\n---\nbody\n"

func TestLoadTemplates_BadMeta(t *testing.T) {
	fsys := fstest.MapFS{
		"broken.yaml": {Data: []byte("not-a-template: true\n")},
	}
	_, err := LoadTemplates(fsys, ".")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "broken.yaml")
}

func TestLoadTemplates_DuplicateID(t *testing.T) {
	doc := strings.Replace(validMeta, "%s", "dup", 1)
	fsys := fstest.MapFS{
		"a.yaml": {Data: []byte(doc)},
		"b.yaml": {Data: []byte(doc)},
	}
	_, err := LoadTemplates(fsys, ".")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "duplicate")
}

func TestLoadTemplates_NonYAMLFilesIgnored(t *testing.T) {
	fsys := fstest.MapFS{
		"README.md":  {Data: []byte("# not a template")},
		"good.yaml":  {Data: []byte(strings.Replace(validMeta, "%s", "ok", 1))},
		"sub/nested": {Data: []byte("ignored")},
	}
	tmpls, err := LoadTemplates(fsys, ".")
	require.NoError(t, err)
	require.Len(t, tmpls, 1)
	assert.Equal(t, "ok", tmpls[0].Meta.ID)
}

func TestLoadTemplates_WalkError(t *testing.T) {
	// Walking a root that does not exist surfaces the WalkDir error.
	_, err := LoadTemplates(fstest.MapFS{}, "no-such-root")
	require.Error(t, err)
}

// --- LoadHosts error paths --------------------------------------------------

func TestLoadHosts_ParseError(t *testing.T) {
	dir := t.TempDir()
	require.NoError(t, writeFile(dir+"/bad.yaml", "id: h\naddr: unix\nbogus_field: x\n"))
	_, err := LoadHosts(dir)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "parse")
}

func TestLoadHosts_MissingID(t *testing.T) {
	dir := t.TempDir()
	require.NoError(t, writeFile(dir+"/noid.yaml", "addr: unix\nsocket: /tmp/x\n"))
	_, err := LoadHosts(dir)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "id is required")
}

func TestLoadHosts_SkipsNonYAMLAndDirs(t *testing.T) {
	dir := t.TempDir()
	require.NoError(t, writeFile(dir+"/note.txt", "not a host"))
	require.NoError(t, writeFile(dir+"/h.yaml", "id: only\naddr: unix\nsocket: /tmp/x\n"))
	hosts, err := LoadHosts(dir)
	require.NoError(t, err)
	require.Len(t, hosts, 1)
	assert.Equal(t, "only", hosts[0].ID)
}

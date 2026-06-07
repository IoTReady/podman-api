package instance

import (
	"archive/tar"
	"bytes"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// tarBytes builds an uncompressed tar (one regular file per map entry) for use
// as fake volume contents. Map iteration order varies, which also exercises the
// manifest's order-independence.
func tarBytes(t *testing.T, files map[string]string) []byte {
	t.Helper()
	var buf bytes.Buffer
	tw := tar.NewWriter(&buf)
	for name, content := range files {
		require.NoError(t, tw.WriteHeader(&tar.Header{
			Name: name, Mode: 0o644, Size: int64(len(content)), Typeflag: tar.TypeReg,
		}))
		_, err := tw.Write([]byte(content))
		require.NoError(t, err)
	}
	require.NoError(t, tw.Close())
	return buf.Bytes()
}

func TestBuildManifest(t *testing.T) {
	a, err := buildManifest(bytes.NewReader(tarBytes(t, map[string]string{"f1": "hello", "dir/f2": "world"})))
	require.NoError(t, err)
	require.Len(t, a, 2)
	assert.Equal(t, int64(5), a["f1"].size)
	assert.NotEmpty(t, a["f1"].sha256)
}

func TestManifest_FirstDiff(t *testing.T) {
	base, _ := buildManifest(bytes.NewReader(tarBytes(t, map[string]string{"f1": "hello", "dir/f2": "world"})))

	// Same content, different write order -> equal.
	same, _ := buildManifest(bytes.NewReader(tarBytes(t, map[string]string{"dir/f2": "world", "f1": "hello"})))
	_, ok := base.firstDiff(same)
	assert.True(t, ok, "identical content must compare equal")

	// Changed content -> differs at that path.
	changed, _ := buildManifest(bytes.NewReader(tarBytes(t, map[string]string{"f1": "HELLO", "dir/f2": "world"})))
	diff, ok := base.firstDiff(changed)
	assert.False(t, ok)
	assert.Equal(t, "f1", diff)

	// Missing file -> differs.
	missing, _ := buildManifest(bytes.NewReader(tarBytes(t, map[string]string{"f1": "hello"})))
	_, ok = base.firstDiff(missing)
	assert.False(t, ok)
}

func TestBuildManifest_EmptyStream(t *testing.T) {
	m, err := buildManifest(bytes.NewReader(nil))
	require.NoError(t, err)
	assert.Empty(t, m)
}

func TestBuildManifest_NotTar(t *testing.T) {
	_, err := buildManifest(bytes.NewReader([]byte("this is definitely not a tar archive")))
	require.Error(t, err)
}

func TestManifest_JSONRoundTrip(t *testing.T) {
	m := Manifest{
		"data/file": fileInfo{typ: tar.TypeReg, size: 5, sha256: "abc123"},
		"data/link": fileInfo{typ: tar.TypeSymlink, link: "file"},
		"data":      fileInfo{typ: tar.TypeDir},
	}
	raw, err := json.Marshal(m)
	require.NoError(t, err)
	var got Manifest
	require.NoError(t, json.Unmarshal(raw, &got))
	diff, equal := m.firstDiff(got)
	assert.True(t, equal, "round-trip changed manifest at %q", diff)
}

func TestManifest_JSONRoundTrip_DetectsDiff(t *testing.T) {
	m := Manifest{"f": fileInfo{typ: tar.TypeReg, size: 1, sha256: "aa"}}
	raw, _ := json.Marshal(m)
	var got Manifest
	require.NoError(t, json.Unmarshal(raw, &got))
	got["f"] = fileInfo{typ: tar.TypeReg, size: 1, sha256: "bb"}
	_, equal := m.firstDiff(got)
	assert.False(t, equal)
}

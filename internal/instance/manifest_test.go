package instance

import (
	"archive/tar"
	"bytes"
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

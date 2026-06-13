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

func TestExcludePath_Litestream(t *testing.T) {
	tests := []struct {
		path string
		want bool
	}{
		{"data/mydb.db-litestream/generations/abc123/wal/0000000000000001", true},
		{"mydb.db-litestream/generations/abc123/snapshots/snap", true},
		{".engine.db-litestream/generations/abc123/wal/0001", true},
		{"data/file.txt", false},
		{"subdir/other.log", false},
		{"-litestream", true},
		{"somedir/foo", false},
	}
	for _, tt := range tests {
		got := excludePath(tt.path)
		if got != tt.want {
			t.Errorf("excludePath(%q) = %v, want %v", tt.path, got, tt.want)
		}
	}
}

func TestBuildManifest_SkipsLitestreamDirs(t *testing.T) {
	files := map[string]string{
		"PG_VERSION": "16",
		"mydb.db-litestream/generations/abc/wal/0001":       "wal content",
		"mydb.db-litestream/generations/abc/snapshots/snap": "snap content",
	}
	m, err := buildManifest(bytes.NewReader(tarBytes(t, files)))
	require.NoError(t, err)
	// Only PG_VERSION should be in the manifest; litestream paths excluded.
	if _, ok := m["PG_VERSION"]; !ok {
		t.Error("expected PG_VERSION to be present")
	}
	if _, ok := m["mydb.db-litestream/generations/abc/wal/0001"]; ok {
		t.Error("expected litestream WAL path to be excluded")
	}
	if _, ok := m["mydb.db-litestream/generations/abc/snapshots/snap"]; ok {
		t.Error("expected litestream snapshot path to be excluded")
	}
	if len(m) != 1 {
		t.Errorf("expected 1 entry, got %d", len(m))
	}
}

func TestManifest_JSONRoundTrip(t *testing.T) {
	m := Manifest{
		"data/file":  fileInfo{typ: tar.TypeReg, size: 5, sha256: "abc123"},
		"data/link":  fileInfo{typ: tar.TypeSymlink, link: "file"},
		"data":       fileInfo{typ: tar.TypeDir},
		"data/empty": fileInfo{typ: tar.TypeReg, size: 0, sha256: "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"},
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

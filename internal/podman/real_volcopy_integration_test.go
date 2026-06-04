//go:build integration

package podman

import (
	"archive/tar"
	"bytes"
	"context"
	"io"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/iotready/podman-api/internal/config"
)

// makeTar builds an uncompressed tar containing a single file.
func makeTar(t *testing.T, name string, content []byte) []byte {
	t.Helper()
	var buf bytes.Buffer
	tw := tar.NewWriter(&buf)
	require.NoError(t, tw.WriteHeader(&tar.Header{
		Name: name,
		Mode: 0o644,
		Size: int64(len(content)),
	}))
	_, err := tw.Write(content)
	require.NoError(t, err)
	require.NoError(t, tw.Close())
	return buf.Bytes()
}

// readFileFromTar returns the contents of the named file from a tar stream.
func readFileFromTar(t *testing.T, r io.Reader, name string) []byte {
	t.Helper()
	tr := tar.NewReader(r)
	for {
		h, err := tr.Next()
		if err == io.EOF {
			t.Fatalf("file %q not found in tar", name)
		}
		require.NoError(t, err)
		if h.Name == name {
			b, err := io.ReadAll(tr)
			require.NoError(t, err)
			return b
		}
	}
}

func TestReal_VolumeCopy_LocalOnly(t *testing.T) {
	sock := localSocket(t)
	c, err := NewReal([]config.Host{{ID: "local", Addr: "unix", Socket: sock}})
	require.NoError(t, err)
	ctx := context.Background()

	const src, dst = "podman-api-itest-vol-src", "podman-api-itest-vol-dst"

	// Register teardown before creating anything so both volumes are removed even
	// if a create fails partway. Pre-remove clears leftovers from a prior crashed
	// run so reruns self-heal.
	t.Cleanup(func() {
		_ = c.VolumeRemove(context.Background(), "local", src, true)
		_ = c.VolumeRemove(context.Background(), "local", dst, true)
	})
	for _, name := range []string{src, dst} {
		_ = c.VolumeRemove(ctx, "local", name, true) // clear any leftover from a prior crashed run
		require.NoError(t, c.VolumeCreate(ctx, "local", name))
	}

	// VolumeCreate is idempotent — creating an existing volume is not an error.
	require.NoError(t, c.VolumeCreate(ctx, "local", src))

	// Seed the source volume with a known file via VolumeImport.
	want := []byte("cold-copy-payload")
	require.NoError(t, c.VolumeImport(ctx, "local", src, bytes.NewReader(makeTar(t, "hello.txt", want))))

	// Export source, import into dest — the primitive under test.
	rc, err := c.VolumeExport(ctx, "local", src)
	require.NoError(t, err)
	require.NoError(t, c.VolumeImport(ctx, "local", dst, rc))
	require.NoError(t, rc.Close())

	// Export dest and assert the file survived the round-trip.
	out, err := c.VolumeExport(ctx, "local", dst)
	require.NoError(t, err)
	defer out.Close()
	assert.Equal(t, want, readFileFromTar(t, out, "hello.txt"))

	// Not-found mapping.
	_, err = c.VolumeExport(ctx, "local", "podman-api-itest-vol-missing")
	require.ErrorIs(t, err, ErrNotFound)
}

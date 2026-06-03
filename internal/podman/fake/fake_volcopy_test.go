package fake

import (
	"bytes"
	"context"
	"errors"
	"io"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/iotready/podman-api/internal/podman"
)

func TestFake_VolumeExportImport_RoundTrip(t *testing.T) {
	f := New()
	ctx := context.Background()
	want := []byte("tarball-bytes")

	// Seed a source volume with contents.
	f.SetVolumeData("src", "vol", want)
	// Destination volume must exist before import (matches real podman).
	f.AddVolume("dst", podman.Volume{Name: "vol"})

	rc, err := f.VolumeExport(ctx, "src", "vol")
	require.NoError(t, err)
	defer rc.Close()
	require.NoError(t, f.VolumeImport(ctx, "dst", "vol", rc))

	assert.Equal(t, want, f.VolumeData("dst", "vol"))
}

func TestFake_VolumeExport_NotFound(t *testing.T) {
	f := New()
	_, err := f.VolumeExport(context.Background(), "src", "missing")
	require.ErrorIs(t, err, podman.ErrNotFound)
}

func TestFake_VolumeImport_NotFound(t *testing.T) {
	f := New()
	err := f.VolumeImport(context.Background(), "dst", "missing", bytes.NewReader([]byte("x")))
	require.ErrorIs(t, err, podman.ErrNotFound)
}

func TestFake_VolumeExport_ErrHook(t *testing.T) {
	f := New()
	boom := errors.New("boom")
	f.ExportErr = boom
	_, err := f.VolumeExport(context.Background(), "src", "vol")
	require.ErrorIs(t, err, boom)
}

func TestFake_VolumeImport_ErrHook(t *testing.T) {
	f := New()
	boom := errors.New("boom")
	f.ImportErr = boom
	err := f.VolumeImport(context.Background(), "dst", "vol", bytes.NewReader([]byte("x")))
	require.ErrorIs(t, err, boom)
}

// errReader yields some bytes then a hard error, simulating a volume export
// stream that breaks mid-transfer.
type errReader struct {
	data []byte
	off  int
	err  error
}

func (r *errReader) Read(p []byte) (int, error) {
	if r.off < len(r.data) {
		n := copy(p, r.data[r.off:])
		r.off += n
		return n, nil
	}
	return 0, r.err
}
func (r *errReader) Close() error { return nil }

func TestFake_VolumeImport_PropagatesReaderError(t *testing.T) {
	f := New()
	f.AddVolume("dst", podman.Volume{Name: "vol"})
	boom := errors.New("stream broke")
	err := f.VolumeImport(context.Background(), "dst", "vol", &errReader{data: []byte("partial"), err: boom})
	require.ErrorIs(t, err, boom)
	// Nothing should have been committed.
	assert.Nil(t, f.VolumeData("dst", "vol"))
}

func TestFake_VolumeExport_ReaderHook(t *testing.T) {
	f := New()
	want := io.NopCloser(bytes.NewReader([]byte("hooked")))
	f.ExportReader = func(host, name string) io.ReadCloser { return want }
	rc, err := f.VolumeExport(context.Background(), "any", "any")
	require.NoError(t, err)
	got, _ := io.ReadAll(rc)
	assert.Equal(t, []byte("hooked"), got)
}

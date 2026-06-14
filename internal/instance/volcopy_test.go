package instance

import (
	"context"
	"errors"
	"io"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/iotready/podman-api/internal/config"
	"github.com/iotready/podman-api/internal/podman"
	"github.com/iotready/podman-api/internal/podman/fake"
)

func newVolSvc(f *fake.Fake) *Service {
	hosts := []config.Host{{ID: "a", Addr: "unix", Socket: "/x"}, {ID: "b", Addr: "unix", Socket: "/y"}}
	return NewService(f, hosts)
}

func TestCopyVolume_HappyPath(t *testing.T) {
	f := fake.New()
	want := tarBytes(t, map[string]string{"PG_VERSION": "16"})
	f.SetVolumeData("a", "vol", want)
	f.AddVolume("b", podman.Volume{Name: "vol"})

	svc := newVolSvc(f)
	manifest, err := svc.CopyVolume(context.Background(), "a", "b", "vol")
	require.NoError(t, err)

	assert.Equal(t, want, f.VolumeData("b", "vol"))
	assert.NotEmpty(t, manifest, "expected non-empty source manifest from copy")
}

func TestCopyVolume_EmptyVolume(t *testing.T) {
	f := fake.New()
	f.SetVolumeData("a", "vol", []byte{}) // source exists but has no contents
	f.AddVolume("b", podman.Volume{Name: "vol"})

	svc := newVolSvc(f)
	_, err := svc.CopyVolume(context.Background(), "a", "b", "vol")
	require.NoError(t, err)

	// An empty volume copies cleanly: dest exists and is empty (not an error).
	assert.Empty(t, f.VolumeData("b", "vol"))
}

func TestCopyVolume_ImportFails_SourceIntact(t *testing.T) {
	f := fake.New()
	want := tarBytes(t, map[string]string{"PG_VERSION": "16"})
	f.SetVolumeData("a", "vol", want)
	f.AddVolume("b", podman.Volume{Name: "vol"})
	boom := errors.New("dest rejected")
	f.ImportErr = boom

	svc := newVolSvc(f)
	_, err := svc.CopyVolume(context.Background(), "a", "b", "vol")
	require.ErrorIs(t, err, boom)
	assert.ErrorContains(t, err, "import volume")

	// Source is read-only — unchanged. Dest never committed.
	assert.Equal(t, want, f.VolumeData("a", "vol"))
	assert.Nil(t, f.VolumeData("b", "vol"))
	// Test returning means the copy goroutine was not left blocked on the pipe.
}

func TestCopyVolume_ExportFailsMidStream_Aborts(t *testing.T) {
	f := fake.New()
	f.AddVolume("b", podman.Volume{Name: "vol"})
	boom := errors.New("source stream broke")
	// Valid tar that's truncated so buildManifest fails mid-stream.
	srcTar := tarBytes(t, map[string]string{"PG_VERSION": "16"})
	partialTar := srcTar[:len(srcTar)-1]
	f.ExportReader = func(host, name string) io.ReadCloser {
		return &midStreamReader{data: partialTar, err: boom}
	}

	svc := newVolSvc(f)
	_, err := svc.CopyVolume(context.Background(), "a", "b", "vol")
	require.Error(t, err)
	assert.ErrorContains(t, err, "source stream broke")
	assert.Nil(t, f.VolumeData("b", "vol"))
}

type midStreamReader struct {
	data []byte
	off  int
	err  error
}

func (r *midStreamReader) Read(p []byte) (int, error) {
	if r.off < len(r.data) {
		n := copy(p, r.data[r.off:])
		r.off += n
		return n, nil
	}
	return 0, r.err
}
func (r *midStreamReader) Close() error { return nil }

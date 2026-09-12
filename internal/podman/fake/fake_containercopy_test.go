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

func TestFake_ContainerCopyInOut_RoundTrip(t *testing.T) {
	f := New()
	ctx := context.Background()
	want := []byte("tarball-bytes")

	require.NoError(t, f.ContainerCopyIn(ctx, "h1", "app", "/tmp/in", bytes.NewReader(want)))

	rc, err := f.ContainerCopyOut(ctx, "h1", "app", "/tmp/in")
	require.NoError(t, err)
	defer rc.Close()
	got, err := io.ReadAll(rc)
	require.NoError(t, err)
	assert.Equal(t, want, got)

	require.Len(t, f.CopyInCalls, 1)
	assert.Equal(t, CopyPathCall{Host: "h1", Container: "app", Path: "/tmp/in"}, f.CopyInCalls[0])
	require.Len(t, f.CopyOutCalls, 1)
	assert.Equal(t, CopyPathCall{Host: "h1", Container: "app", Path: "/tmp/in"}, f.CopyOutCalls[0])
}

func TestFake_ContainerCopyOut_NotFound(t *testing.T) {
	f := New()
	_, err := f.ContainerCopyOut(context.Background(), "h1", "app", "/tmp/missing")
	require.ErrorIs(t, err, podman.ErrNotFound)
}

func TestFake_ContainerCopyOut_ErrHook(t *testing.T) {
	f := New()
	boom := errors.New("boom")
	f.CopyOutErr = boom
	require.NoError(t, f.ContainerCopyIn(context.Background(), "h1", "app", "/tmp/in", bytes.NewReader([]byte("x"))))
	_, err := f.ContainerCopyOut(context.Background(), "h1", "app", "/tmp/in")
	require.ErrorIs(t, err, boom)
}

func TestFake_ContainerCopyIn_ErrHook(t *testing.T) {
	f := New()
	boom := errors.New("boom")
	f.CopyInErr = boom
	err := f.ContainerCopyIn(context.Background(), "h1", "app", "/tmp/in", bytes.NewReader([]byte("x")))
	require.ErrorIs(t, err, boom)
}

func TestFake_ContainerCopyOut_ReaderHook(t *testing.T) {
	f := New()
	want := io.NopCloser(bytes.NewReader([]byte("hooked")))
	f.CopyOutReader = func(host, container, path string) io.ReadCloser { return want }
	rc, err := f.ContainerCopyOut(context.Background(), "any", "any", "any")
	require.NoError(t, err)
	got, _ := io.ReadAll(rc)
	assert.Equal(t, []byte("hooked"), got)
}

// TestFake_ContainerCopyIn_PropagatesReaderError mirrors
// TestFake_VolumeImport_PropagatesReaderError: a broken source stream must
// surface as an error and commit nothing.
func TestFake_ContainerCopyIn_PropagatesReaderError(t *testing.T) {
	f := New()
	boom := errors.New("stream broke")
	err := f.ContainerCopyIn(context.Background(), "h1", "app", "/tmp/in", &errReader{data: []byte("partial"), err: boom})
	require.ErrorIs(t, err, boom)
	_, getErr := f.ContainerCopyOut(context.Background(), "h1", "app", "/tmp/in")
	assert.ErrorIs(t, getErr, podman.ErrNotFound, "nothing should have been committed")
}

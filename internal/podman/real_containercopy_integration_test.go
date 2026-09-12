//go:build integration

package podman

import (
	"archive/tar"
	"bytes"
	"context"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/iotready/podman-api/internal/config"
)

const copyPodYAML = `
apiVersion: v1
kind: Pod
metadata:
  name: podman-api-copy-itest
  labels:
    podman-api/itest: "true"
spec:
  containers:
    - name: sh
      image: docker.io/library/alpine:latest
      command: ["sleep", "300"]
`

// TestReal_ContainerCopyOut_StreamsTarOfPath and
// TestReal_ContainerCopyIn_WritesTarIntoContainer are the real-podman
// counterparts to VolumeExport/VolumeImport's own LocalOnly tests: the
// archive endpoints are a plain HTTP GET/PUT, so — unlike ContainerExec —
// there's no attach/hijack behavior a fake could get wrong, but exercising
// them against a real container is still the only way to prove the
// container/path plumbing (as opposed to the fake's in-memory map) actually
// round-trips through podman's own tar encoder/decoder.
func TestReal_ContainerCopyOut_StreamsTarOfPath(t *testing.T) {
	sock := localSocket(t)
	c, err := NewReal([]config.Host{{ID: "local", Addr: "unix", Socket: sock}})
	require.NoError(t, err)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	const pod = "podman-api-copy-itest"
	t.Cleanup(func() { _ = c.PodRemove(context.Background(), "local", pod, true) })
	_ = c.PodRemove(ctx, "local", pod, true)
	require.NoError(t, c.PlayKube(ctx, "local", copyPodYAML, true))

	container := pod + "-sh"
	res, err := c.ContainerExec(ctx, "local", container, []string{"/bin/sh", "-c", "mkdir -p /tmp/out && echo hi > /tmp/out/f.txt"})
	require.NoError(t, err)
	require.Equal(t, 0, res.ExitCode)

	rc, err := c.ContainerCopyOut(ctx, "local", container, "/tmp/out")
	require.NoError(t, err)
	defer rc.Close()

	tr := tar.NewReader(rc)
	var found bool
	for {
		hdr, err := tr.Next()
		if err == io.EOF {
			break
		}
		require.NoError(t, err)
		if strings.HasSuffix(hdr.Name, "f.txt") {
			found = true
			data, err := io.ReadAll(tr)
			require.NoError(t, err)
			assert.Equal(t, "hi\n", string(data))
		}
	}
	assert.True(t, found, "expected f.txt in the copied-out tar")

	// A path that doesn't exist in the container is an error. It is NOT
	// mapped to ErrNotFound: podman's archive endpoint reports it as "no such
	// file or directory" (a missing path inside the container), which is a
	// different condition from isNotFound's "no such pod/container/..."
	// resource-missing set and deliberately left unmapped here rather than
	// widening that shared helper's meaning for every other caller.
	_, err = c.ContainerCopyOut(ctx, "local", container, "/tmp/does-not-exist")
	assert.Error(t, err)
}

func TestReal_ContainerCopyIn_WritesTarIntoContainer(t *testing.T) {
	sock := localSocket(t)
	c, err := NewReal([]config.Host{{ID: "local", Addr: "unix", Socket: sock}})
	require.NoError(t, err)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	const pod = "podman-api-copyin-itest"
	t.Cleanup(func() { _ = c.PodRemove(context.Background(), "local", pod, true) })
	_ = c.PodRemove(ctx, "local", pod, true)
	require.NoError(t, c.PlayKube(ctx, "local", strings.Replace(copyPodYAML, "podman-api-copy-itest", pod, 1), true))

	container := pod + "-sh"
	res, err := c.ContainerExec(ctx, "local", container, []string{"/bin/sh", "-c", "mkdir -p /tmp/in"})
	require.NoError(t, err)
	require.Equal(t, 0, res.ExitCode)

	require.NoError(t, c.ContainerCopyIn(ctx, "local", container, "/tmp/in", bytes.NewReader(makeTar(t, "restored.txt", []byte("restored-content")))))

	res, err = c.ContainerExec(ctx, "local", container, []string{"cat", "/tmp/in/restored.txt"})
	require.NoError(t, err)
	assert.Equal(t, 0, res.ExitCode)
	assert.Equal(t, "restored-content", res.Output)
}

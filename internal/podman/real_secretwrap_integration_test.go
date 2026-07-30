//go:build integration

package podman

import (
	"archive/tar"
	"bytes"
	"context"
	"encoding/base64"
	"fmt"
	"io"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/iotready/podman-api/internal/config"
)

// TestReal_SecretVolumeMount_LeavesPlaintextWrapperVolume_AndPruneRemovesIt is
// the load-bearing proof behind #214: a `podman kube play` pod that mounts a
// wrapped K8s Secret as a `secret:` volume creates a NAMED VOLUME — same name
// as the secret — holding the DECODED PLAINTEXT credential on disk, and
// removing the pod does not remove that volume. A fake client cannot
// reproduce this (it's podman's own play-kube volume materialization), so
// this has to run against a real daemon.
//
// It also proves the fix: internal/instance.pruneInstanceResources removes
// this exact volume (Client.VolumeRemove(ctx, host, secretName, true)) — the
// same call this test issues directly, since the podman package cannot
// import internal/instance (import cycle: instance imports podman).
//
// Every name is prefixed podman-api-itest-secretwrap- and cleaned up
// unconditionally, including on failure; it never touches any pre-existing
// pod/secret/volume.
func TestReal_SecretVolumeMount_LeavesPlaintextWrapperVolume_AndPruneRemovesIt(t *testing.T) {
	sock := localSocket(t)
	c, err := NewReal([]config.Host{{ID: "local", Addr: "unix", Socket: sock}})
	require.NoError(t, err)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	const (
		podName    = "podman-api-itest-secretwrap-pod"
		secretName = "podman-api-itest-secretwrap-cred"
		dataKey    = "password"
		plaintext  = "podman-api-itest-secretwrap-plaintext-credential"
	)

	cleanup := func() {
		_ = c.PodRemove(context.Background(), "local", podName, true)
		_ = c.VolumeRemove(context.Background(), "local", secretName, true)
		_ = c.SecretRemove(context.Background(), "local", secretName)
	}
	// Pre-remove self-heals a prior crashed run; deferred cleanup runs even on
	// failure (t.Cleanup runs regardless of t.Fatal/require).
	cleanup()
	t.Cleanup(cleanup)

	// 1. Create a podman secret whose content IS a wrapped K8s Secret — the
	// same shape internal/instance.wrapAsKubeSecret produces, and the only
	// shape `podman kube play` resolves a secret: volume mount against.
	wrapped := fmt.Sprintf(`apiVersion: v1
kind: Secret
type: Opaque
metadata:
  name: %s
data:
  %s: %s
`, secretName, dataKey, base64.StdEncoding.EncodeToString([]byte(plaintext)))
	require.NoError(t, c.SecretCreate(ctx, "local", secretName, []byte(wrapped)))

	// 2. kube play a pod mounting it as a secret: volume (not secretKeyRef —
	// only the volume-mount resolution path creates the wrapper volume).
	podYAML := fmt.Sprintf(`
apiVersion: v1
kind: Pod
metadata:
  name: %s
  labels:
    podman-api/itest: "true"
spec:
  containers:
    - name: c
      image: docker.io/library/alpine:latest
      command: ["sleep", "60"]
      volumeMounts:
        - name: cred
          mountPath: /secrets
  volumes:
    - name: cred
      secret:
        secretName: %s
`, podName, secretName)
	require.NoError(t, c.PlayKube(ctx, "local", podYAML, true))

	_, err = c.PodInspect(ctx, "local", podName)
	require.NoError(t, err)

	// 3. Remove the pod — this is the step the bug report says does NOT clean
	// up the wrapper volume.
	require.NoError(t, c.PodRemove(ctx, "local", podName, true))

	// 4. Assert the wrapper volume exists, named exactly like the secret, and
	// holds the DECODED PLAINTEXT (not the wrapped K8s Secret YAML, not
	// base64).
	_, err = c.VolumeInspect(ctx, "local", secretName)
	require.NoError(t, err, "podman kube play must have created a volume named after the secret")

	rc, err := c.VolumeExport(ctx, "local", secretName)
	require.NoError(t, err)
	tr := tar.NewReader(rc)
	var found bool
	var content []byte
	for {
		hdr, terr := tr.Next()
		if terr == io.EOF {
			break
		}
		require.NoError(t, terr)
		if hdr.Typeflag != tar.TypeReg {
			continue
		}
		buf, rerr := io.ReadAll(tr)
		require.NoError(t, rerr)
		if bytes.Equal(bytes.TrimSpace(buf), []byte(plaintext)) {
			found = true
			content = buf
			break
		}
	}
	_ = rc.Close()
	require.True(t, found, "wrapper volume must contain the decoded plaintext credential somewhere under its root")
	assert.Equal(t, plaintext, string(bytes.TrimSpace(content)))

	// 5. Assert the prune path (the same Client.VolumeRemove call
	// internal/instance.pruneInstanceResources now issues alongside
	// SecretRemove) actually removes it.
	require.NoError(t, c.VolumeRemove(ctx, "local", secretName, true))
	_, err = c.VolumeInspect(ctx, "local", secretName)
	assert.ErrorIs(t, err, ErrNotFound, "prune must remove the plaintext wrapper volume")
}

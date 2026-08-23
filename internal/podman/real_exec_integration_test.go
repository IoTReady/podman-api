//go:build integration

package podman

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/containers/podman/v5/pkg/bindings"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/iotready/podman-api/internal/config"
)

const execPodYAML = `
apiVersion: v1
kind: Pod
metadata:
  name: podman-api-exec-itest
  labels:
    podman-api/itest: "true"
spec:
  containers:
    - name: sh
      image: docker.io/library/alpine:latest
      command: ["sleep", "300"]
`

// TestReal_ContainerExec_LocalOnly pins #273 against a real podman.
//
// ContainerExec is the one method no unit test can cover honestly: it attaches,
// so it goes through podman's ExecStartAndAttach → newUpgradeRequest, which
// type-asserts conn.Client.Transport.(*http.Transport) with no comma-ok and
// bypasses the *http.Client entirely to dial with that transport's DialContext.
// The fake models neither. Wrapping the transport in a custom RoundTripper for
// connection invalidation (#252) therefore panicked the daemon on every exec
// against every real host — the pre_backup hook, registry blob GC — and every
// unit test still passed. Only a real exec catches that class of break.
func TestReal_ContainerExec_LocalOnly(t *testing.T) {
	sock := localSocket(t)
	c, err := NewReal([]config.Host{{ID: "local", Addr: "unix", Socket: sock}})
	require.NoError(t, err)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	const pod = "podman-api-exec-itest"
	t.Cleanup(func() { _ = c.PodRemove(context.Background(), "local", pod, true) })
	_ = c.PodRemove(ctx, "local", pod, true)
	require.NoError(t, c.PlayKube(ctx, "local", execPodYAML, true))

	// The container name libpod gives a kube-played container, and the exact
	// shape the pre_backup hook (instance/backup.go) execs into.
	container := pod + "-sh"

	res, err := c.ContainerExec(ctx, "local", container, []string{"/bin/sh", "-c", "echo hello-273"})
	require.NoError(t, err)
	assert.Equal(t, 0, res.ExitCode)
	assert.Contains(t, res.Output, "hello-273", "attached output must come back to the caller")

	// A non-zero exit is reported, not swallowed: pre_backup and blob GC both
	// branch on ExitCode, so "the command failed" must be distinguishable from
	// "the command succeeded silently".
	res, err = c.ContainerExec(ctx, "local", container, []string{"/bin/sh", "-c", "echo to-stderr >&2; exit 7"})
	require.NoError(t, err)
	assert.Equal(t, 7, res.ExitCode)
	assert.True(t, strings.Contains(res.Output, "to-stderr"), "stderr must be captured too, got %q", res.Output)
}

// TestReal_ContainerExec_KeepsInvalidationHook pins what podman does to the
// connection on the way out: newUpgradeRequest builds its own *http.Transport
// and assigns it to conn.Client.Transport permanently (podman v5.8.2
// attach.go:611), never restoring the original. Left alone, every exec strands
// the transport wireInvalidation installed — with its idle conns and their
// goroutines, and IdleConnTimeout: 0 on the replacement so nothing reaps them —
// and clones one layer deeper the next time round. This daemon execs on every
// pre_backup hook and every registry blob GC, so it accumulates for the
// process lifetime.
func TestReal_ContainerExec_KeepsInvalidationHook(t *testing.T) {
	sock := localSocket(t)
	c, err := NewReal([]config.Host{{ID: "local", Addr: "unix", Socket: sock}})
	require.NoError(t, err)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	const pod = "podman-api-exec-hook-itest"
	t.Cleanup(func() { _ = c.PodRemove(context.Background(), "local", pod, true) })
	_ = c.PodRemove(ctx, "local", pod, true)
	require.NoError(t, c.PlayKube(ctx, "local", strings.Replace(execPodYAML, "podman-api-exec-itest", pod, 1), true))

	cctx, err := c.ctxFor(context.Background(), "local")
	require.NoError(t, err)
	primary, err := bindings.GetClient(cctx)
	require.NoError(t, err)
	primaryTransport := primary.Client.Transport

	// First exec dials the exec-only connection; take its client afterwards.
	require.NoError(t, execOK(c, ctx, pod))
	c.mu.Lock()
	ectx := c.execCtx["local"]
	c.mu.Unlock()
	require.NotNil(t, ectx, "exec must run on its own connection")
	execConn, err := bindings.GetClient(ectx.ctx)
	require.NoError(t, err)
	execTransport := execConn.Client.Transport

	for i := range 2 {
		require.NoError(t, execOK(c, ctx, pod), "exec %d", i)
		assert.Same(t, execTransport, execConn.Client.Transport,
			"exec %d left podman's transport in place, dropping the invalidation hook", i)
		assert.Same(t, primaryTransport, primary.Client.Transport,
			"exec %d touched the connection every other operation uses", i)
	}
}

func execOK(c *Real, ctx context.Context, pod string) error {
	_, err := c.ContainerExec(ctx, "local", pod+"-sh", []string{"/bin/true"})
	return err
}

// TestReal_ContainerExec_ConcurrentOnOneHost covers the race podman flags in
// its own source ("FIXME: This is one giant race condition", attach.go:566):
// newUpgradeRequest stashes the dialed socket in a closure variable on the
// shared connection, so two execs in flight at once on the same host can
// cross-wire — one exec's closeWrite half-closing the other's socket, or one's
// output landing on the other's stream. Two instances' pre_backup hooks, or a
// hook plus a blob GC, are exactly that.
func TestReal_ContainerExec_ConcurrentOnOneHost(t *testing.T) {
	sock := localSocket(t)
	c, err := NewReal([]config.Host{{ID: "local", Addr: "unix", Socket: sock}})
	require.NoError(t, err)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	const pod = "podman-api-exec-conc-itest"
	t.Cleanup(func() { _ = c.PodRemove(context.Background(), "local", pod, true) })
	_ = c.PodRemove(ctx, "local", pod, true)
	require.NoError(t, c.PlayKube(ctx, "local", strings.Replace(execPodYAML, "podman-api-exec-itest", pod, 1), true))

	const n = 6
	var wg sync.WaitGroup
	out := make([]string, n)
	errs := make([]error, n)
	for i := range n {
		wg.Add(1)
		go func() {
			defer wg.Done()
			token := fmt.Sprintf("tok-%d", i)
			res, err := c.ContainerExec(ctx, "local", pod+"-sh", []string{"/bin/echo", token})
			errs[i] = err
			out[i] = res.Output
		}()
	}
	wg.Wait()

	for i := range n {
		require.NoError(t, errs[i], "exec %d failed", i)
		assert.Contains(t, out[i], fmt.Sprintf("tok-%d", i),
			"exec %d got another exec's output — the streams crossed", i)
	}
}

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

// TestReal_ContainerExec_LeavesNothingOnTheHost pins what podman does to the
// connection on the way out and what must be true of it afterwards.
// newUpgradeRequest builds its own *http.Transport and assigns it to
// conn.Client.Transport permanently (podman v5.8.2 attach.go:611), never
// restoring the original. Since #278 an exec's connection is separate from
// the one every other operation uses, so it cannot touch that connection's
// transport; since #307 (execPool) that connection may be REUSED by a later
// exec on the same host rather than dialed fresh every time, so what must
// hold on release is not "each exec gets its own" but "whichever connection
// an exec used has wireInvalidation's transport back in place before the
// next exec (or anything else) can see it" — otherwise a reused connection
// would hand the next exec podman's throwaway transport, and closing it as
// the exec after that releases it would strand ours, keep-alive sockets and
// their goroutines included.
func TestReal_ContainerExec_LeavesNothingOnTheHost(t *testing.T) {
	sock := localSocket(t)
	c, err := NewReal([]config.Host{{ID: "local", Addr: "unix", Socket: sock}})
	require.NoError(t, err)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	var mu sync.Mutex
	var execConns []*connEntry
	c.hooks = &testHooks{execDialed: func(e *connEntry) {
		mu.Lock()
		defer mu.Unlock()
		execConns = append(execConns, e)
	}}

	const pod = "podman-api-exec-hook-itest"
	t.Cleanup(func() { _ = c.PodRemove(context.Background(), "local", pod, true) })
	_ = c.PodRemove(ctx, "local", pod, true)
	require.NoError(t, c.PlayKube(ctx, "local", strings.Replace(execPodYAML, "podman-api-exec-itest", pod, 1), true))

	cctx, err := c.ctxFor(context.Background(), "local")
	require.NoError(t, err)
	primary, err := bindings.GetClient(cctx)
	require.NoError(t, err)
	primaryTransport := primary.Client.Transport

	for i := range 3 {
		require.NoError(t, execOK(c, ctx, pod), "exec %d", i)
		assert.Same(t, primaryTransport, primary.Client.Transport,
			"exec %d touched the connection every other operation uses", i)
	}

	mu.Lock()
	defer mu.Unlock()
	// Sequential, seconds apart, on an otherwise idle host: execPool's slot
	// is free every time, so this pins reuse actually happening rather than
	// merely being harmless if it did — three execs, one dial.
	require.Len(t, execConns, 1,
		"three sequential execs on an idle host must reuse one connection, not dial three")
	e := execConns[0]
	conn, err := bindings.GetClient(e.ctx)
	require.NoError(t, err)
	assert.Same(t, e.hooked, conn.Client.Transport,
		"the reused connection must have wireInvalidation's transport back in place after the last exec")
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

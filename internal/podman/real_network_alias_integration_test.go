//go:build integration

package podman

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/iotready/podman-api/internal/config"
)

const aliasPeerYAML = `
apiVersion: v1
kind: Pod
metadata:
  name: podman-api-alias-peer
  labels:
    podman-api/itest: "true"
spec:
  containers:
    - name: peer
      image: docker.io/library/alpine:latest
      command: ["sleep", "120"]
`

// aliasProbeYAML is a one-shot pod that resolves a name and exits with the
// result. Resolution is asserted through the pod's EXIT CODE rather than an
// exec, because ContainerExec panics against a real host (#273) — and a probe
// pod is the more honest test anyway: it exercises the same DNS path a real
// peer container would use, with no attach/upgrade machinery in between.
func aliasProbeYAML(name, host string) string {
	return fmt.Sprintf(`
apiVersion: v1
kind: Pod
metadata:
  name: %s
  labels:
    podman-api/itest: "true"
spec:
  restartPolicy: Never
  containers:
    - name: probe
      image: docker.io/library/alpine:latest
      command: ["getent", "hosts", %q]
`, name, host)
}

// The whole no-new-client-call design of #269 rests on one claim about podman
// that no unit test can check: that kube play parses "name:alias=x" server-side
// and registers x as a DNS name for the pod. The fake was taught to split on
// ":", so the unit tests would pass whatever podman actually does.
//
// This pins it against a real host. It also pins the property that makes an
// incremental migration safe — the pod's own "<name>" DNS entry keeps resolving
// alongside the alias — so consumers can be moved over one at a time.
func TestReal_PlayKubeNetworkAlias_LocalOnly(t *testing.T) {
	sock := localSocket(t)
	c, err := NewReal([]config.Host{{ID: "local", Addr: "unix", Socket: sock}})
	require.NoError(t, err)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	const net = "podman-api-alias-itest"
	pods := []string{"podman-api-alias-peer", "podman-api-alias-probe", "podman-api-alias-probe2"}
	t.Cleanup(func() {
		for _, p := range pods {
			_ = c.PodRemove(context.Background(), "local", p, true)
		}
	})
	for _, p := range pods {
		_ = c.PodRemove(ctx, "local", p, true)
	}

	require.NoError(t, c.NetworkEnsure(ctx, "local", net))
	require.NoError(t, c.PlayKube(ctx, "local", aliasPeerYAML, true, net+":alias=itest-db"))

	// The declared alias resolves from another pod on the same network.
	require.NoError(t, c.PlayKube(ctx, "local",
		aliasProbeYAML("podman-api-alias-probe", "itest-db"), true, net))
	code, err := c.WaitForPodCompletion(ctx, "local", "podman-api-alias-probe", 90*time.Second)
	require.NoError(t, err)
	assert.Equal(t, 0, code, "alias itest-db did not resolve on network %s", net)

	// ...and so does the pod's own DNS name, which is what makes "add the alias
	// now, move consumers over later" a safe migration.
	require.NoError(t, c.PlayKube(ctx, "local",
		aliasProbeYAML("podman-api-alias-probe2", "podman-api-alias-peer"), true, net))
	code, err = c.WaitForPodCompletion(ctx, "local", "podman-api-alias-probe2", 90*time.Second)
	require.NoError(t, err)
	assert.Equal(t, 0, code, "pod DNS name stopped resolving once an alias was declared")

	// A malformed option in the same position must fail loudly rather than be
	// ignored, or a typo'd alias would look like it worked.
	err = c.PlayKube(ctx, "local", aliasPeerYAML, true, net+":alias=")
	require.Error(t, err)
	assert.True(t, strings.Contains(strings.ToLower(err.Error()), "alias"),
		"expected the error to name the bad option, got: %v", err)
}

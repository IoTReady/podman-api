//go:build integration

package podman

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/iotready/podman-api/internal/config"
)

// statsTestYAML mirrors podsTestYAML (real_pods_integration_test.go): a single
// alpine container that sleeps long enough to be observed by both stats and
// volume-usage calls. PlayKube starts the pod immediately, so by the time
// ContainerStats is called the container has been running long enough to have
// accrued some resident memory — a freshly created-but-not-started container
// would report zero and make the MemUsageBytes assertion meaningless.
const statsTestYAML = `
apiVersion: v1
kind: Pod
metadata:
  name: podman-api-itest-stats
  labels:
    podman-api/itest: "true"
spec:
  containers:
    - name: c
      image: docker.io/library/alpine:latest
      command: ["sleep", "60"]
`

// TestContainerStatsIntegration exercises Real.ContainerStats against a real
// podman socket. It asserts MemUsageBytes != 0 for the running test
// container: even a freshly started alpine process has some resident memory,
// so this is a meaningful, non-flaky signal that podman actually returned a
// populated stats sample rather than a zero-valued placeholder. CPUNano is
// deliberately not asserted — a container that has just started can
// legitimately report zero accumulated CPU time.
func TestContainerStatsIntegration(t *testing.T) {
	sock := localSocket(t)
	c, err := NewReal([]config.Host{{ID: "local", Addr: "unix", Socket: sock}})
	require.NoError(t, err)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	t.Cleanup(func() {
		_ = c.PodRemove(context.Background(), "local", "podman-api-itest-stats", true)
	})
	_ = c.PodRemove(ctx, "local", "podman-api-itest-stats", true) // clear any leftover from a prior crashed run

	require.NoError(t, c.PlayKube(ctx, "local", statsTestYAML, true))

	// Give the container a brief moment to actually run before sampling, so
	// its memory accounting has settled past the "just created" instant.
	time.Sleep(500 * time.Millisecond)

	const containerName = "podman-api-itest-stats-c"
	stats, err := c.ContainerStats(ctx, "local")
	if err != nil {
		t.Fatalf("ContainerStats: %v", err)
	}
	var found bool
	for _, s := range stats {
		if s.Name == containerName {
			found = true
			if s.MemUsageBytes == 0 {
				t.Errorf("MemUsageBytes = 0 for a running container")
			}
		}
	}
	if !found {
		t.Fatalf("container %q absent from stats: %+v", containerName, stats)
	}
}

// TestVolumeUsageIntegration exercises Real.VolumeUsage against a real podman
// socket. It only asserts the test volume's presence in the returned map, not
// any particular size — an empty volume's on-disk size is not a stable number
// across podman versions or filesystems.
func TestVolumeUsageIntegration(t *testing.T) {
	sock := localSocket(t)
	c, err := NewReal([]config.Host{{ID: "local", Addr: "unix", Socket: sock}})
	require.NoError(t, err)
	ctx := context.Background()

	const volName = "podman-api-itest-stats-vol"

	t.Cleanup(func() {
		_ = c.VolumeRemove(context.Background(), "local", volName, true)
	})
	_ = c.VolumeRemove(ctx, "local", volName, true) // clear any leftover from a prior crashed run
	require.NoError(t, c.VolumeCreate(ctx, "local", volName))

	sizes, err := c.VolumeUsage(ctx, "local")
	if err != nil {
		t.Fatalf("VolumeUsage: %v", err)
	}
	if _, ok := sizes[volName]; !ok {
		t.Fatalf("volume %q absent from df: %+v", volName, sizes)
	}
}

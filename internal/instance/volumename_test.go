package instance

import (
	"context"
	"testing"

	"github.com/iotready/podman-api/internal/podman"
	"github.com/stretchr/testify/require"
)

// volumeName is the single definition of a managed volume's name (#211). Lock
// its shape and its relationship to podName + volumeNamePrefix: rename slices
// the prefix off an existing name to recover the short name, so the two must
// stay exact inverses or a rename silently mangles every volume it copies.
func TestVolumeName_ShapeAndPrefixRoundTrip(t *testing.T) {
	require.Equal(t, "postgres-v-data", volumeName("postgres", "v", "data"))
	require.Equal(t, podName("postgres", "v")+"-", volumeNamePrefix("postgres", "v"))

	for _, short := range []string{"data", "archives", "a-b-c", "x"} {
		full := volumeName("postgres", "v", short)
		require.Equal(t, short, full[len(volumeNamePrefix("postgres", "v")):],
			"prefix must be the exact inverse of volumeName for %q", short)
	}
}

// The drift guard the helper exists for. Before #209 a mismatch between two
// construction sites failed loudly (a volume that doesn't exist, an operation
// that errors); now the inventory sweep's name is also the Prometheus label
// podman_api_volume_size_bytes joins on, and a mismatch there fails silently —
// the join misses and the panel is empty. Assert every path that produces a
// volume name produces the same one.
func TestVolumeName_AllProducingPathsAgree(t *testing.T) {
	svc, f := newSvc(t)
	svc.SetInstanceCacheTTL(0) // force a live sweep
	ctx := context.Background()

	const tmpl, slug, short = "postgres", "v", "data"
	want := volumeName(tmpl, slug, short)

	f.AddPod("h1", podman.Pod{
		ID:   podName(tmpl, slug),
		Name: podName(tmpl, slug),
		Labels: map[string]string{
			"podman-api/template": tmpl,
			"podman-api/slug":     slug,
		},
		Status: "Running",
	})
	f.AddVolume("h1", podman.Volume{Name: want})

	// 1. InstanceVolumes (inspecting path, used by migrate/evacuate/rename).
	vols, err := svc.InstanceVolumes(ctx, "h1", tmpl, slug)
	require.NoError(t, err)
	require.Len(t, vols, 1)
	require.Equal(t, want, vols[0].Name)

	// 2. Get (single-instance, inspecting).
	got, err := svc.Get(ctx, "h1", tmpl, slug)
	require.NoError(t, err)
	require.Len(t, got.Volumes, 1)
	require.Equal(t, want, got.Volumes[0].Name)

	// 3. ListAllInstances (the sweep — declared names, no podman call; this is
	//    the name the volume-usage collector joins on). pgTemplate declares two
	//    volumes ("data", "logs" — see internal/instance/service_test.go), so
	//    the declared sweep lists both even though only "data" was added to the
	//    fake host above.
	all, err := svc.ListAllInstances(ctx, "h1")
	require.NoError(t, err)
	var swept []string
	for _, o := range all {
		if o.Template == tmpl && o.Slug == slug {
			for _, v := range o.Volumes {
				swept = append(swept, v.Name)
			}
		}
	}
	require.ElementsMatch(t, []string{want, volumeName(tmpl, slug, "logs")}, swept)
}

package obs

import (
	"testing"
	"time"

	"github.com/iotready/podman-api/internal/instance"
	"github.com/prometheus/client_golang/prometheus"
)

type fakeVolUsage struct {
	snap map[string]instance.HostVolumeUsage
}

func (f *fakeVolUsage) VolumeUsageSnapshot() map[string]instance.HostVolumeUsage { return f.snap }

func volInv(host, tmpl, slug string, vols ...string) *fakeInventory {
	o := instance.Observed{Template: tmpl, Slug: slug}
	for _, v := range vols {
		o.Volumes = append(o.Volumes, instance.ObservedVolume{Name: v})
	}
	return &fakeInventory{snap: map[string]instance.HostInventory{
		host: {Observed: []instance.Observed{o}, HasData: true, Reachable: true},
	}}
}

func newVolReg(t *testing.T, src VolumeUsageSource, inv InventorySource) *prometheus.Registry {
	t.Helper()
	reg := prometheus.NewRegistry()
	c := NewVolumeUsageCollector(reg, src, inv)
	c.now = func() time.Time { return testNow }
	return reg
}

func TestVolumeUsageCollectorRendersSizeAndAge(t *testing.T) {
	src := &fakeVolUsage{snap: map[string]instance.HostVolumeUsage{
		"h1": {
			Sizes:     map[string]int64{"engine-valvo-data": 4096},
			FetchedAt: testNow.Add(-90 * time.Second),
		},
	}}
	got := gathered(t, newVolReg(t, src, volInv("h1", "engine", "valvo", "engine-valvo-data")))
	want(t, got, "podman_api_volume_size_bytes{host=h1,slug=valvo,template=engine,volume=engine-valvo-data}", 4096)
	want(t, got, "podman_api_volume_usage_age_seconds{host=h1}", 90)
}

// A volume podman-api does not manage must not be emitted — there is no
// instance to attribute it to.
func TestVolumeUsageCollectorDropsUnattributable(t *testing.T) {
	src := &fakeVolUsage{snap: map[string]instance.HostVolumeUsage{
		"h1": {Sizes: map[string]int64{"someones-other-volume": 1}, FetchedAt: testNow},
	}}
	got := gathered(t, newVolReg(t, src, volInv("h1", "engine", "valvo", "engine-valvo-data")))
	absent(t, got, "podman_api_volume_size_bytes")
}

// Age is the whole point of the metric: a wedged walk must be visible, so the
// age series is emitted for a host that has been sampled even if no volume of
// its own matched.
func TestVolumeUsageCollectorAgeWithoutSizes(t *testing.T) {
	src := &fakeVolUsage{snap: map[string]instance.HostVolumeUsage{
		"h1": {Sizes: map[string]int64{}, FetchedAt: testNow.Add(-time.Hour)},
	}}
	got := gathered(t, newVolReg(t, src, volInv("h1", "engine", "valvo", "engine-valvo-data")))
	want(t, got, "podman_api_volume_usage_age_seconds{host=h1}", 3600)
}

// A host that has never been sampled has no age to report — emitting 0 would
// read as a walk that just completed.
func TestVolumeUsageCollectorAbsentWhenNeverSampled(t *testing.T) {
	src := &fakeVolUsage{snap: map[string]instance.HostVolumeUsage{}}
	got := gathered(t, newVolReg(t, src, volInv("h1", "engine", "valvo", "engine-valvo-data")))
	absent(t, got, "podman_api_volume_usage_age_seconds")
	absent(t, got, "podman_api_volume_size_bytes")
}

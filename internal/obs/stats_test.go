package obs

import (
	"testing"

	"github.com/iotready/podman-api/internal/instance"
	"github.com/iotready/podman-api/internal/podman"
	"github.com/prometheus/client_golang/prometheus"
)

type fakeStats struct {
	snap map[string]instance.HostStats
}

func (f *fakeStats) StatsSnapshot() map[string]instance.HostStats { return f.snap }

// inv builds an inventory snapshot with one instance owning the named containers.
func statsInv(host, tmpl, slug string, containers ...string) *fakeInventory {
	o := instance.Observed{Template: tmpl, Slug: slug}
	for _, c := range containers {
		o.Containers = append(o.Containers, instance.ObservedContainer{Name: c})
	}
	return &fakeInventory{snap: map[string]instance.HostInventory{
		host: {Observed: []instance.Observed{o}, HasData: true, Reachable: true},
	}}
}

func newStatsReg(t *testing.T, st StatsSource, inv InventorySource) *prometheus.Registry {
	t.Helper()
	reg := prometheus.NewRegistry()
	NewStatsCollector(reg, st, inv)
	return reg
}

func TestStatsCollectorRendersSeries(t *testing.T) {
	st := &fakeStats{snap: map[string]instance.HostStats{
		"h1": {Containers: map[string]podman.ContainerStats{
			"engine-valvo-engine": {
				Name: "engine-valvo-engine", CPUNano: 2_500_000_000,
				MemUsageBytes: 100, MemLimitBytes: 400,
				NetRxBytes: 7, NetTxBytes: 9,
				BlockReadBytes: 11, BlockWriteBytes: 13, PIDs: 3,
			},
		}},
	}}
	got := gathered(t, newStatsReg(t, st, statsInv("h1", "engine", "valvo", "engine-valvo-engine")))
	const l = "{container=engine-valvo-engine,host=h1,slug=valvo,template=engine}"
	want(t, got, "podman_api_container_cpu_seconds_total"+l, 2.5)
	want(t, got, "podman_api_container_memory_bytes"+l, 100)
	want(t, got, "podman_api_container_memory_limit_bytes"+l, 400)
	want(t, got, "podman_api_container_network_receive_bytes_total"+l, 7)
	want(t, got, "podman_api_container_network_transmit_bytes_total"+l, 9)
	want(t, got, "podman_api_container_block_read_bytes_total"+l, 11)
	want(t, got, "podman_api_container_block_write_bytes_total"+l, 13)
	want(t, got, "podman_api_container_processes"+l, 3)
}

// A container podman-api does not manage (no inventory entry) must not produce
// series — there is no template/slug to label it with.
func TestStatsCollectorDropsUnattributable(t *testing.T) {
	st := &fakeStats{snap: map[string]instance.HostStats{
		"h1": {Containers: map[string]podman.ContainerStats{
			"some-random-container": {Name: "some-random-container", CPUNano: 1},
		}},
	}}
	got := gathered(t, newStatsReg(t, st, statsInv("h1", "engine", "valvo", "engine-valvo-engine")))
	absent(t, got, "podman_api_container_cpu_seconds_total")
}

// A managed container with no sample yet yields no series at all — never a zero,
// which would read as a genuinely idle container.
func TestStatsCollectorAbsentWhenNoSample(t *testing.T) {
	st := &fakeStats{snap: map[string]instance.HostStats{}}
	got := gathered(t, newStatsReg(t, st, statsInv("h1", "engine", "valvo", "engine-valvo-engine")))
	absent(t, got, "podman_api_container_cpu_seconds_total")
}

// MemLimit 0 means "no limit declared"; emitting 0 would make every saturation
// panel read as fully saturated.
func TestStatsCollectorOmitsZeroMemLimit(t *testing.T) {
	st := &fakeStats{snap: map[string]instance.HostStats{
		"h1": {Containers: map[string]podman.ContainerStats{
			"engine-valvo-engine": {Name: "engine-valvo-engine", MemUsageBytes: 100, MemLimitBytes: 0},
		}},
	}}
	got := gathered(t, newStatsReg(t, st, statsInv("h1", "engine", "valvo", "engine-valvo-engine")))
	want(t, got, "podman_api_container_memory_bytes{container=engine-valvo-engine,host=h1,slug=valvo,template=engine}", 100)
	absent(t, got, "podman_api_container_memory_limit_bytes")
}

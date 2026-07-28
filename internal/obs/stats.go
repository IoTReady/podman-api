package obs

import (
	"github.com/iotready/podman-api/internal/instance"
	"github.com/prometheus/client_golang/prometheus"
)

// StatsSource supplies the latest per-host container resource samples.
// Implemented by *instance.Service.
type StatsSource interface {
	StatsSnapshot() map[string]instance.HostStats
}

const resetNote = " Resets to 0 when the pod is recreated, so a redeploy appears as a counter reset."

var (
	descCPUSeconds = prometheus.NewDesc(
		"podman_api_container_cpu_seconds_total",
		"Cumulative CPU time consumed by the container, in seconds."+resetNote,
		containerLabels, nil)
	descMemBytes = prometheus.NewDesc(
		"podman_api_container_memory_bytes",
		"Container memory usage in bytes.",
		containerLabels, nil)
	descMemLimitBytes = prometheus.NewDesc(
		"podman_api_container_memory_limit_bytes",
		"Container memory limit in bytes. Absent when the container declares no limit.",
		containerLabels, nil)
	descNetRxBytes = prometheus.NewDesc(
		"podman_api_container_network_receive_bytes_total",
		"Bytes received by the container, summed across interfaces."+resetNote,
		containerLabels, nil)
	descNetTxBytes = prometheus.NewDesc(
		"podman_api_container_network_transmit_bytes_total",
		"Bytes transmitted by the container, summed across interfaces."+resetNote,
		containerLabels, nil)
	descBlockRead = prometheus.NewDesc(
		"podman_api_container_block_read_bytes_total",
		"Bytes read from block devices by the container."+resetNote,
		containerLabels, nil)
	descBlockWrite = prometheus.NewDesc(
		"podman_api_container_block_write_bytes_total",
		"Bytes written to block devices by the container."+resetNote,
		containerLabels, nil)
	descProcesses = prometheus.NewDesc(
		"podman_api_container_processes",
		"Number of processes running in the container.",
		containerLabels, nil)
)

// StatsCollector renders per-container resource samples at scrape time.
//
// Like InventoryCollector it does no podman I/O — it reads two cached
// snapshots. The inventory snapshot is what supplies template/slug: podman
// reports stats by container name only, so a sample whose container is not in
// the inventory is dropped rather than emitted under a guessed identity.
type StatsCollector struct {
	stats StatsSource
	inv   InventorySource
}

func NewStatsCollector(reg prometheus.Registerer, stats StatsSource, inv InventorySource) *StatsCollector {
	c := &StatsCollector{stats: stats, inv: inv}
	reg.MustRegister(c)
	return c
}

func (c *StatsCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- descCPUSeconds
	ch <- descMemBytes
	ch <- descMemLimitBytes
	ch <- descNetRxBytes
	ch <- descNetTxBytes
	ch <- descBlockRead
	ch <- descBlockWrite
	ch <- descProcesses
}

func (c *StatsCollector) Collect(ch chan<- prometheus.Metric) {
	stats := c.stats.StatsSnapshot()
	for host, inv := range c.inv.InventorySnapshot() {
		hs, ok := stats[host]
		if !ok {
			continue
		}
		for _, o := range inv.Observed {
			for _, ct := range o.Containers {
				s, ok := hs.Containers[ct.Name]
				if !ok {
					continue // managed but unsampled: absent beats a false zero
				}
				lv := []string{host, o.Template, o.Slug, ct.Name}
				g := func(d *prometheus.Desc, v float64) {
					ch <- prometheus.MustNewConstMetric(d, prometheus.GaugeValue, v, lv...)
				}
				cnt := func(d *prometheus.Desc, v float64) {
					ch <- prometheus.MustNewConstMetric(d, prometheus.CounterValue, v, lv...)
				}
				cnt(descCPUSeconds, float64(s.CPUNano)/1e9)
				g(descMemBytes, float64(s.MemUsageBytes))
				if s.MemLimitBytes > 0 {
					g(descMemLimitBytes, float64(s.MemLimitBytes))
				}
				cnt(descNetRxBytes, float64(s.NetRxBytes))
				cnt(descNetTxBytes, float64(s.NetTxBytes))
				cnt(descBlockRead, float64(s.BlockReadBytes))
				cnt(descBlockWrite, float64(s.BlockWriteBytes))
				g(descProcesses, float64(s.PIDs))
			}
		}
	}
}

package obs

import (
	"time"

	"github.com/iotready/podman-api/internal/instance"
	"github.com/prometheus/client_golang/prometheus"
)

// VolumeUsageSource supplies the latest per-host volume sizing.
// Implemented by *instance.Service.
type VolumeUsageSource interface {
	VolumeUsageSnapshot() map[string]instance.HostVolumeUsage
}

var (
	descVolumeSize = prometheus.NewDesc(
		"podman_api_volume_size_bytes",
		"On-disk size of a managed volume, from podman system df.",
		[]string{"host", "template", "slug", "volume"}, nil)
	descVolumeUsageAge = prometheus.NewDesc(
		"podman_api_volume_usage_age_seconds",
		"Seconds since the last successful volume-usage walk for this host. Absent until the host has been walked successfully at least once; a climbing value means the walk is wedged or slower than its cadence.",
		[]string{"host"}, nil)
)

// VolumeUsageCollector renders volume sizes at scrape time, attributing each
// volume to its instance via the inventory snapshot. Does no podman I/O.
type VolumeUsageCollector struct {
	src VolumeUsageSource
	inv InventorySource
	now func() time.Time
}

func NewVolumeUsageCollector(reg prometheus.Registerer, src VolumeUsageSource, inv InventorySource) *VolumeUsageCollector {
	c := &VolumeUsageCollector{src: src, inv: inv, now: time.Now}
	reg.MustRegister(c)
	return c
}

func (c *VolumeUsageCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- descVolumeSize
	ch <- descVolumeUsageAge
}

func (c *VolumeUsageCollector) Collect(ch chan<- prometheus.Metric) {
	snap := c.src.VolumeUsageSnapshot()
	now := c.now()
	inv := c.inv.InventorySnapshot()
	for host, u := range snap {
		if !u.FetchedAt.IsZero() {
			ch <- prometheus.MustNewConstMetric(descVolumeUsageAge, prometheus.GaugeValue,
				now.Sub(u.FetchedAt).Seconds(), host)
		}
		for _, o := range inv[host].Observed {
			for _, v := range o.Volumes {
				size, ok := u.Sizes[v.Name]
				if !ok {
					continue
				}
				ch <- prometheus.MustNewConstMetric(descVolumeSize, prometheus.GaugeValue,
					float64(size), host, o.Template, o.Slug, v.Name)
			}
		}
	}
}

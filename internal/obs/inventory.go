package obs

import (
	"strings"
	"time"

	"github.com/iotready/podman-api/internal/instance"
	"github.com/prometheus/client_golang/prometheus"
)

// InventorySource supplies the current warm-cache inventory snapshot.
// Implemented by *instance.Service.
type InventorySource interface {
	InventorySnapshot() map[string]instance.HostInventory
}

var (
	containerLabels = []string{"host", "template", "slug", "container"}
	instanceLabels  = []string{"host", "template", "slug"}

	descContainerRestarts = prometheus.NewDesc(
		"podman_api_container_restarts_total",
		"Container restart count reported by podman. Resets to 0 when the pod is recreated, so a redeploy appears as a counter reset.",
		containerLabels, nil)
	descContainerRunning = prometheus.NewDesc(
		"podman_api_container_running",
		"1 if the container status is running, 0 otherwise.",
		containerLabels, nil)
	descContainerHealthy = prometheus.NewDesc(
		"podman_api_container_healthy",
		"1 if the container healthcheck reports healthy, 0 if unhealthy or starting, -1 if the container declares no healthcheck.",
		containerLabels, nil)
	descInstanceReady = prometheus.NewDesc(
		"podman_api_instance_ready",
		"1 if every container in the instance reports a healthy or absent healthcheck.",
		instanceLabels, nil)
	descHostReachable = prometheus.NewDesc(
		"podman_api_host_reachable",
		"1 if the most recent inventory refresh for this host succeeded.",
		[]string{"host"}, nil)
	descInventoryAge = prometheus.NewDesc(
		"podman_api_inventory_age_seconds",
		"Seconds since the last successful inventory refresh for this host. Absent until the host has been polled successfully at least once.",
		[]string{"host"}, nil)
)

// InventoryCollector renders per-container and per-host state from the warm
// inventory cache at scrape time.
//
// It is a collector rather than a set of pushed gauges for two reasons: it does
// no podman I/O, so a scrape can never block behind an unreachable host; and it
// renders present state, so a deleted or renamed instance's series disappear on
// the next poll instead of freezing at their last value forever.
type InventoryCollector struct {
	src   InventorySource
	hosts func() []string
	now   func() time.Time
}

// NewInventoryCollector builds the collector and registers it on reg.
//
// hosts must be the same host-list function the inventory poller uses. A host
// that has never been polled then still reports podman_api_host_reachable 0
// rather than vanishing from the metric set, which would read as "fine".
func NewInventoryCollector(reg prometheus.Registerer, src InventorySource, hosts func() []string) *InventoryCollector {
	c := &InventoryCollector{src: src, hosts: hosts, now: time.Now}
	reg.MustRegister(c)
	return c
}

func (c *InventoryCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- descContainerRestarts
	ch <- descContainerRunning
	ch <- descContainerHealthy
	ch <- descInstanceReady
	ch <- descHostReachable
	ch <- descInventoryAge
}

func (c *InventoryCollector) Collect(ch chan<- prometheus.Metric) {
	snap := c.src.InventorySnapshot()
	now := c.now()
	seen := make(map[string]bool, len(snap))
	for _, host := range c.hosts() {
		if seen[host] {
			continue // a duplicated host id would emit duplicate series and fail Gather
		}
		seen[host] = true

		inv, ok := snap[host]
		ch <- prometheus.MustNewConstMetric(descHostReachable, prometheus.GaugeValue, boolValue(ok && inv.Reachable), host)
		if !ok || !inv.HasData {
			continue
		}
		if !inv.FetchedAt.IsZero() {
			ch <- prometheus.MustNewConstMetric(descInventoryAge, prometheus.GaugeValue, now.Sub(inv.FetchedAt).Seconds(), host)
		}
		for _, o := range inv.Observed {
			ch <- prometheus.MustNewConstMetric(descInstanceReady, prometheus.GaugeValue, boolValue(o.Ready), host, o.Template, o.Slug)
			for _, ct := range o.Containers {
				ch <- prometheus.MustNewConstMetric(descContainerRestarts, prometheus.CounterValue, float64(ct.RestartCount), host, o.Template, o.Slug, ct.Name)
				ch <- prometheus.MustNewConstMetric(descContainerRunning, prometheus.GaugeValue, boolValue(strings.EqualFold(ct.Status, "running")), host, o.Template, o.Slug, ct.Name)
				ch <- prometheus.MustNewConstMetric(descContainerHealthy, prometheus.GaugeValue, healthValue(ct.Health), host, o.Template, o.Slug, ct.Name)
			}
		}
	}
}

func boolValue(b bool) float64 {
	if b {
		return 1
	}
	return 0
}

// healthValue maps a podman healthcheck status onto a gauge. The -1 case is
// deliberately distinct from 0: a container with no healthcheck declared is not
// the same as one that is failing its healthcheck.
func healthValue(h string) float64 {
	switch {
	case h == "":
		return -1
	case strings.EqualFold(h, "healthy"):
		return 1
	default:
		return 0
	}
}

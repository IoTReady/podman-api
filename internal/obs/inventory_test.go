package obs

import (
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/iotready/podman-api/internal/instance"
	"github.com/prometheus/client_golang/prometheus"
)

type fakeInventory struct {
	snap map[string]instance.HostInventory
}

func (f *fakeInventory) InventorySnapshot() map[string]instance.HostInventory { return f.snap }

var testNow = time.Date(2026, 7, 25, 12, 0, 0, 0, time.UTC)

// gathered flattens a registry into "name{k=v,...}" -> value so tests can assert
// on exact series without depending on collector emission order.
func gathered(t *testing.T, reg *prometheus.Registry) map[string]float64 {
	t.Helper()
	mfs, err := reg.Gather()
	if err != nil {
		t.Fatalf("gather: %v", err)
	}
	out := map[string]float64{}
	for _, mf := range mfs {
		for _, m := range mf.Metric {
			parts := make([]string, 0, len(m.Label))
			for _, lp := range m.Label {
				parts = append(parts, lp.GetName()+"="+lp.GetValue())
			}
			sort.Strings(parts)
			key := mf.GetName() + "{" + strings.Join(parts, ",") + "}"
			switch {
			case m.Gauge != nil:
				out[key] = m.Gauge.GetValue()
			case m.Counter != nil:
				out[key] = m.Counter.GetValue()
			}
		}
	}
	return out
}

func newTestCollector(t *testing.T, src InventorySource, hosts ...string) *prometheus.Registry {
	t.Helper()
	reg := prometheus.NewRegistry()
	c := NewInventoryCollector(reg, src, func() []string { return hosts })
	c.now = func() time.Time { return testNow }
	return reg
}

func want(t *testing.T, got map[string]float64, key string, val float64) {
	t.Helper()
	v, ok := got[key]
	if !ok {
		t.Fatalf("missing series %s (got %v)", key, got)
	}
	if v != val {
		t.Fatalf("%s = %v, want %v", key, v, val)
	}
}

func absent(t *testing.T, got map[string]float64, prefix string) {
	t.Helper()
	for k := range got {
		if strings.HasPrefix(k, prefix) {
			t.Fatalf("series %s present, want absent", k)
		}
	}
}

func TestInventoryCollector_HappyPath(t *testing.T) {
	src := &fakeInventory{snap: map[string]instance.HostInventory{
		"engine-1": {
			HasData:   true,
			Reachable: true,
			FetchedAt: testNow.Add(-15 * time.Second),
			Observed: []instance.Observed{{
				Template: "engine", Slug: "razorpay", Ready: true,
				Containers: []instance.ObservedContainer{
					{Name: "app", Status: "running", RestartCount: 2, Health: "healthy"},
					{Name: "litestream", Status: "exited", RestartCount: 7, Health: "unhealthy"},
				},
			}},
		},
	}}
	got := gathered(t, newTestCollector(t, src, "engine-1"))

	want(t, got, `podman_api_host_reachable{host=engine-1}`, 1)
	want(t, got, `podman_api_inventory_age_seconds{host=engine-1}`, 15)
	want(t, got, `podman_api_instance_ready{host=engine-1,slug=razorpay,template=engine}`, 1)
	want(t, got, `podman_api_container_restarts_total{container=app,host=engine-1,slug=razorpay,template=engine}`, 2)
	want(t, got, `podman_api_container_running{container=app,host=engine-1,slug=razorpay,template=engine}`, 1)
	want(t, got, `podman_api_container_healthy{container=app,host=engine-1,slug=razorpay,template=engine}`, 1)
	want(t, got, `podman_api_container_restarts_total{container=litestream,host=engine-1,slug=razorpay,template=engine}`, 7)
	want(t, got, `podman_api_container_running{container=litestream,host=engine-1,slug=razorpay,template=engine}`, 0)
	want(t, got, `podman_api_container_healthy{container=litestream,host=engine-1,slug=razorpay,template=engine}`, 0)
}

func TestInventoryCollector_NoHealthcheckIsMinusOne(t *testing.T) {
	src := &fakeInventory{snap: map[string]instance.HostInventory{
		"h1": {HasData: true, Reachable: true, FetchedAt: testNow, Observed: []instance.Observed{{
			Template: "t", Slug: "s", Ready: true,
			Containers: []instance.ObservedContainer{{Name: "c", Status: "running", Health: ""}},
		}}},
	}}
	got := gathered(t, newTestCollector(t, src, "h1"))
	want(t, got, `podman_api_container_healthy{container=c,host=h1,slug=s,template=t}`, -1)
}

func TestInventoryCollector_StartingHealthIsZero(t *testing.T) {
	src := &fakeInventory{snap: map[string]instance.HostInventory{
		"h1": {HasData: true, Reachable: true, FetchedAt: testNow, Observed: []instance.Observed{{
			Template: "t", Slug: "s",
			Containers: []instance.ObservedContainer{{Name: "c", Status: "running", Health: "starting"}},
		}}},
	}}
	got := gathered(t, newTestCollector(t, src, "h1"))
	want(t, got, `podman_api_container_healthy{container=c,host=h1,slug=s,template=t}`, 0)
	want(t, got, `podman_api_instance_ready{host=h1,slug=s,template=t}`, 0)
}

func TestInventoryCollector_UnreachableHostStillEmitsLastKnownGood(t *testing.T) {
	src := &fakeInventory{snap: map[string]instance.HostInventory{
		"h1": {
			HasData: true, Reachable: false,
			FetchedAt: testNow.Add(-10 * time.Minute),
			Observed: []instance.Observed{{
				Template: "t", Slug: "s", Ready: true,
				Containers: []instance.ObservedContainer{{Name: "c", Status: "running"}},
			}},
		},
	}}
	got := gathered(t, newTestCollector(t, src, "h1"))

	want(t, got, `podman_api_host_reachable{host=h1}`, 0)
	want(t, got, `podman_api_inventory_age_seconds{host=h1}`, 600)
	want(t, got, `podman_api_container_running{container=c,host=h1,slug=s,template=t}`, 1)
}

func TestInventoryCollector_ColdHostEmitsOnlyReachable(t *testing.T) {
	src := &fakeInventory{snap: map[string]instance.HostInventory{
		"h1": {HasData: false, Reachable: false},
	}}
	got := gathered(t, newTestCollector(t, src, "h1"))

	want(t, got, `podman_api_host_reachable{host=h1}`, 0)
	absent(t, got, "podman_api_inventory_age_seconds")
	absent(t, got, "podman_api_container_")
	absent(t, got, "podman_api_instance_ready")
}

func TestInventoryCollector_UnpolledHostReportsUnreachable(t *testing.T) {
	src := &fakeInventory{snap: map[string]instance.HostInventory{}}
	got := gathered(t, newTestCollector(t, src, "never-polled"))
	want(t, got, `podman_api_host_reachable{host=never-polled}`, 0)
	absent(t, got, "podman_api_container_")
}

func TestInventoryCollector_DeletedInstanceSeriesDisappear(t *testing.T) {
	src := &fakeInventory{snap: map[string]instance.HostInventory{
		"h1": {HasData: true, Reachable: true, FetchedAt: testNow, Observed: []instance.Observed{{
			Template: "t", Slug: "gone",
			Containers: []instance.ObservedContainer{{Name: "c", Status: "running"}},
		}}},
	}}
	reg := newTestCollector(t, src, "h1")
	if _, ok := gathered(t, reg)[`podman_api_container_running{container=c,host=h1,slug=gone,template=t}`]; !ok {
		t.Fatal("series missing before deletion")
	}

	src.snap = map[string]instance.HostInventory{
		"h1": {HasData: true, Reachable: true, FetchedAt: testNow},
	}
	absent(t, gathered(t, reg), "podman_api_container_")
}

func TestInventoryCollector_DuplicateHostsDoNotBreakGather(t *testing.T) {
	src := &fakeInventory{snap: map[string]instance.HostInventory{
		"h1": {HasData: true, Reachable: true, FetchedAt: testNow},
	}}
	got := gathered(t, newTestCollector(t, src, "h1", "h1"))
	want(t, got, `podman_api_host_reachable{host=h1}`, 1)
}

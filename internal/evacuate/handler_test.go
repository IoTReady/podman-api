package evacuate

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/iotready/podman-api/internal/config"
	"github.com/iotready/podman-api/internal/instance"
	"github.com/iotready/podman-api/internal/jobs"
	"github.com/iotready/podman-api/internal/podman/fake"
	"github.com/iotready/podman-api/internal/store"
)

// buildSvc returns a *instance.Service (hosts hostA/hostB/hostC, no templates
// needed — ResolveEvacuation doesn't consult them) backed by a Memory store
// seeded with the given slugs on hostA (template "web"), plus the store handle.
func buildSvc(t *testing.T, slugs ...string) (*instance.Service, *store.Memory) {
	t.Helper()
	hosts := []config.Host{{ID: "hostA"}, {ID: "hostB"}, {ID: "hostC"}}
	svc := instance.NewService(fake.New(), hosts, nil)
	mem := store.NewMemory()
	svc.SetStore(mem)
	for _, s := range slugs {
		if err := mem.PutSpec(context.Background(), store.Spec{
			Host: "hostA", Template: "web", Slug: s,
			Parameters: map[string]any{}, Secrets: map[string]string{},
		}); err != nil {
			t.Fatal(err)
		}
	}
	return svc, mem
}

func mustArgs(t *testing.T, from string, m map[string]string) json.RawMessage {
	t.Helper()
	b, err := json.Marshal(instance.EvacuateRequest{FromHost: from, Map: m})
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func TestEvacuateAllSucceed(t *testing.T) {
	ctx := context.Background()
	svc, mem := buildSvc(t, "acme", "globex")
	parent, _ := mem.Enqueue(ctx, "evacuate", mustArgs(t, "hostA",
		map[string]string{"acme": "hostB", "globex": "hostC"}), "")

	var calls int32
	h := &Handler{Svc: svc, Jobs: mem}
	h.migrate = func(_ context.Context, req instance.MigrateRequest, step func(string, string)) error {
		atomic.AddInt32(&calls, 1)
		step("fake", req.Slug)
		return nil
	}

	if err := h.Run(ctx, parent, jobs.NewJobContext(mem, parent.ID)); err != nil {
		t.Fatalf("want success, got %v", err)
	}
	if calls != 2 {
		t.Fatalf("want 2 migrate calls, got %d", calls)
	}
	children, _ := mem.ListJobs(ctx, store.JobFilter{ParentID: parent.ID})
	if len(children) != 2 {
		t.Fatalf("want 2 children, got %d", len(children))
	}
	for _, c := range children {
		if c.Kind != "migrate" || c.State != store.JobSucceeded {
			t.Fatalf("child not succeeded migrate: %+v", c)
		}
	}
}

func TestEvacuateOneFailsSiblingsRun(t *testing.T) {
	ctx := context.Background()
	svc, mem := buildSvc(t, "acme", "globex", "initech")
	parent, _ := mem.Enqueue(ctx, "evacuate", mustArgs(t, "hostA",
		map[string]string{"acme": "hostB", "globex": "hostC", "initech": "hostB"}), "")

	var ran sync.Map
	h := &Handler{Svc: svc, Jobs: mem}
	h.migrate = func(_ context.Context, req instance.MigrateRequest, step func(string, string)) error {
		ran.Store(req.Slug, true)
		if req.Slug == "globex" {
			return errors.New("boom")
		}
		return nil
	}

	err := h.Run(ctx, parent, jobs.NewJobContext(mem, parent.ID))
	if err == nil {
		t.Fatal("want failure")
	}
	if !strings.Contains(err.Error(), "globex") || !strings.Contains(err.Error(), "1/3") {
		t.Fatalf("aggregate error should name globex and 1/3: %v", err)
	}
	for _, s := range []string{"acme", "globex", "initech"} {
		if _, ok := ran.Load(s); !ok {
			t.Fatalf("%s did not run; a sibling failure aborted others", s)
		}
	}
	children, _ := mem.ListJobs(ctx, store.JobFilter{ParentID: parent.ID})
	var ok, bad int
	for _, c := range children {
		switch c.State {
		case store.JobSucceeded:
			ok++
		case store.JobFailed:
			bad++
		}
	}
	if ok != 2 || bad != 1 {
		t.Fatalf("want 2 ok / 1 failed children, got %d / %d", ok, bad)
	}
}

func TestEvacuateConcurrencyBound(t *testing.T) {
	old := evacuateConcurrency
	evacuateConcurrency = 1
	defer func() { evacuateConcurrency = old }()

	ctx := context.Background()
	svc, mem := buildSvc(t, "a", "b", "c", "d")
	parent, _ := mem.Enqueue(ctx, "evacuate", mustArgs(t, "hostA",
		map[string]string{"a": "hostB", "b": "hostB", "c": "hostB", "d": "hostB"}), "")

	var inFlight, maxInFlight int32
	h := &Handler{Svc: svc, Jobs: mem}
	h.migrate = func(_ context.Context, req instance.MigrateRequest, step func(string, string)) error {
		n := atomic.AddInt32(&inFlight, 1)
		for {
			m := atomic.LoadInt32(&maxInFlight)
			if n <= m || atomic.CompareAndSwapInt32(&maxInFlight, m, n) {
				break
			}
		}
		for i := 0; i < 100000; i++ { // brief spin so overlaps would be observed if the bound broke
		}
		atomic.AddInt32(&inFlight, -1)
		return nil
	}
	if err := h.Run(ctx, parent, jobs.NewJobContext(mem, parent.ID)); err != nil {
		t.Fatal(err)
	}
	if maxInFlight != 1 {
		t.Fatalf("evacuateConcurrency=1 but observed %d in flight", maxInFlight)
	}
}

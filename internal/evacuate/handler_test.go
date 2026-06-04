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
	return mustArgsC(t, from, m, 0)
}

func mustArgsC(t *testing.T, from string, m map[string]string, concurrency int) json.RawMessage {
	t.Helper()
	b, err := json.Marshal(instance.EvacuateRequest{FromHost: from, Map: m, Concurrency: concurrency})
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

func TestEffectiveConcurrency(t *testing.T) {
	cases := []struct{ handler, req, want int }{
		{0, 0, defaultEvacuateConcurrency},  // both unset -> default
		{5, 0, 5},                           // handler default honoured
		{5, 3, 3},                           // request overrides handler
		{0, 1000, maxEvacuateConcurrency},   // request clamped to max
		{0, -4, defaultEvacuateConcurrency}, // negative request ignored -> default
		{1, 0, 1},                           // handler of 1 honoured
		{1000, 0, maxEvacuateConcurrency},   // handler default clamped to max
	}
	for _, c := range cases {
		if got := effectiveConcurrency(c.handler, c.req); got != c.want {
			t.Errorf("effectiveConcurrency(%d,%d)=%d, want %d", c.handler, c.req, got, c.want)
		}
	}
}

// runConcurrencyProbe runs an evacuate of 4 instances and reports the max number
// of child migrations observed in flight at once, given a handler default and a
// per-request override.
func runConcurrencyProbe(t *testing.T, handlerDefault, reqOverride int) int32 {
	t.Helper()
	ctx := context.Background()
	svc, mem := buildSvc(t, "a", "b", "c", "d")
	parent, _ := mem.Enqueue(ctx, "evacuate", mustArgsC(t, "hostA",
		map[string]string{"a": "hostB", "b": "hostB", "c": "hostB", "d": "hostB"}, reqOverride), "")

	var inFlight, maxInFlight int32
	h := &Handler{Svc: svc, Jobs: mem, Concurrency: handlerDefault}
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
	return maxInFlight
}

func TestEvacuateConcurrencyBound(t *testing.T) {
	// Handler default of 1, no request override -> at most 1 in flight.
	if got := runConcurrencyProbe(t, 1, 0); got != 1 {
		t.Fatalf("handler Concurrency=1 but observed %d in flight", got)
	}
}

func TestEvacuateConcurrencyRequestOverride(t *testing.T) {
	// Handler default of 1, but the request asks for 1 explicitly -> still 1.
	if got := runConcurrencyProbe(t, 4, 1); got != 1 {
		t.Fatalf("request concurrency=1 over handler=4, but observed %d in flight", got)
	}
}

func TestEvacuate_CancelMarksChildrenCanceled(t *testing.T) {
	ctx := context.Background()
	svc, mem := buildSvc(t, "acme")
	parent, _ := mem.Enqueue(ctx, "evacuate", mustArgs(t, "hostA",
		map[string]string{"acme": "hostB"}), "")

	h := &Handler{Svc: svc, Jobs: mem}
	h.migrate = func(_ context.Context, req instance.MigrateRequest, step func(string, string)) error {
		return context.Canceled // parent canceled -> child rolled back on shared ctx
	}

	// Run returns an error (the move "failed" from the aggregate's view); we only
	// assert the child's recorded state here.
	_ = h.Run(ctx, parent, jobs.NewJobContext(mem, parent.ID))

	children, _ := mem.ListJobs(ctx, store.JobFilter{ParentID: parent.ID})
	if len(children) != 1 {
		t.Fatalf("want 1 child, got %d", len(children))
	}
	if children[0].State != store.JobCanceled {
		t.Fatalf("child state = %q, want canceled", children[0].State)
	}
}

// ctxAwareFinishStore wraps Memory but rejects Finish on an already-cancelled
// context, reproducing SQLite's behavior (the plain Memory store ignores ctx).
type ctxAwareFinishStore struct {
	*store.Memory
}

func (c ctxAwareFinishStore) Finish(ctx context.Context, id string, st store.JobState, msg string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	return c.Memory.Finish(ctx, id, st, msg)
}

// TestEvacuate_CancelChildFinishUsesDetachedCtx verifies that a cancelled
// parent context cannot prevent the child's terminal state from being written
// to a ctx-aware store (mirrors the SQLite production behavior that the plain
// Memory store hides by ignoring ctx).
func TestEvacuate_CancelChildFinishUsesDetachedCtx(t *testing.T) {
	svc, mem := buildSvc(t, "acme")
	cs := ctxAwareFinishStore{mem}

	// Enqueue via the underlying mem (cs wraps it, so either works the same).
	parent, _ := mem.Enqueue(context.Background(), "evacuate", mustArgs(t, "hostA",
		map[string]string{"acme": "hostB"}), "")

	h := &Handler{Svc: svc, Jobs: cs}
	ctx, cancel := context.WithCancel(context.Background())
	// The migrate stub simulates the operator cancellation landing mid-migration:
	// it cancels the shared ctx before returning, so by the time runChild calls
	// Finish the ctx is already cancelled — reproducing the production failure.
	h.migrate = func(_ context.Context, req instance.MigrateRequest, step func(string, string)) error {
		cancel() // simulate operator cancel landing mid-migration
		return context.Canceled
	}

	_ = h.Run(ctx, parent, jobs.NewJobContext(cs, parent.ID))

	children, _ := mem.ListJobs(context.Background(), store.JobFilter{ParentID: parent.ID})
	if len(children) != 1 {
		t.Fatalf("want 1 child, got %d", len(children))
	}
	if children[0].State != store.JobCanceled {
		t.Fatalf("child state = %q, want canceled (terminal write must use a detached ctx)", children[0].State)
	}
}

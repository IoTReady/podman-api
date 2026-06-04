package prune

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/iotready/podman-api/internal/jobs"
	"github.com/iotready/podman-api/internal/podman"
	"github.com/iotready/podman-api/internal/podman/fake"
	"github.com/iotready/podman-api/internal/store"
)

func runHandler(t *testing.T, h *Handler, payload Payload) (store.Job, error) {
	t.Helper()
	mem := store.NewMemory()
	args, _ := json.Marshal(payload)
	job, err := mem.Enqueue(context.Background(), "prune", args, "")
	if err != nil {
		t.Fatal(err)
	}
	jc := jobs.NewJobContext(mem, job.ID)
	runErr := h.Run(context.Background(), job, jc)
	got, _ := mem.GetJob(context.Background(), job.ID)
	return got, runErr
}

func TestHandlerRunsOnlyEnabledScopesInOrder(t *testing.T) {
	f := fake.New()
	f.PruneReports = map[string]struct {
		Items     []string
		Reclaimed int64
	}{
		"images":     {Items: []string{"i1"}, Reclaimed: 100},
		"containers": {Items: []string{"c1"}, Reclaimed: 200},
		"volumes":    {Items: []string{"v1"}, Reclaimed: 300},
	}
	h := &Handler{Client: f}
	pol := Policy{Scope: []string{ScopeDangling, ScopeContainers, ScopeVolumes}}
	if _, err := runHandler(t, h, Payload{Host: "h1", Policy: pol}); err != nil {
		t.Fatalf("Run: %v", err)
	}
	gotOrder := []string{}
	for _, c := range f.PruneCalls {
		gotOrder = append(gotOrder, c.Scope)
	}
	want := []string{"images", "containers", "volumes"}
	if strings.Join(gotOrder, ",") != strings.Join(want, ",") {
		t.Fatalf("scope order = %v, want %v", gotOrder, want)
	}
	if f.PruneCalls[0].All {
		t.Fatal("dangling scope must call ImagePrune(all=false)")
	}
	if f.PruneCalls[2].Filters["label!"][0] != ProtectLabel+"=true" {
		t.Fatalf("volume prune missing protect filter: %+v", f.PruneCalls[2].Filters)
	}
}

func TestHandlerAllImagesPassesAllTrue(t *testing.T) {
	f := fake.New()
	h := &Handler{Client: f}
	if _, err := runHandler(t, h, Payload{Host: "h1", Policy: Policy{Scope: []string{ScopeAllImages}}}); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if len(f.PruneCalls) != 1 || !f.PruneCalls[0].All {
		t.Fatalf("all-images must call ImagePrune(all=true): %+v", f.PruneCalls)
	}
}

func TestHandlerDryRunRemovesNothing(t *testing.T) {
	f := fake.New()
	f.HostInfoVal = podman.HostInfo{Disk: podman.DiskUsage{Reclaimable: 4096}}
	h := &Handler{Client: f}
	job, err := runHandler(t, h, Payload{Host: "h1", Policy: Policy{Scope: []string{ScopeDangling, ScopeVolumes}, DryRun: true}})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if len(f.PruneCalls) != 0 {
		t.Fatalf("dry-run must not call prune, got %+v", f.PruneCalls)
	}
	joined := ""
	for _, s := range job.Steps {
		joined += s.Step + ":" + s.Detail + "\n"
	}
	if !strings.Contains(joined, "dry-run") || !strings.Contains(joined, "4096") {
		t.Fatalf("dry-run step missing reclaimable: %q", joined)
	}
}

func TestHandlerScopeErrorFailsJobButContinues(t *testing.T) {
	f := fake.New()
	// PruneErr key "images" is the fake's internal scope key for ImagePrune,
	// which handles both ScopeDangling and ScopeAllImages.
	f.PruneErr = map[string]error{"images": errors.New("boom")}
	h := &Handler{Client: f}
	_, err := runHandler(t, h, Payload{Host: "h1", Policy: Policy{Scope: []string{ScopeDangling, ScopeContainers}}})
	if err == nil {
		t.Fatal("expected handler to return error when a scope fails")
	}
	ranContainers := false
	for _, c := range f.PruneCalls {
		if c.Scope == "containers" {
			ranContainers = true
		}
	}
	if !ranContainers {
		t.Fatal("handler must continue remaining scopes after one fails")
	}
}

type spyMetrics struct {
	runDone   []string         // result values, in order
	reclaimed map[string]int64 // scope -> bytes
}

func (s *spyMetrics) RunDone(_, result string)           { s.runDone = append(s.runDone, result) }
func (s *spyMetrics) Reclaimed(_, scope string, b int64) { s.reclaimed[scope] = b }

func TestHandlerMetricsOnSuccess(t *testing.T) {
	f := fake.New()
	f.PruneReports = map[string]struct {
		Items     []string
		Reclaimed int64
	}{"images": {Items: []string{"i1"}, Reclaimed: 512}}
	spy := &spyMetrics{reclaimed: map[string]int64{}}
	h := &Handler{Client: f, Metrics: spy}
	if _, err := runHandler(t, h, Payload{Host: "h1", Policy: Policy{Scope: []string{ScopeDangling}}}); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if len(spy.runDone) != 1 || spy.runDone[0] != "succeeded" {
		t.Fatalf("RunDone = %v, want [succeeded]", spy.runDone)
	}
	if spy.reclaimed["dangling"] != 512 {
		t.Fatalf("Reclaimed[dangling] = %d, want 512", spy.reclaimed["dangling"])
	}
}

func TestHandlerMetricsOnFailure(t *testing.T) {
	f := fake.New()
	f.PruneErr = map[string]error{"images": errors.New("boom")}
	spy := &spyMetrics{reclaimed: map[string]int64{}}
	h := &Handler{Client: f, Metrics: spy}
	if _, err := runHandler(t, h, Payload{Host: "h1", Policy: Policy{Scope: []string{ScopeDangling}}}); err == nil {
		t.Fatal("expected error")
	}
	if len(spy.runDone) != 1 || spy.runDone[0] != "failed" {
		t.Fatalf("RunDone = %v, want [failed]", spy.runDone)
	}
}

func TestHandlerHonorsContextCancellation(t *testing.T) {
	f := fake.New()
	h := &Handler{Client: f}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	mem := store.NewMemory()
	args, _ := json.Marshal(Payload{Host: "h1", Policy: Policy{Scope: []string{ScopeDangling}}})
	job, _ := mem.Enqueue(context.Background(), "prune", args, "")
	jc := jobs.NewJobContext(mem, job.ID)
	if err := h.Run(ctx, job, jc); err == nil {
		t.Fatal("expected cancellation error")
	}
	if len(f.PruneCalls) != 0 {
		t.Fatalf("must not prune after cancellation: %+v", f.PruneCalls)
	}
}

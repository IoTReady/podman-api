package fake

import (
	"context"
	"errors"
	"testing"
)

func TestImagePruneRecordsCallAndReturnsCannedReport(t *testing.T) {
	f := New()
	f.PruneReports["images"] = struct {
		Items     []string
		Reclaimed int64
	}{Items: []string{"sha256:aaa"}, Reclaimed: 4096}

	rep, err := f.ImagePrune(context.Background(), "h1", true)
	if err != nil {
		t.Fatalf("ImagePrune: %v", err)
	}
	if rep.Reclaimed != 4096 || len(rep.Items) != 1 {
		t.Fatalf("unexpected report: %+v", rep)
	}
	if len(f.PruneCalls) != 1 || f.PruneCalls[0].Host != "h1" ||
		f.PruneCalls[0].Scope != "images" || !f.PruneCalls[0].All {
		t.Fatalf("unexpected calls: %+v", f.PruneCalls)
	}
}

func TestVolumePruneRecordsFiltersAndError(t *testing.T) {
	f := New()
	f.PruneErr = map[string]error{"volumes": errors.New("boom")}
	_, err := f.VolumePrune(context.Background(), "h1",
		map[string][]string{"label!": {"podman-api.protect=true"}})
	if err == nil {
		t.Fatal("expected error")
	}
	if len(f.PruneCalls) != 1 || f.PruneCalls[0].Scope != "volumes" ||
		f.PruneCalls[0].Filters["label!"][0] != "podman-api.protect=true" {
		t.Fatalf("unexpected calls: %+v", f.PruneCalls)
	}
}

func TestPruneScopesRecordWithoutHooks(t *testing.T) {
	f := New()
	if _, err := f.ContainerPrune(context.Background(), "h1"); err != nil {
		t.Fatalf("ContainerPrune: %v", err)
	}
	if _, err := f.BuildCachePrune(context.Background(), "h1"); err != nil {
		t.Fatalf("BuildCachePrune: %v", err)
	}
	if len(f.PruneCalls) != 2 ||
		f.PruneCalls[0].Scope != "containers" ||
		f.PruneCalls[1].Scope != "buildcache" {
		t.Fatalf("unexpected scope recording: %+v", f.PruneCalls)
	}
	// With no canned report, each returns the zero report.
	if rep, _ := f.ContainerPrune(context.Background(), "h1"); rep.Reclaimed != 0 || rep.Items != nil {
		t.Fatalf("expected empty report, got %+v", rep)
	}
}

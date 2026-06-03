package store

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
)

func TestMemory_Jobs_EnqueueClaimFinish(t *testing.T) {
	ctx := context.Background()
	m := NewMemory()
	j, err := m.Enqueue(ctx, "migrate", json.RawMessage(`{}`), "")
	if err != nil || j.State != JobQueued {
		t.Fatalf("enqueue: %+v err=%v", j, err)
	}
	c, ok, err := m.ClaimNext(ctx)
	if err != nil || !ok || c.ID != j.ID || c.State != JobRunning {
		t.Fatalf("claim: %+v ok=%v err=%v", c, ok, err)
	}
	if _, ok, _ := m.ClaimNext(ctx); ok {
		t.Fatal("nothing left to claim")
	}
	if err := m.AppendStep(ctx, j.ID, JobStep{Step: "x"}); err != nil {
		t.Fatalf("append: %v", err)
	}
	if err := m.Finish(ctx, j.ID, JobSucceeded, ""); err != nil {
		t.Fatalf("finish: %v", err)
	}
	got, _ := m.GetJob(ctx, j.ID)
	if got.State != JobSucceeded || len(got.Steps) != 1 {
		t.Fatalf("final: %+v", got)
	}
}

func TestMemory_Jobs_GetMissing(t *testing.T) {
	if _, err := NewMemory().GetJob(context.Background(), "nope"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("want ErrNotFound, got %v", err)
	}
}

func TestMemory_Jobs_FailRunning(t *testing.T) {
	ctx := context.Background()
	m := NewMemory()
	j, _ := m.Enqueue(ctx, "migrate", json.RawMessage(`{}`), "")
	_, _, _ = m.ClaimNext(ctx)
	n, err := m.FailRunning(ctx, "boom")
	if err != nil || n != 1 {
		t.Fatalf("failrunning n=%d err=%v", n, err)
	}
	got, _ := m.GetJob(ctx, j.ID)
	if got.State != JobFailed || got.Error != "boom" {
		t.Fatalf("not failed: %+v", got)
	}
}

func TestMemory_Jobs_ListFilterNewestFirst(t *testing.T) {
	ctx := context.Background()
	m := NewMemory()
	_, _ = m.Enqueue(ctx, "migrate", json.RawMessage(`{}`), "")
	b, _ := m.Enqueue(ctx, "evacuate", json.RawMessage(`{}`), "")
	all, _ := m.ListJobs(ctx, JobFilter{})
	if len(all) != 2 || all[0].ID != b.ID {
		t.Fatalf("newest-first failed: %+v", all)
	}
	ev, _ := m.ListJobs(ctx, JobFilter{Kind: "evacuate"})
	if len(ev) != 1 {
		t.Fatalf("filter failed: %+v", ev)
	}
}

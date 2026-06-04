package store

import (
	"context"
	"encoding/json"
	"testing"
	"time"
)

// finishJob marks a job terminal so it is prune-eligible.
func finishJob(t *testing.T, js JobStore, id string) {
	t.Helper()
	if err := js.Finish(context.Background(), id, JobSucceeded, ""); err != nil {
		t.Fatal(err)
	}
}

func testPruneOn(t *testing.T, js JobStore) {
	ctx := context.Background()

	// Case 1: a standalone terminal job is pruned; cutoff in the future so it
	// counts as "old".
	old, _ := js.Enqueue(ctx, "test", json.RawMessage(`{}`), "")
	finishJob(t, js, old.ID)

	// Case 2: a still-queued (non-terminal) job is kept.
	queued, _ := js.Enqueue(ctx, "test", json.RawMessage(`{}`), "")

	// Case 3: a terminal parent whose child is still running must be kept.
	parent, _ := js.Enqueue(ctx, "evacuate", json.RawMessage(`{}`), "")
	finishJob(t, js, parent.ID)
	child, _ := js.StartChild(ctx, "migrate", json.RawMessage(`{}`), parent.ID) // running

	n, err := js.PruneJobs(ctx, time.Now().Add(time.Hour)) // everything finished is "old"
	if err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatalf("pruned %d, want 1 (only the standalone terminal job)", n)
	}
	if _, err := js.GetJob(ctx, old.ID); err != ErrNotFound {
		t.Fatalf("standalone job should be gone, err=%v", err)
	}
	if _, err := js.GetJob(ctx, queued.ID); err != nil {
		t.Fatalf("queued job should survive: %v", err)
	}
	if _, err := js.GetJob(ctx, parent.ID); err != nil {
		t.Fatalf("parent with running child should survive: %v", err)
	}

	// Now finish the child and prune again: both parent and child go.
	finishJob(t, js, child.ID)
	n, err = js.PruneJobs(ctx, time.Now().Add(time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	if n != 2 {
		t.Fatalf("pruned %d, want 2 (parent + child)", n)
	}
	if _, err := js.GetJob(ctx, parent.ID); err != ErrNotFound {
		t.Fatalf("parent should be gone, err=%v", err)
	}
	if _, err := js.GetJob(ctx, child.ID); err != ErrNotFound {
		t.Fatalf("child should be gone, err=%v", err)
	}

	// Recent terminal jobs (cutoff in the past) are kept.
	recent, _ := js.Enqueue(ctx, "test", json.RawMessage(`{}`), "")
	finishJob(t, js, recent.ID)
	n, err = js.PruneJobs(ctx, time.Now().Add(-time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	if n != 0 {
		t.Fatalf("pruned %d, want 0 (nothing older than the past cutoff)", n)
	}
	if _, err := js.GetJob(ctx, recent.ID); err != nil {
		t.Fatalf("recent job should survive: %v", err)
	}
}

func TestPrune_SQLite(t *testing.T) { testPruneOn(t, openJobStore(t)) }
func TestPrune_Memory(t *testing.T) { testPruneOn(t, NewMemory()) }

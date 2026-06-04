package store

import (
	"context"
	"encoding/json"
	"testing"
)

func TestStartChildIsRunningAndUnclaimable(t *testing.T) {
	m := NewMemory()
	ctx := context.Background()

	parent, err := m.Enqueue(ctx, "evacuate", json.RawMessage(`{}`), "")
	if err != nil {
		t.Fatal(err)
	}
	child, err := m.StartChild(ctx, "migrate", json.RawMessage(`{"slug":"acme"}`), parent.ID)
	if err != nil {
		t.Fatal(err)
	}
	if child.State != JobRunning {
		t.Fatalf("child state = %q, want running", child.State)
	}
	if child.ParentID != parent.ID {
		t.Fatalf("child parent = %q, want %q", child.ParentID, parent.ID)
	}
	if child.Started.IsZero() {
		t.Fatal("child Started should be set")
	}

	// ClaimNext must pick the queued PARENT, never the already-running child.
	claimed, ok, err := m.ClaimNext(ctx)
	if err != nil || !ok {
		t.Fatalf("ClaimNext ok=%v err=%v", ok, err)
	}
	if claimed.ID != parent.ID {
		t.Fatalf("claimed %q, want parent %q (child must be unclaimable)", claimed.ID, parent.ID)
	}
	if _, ok, _ := m.ClaimNext(ctx); ok {
		t.Fatal("a second claim returned a job; the running child must not be claimable")
	}
}

func TestListJobsByParentID(t *testing.T) {
	m := NewMemory()
	ctx := context.Background()
	parent, _ := m.Enqueue(ctx, "evacuate", json.RawMessage(`{}`), "")
	c1, _ := m.StartChild(ctx, "migrate", json.RawMessage(`{}`), parent.ID)
	c2, _ := m.StartChild(ctx, "migrate", json.RawMessage(`{}`), parent.ID)
	_, _ = m.Enqueue(ctx, "migrate", json.RawMessage(`{}`), "") // unrelated top-level

	got, err := m.ListJobs(ctx, JobFilter{ParentID: parent.ID})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 {
		t.Fatalf("want 2 children, got %d", len(got))
	}
	ids := map[string]bool{got[0].ID: true, got[1].ID: true}
	if !ids[c1.ID] || !ids[c2.ID] {
		t.Fatalf("children missing from filtered list: %v", got)
	}
}

func TestSQLiteStartChildAndParentFilter(t *testing.T) {
	ctx := context.Background()
	s := openJobStore(t)

	parent, err := s.Enqueue(ctx, "evacuate", json.RawMessage(`{}`), "")
	if err != nil {
		t.Fatal(err)
	}

	c1, err := s.StartChild(ctx, "migrate", json.RawMessage(`{"slug":"acme"}`), parent.ID)
	if err != nil {
		t.Fatal(err)
	}
	c2, err := s.StartChild(ctx, "migrate", json.RawMessage(`{"slug":"beta"}`), parent.ID)
	if err != nil {
		t.Fatal(err)
	}
	for _, child := range []Job{c1, c2} {
		if child.State != JobRunning {
			t.Fatalf("child %q state = %q, want running", child.ID, child.State)
		}
		if child.ParentID != parent.ID {
			t.Fatalf("child %q parent = %q, want %q", child.ID, child.ParentID, parent.ID)
		}
		if child.Started.IsZero() {
			t.Fatalf("child %q Started should be set", child.ID)
		}
	}

	// ClaimNext must pick the queued PARENT, never the already-running children.
	claimed, ok, err := s.ClaimNext(ctx)
	if err != nil || !ok {
		t.Fatalf("ClaimNext ok=%v err=%v", ok, err)
	}
	if claimed.ID != parent.ID {
		t.Fatalf("claimed %q, want parent %q (children must be unclaimable)", claimed.ID, parent.ID)
	}
	if _, ok, _ := s.ClaimNext(ctx); ok {
		t.Fatal("a second claim returned a job; the running children must not be claimable")
	}

	got, err := s.ListJobs(ctx, JobFilter{ParentID: parent.ID})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 {
		t.Fatalf("want 2 children, got %d", len(got))
	}
	ids := map[string]bool{got[0].ID: true, got[1].ID: true}
	if !ids[c1.ID] || !ids[c2.ID] {
		t.Fatalf("children missing from filtered list: %v", got)
	}
}

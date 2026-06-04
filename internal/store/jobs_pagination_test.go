package store

import (
	"context"
	"encoding/json"
	"testing"
)

// enqueueN inserts n jobs and returns them newest-last.
func enqueueN(t *testing.T, js JobStore, n int) []Job {
	t.Helper()
	ctx := context.Background()
	var jobs []Job
	for i := 0; i < n; i++ {
		j, err := js.Enqueue(ctx, "test", json.RawMessage(`{}`), "")
		if err != nil {
			t.Fatal(err)
		}
		jobs = append(jobs, j)
	}
	return jobs
}

func testPaginationOn(t *testing.T, js JobStore) {
	ctx := context.Background()
	created := enqueueN(t, js, 5) // oldest..newest

	// limit caps the page
	page1, err := js.ListJobs(ctx, JobFilter{Limit: 2})
	if err != nil {
		t.Fatal(err)
	}
	if len(page1) != 2 {
		t.Fatalf("page1 len = %d, want 2", len(page1))
	}
	// newest first
	if page1[0].ID != created[4].ID || page1[1].ID != created[3].ID {
		t.Fatalf("page1 order wrong: %s,%s", page1[0].ID, page1[1].ID)
	}

	// before cursor returns the following page with no overlap
	page2, err := js.ListJobs(ctx, JobFilter{Limit: 2, Before: page1[1].ID})
	if err != nil {
		t.Fatal(err)
	}
	if len(page2) != 2 || page2[0].ID != created[2].ID || page2[1].ID != created[1].ID {
		t.Fatalf("page2 wrong: %+v", []string{page2[0].ID, page2[1].ID})
	}

	// zero/negative limit → default (all 5 here, well under DefaultJobLimit)
	all, err := js.ListJobs(ctx, JobFilter{Limit: 0})
	if err != nil {
		t.Fatal(err)
	}
	if len(all) != 5 {
		t.Fatalf("default-limit len = %d, want 5", len(all))
	}
}

func TestPagination_SQLite(t *testing.T) { testPaginationOn(t, openJobStore(t)) }
func TestPagination_Memory(t *testing.T) { testPaginationOn(t, NewMemory()) }

func TestClampJobLimit(t *testing.T) {
	cases := []struct{ in, want int }{
		{0, DefaultJobLimit}, {-5, DefaultJobLimit}, {50, 50},
		{MaxJobLimit, MaxJobLimit}, {MaxJobLimit + 1, MaxJobLimit},
	}
	for _, c := range cases {
		if got := clampJobLimit(c.in); got != c.want {
			t.Errorf("clampJobLimit(%d) = %d, want %d", c.in, got, c.want)
		}
	}
}

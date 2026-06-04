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

// pageAll walks every page via the Before cursor and returns all ids seen,
// failing on a duplicate or a non-descending id (gap/overlap symptoms).
func pageAll(t *testing.T, js JobStore, limit int) map[string]bool {
	t.Helper()
	ctx := context.Background()
	seen := map[string]bool{}
	before, last := "", ""
	for {
		page, err := js.ListJobs(ctx, JobFilter{Limit: limit, Before: before})
		if err != nil {
			t.Fatal(err)
		}
		if len(page) == 0 {
			break
		}
		for _, j := range page {
			if seen[j.ID] {
				t.Fatalf("duplicate id across pages: %s", j.ID)
			}
			if last != "" && j.ID >= last {
				t.Fatalf("ids not strictly descending: %s came after %s", j.ID, last)
			}
			seen[j.ID] = true
			last = j.ID
		}
		before = page[len(page)-1].ID
	}
	return seen
}

// testPaginationCompletenessOn pages through more jobs than a page holds and
// asserts every job appears exactly once, in id order, with no gaps.
func testPaginationCompletenessOn(t *testing.T, js JobStore) {
	want := map[string]bool{}
	for _, j := range enqueueN(t, js, 12) {
		want[j.ID] = true
	}
	seen := pageAll(t, js, 5)
	if len(seen) != len(want) {
		t.Fatalf("paged %d jobs, want %d", len(seen), len(want))
	}
	for id := range want {
		if !seen[id] {
			t.Fatalf("job %s missing from paged results", id)
		}
	}
}

func TestPaginationCompleteness_SQLite(t *testing.T) {
	testPaginationCompletenessOn(t, openJobStore(t))
}
func TestPaginationCompleteness_Memory(t *testing.T) { testPaginationCompletenessOn(t, NewMemory()) }

// rawInsertJob inserts a terminal job row with a caller-chosen id and created
// timestamp, bypassing Enqueue — used to construct a (created, id) inversion.
func rawInsertJob(t *testing.T, s *SQLite, id string, created int64) {
	t.Helper()
	_, err := s.db.ExecContext(context.Background(), `
INSERT INTO jobs (id, kind, args, state, steps, parent_id, error, created, started, finished)
VALUES (?, 'test', 'null', 'succeeded', '[]', NULL, NULL, ?, NULL, ?)`, id, created, created)
	if err != nil {
		t.Fatal(err)
	}
}

// TestPagination_CursorMatchesSortKey guards the bug where ListJobs ordered by
// `created` but cursored on `id`: when the two disagree (concurrent inserts read
// the clock twice), a row could be skipped between pages. Here job A has the
// LARGER id but the SMALLER created, and B the reverse — an id cursor over a
// created ordering would drop one of them.
func TestPagination_CursorMatchesSortKey(t *testing.T) {
	s := openJobStore(t)
	rawInsertJob(t, s, "0000000000000002-aaaaaaaaaaaa", 1000) // big id, small created
	rawInsertJob(t, s, "0000000000000001-aaaaaaaaaaaa", 2000) // small id, big created

	seen := pageAll(t, s, 1) // page size 1 forces the cursor on every step
	if len(seen) != 2 {
		t.Fatalf("paging dropped a row: saw %d of 2: %v", len(seen), seen)
	}
}

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

package store

import (
	"context"
	"encoding/json"
	"errors"
	"regexp"
	"sync"
	"testing"
	"time"
)

func TestNewJobID_Sortable(t *testing.T) {
	// IDs created at different times sort lexicographically by time (the 16-hex
	// unix-nanosecond prefix). A 1ms gap guarantees distinct prefixes.
	a := newJobID()
	time.Sleep(1 * time.Millisecond)
	b := newJobID()
	if !(a < b) {
		t.Fatalf("ids not time-sortable: %q !< %q", a, b)
	}
}

func TestNewJobID_FormatAndUnique(t *testing.T) {
	// The tight loop mostly shares a nanosecond prefix, so this primarily
	// exercises the random-suffix uniqueness path; cross-time ordering is
	// covered by TestNewJobID_Sortable.
	re := regexp.MustCompile(`^[0-9a-f]{16}-[0-9a-f]{12}$`)
	seen := map[string]bool{}
	for i := 0; i < 1000; i++ {
		id := newJobID()
		if !re.MatchString(id) {
			t.Fatalf("id %q does not match expected format", id)
		}
		if seen[id] {
			t.Fatalf("duplicate id %q", id)
		}
		seen[id] = true
	}
}

func openJobStore(t *testing.T) *SQLite {
	t.Helper()
	return openTestStore(t, NewKeyStore(testKey(0x11)))
}

func TestSQLite_Enqueue_Get(t *testing.T) {
	ctx := context.Background()
	s := openJobStore(t)
	j, err := s.Enqueue(ctx, "migrate", json.RawMessage(`{"from":"h1"}`), "")
	if err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	if j.ID == "" || j.State != JobQueued || j.Created.IsZero() {
		t.Fatalf("bad enqueued job: %+v", j)
	}
	got, err := s.GetJob(ctx, j.ID)
	if err != nil {
		t.Fatalf("GetJob: %v", err)
	}
	if got.Kind != "migrate" || string(got.Args) != `{"from":"h1"}` || got.State != JobQueued {
		t.Fatalf("round-trip mismatch: %+v", got)
	}
}

func TestSQLite_GetJob_Missing(t *testing.T) {
	if _, err := openJobStore(t).GetJob(context.Background(), "nope"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("want ErrNotFound, got %v", err)
	}
}

func TestSQLite_ListJobs_FilterAndOrder(t *testing.T) {
	ctx := context.Background()
	s := openJobStore(t)
	a, _ := s.Enqueue(ctx, "migrate", json.RawMessage(`{}`), "")
	b, _ := s.Enqueue(ctx, "evacuate", json.RawMessage(`{}`), "")
	_ = a
	all, err := s.ListJobs(ctx, JobFilter{})
	if err != nil {
		t.Fatalf("ListJobs: %v", err)
	}
	if len(all) != 2 || all[0].ID != b.ID {
		t.Fatalf("expected newest-first, got %d jobs head=%v", len(all), all[0].ID)
	}
	mig, _ := s.ListJobs(ctx, JobFilter{Kind: "migrate"})
	if len(mig) != 1 || mig[0].Kind != "migrate" {
		t.Fatalf("kind filter failed: %+v", mig)
	}
}

func TestSQLite_ClaimNext_AndEmpty(t *testing.T) {
	ctx := context.Background()
	s := openJobStore(t)
	if _, ok, err := s.ClaimNext(ctx); err != nil || ok {
		t.Fatalf("empty claim: ok=%v err=%v", ok, err)
	}
	first, _ := s.Enqueue(ctx, "migrate", json.RawMessage(`{}`), "")
	j, ok, err := s.ClaimNext(ctx)
	if err != nil || !ok {
		t.Fatalf("claim: ok=%v err=%v", ok, err)
	}
	if j.ID != first.ID || j.State != JobRunning || j.Started.IsZero() {
		t.Fatalf("bad claimed job: %+v", j)
	}
	if _, ok, _ := s.ClaimNext(ctx); ok {
		t.Fatal("second claim should find nothing (only running left)")
	}
}

func TestSQLite_ClaimNext_NoDoubleClaim(t *testing.T) {
	ctx := context.Background()
	s := openJobStore(t)
	const n = 20
	for i := 0; i < n; i++ {
		if _, err := s.Enqueue(ctx, "migrate", json.RawMessage(`{}`), ""); err != nil {
			t.Fatal(err)
		}
	}
	var mu sync.Mutex
	claimed := map[string]int{}
	errCh := make(chan error, 4)
	var wg sync.WaitGroup
	for w := 0; w < 4; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				j, ok, err := s.ClaimNext(ctx)
				if err != nil {
					errCh <- err
					return
				}
				if !ok {
					return
				}
				mu.Lock()
				claimed[j.ID]++
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Fatalf("ClaimNext error: %v", err)
	}
	if len(claimed) != n {
		t.Fatalf("claimed %d distinct jobs, want %d", len(claimed), n)
	}
	for id, c := range claimed {
		if c != 1 {
			t.Fatalf("job %s claimed %d times", id, c)
		}
	}
}

func TestSQLite_AppendStep_Finish_FailRunning(t *testing.T) {
	ctx := context.Background()
	s := openJobStore(t)
	j, _ := s.Enqueue(ctx, "migrate", json.RawMessage(`{}`), "")
	_, _, _ = s.ClaimNext(ctx)
	if err := s.AppendStep(ctx, j.ID, JobStep{TS: time.Unix(100, 0), Step: "stop", Detail: "src"}); err != nil {
		t.Fatalf("AppendStep: %v", err)
	}
	got, _ := s.GetJob(ctx, j.ID)
	if len(got.Steps) != 1 || got.Steps[0].Step != "stop" {
		t.Fatalf("step not recorded: %+v", got.Steps)
	}
	if err := s.Finish(ctx, j.ID, JobSucceeded, ""); err != nil {
		t.Fatalf("Finish: %v", err)
	}
	got, _ = s.GetJob(ctx, j.ID)
	if got.State != JobSucceeded || got.Finished.IsZero() {
		t.Fatalf("finish not applied: %+v", got)
	}

	j2, _ := s.Enqueue(ctx, "migrate", json.RawMessage(`{}`), "")
	_, _, _ = s.ClaimNext(ctx)
	n, err := s.FailRunning(ctx, "interrupted")
	if err != nil || n != 1 {
		t.Fatalf("FailRunning n=%d err=%v", n, err)
	}
	got2, _ := s.GetJob(ctx, j2.ID)
	if got2.State != JobFailed || got2.Error != "interrupted" {
		t.Fatalf("FailRunning did not mark job: %+v", got2)
	}
}

func TestSQLite_Finish_Missing(t *testing.T) {
	if err := openJobStore(t).Finish(context.Background(), "nope", JobSucceeded, ""); !errors.Is(err, ErrNotFound) {
		t.Fatalf("want ErrNotFound, got %v", err)
	}
}

func TestSQLite_AppendStep_Missing(t *testing.T) {
	if err := openJobStore(t).AppendStep(context.Background(), "nope", JobStep{Step: "x"}); !errors.Is(err, ErrNotFound) {
		t.Fatalf("want ErrNotFound, got %v", err)
	}
}

func TestSQLite_Finish_RejectsNonTerminal(t *testing.T) {
	ctx := context.Background()
	s := openJobStore(t)
	j, _ := s.Enqueue(ctx, "migrate", json.RawMessage(`{}`), "")
	if err := s.Finish(ctx, j.ID, JobQueued, ""); err == nil {
		t.Fatal("Finish with non-terminal state should error")
	}
}

func TestSQLite_Finish_AcceptsCanceled(t *testing.T) {
	ctx := context.Background()
	s := openJobStore(t)
	j, _ := s.Enqueue(ctx, "migrate", json.RawMessage(`{}`), "")
	if _, _, err := s.ClaimNext(ctx); err != nil {
		t.Fatalf("claim: %v", err)
	}
	if err := s.Finish(ctx, j.ID, JobCanceled, "canceled by operator"); err != nil {
		t.Fatalf("Finish canceled: %v", err)
	}
	got, _ := s.GetJob(ctx, j.ID)
	if got.State != JobCanceled || got.Error != "canceled by operator" || got.Finished.IsZero() {
		t.Fatalf("bad canceled job: %+v", got)
	}
}

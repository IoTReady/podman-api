package store

import (
	"context"
	"encoding/json"
	"testing"
)

// jobStores returns the two JobStore implementations under test, each seeded
// independently. The factory lets each subtest get a fresh store.
func jobStores(t *testing.T) map[string]func() JobStore {
	t.Helper()
	return map[string]func() JobStore{
		"memory": func() JobStore { return NewMemory() },
		"sqlite": func() JobStore { return openTestStore(t, NewKeyStore(testKey(0x11))) },
	}
}

func TestMarkReconciling(t *testing.T) {
	for name, mk := range jobStores(t) {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			js := mk()

			// A running migrate, a running evacuate, and a queued migrate.
			mig, err := js.Enqueue(ctx, "migrate", json.RawMessage(`{}`), "")
			if err != nil {
				t.Fatal(err)
			}
			evac, err := js.Enqueue(ctx, "evacuate", json.RawMessage(`{}`), "")
			if err != nil {
				t.Fatal(err)
			}
			queued, err := js.Enqueue(ctx, "migrate", json.RawMessage(`{}`), "")
			if err != nil {
				t.Fatal(err)
			}
			// Claim the first two so they are running; leave `queued` queued.
			if _, _, err := js.ClaimNext(ctx); err != nil { // claims mig (oldest)
				t.Fatal(err)
			}
			if _, _, err := js.ClaimNext(ctx); err != nil { // claims evac
				t.Fatal(err)
			}

			n, err := js.MarkReconciling(ctx, []string{"migrate"})
			if err != nil {
				t.Fatal(err)
			}
			if n != 1 {
				t.Fatalf("MarkReconciling moved %d jobs, want 1", n)
			}

			assertState(t, js, mig.ID, JobReconciling) // running migrate -> reconciling
			assertState(t, js, evac.ID, JobRunning)    // running evacuate untouched
			assertState(t, js, queued.ID, JobQueued)   // queued migrate untouched
		})
	}
}

func TestMarkReconciling_EmptyKinds(t *testing.T) {
	for name, mk := range jobStores(t) {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			js := mk()
			if _, err := js.Enqueue(ctx, "migrate", json.RawMessage(`{}`), ""); err != nil {
				t.Fatal(err)
			}
			if _, _, err := js.ClaimNext(ctx); err != nil {
				t.Fatal(err)
			}
			n, err := js.MarkReconciling(ctx, nil)
			if err != nil {
				t.Fatal(err)
			}
			if n != 0 {
				t.Fatalf("MarkReconciling(nil) moved %d, want 0", n)
			}
		})
	}
}

func assertState(t *testing.T, js JobStore, id string, want JobState) {
	t.Helper()
	j, err := js.GetJob(context.Background(), id)
	if err != nil {
		t.Fatalf("GetJob %s: %v", id, err)
	}
	if j.State != want {
		t.Fatalf("job %s state = %q, want %q", id, j.State, want)
	}
}

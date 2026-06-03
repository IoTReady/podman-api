package store

import (
	"context"
	"path/filepath"
	"sync"
	"testing"
)

func TestSQLite_JobsTableExists(t *testing.T) {
	s := openTestStore(t, NewKeyStore(testKey(0x11)))
	var name string
	err := s.db.QueryRow(`SELECT name FROM sqlite_master WHERE type='table' AND name='jobs'`).Scan(&name)
	if err != nil {
		t.Fatalf("jobs table not found: %v", err)
	}
	if name != "jobs" {
		t.Fatalf("got table %q", name)
	}
}

func TestSQLite_BusyTimeoutSet(t *testing.T) {
	s := openTestStore(t, NewKeyStore(testKey(0x11)))
	var ms int
	if err := s.db.QueryRow(`PRAGMA busy_timeout`).Scan(&ms); err != nil {
		t.Fatalf("read busy_timeout: %v", err)
	}
	if ms != 5000 {
		t.Fatalf("busy_timeout = %d, want 5000", ms)
	}
}

func TestSQLite_ConcurrentSpecWrites_NoLockError(t *testing.T) {
	ctx := context.Background()
	db := filepath.Join(t.TempDir(), "state.db")
	s, err := OpenSQLite(db, NewKeyStore(testKey(0x11)))
	if err != nil {
		t.Fatalf("OpenSQLite: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })

	var wg sync.WaitGroup
	errs := make(chan error, 16)
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			sp := sampleSpec()
			sp.Slug = string(rune('a' + i))
			if err := s.PutSpec(ctx, sp); err != nil {
				errs <- err
			}
		}(i)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatalf("concurrent PutSpec failed (likely 'database is locked'): %v", err)
	}
}

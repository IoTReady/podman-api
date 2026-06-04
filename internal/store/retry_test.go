package store

import (
	"context"
	"database/sql"
	"errors"
	"net/url"
	"path/filepath"
	"testing"

	sqlite "modernc.org/sqlite"
)

func TestRetry_Mechanics(t *testing.T) {
	ctx := context.Background()
	retryable := func(err error) bool { return err != nil && err.Error() == "retry me" }

	t.Run("non-retryable returns immediately", func(t *testing.T) {
		calls := 0
		want := errors.New("boom")
		err := retry(ctx, retryable, func() error { calls++; return want })
		if !errors.Is(err, want) {
			t.Fatalf("got %v, want %v", err, want)
		}
		if calls != 1 {
			t.Fatalf("non-retryable retried: %d calls, want 1", calls)
		}
	})

	t.Run("success returns immediately", func(t *testing.T) {
		calls := 0
		if err := retry(ctx, retryable, func() error { calls++; return nil }); err != nil {
			t.Fatalf("got %v, want nil", err)
		}
		if calls != 1 {
			t.Fatalf("success retried: %d calls, want 1", calls)
		}
	})

	t.Run("retries then succeeds", func(t *testing.T) {
		calls := 0
		err := retry(ctx, retryable, func() error {
			calls++
			if calls < 3 {
				return errors.New("retry me")
			}
			return nil
		})
		if err != nil {
			t.Fatalf("got %v, want nil after retries", err)
		}
		if calls != 3 {
			t.Fatalf("retried %d times, want 3", calls)
		}
	})

	t.Run("cancelled ctx stops retrying", func(t *testing.T) {
		cctx, cancel := context.WithCancel(ctx)
		cancel()
		calls := 0
		err := retry(cctx, retryable, func() error { calls++; return errors.New("retry me") })
		if err == nil {
			t.Fatal("want the last error, got nil")
		}
		if calls != 1 {
			t.Fatalf("cancelled ctx retried: %d calls, want 1", calls)
		}
	})
}

func TestIsBusy(t *testing.T) {
	if isBusy(nil) {
		t.Fatal("isBusy(nil) = true, want false")
	}
	if isBusy(errors.New("plain")) {
		t.Fatal("isBusy(plain error) = true, want false")
	}

	// A real SQLITE_BUSY: hold an exclusive write lock on one connection with
	// busy_timeout(0) and trigger a write from another — modernc returns
	// *sqlite.Error with primary code 5 (SQLITE_BUSY) immediately.
	err := realBusyError(t)
	var se *sqlite.Error
	if !errors.As(err, &se) {
		t.Fatalf("expected *sqlite.Error, got %T: %v", err, err)
	}
	if !isBusy(err) {
		t.Fatalf("isBusy(real busy err code=%d) = false, want true", se.Code())
	}
}

// realBusyError deterministically produces a genuine SQLITE_BUSY/LOCKED error.
func realBusyError(t *testing.T) error {
	t.Helper()
	path := filepath.Join(t.TempDir(), "busy.db")
	dsn := "file:" + url.PathEscape(path) + "?_pragma=busy_timeout(0)"

	a, err := sql.Open("sqlite", dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = a.Close() })
	b, err := sql.Open("sqlite", dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = b.Close() })

	ctx := context.Background()
	if _, err := a.Exec(`CREATE TABLE t(x)`); err != nil {
		t.Fatal(err)
	}
	// Hold the write lock on a single pinned connection.
	tx, err := a.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback() }()
	if _, err := tx.ExecContext(ctx, `INSERT INTO t(x) VALUES (1)`); err != nil {
		t.Fatal(err)
	}
	// Competing write on the other DB with busy_timeout(0) -> immediate BUSY.
	_, got := b.ExecContext(ctx, `INSERT INTO t(x) VALUES (2)`)
	if got == nil {
		t.Skip("environment did not produce a BUSY contention; skipping positive isBusy assertion")
	}
	return got
}

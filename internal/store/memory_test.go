package store

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func sampleSpec() Spec {
	return Spec{
		Host: "h1", Template: "postgres", Slug: "demo",
		Parameters: map[string]any{"image": "postgres:16", "user": "app"},
		Secrets:    map[string]string{"password": "hunter2"},
	}
}

func TestMemory_PutGetDelete(t *testing.T) {
	ctx := context.Background()
	m := NewMemory()
	if err := m.PutSpec(ctx, sampleSpec()); err != nil {
		t.Fatalf("PutSpec: %v", err)
	}
	got, err := m.GetSpec(ctx, "h1", "postgres", "demo")
	if err != nil {
		t.Fatalf("GetSpec: %v", err)
	}
	if got.Secrets["password"] != "hunter2" || got.Parameters["image"] != "postgres:16" {
		t.Fatalf("round-trip mismatch: %+v", got)
	}
	if err := m.DeleteSpec(ctx, "h1", "postgres", "demo"); err != nil {
		t.Fatalf("DeleteSpec: %v", err)
	}
	if _, err := m.GetSpec(ctx, "h1", "postgres", "demo"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("expected ErrNotFound, got %v", err)
	}
}

func TestMemory_GetMissing(t *testing.T) {
	if _, err := NewMemory().GetSpec(context.Background(), "h1", "x", "y"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("expected ErrNotFound, got %v", err)
	}
}

func TestMemory_DeleteMissing(t *testing.T) {
	if err := NewMemory().DeleteSpec(context.Background(), "h1", "x", "y"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("expected ErrNotFound, got %v", err)
	}
}

func TestMemory_ErrorHooks(t *testing.T) {
	ctx := context.Background()
	m := NewMemory()
	putErr := errors.New("put boom")
	m.PutErr = putErr
	if err := m.PutSpec(ctx, sampleSpec()); !errors.Is(err, putErr) {
		t.Fatalf("expected PutErr, got %v", err)
	}
	m.PutErr = nil
	_ = m.PutSpec(ctx, sampleSpec())
	delErr := errors.New("del boom")
	m.DeleteErr = delErr
	if err := m.DeleteSpec(ctx, "h1", "postgres", "demo"); !errors.Is(err, delErr) {
		t.Fatalf("expected DeleteErr, got %v", err)
	}
}

func TestMemoryRenameHost(t *testing.T) {
	m := NewMemory()
	ctx := context.Background()

	require.NoError(t, m.PutSpec(ctx, Spec{Host: "h1", Template: "app", Slug: "a", Parameters: map[string]any{}}))
	require.NoError(t, m.PutHostSecret(ctx, "h1", "tok", []byte("secret")))

	require.NoError(t, m.RenameHost(ctx, "h1", "h2"))

	_, err := m.GetSpec(ctx, "h1", "app", "a")
	require.ErrorIs(t, err, ErrNotFound)
	got, err := m.GetSpec(ctx, "h2", "app", "a")
	require.NoError(t, err)
	require.Equal(t, "h2", got.Host)

	_, err = m.GetHostSecret(ctx, "h1", "tok")
	require.ErrorIs(t, err, ErrNotFound)
	val, err := m.GetHostSecret(ctx, "h2", "tok")
	require.NoError(t, err)
	require.Equal(t, []byte("secret"), val)
}

func TestMemoryRenameHostConflict(t *testing.T) {
	m := NewMemory()
	ctx := context.Background()

	require.NoError(t, m.PutSpec(ctx, Spec{Host: "h1", Template: "app", Slug: "a", Parameters: map[string]any{}}))
	require.NoError(t, m.PutSpec(ctx, Spec{Host: "h2", Template: "app", Slug: "b", Parameters: map[string]any{}}))

	require.ErrorIs(t, m.RenameHost(ctx, "h1", "h2"), ErrHostRenameConflict)
}

func TestMemoryRenameHostHasBackups(t *testing.T) {
	m := NewMemory()
	ctx := context.Background()

	require.NoError(t, m.PutSpec(ctx, Spec{Host: "h1", Template: "app", Slug: "a", Parameters: map[string]any{}}))
	require.NoError(t, m.CreateBackup(ctx, Backup{ID: "b1", Host: "h1", Template: "app", Slug: "a", State: BackupCreating, Created: time.Now()}))

	require.ErrorIs(t, m.RenameHost(ctx, "h1", "h2"), ErrHostRenameHasBackups)
}

// A host_secrets row under newID is a conflict even when newID has no specs.
// Memory used to silently overwrite it (final-review finding #5).
func TestMemoryRenameHostSecretConflict(t *testing.T) {
	m := NewMemory()
	ctx := context.Background()

	require.NoError(t, m.PutSpec(ctx, Spec{Host: "h1", Template: "app", Slug: "a", Parameters: map[string]any{}}))
	require.NoError(t, m.PutHostSecret(ctx, "h1", "tok", []byte("old-value")))
	require.NoError(t, m.PutHostSecret(ctx, "h2", "tok", []byte("new-host-value")))

	require.ErrorIs(t, m.RenameHost(ctx, "h1", "h2"), ErrHostRenameConflict)

	// Nothing moved, and h2's pre-existing secret is intact.
	val, err := m.GetHostSecret(ctx, "h2", "tok")
	require.NoError(t, err)
	require.Equal(t, []byte("new-host-value"), val)
	val, err = m.GetHostSecret(ctx, "h1", "tok")
	require.NoError(t, err)
	require.Equal(t, []byte("old-value"), val)
}

// A backups row under newID is a conflict too (full parity with SQLite).
func TestMemoryRenameHostTargetHasBackups(t *testing.T) {
	m := NewMemory()
	ctx := context.Background()

	require.NoError(t, m.PutSpec(ctx, Spec{Host: "h1", Template: "app", Slug: "a", Parameters: map[string]any{}}))
	require.NoError(t, m.CreateBackup(ctx, Backup{ID: "b1", Host: "h2", Template: "app", Slug: "b", State: BackupCreating, Created: time.Now()}))

	require.ErrorIs(t, m.RenameHost(ctx, "h1", "h2"), ErrHostRenameConflict)
}

func TestMemoryCheckHostRename(t *testing.T) {
	m := NewMemory()
	ctx := context.Background()

	require.NoError(t, m.PutSpec(ctx, Spec{Host: "h1", Template: "app", Slug: "a", Parameters: map[string]any{}}))
	require.NoError(t, m.CheckHostRename(ctx, "h1", "h2"))

	require.NoError(t, m.CreateBackup(ctx, Backup{ID: "b1", Host: "h1", Template: "app", Slug: "a", State: BackupCreating, Created: time.Now()}))
	require.ErrorIs(t, m.CheckHostRename(ctx, "h1", "h2"), ErrHostRenameHasBackups)

	// Read-only: h1's rows are still there and still under h1.
	got, err := m.GetSpec(ctx, "h1", "app", "a")
	require.NoError(t, err)
	require.Equal(t, "h1", got.Host)

	require.NoError(t, m.PutHostSecret(ctx, "h3", "tok", []byte("v")))
	require.ErrorIs(t, m.CheckHostRename(ctx, "h1", "h3"), ErrHostRenameConflict)
}

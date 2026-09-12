package store

import (
	"context"
	"encoding/json"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func backupStores(t *testing.T) map[string]BackupStore {
	t.Helper()
	sq, err := OpenSQLite(filepath.Join(t.TempDir(), "s.db"), nil)
	require.NoError(t, err)
	t.Cleanup(func() { sq.Close() })
	return map[string]BackupStore{"sqlite": sq, "memory": NewMemory()}
}

func TestBackups_CreateGetRoundTrip(t *testing.T) {
	for name, bs := range backupStores(t) {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			id := NewBackupID()
			b := Backup{
				ID:       id,
				Host:     "h1",
				Template: "pg",
				Slug:     "a",
				Image:    "postgres:16",
			}
			require.NoError(t, bs.CreateBackup(ctx, b))

			got, err := bs.GetBackup(ctx, id)
			require.NoError(t, err)
			assert.Equal(t, id, got.ID)
			assert.Equal(t, "h1", got.Host)
			assert.Equal(t, "pg", got.Template)
			assert.Equal(t, "a", got.Slug)
			assert.Equal(t, BackupCreating, got.State)
			assert.Equal(t, "postgres:16", got.Image)
			assert.False(t, got.Created.IsZero(), "Created should be non-zero")
			assert.True(t, got.Finished.IsZero(), "Finished should be zero")
		})
	}
}

// TestBackups_ModeRoundTrips guards the #135 SQLite schema addition: CreateBackup
// writes Mode into the new `mode` column and GetBackup reads it back, on both
// backends. A row that never set Mode (the pre-existing, snapshot-mode shape)
// must read back "" rather than erroring or panicking on a NULL/missing
// column — this is what every backup row before live mode existed as.
func TestBackups_ModeRoundTrips(t *testing.T) {
	for name, bs := range backupStores(t) {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()

			liveID := NewBackupID()
			require.NoError(t, bs.CreateBackup(ctx, Backup{ID: liveID, Host: "h1", Template: "sites-tpl", Slug: "s1", Mode: "live"}))
			live, err := bs.GetBackup(ctx, liveID)
			require.NoError(t, err)
			assert.Equal(t, "live", live.Mode)

			snapID := NewBackupID()
			require.NoError(t, bs.CreateBackup(ctx, Backup{ID: snapID, Host: "h1", Template: "pg", Slug: "a"}))
			snap, err := bs.GetBackup(ctx, snapID)
			require.NoError(t, err)
			assert.Equal(t, "", snap.Mode, "an unset Mode must read back as the snapshot-mode zero value")
		})
	}
}

func TestBackups_CompleteRecordsVolumesAndFinished(t *testing.T) {
	for name, bs := range backupStores(t) {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			id := NewBackupID()
			require.NoError(t, bs.CreateBackup(ctx, Backup{
				ID: id, Host: "h1", Template: "pg", Slug: "a",
			}))

			vols := []BackupVolume{{
				Name:      "pg-a-data",
				SizeBytes: 42,
				Manifest:  json.RawMessage(`{"f":{"type":48}}`),
			}}
			ok, err := bs.CompleteBackup(ctx, id, vols)
			require.NoError(t, err)
			assert.True(t, ok, "CompleteBackup should return ok=true for a creating row")

			got, err := bs.GetBackup(ctx, id)
			require.NoError(t, err)
			assert.Equal(t, BackupComplete, got.State)
			assert.False(t, got.Finished.IsZero(), "Finished should be non-zero after complete")
			require.Len(t, got.Volumes, 1)
			assert.Equal(t, "pg-a-data", got.Volumes[0].Name)
			assert.Equal(t, int64(42), got.Volumes[0].SizeBytes)
			assert.JSONEq(t, `{"f":{"type":48}}`, string(got.Volumes[0].Manifest))
		})
	}
}

func TestBackups_CompleteCAS_NoOpWhenNotCreating(t *testing.T) {
	for name, bs := range backupStores(t) {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			id := NewBackupID()
			require.NoError(t, bs.CreateBackup(ctx, Backup{
				ID: id, Host: "h1", Template: "pg", Slug: "a",
			}))

			// First, fail the backup (transitions creating → failed)
			ok, err := bs.FailBackup(ctx, id)
			require.NoError(t, err)
			assert.True(t, ok, "FailBackup should return ok=true for a creating row")

			// Now CompleteBackup should be a no-op (row is failed, not creating)
			ok, err = bs.CompleteBackup(ctx, id, []BackupVolume{{Name: "pg-a-data", SizeBytes: 1, Manifest: json.RawMessage(`{}`)}})
			require.NoError(t, err)
			assert.False(t, ok, "CompleteBackup should return ok=false when row is not creating")

			// State should still be failed
			got, err := bs.GetBackup(ctx, id)
			require.NoError(t, err)
			assert.Equal(t, BackupFailed, got.State)
		})
	}
}

func TestBackups_ListNewestFirstScopedAndLimited(t *testing.T) {
	for name, bs := range backupStores(t) {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()

			// Create 3 rows for (h1, pg, a) and 1 for (h1, pg, b).
			// NewBackupID uses time-prefixed hex, so ids are sortable by creation order.
			var aIDs []string
			for i := 0; i < 3; i++ {
				// Sleep 1ns between creates to ensure distinct nanosecond timestamps.
				if i > 0 {
					time.Sleep(time.Nanosecond)
				}
				id := NewBackupID()
				aIDs = append(aIDs, id)
				require.NoError(t, bs.CreateBackup(ctx, Backup{
					ID: id, Host: "h1", Template: "pg", Slug: "a",
				}))
			}

			// One row for a different slug — must not appear in results.
			bID := NewBackupID()
			require.NoError(t, bs.CreateBackup(ctx, Backup{
				ID: bID, Host: "h1", Template: "pg", Slug: "b",
			}))

			// ListBackups with limit=2 should return the 2 newest of (h1, pg, a).
			results, err := bs.ListBackups(ctx, "h1", "pg", "a", 2)
			require.NoError(t, err)
			assert.Len(t, results, 2)

			// Verify scoping: no "b" slug in results.
			for _, r := range results {
				assert.Equal(t, "a", r.Slug)
			}

			// Verify newest-first: the last two created aIDs should be returned,
			// with the newest first. ORDER BY created DESC, id DESC tiebreak.
			assert.Equal(t, aIDs[2], results[0].ID)
			assert.Equal(t, aIDs[1], results[1].ID)
		})
	}
}

func TestBackups_DeleteAndNotFound(t *testing.T) {
	for name, bs := range backupStores(t) {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()

			// GetBackup of missing id → ErrNotFound.
			_, err := bs.GetBackup(ctx, "bk_missing")
			assert.ErrorIs(t, err, ErrNotFound)

			// DeleteBackup of missing id → ErrNotFound.
			err = bs.DeleteBackup(ctx, "bk_missing")
			assert.ErrorIs(t, err, ErrNotFound)

			// Create then delete then GetBackup → ErrNotFound.
			id := NewBackupID()
			require.NoError(t, bs.CreateBackup(ctx, Backup{
				ID: id, Host: "h1", Template: "pg", Slug: "a",
			}))
			require.NoError(t, bs.DeleteBackup(ctx, id))

			_, err = bs.GetBackup(ctx, id)
			assert.ErrorIs(t, err, ErrNotFound)
		})
	}
}

// TestCompleteBackup_roundTripsExcluded verifies a volume's Excluded record
// (patterns applied + what they dropped) survives CompleteBackup -> GetBackup
// on every store backend. (#248)
func TestCompleteBackup_roundTripsExcluded(t *testing.T) {
	for name, bs := range backupStores(t) {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			id := NewBackupID()
			require.NoError(t, bs.CreateBackup(ctx, Backup{
				ID: id, Host: "h", Template: "t", Slug: "s", State: BackupCreating,
			}))
			vols := []BackupVolume{{
				Name: "t-s-sites", SizeBytes: 10,
				Excluded: &ExcludedPaths{Patterns: []string{"*/private/backups/**"}, Entries: 3, Bytes: 999},
			}}
			ok, err := bs.CompleteBackup(ctx, id, vols)
			require.NoError(t, err)
			require.True(t, ok)

			got, err := bs.GetBackup(ctx, id)
			require.NoError(t, err)
			require.Len(t, got.Volumes, 1)
			ex := got.Volumes[0].Excluded
			require.NotNil(t, ex, "excluded round-trip lost data")
			assert.Equal(t, 3, ex.Entries)
			assert.Equal(t, int64(999), ex.Bytes)
			assert.Equal(t, []string{"*/private/backups/**"}, ex.Patterns)
		})
	}
}

// TestCompleteBackup_omitsExcludedWhenNil pins the nil case: a volume that
// declared no exclude patterns must serialize with no "excluded" key at all,
// so existing rows/wire payloads are unaffected. (#248)
func TestCompleteBackup_omitsExcludedWhenNil(t *testing.T) {
	b, err := json.Marshal(BackupVolume{Name: "v", SizeBytes: 1})
	require.NoError(t, err)
	assert.NotContains(t, string(b), "excluded", "unfiltered volume must not carry an excluded key: %s", b)

	if !strings.Contains(string(b), `"name":"v"`) {
		t.Fatalf("sanity check failed, marshal shape unexpected: %s", b)
	}
}

func TestBackups_CreateDuplicateIDErrors(t *testing.T) {
	for name, bs := range backupStores(t) {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			id := NewBackupID()
			b := Backup{ID: id, Host: "h1", Template: "pg", Slug: "a"}

			require.NoError(t, bs.CreateBackup(ctx, b))
			err := bs.CreateBackup(ctx, b)
			require.Error(t, err, "second CreateBackup with the same ID must error")
		})
	}
}

func TestBackups_SQLitePersistsAcrossReopen(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "s.db")

	sq, err := OpenSQLite(dbPath, nil)
	require.NoError(t, err)

	id := NewBackupID()
	require.NoError(t, sq.CreateBackup(ctx, Backup{
		ID: id, Host: "h1", Template: "pg", Slug: "a", Image: "postgres:16",
	}))
	require.NoError(t, sq.Close())

	// Reopen and verify the row persisted.
	sq2, err := OpenSQLite(dbPath, nil)
	require.NoError(t, err)
	t.Cleanup(func() { sq2.Close() })

	got, err := sq2.GetBackup(ctx, id)
	require.NoError(t, err)
	assert.Equal(t, id, got.ID)
	assert.Equal(t, "postgres:16", got.Image)
	assert.Equal(t, BackupCreating, got.State)
}

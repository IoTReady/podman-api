package store

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	_ "modernc.org/sqlite"

	"github.com/iotready/podman-api/internal/render"
)

func openTestStore(t *testing.T, ks *KeyStore) *SQLite {
	t.Helper()
	db := filepath.Join(t.TempDir(), "state.db")
	s, err := OpenSQLite(db, ks)
	if err != nil {
		t.Fatalf("OpenSQLite: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}

func TestSQLite_PutGet_RoundTrip(t *testing.T) {
	ctx := context.Background()
	s := openTestStore(t, NewKeyStore(testKey(0x11)))
	if err := s.PutSpec(ctx, sampleSpec()); err != nil {
		t.Fatalf("PutSpec: %v", err)
	}
	got, err := s.GetSpec(ctx, "h1", "postgres", "demo")
	if err != nil {
		t.Fatalf("GetSpec: %v", err)
	}
	if got.Secrets["password"] != "hunter2" {
		t.Fatalf("secret not decrypted: %+v", got.Secrets)
	}
	if got.Parameters["user"] != "app" {
		t.Fatalf("parameter mismatch: %+v", got.Parameters)
	}
	if got.Created.IsZero() || got.Updated.IsZero() {
		t.Fatal("timestamps not set")
	}
}

func TestSQLite_GetMissing(t *testing.T) {
	s := openTestStore(t, NewKeyStore(testKey(0x11)))
	if _, err := s.GetSpec(context.Background(), "h1", "x", "y"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("expected ErrNotFound, got %v", err)
	}
}

func TestSQLite_Delete(t *testing.T) {
	ctx := context.Background()
	s := openTestStore(t, NewKeyStore(testKey(0x11)))
	_ = s.PutSpec(ctx, sampleSpec())
	if err := s.DeleteSpec(ctx, "h1", "postgres", "demo"); err != nil {
		t.Fatalf("DeleteSpec: %v", err)
	}
	if _, err := s.GetSpec(ctx, "h1", "postgres", "demo"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("expected ErrNotFound after delete, got %v", err)
	}
	if err := s.DeleteSpec(ctx, "h1", "postgres", "demo"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("delete of absent row should return ErrNotFound, got %v", err)
	}
}

func TestSQLite_Upsert_PreservesCreated(t *testing.T) {
	ctx := context.Background()
	s := openTestStore(t, NewKeyStore(testKey(0x11)))
	_ = s.PutSpec(ctx, sampleSpec())
	first, _ := s.GetSpec(ctx, "h1", "postgres", "demo")

	// Re-put with changed secret/param (rotation).
	sp := sampleSpec()
	sp.Secrets["password"] = "rotated"
	sp.Parameters["user"] = "admin"
	if err := s.PutSpec(ctx, sp); err != nil {
		t.Fatalf("re-PutSpec: %v", err)
	}
	second, _ := s.GetSpec(ctx, "h1", "postgres", "demo")

	if !second.Created.Equal(first.Created) {
		t.Fatalf("created changed on upsert: %v -> %v", first.Created, second.Created)
	}
	if second.Secrets["password"] != "rotated" || second.Parameters["user"] != "admin" {
		t.Fatalf("upsert did not overwrite payload: %+v", second)
	}
	if second.Updated.Before(first.Updated) {
		t.Fatal("updated went backwards on upsert")
	}
}

func TestSQLite_WrongKey_FailsDecrypt(t *testing.T) {
	ctx := context.Background()
	ks := NewKeyStore(testKey(0x11))
	s := openTestStore(t, ks)
	_ = s.PutSpec(ctx, sampleSpec())
	ks.Store(testKey(0x22)) // rotate to the wrong key
	if _, err := s.GetSpec(ctx, "h1", "postgres", "demo"); err == nil {
		t.Fatal("GetSpec with wrong key should fail, not panic")
	}
}

func TestSQLite_EncryptsSecretsAtRest(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	db := filepath.Join(dir, "state.db")
	s, err := OpenSQLite(db, NewKeyStore(testKey(0x11)))
	if err != nil {
		t.Fatalf("OpenSQLite: %v", err)
	}
	if err := s.PutSpec(ctx, sampleSpec()); err != nil {
		t.Fatalf("PutSpec: %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range entries {
		raw, err := os.ReadFile(filepath.Join(dir, e.Name()))
		if err != nil {
			t.Fatal(err)
		}
		if bytes.Contains(raw, []byte("hunter2")) {
			t.Fatalf("secret value found in plaintext in %s", e.Name())
		}
	}
}

func TestSpecDomainsRoundTrip(t *testing.T) {
	s := openTestStore(t, NewKeyStore(testKey(0x11)))
	ctx := context.Background()
	in := Spec{
		Host: "h1", Template: "web", Slug: "a",
		Parameters: map[string]any{},
		Secrets:    map[string]string{},
		Domains:    []string{"a.example.com", "b.example.com"},
	}
	require.NoError(t, s.PutSpec(ctx, in))
	got, err := s.GetSpec(ctx, "h1", "web", "a")
	require.NoError(t, err)
	require.Equal(t, []string{"a.example.com", "b.example.com"}, got.Domains)
}

func TestSpecDomainsDefaultsEmpty(t *testing.T) {
	s := openTestStore(t, NewKeyStore(testKey(0x11)))
	ctx := context.Background()
	require.NoError(t, s.PutSpec(ctx, Spec{
		Host: "h1", Template: "pg", Slug: "a",
		Parameters: map[string]any{}, Secrets: map[string]string{},
	}))
	got, err := s.GetSpec(ctx, "h1", "pg", "a")
	require.NoError(t, err)
	require.Empty(t, got.Domains)
}

func TestMigrateAddsDomainsColumn(t *testing.T) {
	dir := t.TempDir()
	path := dir + "/old.db"
	raw, err := sql.Open("sqlite", "file:"+path)
	require.NoError(t, err)
	_, err = raw.Exec(`CREATE TABLE specs (
  host TEXT NOT NULL, template TEXT NOT NULL, slug TEXT NOT NULL,
  parameters TEXT NOT NULL, secrets BLOB NOT NULL,
  created INTEGER NOT NULL, updated INTEGER NOT NULL,
  PRIMARY KEY (host, template, slug));`)
	require.NoError(t, err)
	_, err = raw.Exec(`PRAGMA user_version = 3`)
	require.NoError(t, err)
	require.NoError(t, raw.Close())

	keys := NewKeyStore(testKey(0x11))
	s, err := OpenSQLite(path, keys)
	require.NoError(t, err)
	t.Cleanup(func() { _ = s.Close() })

	ctx := context.Background()
	require.NoError(t, s.PutSpec(ctx, Spec{
		Host: "h", Template: "web", Slug: "x",
		Parameters: map[string]any{}, Secrets: map[string]string{},
		Domains: []string{"x.example.com"},
	}))
	got, err := s.GetSpec(ctx, "h", "web", "x")
	require.NoError(t, err)
	require.Equal(t, []string{"x.example.com"}, got.Domains)
}

func TestSQLite_GetSpec_WrongKey_IsErrSecretsUndecryptable(t *testing.T) {
	ctx := context.Background()
	ks := NewKeyStore(testKey(0x11))
	s := openTestStore(t, ks)
	require.NoError(t, s.PutSpec(ctx, sampleSpec()))
	ks.Store(testKey(0x22)) // rotate to the wrong key → decrypt fails
	_, err := s.GetSpec(ctx, "h1", "postgres", "demo")
	// A wrong/missing key is recoverable by a restart with the correct key — it is
	// NOT the permanent-corruption ErrSpecCorrupt.
	require.ErrorIs(t, err, ErrSecretsUndecryptable)
	require.NotErrorIs(t, err, ErrSpecCorrupt)
}

func TestSQLite_GetSpec_CorruptParamsJSON_IsErrSpecCorrupt(t *testing.T) {
	ctx := context.Background()
	s := openTestStore(t, NewKeyStore(testKey(0x11)))
	require.NoError(t, s.PutSpec(ctx, sampleSpec()))
	_, err := s.db.ExecContext(ctx,
		`UPDATE specs SET parameters='{not json' WHERE host='h1' AND template='postgres' AND slug='demo'`)
	require.NoError(t, err)
	_, err = s.GetSpec(ctx, "h1", "postgres", "demo")
	require.ErrorIs(t, err, ErrSpecCorrupt)
}

func TestSQLite_GetSpec_CorruptDomainsJSON_IsErrSpecCorrupt(t *testing.T) {
	ctx := context.Background()
	s := openTestStore(t, NewKeyStore(testKey(0x11)))
	require.NoError(t, s.PutSpec(ctx, sampleSpec()))
	_, err := s.db.ExecContext(ctx,
		`UPDATE specs SET domains='{not json' WHERE host='h1' AND template='postgres' AND slug='demo'`)
	require.NoError(t, err)
	_, err = s.GetSpec(ctx, "h1", "postgres", "demo")
	require.ErrorIs(t, err, ErrSpecCorrupt)
}

func TestSQLite_GetSpec_NotFound_IsNotErrSpecCorrupt(t *testing.T) {
	s := openTestStore(t, NewKeyStore(testKey(0x11)))
	_, err := s.GetSpec(context.Background(), "h1", "x", "y")
	require.ErrorIs(t, err, ErrNotFound)
	require.NotErrorIs(t, err, ErrSpecCorrupt)
}

// TestSQLite_GetSpec_CorruptSecretsJSON_IsErrSpecCorrupt covers the one wrapped
// path that touches decrypted plaintext: a blob that decrypts cleanly under the
// live key but yields non-JSON. Sealing "{bad" under the keystore's key exercises
// the post-decrypt json.Unmarshal failure that the other corruption tests can't.
func TestSQLite_GetSpec_CorruptSecretsJSON_IsErrSpecCorrupt(t *testing.T) {
	ctx := context.Background()
	ks := NewKeyStore(testKey(0x11))
	s := openTestStore(t, ks)
	require.NoError(t, s.PutSpec(ctx, sampleSpec()))
	blob, err := seal(ks.Load(), []byte("{bad"))
	require.NoError(t, err)
	_, err = s.db.ExecContext(ctx,
		`UPDATE specs SET secrets=? WHERE host='h1' AND template='postgres' AND slug='demo'`, blob)
	require.NoError(t, err)
	_, err = s.GetSpec(ctx, "h1", "postgres", "demo")
	require.ErrorIs(t, err, ErrSpecCorrupt)
}

func TestSQLite_InjectorSecrets_RoundTrip(t *testing.T) {
	ctx := context.Background()
	s := openTestStore(t, NewKeyStore(testKey(0x11)))
	sp := sampleSpec()
	sp.InjectorSecrets = []InjectorSecret{
		{Name: "litestream-s3-key", Key: "access-key-id", Value: "s3-secret-value"},
	}
	require.NoError(t, s.PutSpec(ctx, sp))

	got, err := s.GetSpec(ctx, "h1", "postgres", "demo")
	require.NoError(t, err)
	require.Len(t, got.InjectorSecrets, 1)
	require.Equal(t, "litestream-s3-key", got.InjectorSecrets[0].Name)
	require.Equal(t, "access-key-id", got.InjectorSecrets[0].Key)
	require.Equal(t, "s3-secret-value", got.InjectorSecrets[0].Value)
}

func TestSQLite_InjectorSecrets_EncryptedAtRest(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	db := filepath.Join(dir, "state.db")
	s, err := OpenSQLite(db, NewKeyStore(testKey(0x11)))
	require.NoError(t, err)
	sp := sampleSpec()
	sp.InjectorSecrets = []InjectorSecret{
		{Name: "litestream-s3-key", Key: "access-key-id", Value: "s3-secret-value"},
	}
	require.NoError(t, s.PutSpec(ctx, sp))
	require.NoError(t, s.Close())

	entries, err := os.ReadDir(dir)
	require.NoError(t, err)
	for _, e := range entries {
		raw, err := os.ReadFile(filepath.Join(dir, e.Name()))
		require.NoError(t, err)
		require.NotContains(t, string(raw), "s3-secret-value", "plaintext must not appear in the raw DB")
		require.NotContains(t, string(raw), "access-key-id", "plaintext must not appear in the raw DB")
		require.NotContains(t, string(raw), "litestream-s3-key", "plaintext must not appear in the raw DB")
	}
}

// TestSQLite_AppliedVolumes_RoundTrip (#257): a known, non-empty applied set
// round-trips exactly.
func TestSQLite_AppliedVolumes_RoundTrip(t *testing.T) {
	ctx := context.Background()
	s := openTestStore(t, NewKeyStore(testKey(0x11)))
	sp := sampleSpec()
	sp.AppliedVolumes = []string{"data", "logs"}
	require.NoError(t, s.PutSpec(ctx, sp))

	got, err := s.GetSpec(ctx, "h1", "postgres", "demo")
	require.NoError(t, err)
	require.Equal(t, []string{"data", "logs"}, got.AppliedVolumes)
}

// TestSQLite_AppliedVolumes_KnownEmptyRoundTripsNonNil (#257): a spec applied
// against a template declaring no volumes must read back as a known, EMPTY set
// ([]string{}), not as nil (unknown) — those two mean different things to
// CheckBackupable.
func TestSQLite_AppliedVolumes_KnownEmptyRoundTripsNonNil(t *testing.T) {
	ctx := context.Background()
	s := openTestStore(t, NewKeyStore(testKey(0x11)))
	sp := sampleSpec()
	sp.AppliedVolumes = []string{}
	require.NoError(t, s.PutSpec(ctx, sp))

	got, err := s.GetSpec(ctx, "h1", "postgres", "demo")
	require.NoError(t, err)
	require.NotNil(t, got.AppliedVolumes)
	require.Empty(t, got.AppliedVolumes)
}

// TestSQLite_AppliedVolumes_UnsetIsNil (#257): a spec never given an applied
// set (the zero value, as every caller before #257 would produce) must read
// back nil (unknown), not an empty slice — the two are not interchangeable.
func TestSQLite_AppliedVolumes_UnsetIsNil(t *testing.T) {
	ctx := context.Background()
	s := openTestStore(t, NewKeyStore(testKey(0x11)))
	require.NoError(t, s.PutSpec(ctx, sampleSpec())) // AppliedVolumes left as the zero value (nil)

	got, err := s.GetSpec(ctx, "h1", "postgres", "demo")
	require.NoError(t, err)
	require.Nil(t, got.AppliedVolumes)
}

// TestSQLite_AppliedVolumeMeta_RoundTrip (#256 review round-4, blocking
// finding): a known, non-empty applied-volume-meta map round-trips exactly,
// including a `backup: none` marker and exclude patterns — the two pieces of
// information a renamed-but-not-reapplied volume would otherwise silently
// lose.
func TestSQLite_AppliedVolumeMeta_RoundTrip(t *testing.T) {
	ctx := context.Background()
	s := openTestStore(t, NewKeyStore(testKey(0x11)))
	sp := sampleSpec()
	sp.AppliedVolumes = []string{"data", "cache"}
	sp.AppliedVolumeMeta = map[string]AppliedVolumeMarker{
		"data":  {Backup: "s3; interval=24h", Exclude: []string{"*.tmp"}},
		"cache": {Backup: "none"},
	}
	require.NoError(t, s.PutSpec(ctx, sp))

	got, err := s.GetSpec(ctx, "h1", "postgres", "demo")
	require.NoError(t, err)
	require.Equal(t, sp.AppliedVolumeMeta, got.AppliedVolumeMeta)
}

// TestSQLite_AppliedVolumeMeta_UnsetIsNil mirrors
// TestSQLite_AppliedVolumes_UnsetIsNil: a spec never given applied-volume
// metadata (every caller before this field existed) must read back nil
// (unknown), not an empty map.
func TestSQLite_AppliedVolumeMeta_UnsetIsNil(t *testing.T) {
	ctx := context.Background()
	s := openTestStore(t, NewKeyStore(testKey(0x11)))
	require.NoError(t, s.PutSpec(ctx, sampleSpec()))

	got, err := s.GetSpec(ctx, "h1", "postgres", "demo")
	require.NoError(t, err)
	require.Nil(t, got.AppliedVolumeMeta)
}

// TestMigrateAddsAppliedVolumeMetaColumn (#256 review round-4): a pre-v10 DB
// has no applied_volume_meta column; migrating it in must backfill existing
// rows with NULL (unknown), and the column must then work for new writes.
func TestMigrateAddsAppliedVolumeMetaColumn(t *testing.T) {
	dir := t.TempDir()
	path := dir + "/old.db"
	raw, err := sql.Open("sqlite", "file:"+path)
	require.NoError(t, err)
	_, err = raw.Exec(`CREATE TABLE specs (
  host TEXT NOT NULL, template TEXT NOT NULL, slug TEXT NOT NULL,
  parameters TEXT NOT NULL, secrets BLOB, injector_secrets BLOB,
  domains TEXT NOT NULL DEFAULT '[]', applied_volumes TEXT,
  created INTEGER NOT NULL, updated INTEGER NOT NULL,
  PRIMARY KEY (host, template, slug));`)
	require.NoError(t, err)
	_, err = raw.Exec(`INSERT INTO specs (host, template, slug, parameters, domains, created, updated)
VALUES ('h', 'pre-existing', 'x', '{}', '[]', 0, 0)`)
	require.NoError(t, err)
	_, err = raw.Exec(`PRAGMA user_version = 9`)
	require.NoError(t, err)
	require.NoError(t, raw.Close())

	keys := NewKeyStore(testKey(0x11))
	s, err := OpenSQLite(path, keys)
	require.NoError(t, err)
	t.Cleanup(func() { _ = s.Close() })

	ctx := context.Background()

	got, err := s.GetSpec(ctx, "h", "pre-existing", "x")
	require.NoError(t, err)
	require.Nil(t, got.AppliedVolumeMeta)

	require.NoError(t, s.PutSpec(ctx, Spec{
		Host: "h", Template: "web", Slug: "x",
		Parameters: map[string]any{}, Secrets: map[string]string{},
		AppliedVolumes:    []string{"data"},
		AppliedVolumeMeta: map[string]AppliedVolumeMarker{"data": {Backup: "none"}},
	}))
	got, err = s.GetSpec(ctx, "h", "web", "x")
	require.NoError(t, err)
	require.Equal(t, map[string]AppliedVolumeMarker{"data": {Backup: "none"}}, got.AppliedVolumeMeta)
}

// TestMigrateAddsAppliedVolumesColumn (#257): a pre-v9 DB has no
// applied_volumes column; migrating it in must backfill existing rows with
// NULL (unknown), and the column must then work for new writes.
func TestMigrateAddsAppliedVolumesColumn(t *testing.T) {
	dir := t.TempDir()
	path := dir + "/old.db"
	raw, err := sql.Open("sqlite", "file:"+path)
	require.NoError(t, err)
	_, err = raw.Exec(`CREATE TABLE specs (
  host TEXT NOT NULL, template TEXT NOT NULL, slug TEXT NOT NULL,
  parameters TEXT NOT NULL, secrets BLOB, injector_secrets BLOB,
  domains TEXT NOT NULL DEFAULT '[]',
  created INTEGER NOT NULL, updated INTEGER NOT NULL,
  PRIMARY KEY (host, template, slug));`)
	require.NoError(t, err)
	_, err = raw.Exec(`INSERT INTO specs (host, template, slug, parameters, domains, created, updated)
VALUES ('h', 'pre-existing', 'x', '{}', '[]', 0, 0)`)
	require.NoError(t, err)
	_, err = raw.Exec(`PRAGMA user_version = 8`)
	require.NoError(t, err)
	require.NoError(t, raw.Close())

	keys := NewKeyStore(testKey(0x11))
	s, err := OpenSQLite(path, keys)
	require.NoError(t, err)
	t.Cleanup(func() { _ = s.Close() })

	ctx := context.Background()

	// The pre-existing row, never touched by this migration's writer, must
	// read back as unknown.
	got, err := s.GetSpec(ctx, "h", "pre-existing", "x")
	require.NoError(t, err)
	require.Nil(t, got.AppliedVolumes)

	// A fresh write on the migrated DB round-trips normally.
	require.NoError(t, s.PutSpec(ctx, Spec{
		Host: "h", Template: "web", Slug: "x",
		Parameters: map[string]any{}, Secrets: map[string]string{},
		AppliedVolumes: []string{"data"},
	}))
	got, err = s.GetSpec(ctx, "h", "web", "x")
	require.NoError(t, err)
	require.Equal(t, []string{"data"}, got.AppliedVolumes)
}

func TestSQLiteRenameHost(t *testing.T) {
	s := openTestStore(t, NewKeyStore(testKey(0x11)))
	ctx := context.Background()

	require.NoError(t, s.PutSpec(ctx, Spec{Host: "h1", Template: "app", Slug: "a", Parameters: map[string]any{}, Domains: []string{}}))
	require.NoError(t, s.PutHostSecret(ctx, "h1", "tok", []byte("secret")))

	require.NoError(t, s.RenameHost(ctx, "h1", "h2"))

	_, err := s.GetSpec(ctx, "h1", "app", "a")
	require.ErrorIs(t, err, ErrNotFound)
	got, err := s.GetSpec(ctx, "h2", "app", "a")
	require.NoError(t, err)
	require.Equal(t, "h2", got.Host)

	_, err = s.GetHostSecret(ctx, "h1", "tok")
	require.ErrorIs(t, err, ErrNotFound)
	val, err := s.GetHostSecret(ctx, "h2", "tok")
	require.NoError(t, err)
	require.Equal(t, []byte("secret"), val)
}

func TestSQLiteRenameHostConflict(t *testing.T) {
	s := openTestStore(t, NewKeyStore(testKey(0x11)))
	ctx := context.Background()

	require.NoError(t, s.PutSpec(ctx, Spec{Host: "h1", Template: "app", Slug: "a", Parameters: map[string]any{}, Domains: []string{}}))
	require.NoError(t, s.PutSpec(ctx, Spec{Host: "h2", Template: "app", Slug: "b", Parameters: map[string]any{}, Domains: []string{}}))

	err := s.RenameHost(ctx, "h1", "h2")
	require.ErrorIs(t, err, ErrHostRenameConflict)

	// h1's row must be untouched.
	_, err = s.GetSpec(ctx, "h1", "app", "a")
	require.NoError(t, err)
}

func TestSQLiteRenameHostHasBackups(t *testing.T) {
	s := openTestStore(t, NewKeyStore(testKey(0x11)))
	ctx := context.Background()

	require.NoError(t, s.PutSpec(ctx, Spec{Host: "h1", Template: "app", Slug: "a", Parameters: map[string]any{}, Domains: []string{}}))
	require.NoError(t, s.CreateBackup(ctx, Backup{ID: "b1", Host: "h1", Template: "app", Slug: "a", State: BackupCreating, Created: time.Now()}))

	err := s.RenameHost(ctx, "h1", "h2")
	require.ErrorIs(t, err, ErrHostRenameHasBackups)

	_, err = s.GetSpec(ctx, "h1", "app", "a")
	require.NoError(t, err) // untouched
}

// A host_secrets row under newID is a conflict even when newID has no specs
// (final-review finding #5 — SQLite used to surface this as an opaque PK
// violation, and Memory silently overwrote it).
func TestSQLiteRenameHostSecretConflict(t *testing.T) {
	s := openTestStore(t, NewKeyStore(testKey(0x11)))
	ctx := context.Background()

	require.NoError(t, s.PutSpec(ctx, Spec{Host: "h1", Template: "app", Slug: "a", Parameters: map[string]any{}, Domains: []string{}}))
	require.NoError(t, s.PutHostSecret(ctx, "h1", "tok", []byte("old-value")))
	require.NoError(t, s.PutHostSecret(ctx, "h2", "tok", []byte("new-host-value")))

	require.ErrorIs(t, s.RenameHost(ctx, "h1", "h2"), ErrHostRenameConflict)

	val, err := s.GetHostSecret(ctx, "h2", "tok")
	require.NoError(t, err)
	require.Equal(t, []byte("new-host-value"), val)
	val, err = s.GetHostSecret(ctx, "h1", "tok")
	require.NoError(t, err)
	require.Equal(t, []byte("old-value"), val)
}

func TestSQLiteRenameHostTargetHasBackups(t *testing.T) {
	s := openTestStore(t, NewKeyStore(testKey(0x11)))
	ctx := context.Background()

	require.NoError(t, s.PutSpec(ctx, Spec{Host: "h1", Template: "app", Slug: "a", Parameters: map[string]any{}, Domains: []string{}}))
	require.NoError(t, s.CreateBackup(ctx, Backup{ID: "b1", Host: "h2", Template: "app", Slug: "b", State: BackupCreating, Created: time.Now()}))

	require.ErrorIs(t, s.RenameHost(ctx, "h1", "h2"), ErrHostRenameConflict)
}

func TestSQLiteCheckHostRename(t *testing.T) {
	s := openTestStore(t, NewKeyStore(testKey(0x11)))
	ctx := context.Background()

	require.NoError(t, s.PutSpec(ctx, Spec{Host: "h1", Template: "app", Slug: "a", Parameters: map[string]any{}, Domains: []string{}}))
	require.NoError(t, s.CheckHostRename(ctx, "h1", "h2"))

	require.NoError(t, s.CreateBackup(ctx, Backup{ID: "b1", Host: "h1", Template: "app", Slug: "a", State: BackupCreating, Created: time.Now()}))
	require.ErrorIs(t, s.CheckHostRename(ctx, "h1", "h2"), ErrHostRenameHasBackups)

	// Read-only: nothing moved.
	got, err := s.GetSpec(ctx, "h1", "app", "a")
	require.NoError(t, err)
	require.Equal(t, "h1", got.Host)

	require.NoError(t, s.PutHostSecret(ctx, "h3", "tok", []byte("v")))
	require.ErrorIs(t, s.CheckHostRename(ctx, "h1", "h3"), ErrHostRenameConflict)
}

// TestMigrateAddsAppliedNetworksColumn (#270): a pre-v11 DB has no
// applied_networks column; migrating it in must leave existing rows with no
// per-instance networks, and the column must then work for new writes.
func TestMigrateAddsAppliedNetworksColumn(t *testing.T) {
	dir := t.TempDir()
	path := dir + "/old.db"
	raw, err := sql.Open("sqlite", "file:"+path)
	require.NoError(t, err)
	_, err = raw.Exec(`CREATE TABLE specs (
  host TEXT NOT NULL, template TEXT NOT NULL, slug TEXT NOT NULL,
  parameters TEXT NOT NULL, secrets BLOB, injector_secrets BLOB,
  domains TEXT NOT NULL DEFAULT '[]', applied_volumes TEXT,
  applied_volume_meta TEXT,
  created INTEGER NOT NULL, updated INTEGER NOT NULL,
  PRIMARY KEY (host, template, slug));`)
	require.NoError(t, err)
	_, err = raw.Exec(`INSERT INTO specs (host, template, slug, parameters, domains, created, updated)
VALUES ('h', 'pre-existing', 'x', '{}', '[]', 0, 0)`)
	require.NoError(t, err)
	_, err = raw.Exec(`PRAGMA user_version = 10`)
	require.NoError(t, err)
	require.NoError(t, raw.Close())

	keys := NewKeyStore(testKey(0x11))
	s, err := OpenSQLite(path, keys)
	require.NoError(t, err)
	t.Cleanup(func() { _ = s.Close() })

	ctx := context.Background()

	got, err := s.GetSpec(ctx, "h", "pre-existing", "x")
	require.NoError(t, err)
	require.Nil(t, got.AppliedNetworks)

	nets := []render.Network{{Name: "customer-a", Aliases: []string{"db"}}}
	require.NoError(t, s.PutSpec(ctx, Spec{
		Host: "h", Template: "web", Slug: "x",
		Parameters: map[string]any{}, Secrets: map[string]string{},
		AppliedNetworks: nets,
	}))
	got, err = s.GetSpec(ctx, "h", "web", "x")
	require.NoError(t, err)
	require.Equal(t, nets, got.AppliedNetworks)
}

// ListSpecNetworks is the projection the apply-time DNS uniqueness check reads
// (#270): it must carry each instance's own networks, and — unlike GetSpec —
// must work on a key-less open, since applied_networks is plaintext.
func TestSQLite_ListSpecNetworks(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	path := dir + "/s.db"
	s, err := OpenSQLite(path, NewKeyStore(testKey(0x11)))
	require.NoError(t, err)
	nets := []render.Network{{Name: "customer-a", Aliases: []string{"db"}}}
	require.NoError(t, s.PutSpec(ctx, Spec{
		Host: "h1", Template: "web", Slug: "a",
		Parameters: map[string]any{}, Secrets: map[string]string{"p": "v"},
		AppliedNetworks: nets,
	}))
	require.NoError(t, s.PutSpec(ctx, Spec{
		Host: "h1", Template: "web", Slug: "b", Parameters: map[string]any{},
	}))
	require.NoError(t, s.Close())

	keyless, err := OpenSQLite(path, nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = keyless.Close() })
	got, err := keyless.ListSpecNetworks(ctx, "h1")
	require.NoError(t, err)
	require.Equal(t, []SpecNetworks{
		{SpecKey: SpecKey{Template: "web", Slug: "a"}, Networks: nets},
		{SpecKey: SpecKey{Template: "web", Slug: "b"}},
	}, got)
}

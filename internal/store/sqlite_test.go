package store

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	_ "modernc.org/sqlite"
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

func TestSQLite_GetSpec_WrongKey_IsErrSpecCorrupt(t *testing.T) {
	ctx := context.Background()
	ks := NewKeyStore(testKey(0x11))
	s := openTestStore(t, ks)
	require.NoError(t, s.PutSpec(ctx, sampleSpec()))
	ks.Store(testKey(0x22)) // rotate to the wrong key → decrypt fails
	_, err := s.GetSpec(ctx, "h1", "postgres", "demo")
	require.ErrorIs(t, err, ErrSpecCorrupt)
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

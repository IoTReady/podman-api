package store

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
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

package auth_test

import (
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/iotready/podman-api/internal/auth"
	"github.com/iotready/podman-api/internal/config"
)

func newTestManager(t *testing.T) (*auth.TokenManager, string) {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "keys.yaml")
	// Start with one pre-existing key so List works on a non-empty file.
	hash, err := config.HashToken("existingsecret")
	if err != nil {
		t.Fatal(err)
	}
	content := "keys:\n  - id: existing\n    secret_hash: " + hash + "\n    scopes: [instances:read]\n"
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	store := auth.NewKeyStore(nil)
	return auth.NewTokenManager(path, store), path
}

func TestTokenManagerList(t *testing.T) {
	mgr, _ := newTestManager(t)
	keys, err := mgr.List()
	if err != nil {
		t.Fatal(err)
	}
	if len(keys) != 1 || keys[0].ID != "existing" {
		t.Fatalf("want [existing], got %v", keys)
	}
	// SecretHash must be redacted.
	if keys[0].SecretHash != "" {
		t.Error("SecretHash should be empty in List output")
	}
}

func TestTokenManagerCreate(t *testing.T) {
	mgr, path := newTestManager(t)

	plain, err := mgr.Create("ci", "CI deploy token", []string{"instances:write"})
	if err != nil {
		t.Fatal(err)
	}
	if plain == "" {
		t.Fatal("expected non-empty plaintext token")
	}

	// List must include the new key.
	keys, err := mgr.List()
	if err != nil {
		t.Fatal(err)
	}
	ids := make([]string, len(keys))
	for i, k := range keys {
		ids[i] = k.ID
	}
	if !slices.Contains(ids, "ci") {
		t.Fatalf("new key missing from List; got %v", ids)
	}

	// The live KeyStore must accept the plaintext token.
	store := mgr.Store()
	found := false
	for _, k := range store.Load() {
		if k.ID == "ci" {
			ok, err := config.VerifyToken(plain, k.SecretHash)
			if err != nil || !ok {
				t.Errorf("token verify failed: ok=%v err=%v", ok, err)
			}
			found = true
			break
		}
	}
	if !found {
		t.Error("new key not reloaded into KeyStore")
	}

	// The file on disk must contain the new entry.
	raw, _ := os.ReadFile(path)
	if !strings.Contains(string(raw), "ci") {
		t.Error("new key not written to keys.yaml")
	}
}

func TestTokenManagerCreateDuplicateID(t *testing.T) {
	mgr, _ := newTestManager(t)
	if _, err := mgr.Create("existing", "dup", []string{"instances:read"}); err == nil {
		t.Error("want error for duplicate ID")
	}
}

func TestTokenManagerCreateInvalidID(t *testing.T) {
	mgr, _ := newTestManager(t)
	for _, bad := range []string{"ci/prod", "a b", "a@b", "a.b"} {
		if _, err := mgr.Create(bad, "bad", []string{"instances:read"}); err == nil {
			t.Errorf("want error for id %q", bad)
		}
	}
}

func TestTokenManagerCreateEmptyScopes(t *testing.T) {
	mgr, _ := newTestManager(t)
	if _, err := mgr.Create("noscopes", "no scopes", nil); err == nil {
		t.Error("want error for empty scopes")
	}
	if _, err := mgr.Create("noscopes2", "no scopes", []string{}); err == nil {
		t.Error("want error for empty scopes slice")
	}
}

func TestTokenManagerRevoke(t *testing.T) {
	mgr, path := newTestManager(t)
	// Create then revoke.
	if _, err := mgr.Create("temp", "temp token", []string{"hosts:read"}); err != nil {
		t.Fatal(err)
	}
	if err := mgr.Revoke("temp"); err != nil {
		t.Fatal(err)
	}
	keys, _ := mgr.List()
	for _, k := range keys {
		if k.ID == "temp" {
			t.Error("revoked key still present in List")
		}
	}
	// Must be gone from KeyStore.
	for _, k := range mgr.Store().Load() {
		if k.ID == "temp" {
			t.Error("revoked key still in KeyStore")
		}
	}
	// Must be gone from file.
	raw, _ := os.ReadFile(path)
	if strings.Contains(string(raw), "temp") {
		t.Error("revoked key still in keys.yaml")
	}
}

func TestTokenManagerRevokeNotFound(t *testing.T) {
	mgr, _ := newTestManager(t)
	if err := mgr.Revoke("nonexistent"); err == nil {
		t.Error("want error revoking nonexistent ID")
	}
}

func TestTokenManagerRevokeLastTokenBlocked(t *testing.T) {
	mgr, _ := newTestManager(t)
	// "existing" is the only token; revoking it must be rejected.
	if err := mgr.Revoke("existing"); err == nil {
		t.Error("want error when revoking the last token")
	}
	// Token must still be in the store.
	found := false
	for _, k := range mgr.Store().Load() {
		if k.ID == "existing" {
			found = true
		}
	}
	if !found {
		t.Error("last token was removed from KeyStore despite error")
	}
}

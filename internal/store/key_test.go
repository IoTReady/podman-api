package store

import (
	"encoding/base64"
	"os"
	"path/filepath"
	"testing"
)

func writeFile(t *testing.T, name string, data []byte) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), name)
	if err := os.WriteFile(p, data, 0o600); err != nil {
		t.Fatalf("write %s: %v", name, err)
	}
	return p
}

func TestLoadKeyFile_Raw32(t *testing.T) {
	raw := make([]byte, 32)
	for i := range raw {
		raw[i] = byte(i)
	}
	k, err := LoadKeyFile(writeFile(t, "raw.key", raw))
	if err != nil {
		t.Fatalf("LoadKeyFile: %v", err)
	}
	if k != *(*[32]byte)(raw) {
		t.Fatal("raw key mismatch")
	}
}

func TestLoadKeyFile_Base64WithNewline(t *testing.T) {
	raw := make([]byte, 32)
	for i := range raw {
		raw[i] = byte(0xA0 + i)
	}
	enc := base64.StdEncoding.EncodeToString(raw) + "\n"
	k, err := LoadKeyFile(writeFile(t, "b64.key", []byte(enc)))
	if err != nil {
		t.Fatalf("LoadKeyFile: %v", err)
	}
	if k != *(*[32]byte)(raw) {
		t.Fatal("base64 key mismatch")
	}
}

func TestLoadKeyFile_WrongLength(t *testing.T) {
	if _, err := LoadKeyFile(writeFile(t, "short.key", []byte("too short"))); err == nil {
		t.Fatal("expected error for non-32-byte key")
	}
}

func TestLoadKeyFile_Missing(t *testing.T) {
	if _, err := LoadKeyFile(filepath.Join(t.TempDir(), "nope.key")); err == nil {
		t.Fatal("expected error for missing file")
	}
}

func TestKeyStore_StoreLoad(t *testing.T) {
	ks := NewKeyStore(testKey(0x01))
	if ks.Load() != testKey(0x01) {
		t.Fatal("initial key mismatch")
	}
	ks.Store(testKey(0x02))
	if ks.Load() != testKey(0x02) {
		t.Fatal("after Store, key not updated")
	}
}

func TestKeyStore_ZeroLoad(t *testing.T) {
	var ks KeyStore
	if ks.Load() != ([32]byte{}) {
		t.Fatal("zero KeyStore should return zero key")
	}
}

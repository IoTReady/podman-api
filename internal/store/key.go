package store

import (
	"bytes"
	"encoding/base64"
	"fmt"
	"os"
	"sync/atomic"
)

// KeyStore is an atomically-swappable holder for the 32-byte secret key.
// Safe for concurrent Load/Store; mirrors internal/auth.KeyStore so a SIGHUP
// reload in main takes effect on the next seal/open without a restart.
type KeyStore struct {
	key atomic.Pointer[[32]byte]
}

// NewKeyStore returns a store seeded with k.
func NewKeyStore(k [32]byte) *KeyStore {
	s := &KeyStore{}
	s.Store(k)
	return s
}

// Store atomically replaces the live key.
func (s *KeyStore) Store(k [32]byte) {
	kk := k
	s.key.Store(&kk)
}

// Load returns the current key (zero value if never set).
func (s *KeyStore) Load() [32]byte {
	p := s.key.Load()
	if p == nil {
		return [32]byte{}
	}
	return *p
}

// LoadKeyFile reads a 32-byte encryption key from path. The file may contain
// either the 32 raw bytes, or the base64 (std) encoding of 32 bytes. Trailing
// whitespace/newlines are ignored. Anything else is an error.
func LoadKeyFile(path string) ([32]byte, error) {
	var k [32]byte
	raw, err := os.ReadFile(path)
	if err != nil {
		return k, err
	}
	trimmed := bytes.TrimSpace(raw)
	if len(trimmed) == 32 {
		copy(k[:], trimmed)
		return k, nil
	}
	if dec, err := base64.StdEncoding.DecodeString(string(trimmed)); err == nil && len(dec) == 32 {
		copy(k[:], dec)
		return k, nil
	}
	return k, fmt.Errorf("store: spec key in %s must be 32 raw bytes or base64 of 32 bytes", path)
}

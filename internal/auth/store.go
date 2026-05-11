package auth

import (
	"sync/atomic"

	"github.com/iotready/podman-api/internal/config"
)

// KeyStore is an atomically-swappable snapshot of the bearer-key list.
// It is safe for concurrent Load() and Store() — readers see either the
// previous snapshot or the new one, never a partial mix.
//
// The middleware reads the snapshot per-request, so a SIGHUP reload in
// main is reflected on the next inbound request without restarting the
// process or interrupting any in-flight stream.
type KeyStore struct {
	keys atomic.Pointer[[]config.APIKey]
}

// NewKeyStore returns a store seeded with the initial key list.
func NewKeyStore(initial []config.APIKey) *KeyStore {
	s := &KeyStore{}
	s.Store(initial)
	return s
}

// Store atomically replaces the live key list. A nil or empty slice is
// allowed but means every subsequent request will fail authentication.
func (s *KeyStore) Store(keys []config.APIKey) {
	cp := append([]config.APIKey(nil), keys...)
	s.keys.Store(&cp)
}

// Load returns the current snapshot. The returned slice must not be mutated.
func (s *KeyStore) Load() []config.APIKey {
	p := s.keys.Load()
	if p == nil {
		return nil
	}
	return *p
}

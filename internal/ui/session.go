package ui

import (
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"sync"
	"time"
)

const (
	sessionCookie = "pa_session"
	csrfField     = "csrf_token"
	csrfHeader    = "X-CSRF-Token"
)

// SessionStore persists active sessions. In-process now; swappable later.
type SessionStore interface {
	Create(id Identity) (token string, err error)
	Lookup(token string) (Identity, bool)
	Delete(token string)
}

type sessionEntry struct {
	id      Identity
	expires time.Time
}

// MemorySessionStore is an in-process session store with a sliding TTL.
type MemorySessionStore struct {
	ttl time.Duration
	now func() time.Time

	mu sync.Mutex
	m  map[string]sessionEntry
}

func NewMemorySessionStore(ttl time.Duration) *MemorySessionStore {
	return &MemorySessionStore{ttl: ttl, now: time.Now, m: map[string]sessionEntry{}}
}

func (s *MemorySessionStore) Create(id Identity) (string, error) {
	b := make([]byte, 32)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	tok := base64.RawURLEncoding.EncodeToString(b)
	s.mu.Lock()
	s.m[tok] = sessionEntry{id: id, expires: s.now().Add(s.ttl)}
	s.mu.Unlock()
	return tok, nil
}

func (s *MemorySessionStore) Lookup(tok string) (Identity, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	e, ok := s.m[tok]
	if !ok {
		return Identity{}, false
	}
	if s.now().After(e.expires) {
		delete(s.m, tok)
		return Identity{}, false
	}
	// Sliding expiry.
	e.expires = s.now().Add(s.ttl)
	s.m[tok] = e
	return e.id, true
}

func (s *MemorySessionStore) Delete(tok string) {
	s.mu.Lock()
	delete(s.m, tok)
	s.mu.Unlock()
}

// csrfKey is process-random; CSRF tokens are HMAC(sessionID) under it, so they
// are stable per session within a process lifetime and unforgeable without it.
var csrfKey = func() []byte {
	b := make([]byte, 32)
	_, _ = rand.Read(b)
	return b
}()

func csrfToken(sessionID string) string {
	mac := hmac.New(sha256.New, csrfKey)
	mac.Write([]byte(sessionID))
	return base64.RawURLEncoding.EncodeToString(mac.Sum(nil))
}

func csrfEqual(a, b string) bool {
	return subtle.ConstantTimeCompare([]byte(a), []byte(b)) == 1
}

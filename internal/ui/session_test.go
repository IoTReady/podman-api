package ui

import (
	"testing"
	"time"
)

func TestMemorySessionStoreLifecycle(t *testing.T) {
	now := time.Unix(1_000_000, 0)
	s := NewMemorySessionStore(time.Hour)
	s.now = func() time.Time { return now }

	id := Identity{Subject: "operator", Scopes: []string{"*"}}
	tok, err := s.Create(id)
	if err != nil {
		t.Fatal(err)
	}
	if tok == "" {
		t.Fatal("empty token")
	}

	got, ok := s.Lookup(tok)
	if !ok || got.Subject != "operator" {
		t.Fatalf("lookup = %+v, %v", got, ok)
	}

	// Past expiry → gone.
	now = now.Add(2 * time.Hour)
	if _, ok := s.Lookup(tok); ok {
		t.Error("expired session should not resolve")
	}
}

func TestMemorySessionStoreSlidingExpiry(t *testing.T) {
	now := time.Unix(1_000_000, 0)
	s := NewMemorySessionStore(time.Hour)
	s.now = func() time.Time { return now }

	tok, err := s.Create(Identity{Subject: "operator"})
	if err != nil {
		t.Fatal(err)
	}

	// Just before the original expiry: a successful lookup must renew the TTL.
	now = now.Add(59 * time.Minute)
	if _, ok := s.Lookup(tok); !ok {
		t.Fatal("session should still resolve before expiry")
	}

	// Past the ORIGINAL expiry but within one TTL of the renewing lookup: the
	// slide must have kept it alive.
	now = now.Add(30 * time.Minute) // t+89m; original expiry was t+60m
	if _, ok := s.Lookup(tok); !ok {
		t.Error("sliding expiry should have renewed the session on the prior lookup")
	}
}

func TestMemorySessionStoreDelete(t *testing.T) {
	s := NewMemorySessionStore(time.Hour)
	tok, _ := s.Create(Identity{Subject: "operator"})
	s.Delete(tok)
	if _, ok := s.Lookup(tok); ok {
		t.Error("deleted session should not resolve")
	}
}

func TestCSRFTokenStablePerSession(t *testing.T) {
	a := csrfToken("session-abc")
	b := csrfToken("session-abc")
	c := csrfToken("session-xyz")
	if a != b {
		t.Error("csrf token must be stable for a session id")
	}
	if a == c {
		t.Error("csrf token must differ across session ids")
	}
}

func TestCSRFEqual(t *testing.T) {
	tok := csrfToken("session-abc")
	if !csrfEqual(tok, tok) {
		t.Error("csrfEqual must return true for equal tokens")
	}
	if csrfEqual(tok, csrfToken("session-xyz")) {
		t.Error("csrfEqual must return false for different tokens")
	}
}

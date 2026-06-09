package ui

import (
	"context"
	"net/http"
	"sync"
	"time"
)

type ctxKey int

const (
	identityKey ctxKey = iota
	sessionTokenKey
)

// rateLimiter is a per-IP sliding-window rate limiter for the login endpoint.
// It allows burst up to maxAttempts within a window, then returns 429 until the
// window expires. Thread-safe.
type ipRateLimiter struct {
	mu          sync.Mutex
	attempts    map[string][]time.Time
	maxAttempts int
	window      time.Duration
}

func newIPRateLimiter(maxAttempts int, window time.Duration) *ipRateLimiter {
	return &ipRateLimiter{
		attempts:    make(map[string][]time.Time),
		maxAttempts: maxAttempts,
		window:      window,
	}
}

func (rl *ipRateLimiter) Allow(ip string) bool {
	now := time.Now()
	rl.mu.Lock()
	defer rl.mu.Unlock()

	// Prune entries older than the window.
	cutoff := now.Add(-rl.window)
	recent := rl.attempts[ip][:0]
	for _, t := range rl.attempts[ip] {
		if t.After(cutoff) {
			recent = append(recent, t)
		}
	}
	rl.attempts[ip] = recent

	if len(recent) >= rl.maxAttempts {
		return false
	}
	rl.attempts[ip] = append(rl.attempts[ip], now)
	return true
}

// loginThrottle wraps a login handler with per-IP rate limiting. Returns 429
// Too Many Requests when the limit is exceeded.
func (u *UI) loginThrottle(next http.HandlerFunc) http.Handler {
	rl := newIPRateLimiter(5, 10*time.Second) // 5 attempts per 10s per IP
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Strip port from RemoteAddr for the IP key.
		ip := r.RemoteAddr
		for i := len(ip) - 1; i >= 0; i-- {
			if ip[i] == ':' {
				ip = ip[:i]
				break
			}
		}
		if !rl.Allow(ip) {
			http.Error(w, "too many login attempts", http.StatusTooManyRequests)
			return
		}
		next.ServeHTTP(w, r)
	})
}

// requireSession resolves the session cookie to an Identity and injects both the
// Identity and the raw session token into the context, or redirects to
// /ui/login when absent/invalid.
func (u *UI) requireSession(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c, err := r.Cookie(sessionCookie)
		if err != nil {
			redirectToLogin(w, r)
			return
		}
		id, ok := u.cfg.Sessions.Lookup(c.Value)
		if !ok {
			redirectToLogin(w, r)
			return
		}
		ctx := context.WithValue(r.Context(), identityKey, id)
		ctx = context.WithValue(ctx, sessionTokenKey, c.Value)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

// requireCSRF rejects unsafe methods whose CSRF token (form field or header)
// does not match the token derived from the active session. It reads the
// session token from the context populated by requireSession, so it must be
// nested inside requireSession; a request without a verified session in context
// is rejected rather than trusting a raw cookie.
func (u *UI) requireCSRF(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet || r.Method == http.MethodHead {
			next.ServeHTTP(w, r)
			return
		}
		sessTok, ok := r.Context().Value(sessionTokenKey).(string)
		if !ok || sessTok == "" {
			http.Error(w, "forbidden", http.StatusForbidden)
			return
		}
		want := csrfToken(sessTok)
		got := r.Header.Get(csrfHeader)
		if got == "" {
			got = r.FormValue(csrfField)
		}
		if !csrfEqual(got, want) {
			http.Error(w, "bad csrf token", http.StatusForbidden)
			return
		}
		next.ServeHTTP(w, r)
	})
}

// redirectToLogin sends the browser to /ui/login. For an htmx request a plain
// 303 would be followed by XHR and the login page swapped into the target
// panel; instead we send an HX-Redirect header so htmx navigates the whole
// window. Non-htmx requests get an ordinary 303.
func redirectToLogin(w http.ResponseWriter, r *http.Request) {
	if r.Header.Get("HX-Request") == "true" {
		w.Header().Set("HX-Redirect", "/ui/login")
		w.WriteHeader(http.StatusOK)
		return
	}
	http.Redirect(w, r, "/ui/login", http.StatusSeeOther)
}

func identityFrom(r *http.Request) (Identity, bool) {
	id, ok := r.Context().Value(identityKey).(Identity)
	return id, ok
}

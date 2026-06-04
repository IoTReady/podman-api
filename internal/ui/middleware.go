package ui

import (
	"context"
	"net/http"
)

type ctxKey int

const identityKey ctxKey = 0

// requireSession resolves the session cookie to an Identity and injects it into
// the context, or redirects to /ui/login when absent/invalid.
func (u *UI) requireSession(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c, err := r.Cookie(sessionCookie)
		if err != nil {
			http.Redirect(w, r, "/ui/login", http.StatusSeeOther)
			return
		}
		id, ok := u.cfg.Sessions.Lookup(c.Value)
		if !ok {
			http.Redirect(w, r, "/ui/login", http.StatusSeeOther)
			return
		}
		ctx := context.WithValue(r.Context(), identityKey, id)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

// requireCSRF rejects unsafe methods whose CSRF token (form field or header)
// does not match the session-derived token.
func (u *UI) requireCSRF(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet || r.Method == http.MethodHead {
			next.ServeHTTP(w, r)
			return
		}
		c, err := r.Cookie(sessionCookie)
		if err != nil {
			http.Error(w, "forbidden", http.StatusForbidden)
			return
		}
		want := csrfToken(c.Value)
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

func identityFrom(r *http.Request) (Identity, bool) {
	id, ok := r.Context().Value(identityKey).(Identity)
	return id, ok
}

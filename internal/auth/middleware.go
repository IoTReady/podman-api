package auth

import (
	"context"
	"net/http"
	"strings"

	"github.com/iotready/podman-api/internal/config"
)

type ctxKey int

const keyIDKey ctxKey = 0

// New returns middleware that requires a Bearer token matching one of the
// keys currently held by store, AND that the matching key has the
// requiredScope. The store snapshot is read per request, so a SIGHUP-triggered
// reload takes effect on the next inbound request.
//
// On failure: 401 (no/invalid token) or 403 (missing scope), with a JSON body.
func New(store *KeyStore, requiredScope string) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			tok := bearer(r)
			if tok == "" {
				writeErr(w, http.StatusUnauthorized, "missing_token", "missing or malformed Authorization header")
				return
			}
			matched, ok := match(store.Load(), tok)
			if !ok {
				writeErr(w, http.StatusUnauthorized, "invalid_token", "token not recognised")
				return
			}
			if !matched.HasScope(requiredScope) {
				writeErr(w, http.StatusForbidden, "missing_scope", "token lacks required scope")
				return
			}
			ctx := context.WithValue(r.Context(), keyIDKey, matched.ID)
			next.ServeHTTP(w, r.WithContext(ctx))
		})
	}
}

// KeyIDFromContext returns the authenticated key id, or "" if unauthenticated.
func KeyIDFromContext(ctx context.Context) string {
	v, _ := ctx.Value(keyIDKey).(string)
	return v
}

func bearer(r *http.Request) string {
	h := r.Header.Get("Authorization")
	const p = "Bearer "
	if !strings.HasPrefix(h, p) {
		return ""
	}
	return strings.TrimSpace(h[len(p):])
}

func match(keys []config.APIKey, tok string) (config.APIKey, bool) {
	for _, k := range keys {
		ok, err := config.VerifyToken(tok, k.SecretHash)
		if err == nil && ok {
			return k, true
		}
	}
	return config.APIKey{}, false
}

// writeErr is a stripped-down JSON writer used only inside this package.
// The api package has the canonical version; this avoids a circular import.
func writeErr(w http.ResponseWriter, status int, code, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_, _ = w.Write([]byte(`{"code":"` + code + `","message":"` + msg + `"}`))
}

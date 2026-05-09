// Package obs holds the observability primitives: structured audit log
// middleware and Prometheus metrics.
package obs

import (
	"encoding/json"
	"io"
	"net/http"
	"time"

	"github.com/iotready/podman-api/internal/auth"
)

// NewAuditMiddleware writes one JSON line per state-changing request to w.
// It logs nothing for safe methods (GET/HEAD/OPTIONS).
func NewAuditMiddleware(w io.Writer) func(http.Handler) http.Handler {
	enc := json.NewEncoder(w)
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
			if r.Method == http.MethodGet || r.Method == http.MethodHead || r.Method == http.MethodOptions {
				next.ServeHTTP(rw, r)
				return
			}
			start := time.Now()
			rec := &statusRecorder{ResponseWriter: rw, status: http.StatusOK}
			next.ServeHTTP(rec, r)
			_ = enc.Encode(map[string]any{
				"ts":          start.UTC().Format(time.RFC3339Nano),
				"method":      r.Method,
				"path":        r.URL.Path,
				"status":      rec.status,
				"duration_ms": time.Since(start).Milliseconds(),
				"key_id":      auth.KeyIDFromContext(r.Context()),
			})
		})
	}
}

type statusRecorder struct {
	http.ResponseWriter
	status int
}

func (s *statusRecorder) WriteHeader(c int) { s.status = c; s.ResponseWriter.WriteHeader(c) }

package api

import (
	"net/http"

	apispec "github.com/iotready/podman-api/api"
	"github.com/iotready/podman-api/internal/auth"
	"github.com/iotready/podman-api/internal/instance"
	"github.com/iotready/podman-api/internal/store"
)

// NewRouter builds the full HTTP handler tree.
// audit is an optional middleware applied around auth-guarded handlers; pass nil for no-op.
// metricsHandler is an optional handler mounted at GET /metrics; pass nil to omit the endpoint.
func NewRouter(svc *instance.Service, jobs store.JobStore, keys *auth.KeyStore, audit func(http.Handler) http.Handler, metricsHandler http.Handler) http.Handler {
	mux := http.NewServeMux()
	h := &handlers{svc: svc, jobs: jobs}

	if audit == nil {
		audit = func(h http.Handler) http.Handler { return h }
	}

	// Process endpoints (no auth).
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"status":"ok"}`))
	})
	mux.HandleFunc("GET /openapi.yaml", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/yaml; charset=utf-8")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(apispec.Spec)
	})

	if metricsHandler != nil {
		mux.Handle("GET /metrics", metricsHandler)
	}

	guard := func(scope string, h http.Handler) http.Handler {
		return auth.New(keys, scope)(audit(h))
	}

	// Hosts (read).
	mux.Handle("GET /hosts", guard("hosts:read", http.HandlerFunc(h.listHosts)))
	mux.Handle("GET /hosts/{host}", guard("hosts:read", http.HandlerFunc(h.getHost)))
	mux.Handle("GET /hosts/{host}/healthz", guard("hosts:read", http.HandlerFunc(h.hostHealthz)))
	mux.Handle("GET /hosts/{host}/ports-in-use", guard("hosts:read", http.HandlerFunc(h.portsInUse)))

	// Templates (read).
	mux.Handle("GET /templates", guard("instances:read", http.HandlerFunc(h.listTemplates)))
	mux.Handle("GET /templates/{id}", guard("instances:read", http.HandlerFunc(h.getTemplate)))
	mux.Handle("GET /templates/{id}/render", guard("instances:read", http.HandlerFunc(h.renderTemplate)))

	// Host secrets.
	mux.Handle("GET /hosts/{host}/secrets", guard("secrets:read", http.HandlerFunc(h.listSecrets)))
	mux.Handle("PUT /hosts/{host}/secrets/{name}", guard("secrets:write", http.HandlerFunc(h.putSecret)))
	mux.Handle("DELETE /hosts/{host}/secrets/{name}", guard("secrets:write", http.HandlerFunc(h.deleteSecret)))

	// Instances.
	mux.Handle("GET /hosts/{host}/instances", guard("instances:read", http.HandlerFunc(h.listInstances)))
	mux.Handle("GET /hosts/{host}/instances/{template}/{slug}", guard("instances:read", http.HandlerFunc(h.getInstance)))
	mux.Handle("POST /hosts/{host}/instances", guard("instances:write", http.HandlerFunc(h.createInstance)))
	mux.Handle("PUT /hosts/{host}/instances/{template}/{slug}", guard("instances:write", http.HandlerFunc(h.applyInstance)))
	mux.Handle("DELETE /hosts/{host}/instances/{template}/{slug}", guard("instances:write", http.HandlerFunc(h.deleteInstance)))

	// Lifecycle.
	mux.Handle("POST /hosts/{host}/instances/{template}/{slug}/start", guard("instances:write", http.HandlerFunc(h.startInstance)))
	mux.Handle("POST /hosts/{host}/instances/{template}/{slug}/stop", guard("instances:write", http.HandlerFunc(h.stopInstance)))
	mux.Handle("POST /hosts/{host}/instances/{template}/{slug}/restart", guard("instances:write", http.HandlerFunc(h.restartInstance)))
	mux.Handle("POST /hosts/{host}/instances/{template}/{slug}/upgrade", guard("instances:write", http.HandlerFunc(h.upgradeInstance)))

	// Bulk lifecycle operations against many instances on one host.
	mux.Handle("POST /hosts/{host}/bulk", guard("instances:write", http.HandlerFunc(h.bulk)))

	// Logs.
	mux.Handle("GET /hosts/{host}/instances/{template}/{slug}/logs", guard("instances:read", http.HandlerFunc(h.logsInstance)))

	// Volumes.
	mux.Handle("GET /hosts/{host}/instances/{template}/{slug}/volumes", guard("instances:read", http.HandlerFunc(h.instanceVolumes)))
	mux.Handle("DELETE /hosts/{host}/volumes/{name}", guard("instances:write", http.HandlerFunc(h.deleteVolume)))

	// Jobs (read). 501 when the store is disabled.
	mux.Handle("GET /jobs", guard("jobs:read", http.HandlerFunc(h.listJobs)))
	mux.Handle("GET /jobs/{id}", guard("jobs:read", http.HandlerFunc(h.getJob)))

	// Migrate enqueues a job; 501 when the store is disabled.
	mux.Handle("POST /migrate", guard("instances:write", http.HandlerFunc(h.migrate)))

	return mux
}

// handlers holds per-request dependencies. Each method is a thin adapter
// around svc.
type handlers struct {
	svc  *instance.Service
	jobs store.JobStore
}

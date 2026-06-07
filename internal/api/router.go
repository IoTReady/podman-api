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
// canceller is an optional JobCanceller for the POST /jobs/{id}/cancel route; pass nil when the job runner is not wired.
func NewRouter(svc *instance.Service, jobs store.JobStore, keys *auth.KeyStore, audit func(http.Handler) http.Handler, metricsHandler http.Handler, canceller JobCanceller) http.Handler {
	mux := http.NewServeMux()
	h := &handlers{svc: svc, jobs: jobs, canceller: canceller}

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

	// Templates (catalog CRUD + clone).
	mux.Handle("GET /templates", guard("templates:read", http.HandlerFunc(h.listTemplates)))
	mux.Handle("POST /templates", guard("templates:write", http.HandlerFunc(h.createTemplate)))
	mux.Handle("GET /templates/{id}", guard("templates:read", http.HandlerFunc(h.getTemplate)))
	mux.Handle("PUT /templates/{id}", guard("templates:write", http.HandlerFunc(h.updateTemplate)))
	mux.Handle("DELETE /templates/{id}", guard("templates:write", http.HandlerFunc(h.deleteTemplate)))
	mux.Handle("POST /templates/{id}/clone", guard("templates:write", http.HandlerFunc(h.cloneTemplate)))
	mux.Handle("GET /templates/{id}/render", guard("templates:read", http.HandlerFunc(h.renderTemplate)))

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
	mux.Handle("POST /jobs/{id}/cancel", guard("instances:write", http.HandlerFunc(h.cancelJob)))

	// Migrate enqueues a job; 501 when the store is disabled.
	mux.Handle("POST /migrate", guard("instances:write", http.HandlerFunc(h.migrate)))

	// Backups: enqueue/list per instance; restore/delete per backup (#66).
	mux.Handle("POST /hosts/{host}/instances/{template}/{slug}/backup", guard("instances:write", http.HandlerFunc(h.postBackup)))
	mux.Handle("GET /hosts/{host}/instances/{template}/{slug}/backups", guard("instances:read", http.HandlerFunc(h.listBackups)))
	mux.Handle("POST /backups/{id}/restore", guard("instances:write", http.HandlerFunc(h.postRestore)))
	mux.Handle("DELETE /backups/{id}", guard("instances:write", http.HandlerFunc(h.deleteBackup)))

	// Evacuate enqueues a parent job that fans out child migrate jobs; 501 when the store is disabled.
	mux.Handle("POST /evacuate", guard("instances:write", http.HandlerFunc(h.evacuate)))

	// Evacuate plan is a read-only dry-run: resolve the map and run live
	// destination preflight checks, returning a per-move report. No job, no
	// mutation; 501 when the store is disabled.
	mux.Handle("POST /evacuate/plan", guard("instances:read", http.HandlerFunc(h.evacuatePlan)))

	return mux
}

// handlers holds per-request dependencies. Each method is a thin adapter
// around svc.
type handlers struct {
	svc       *instance.Service
	jobs      store.JobStore
	canceller JobCanceller
}

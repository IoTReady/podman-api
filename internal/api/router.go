package api

import (
	"net/http"

	"github.com/iotready/podman-api/internal/auth"
	"github.com/iotready/podman-api/internal/config"
	"github.com/iotready/podman-api/internal/instance"
)

// NewRouter builds the full HTTP handler tree.
func NewRouter(svc *instance.Service, keys []config.APIKey) http.Handler {
	mux := http.NewServeMux()
	h := &handlers{svc: svc}

	// Process endpoints (no auth).
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"status":"ok"}`))
	})

	guard := func(scope string, h http.Handler) http.Handler {
		return auth.New(keys, scope)(h)
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

	// Logs.
	mux.Handle("GET /hosts/{host}/instances/{template}/{slug}/logs", guard("instances:read", http.HandlerFunc(h.logsInstance)))

	return mux
}

// handlers holds per-request dependencies. Each method is a thin adapter
// around svc.
type handlers struct {
	svc *instance.Service
}

// Stub implementations for handlers defined in subsequent task files.
// Each stub returns 501 Not Implemented; later tasks replace them.
func (h *handlers) listHosts(w http.ResponseWriter, _ *http.Request)       { notImpl(w) }
func (h *handlers) getHost(w http.ResponseWriter, _ *http.Request)         { notImpl(w) }
func (h *handlers) hostHealthz(w http.ResponseWriter, _ *http.Request)     { notImpl(w) }
func (h *handlers) portsInUse(w http.ResponseWriter, _ *http.Request)      { notImpl(w) }
func (h *handlers) listTemplates(w http.ResponseWriter, _ *http.Request)   { notImpl(w) }
func (h *handlers) getTemplate(w http.ResponseWriter, _ *http.Request)     { notImpl(w) }
func (h *handlers) renderTemplate(w http.ResponseWriter, _ *http.Request)  { notImpl(w) }
func (h *handlers) listSecrets(w http.ResponseWriter, _ *http.Request)     { notImpl(w) }
func (h *handlers) putSecret(w http.ResponseWriter, _ *http.Request)       { notImpl(w) }
func (h *handlers) deleteSecret(w http.ResponseWriter, _ *http.Request)    { notImpl(w) }
func (h *handlers) listInstances(w http.ResponseWriter, _ *http.Request)   { notImpl(w) }
func (h *handlers) getInstance(w http.ResponseWriter, _ *http.Request)     { notImpl(w) }
func (h *handlers) createInstance(w http.ResponseWriter, _ *http.Request)  { notImpl(w) }
func (h *handlers) applyInstance(w http.ResponseWriter, _ *http.Request)   { notImpl(w) }
func (h *handlers) deleteInstance(w http.ResponseWriter, _ *http.Request)  { notImpl(w) }
func (h *handlers) startInstance(w http.ResponseWriter, _ *http.Request)   { notImpl(w) }
func (h *handlers) stopInstance(w http.ResponseWriter, _ *http.Request)    { notImpl(w) }
func (h *handlers) restartInstance(w http.ResponseWriter, _ *http.Request) { notImpl(w) }
func (h *handlers) upgradeInstance(w http.ResponseWriter, _ *http.Request) { notImpl(w) }
func (h *handlers) logsInstance(w http.ResponseWriter, _ *http.Request)    { notImpl(w) }

func notImpl(w http.ResponseWriter) {
	WriteJSON(w, http.StatusNotImplemented, ErrorBody{Code: "not_implemented", Message: "handler not yet implemented"})
}

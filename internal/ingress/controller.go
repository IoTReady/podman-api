package ingress

import (
	"context"
	"sync"

	"github.com/iotready/podman-api/internal/podman"
	"github.com/iotready/podman-api/internal/store"
)

// Controller reconciles a host's ingress (Caddy) state with the store.
type Controller interface {
	// Reconcile makes the host's Caddy proxy match the routes derived from the
	// store: ensures the network + Caddy pod exist, renders the Caddyfile, and
	// applies it (zero-downtime reload). Safe to call repeatedly; serialized
	// per host.
	Reconcile(ctx context.Context, host string) error
}

// Disabled is the no-op Controller used when ingress is turned off. Reconcile
// does nothing so the rest of the system can call it unconditionally.
type Disabled struct{}

func (Disabled) Reconcile(context.Context, string) error { return nil }

// TemplateIngress is a template's ingress declaration, supplied to the
// controller so it can compute backends without importing the template loader.
// Only templates whose meta declares ingress: appear in the controller's map.
type TemplateIngress struct {
	Container string // declared HTTP container name (informational)
	Port      int    // container port the backend points at
}

// Config holds the operator-set knobs for the Caddy controller.
type Config struct {
	Network    string // shared ingress network name (e.g. "podman-api-ingress")
	CaddyImage string // e.g. "docker.io/library/caddy:2"
	ACMEEmail  string // ACME account email for the global Caddyfile block
}

// CaddyController is the production Controller. It drives a per-host Caddy pod
// over the existing podman socket.
type CaddyController struct {
	client    podman.Client
	store     store.Store
	templates map[string]TemplateIngress // template id -> ingress decl
	cfg       Config

	mu    sync.Mutex
	locks map[string]*sync.Mutex // per-host serialization
}

// NewCaddyController builds a controller. templates must include an entry for
// every template that declares ingress: in its meta.
func NewCaddyController(client podman.Client, st store.Store, templates map[string]TemplateIngress, cfg Config) *CaddyController {
	return &CaddyController{
		client:    client,
		store:     st,
		templates: templates,
		cfg:       cfg,
		locks:     map[string]*sync.Mutex{},
	}
}

// hostLock returns the per-host mutex, creating it on first use.
func (c *CaddyController) hostLock(host string) *sync.Mutex {
	c.mu.Lock()
	defer c.mu.Unlock()
	m, ok := c.locks[host]
	if !ok {
		m = &sync.Mutex{}
		c.locks[host] = m
	}
	return m
}

// Compile-time guarantees.
var (
	_ Controller = Disabled{}
	// _ Controller = (*CaddyController)(nil) // TODO(task 8): re-enable once Reconcile lands
)

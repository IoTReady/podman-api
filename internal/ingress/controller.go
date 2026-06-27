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

// Store is the controller's view of the durable state. It is the spec store
// plus template lookups: templates are mutable (created/edited at runtime), so
// the controller resolves each instance's ingress declaration from the store at
// reconcile time rather than caching a boot-time snapshot. *store.DB satisfies
// this; so does *store.Memory.
type Store interface {
	store.Store
	GetTemplate(ctx context.Context, id string) (store.Template, error)
}

// Config holds the operator-set knobs for the Caddy controller.
type Config struct {
	Network    string // shared ingress network name (e.g. "podman-api-ingress")
	CaddyImage string // e.g. "docker.io/library/caddy:2"
	ACMEEmail  string // ACME account email for the global Caddyfile block
	// AdminAddr is the Caddy admin API address (host:port) used when no
	// per-host override is set. Default "localhost:2019".
	AdminAddr string
	// HostAdmins maps hostID to a per-host Caddy admin API address. Takes
	// precedence over AdminAddr when set for the host being reconciled.
	HostAdmins map[string]string
}

// CaddyController is the production Controller. It drives a per-host Caddy pod
// over the existing podman socket.
type CaddyController struct {
	client podman.Client
	store  Store
	cfg    Config

	// adminDo dispatches HTTP requests to the Caddy admin API. Overridden in
	// tests with a recording stub; production uses caddyAdminDo (net/http).
	adminDo func(ctx context.Context, addr, method, path string, body []byte) (int, []byte, error)

	mu    sync.Mutex
	locks map[string]*sync.Mutex // per-host serialization
}

// NewCaddyController builds a controller. st serves both spec storage and
// template lookups, so ingress declarations are always read fresh from the
// store (no stale boot-time template snapshot).
func NewCaddyController(client podman.Client, st Store, cfg Config) *CaddyController {
	if cfg.AdminAddr == "" {
		cfg.AdminAddr = "localhost:2019"
	}
	return &CaddyController{
		client: client,
		store:  st,
		cfg:    cfg,
		locks:  map[string]*sync.Mutex{},
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

// resolveAdminAddr returns the Caddy admin API addr for the given host: the
// per-host override from cfg.HostAdmins if present, otherwise cfg.AdminAddr.
func (c *CaddyController) resolveAdminAddr(hostID string) string {
	if c.cfg.HostAdmins != nil {
		if addr, ok := c.cfg.HostAdmins[hostID]; ok && addr != "" {
			return addr
		}
	}
	return c.cfg.AdminAddr
}

// Compile-time guarantees.
var (
	_ Controller = Disabled{}
	_ Controller = (*CaddyController)(nil)
)

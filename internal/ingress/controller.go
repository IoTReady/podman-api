package ingress

import (
	"context"
	"log"
	"sync"

	"github.com/iotready/podman-api/internal/store"
)

// Controller reconciles a host's ingress (Caddy) state with the store.
type Controller interface {
	// Reconcile makes the host's Caddy proxy match the routes derived from the
	// store: derives routes and pushes JSON routes via the Caddy admin API.
	// Safe to call repeatedly; serialized per host.
	Reconcile(ctx context.Context, host string) error
}

// Disabled is the no-op Controller used when ingress is turned off. Reconcile
// does nothing so the rest of the system can call it unconditionally.
type Disabled struct{}

func (Disabled) Reconcile(context.Context, string) error { return nil }

// Store is the controller's view of the durable state.
type Store interface {
	store.Store
	GetTemplate(ctx context.Context, id string) (store.Template, error)
}

// Config holds the operator-set knobs for the Caddy controller.
type Config struct {
	// AdminAddr is the Caddy admin API address (host:port) used when no
	// per-host override is set. Default "localhost:2019".
	AdminAddr string
	// HostAdmins maps hostID to a per-host Caddy admin API address. Takes
	// precedence over AdminAddr when set for the host being reconciled.
	HostAdmins map[string]string
}

// CaddyController is the production Controller. It drives routes on an
// operator-managed Caddy instance via the admin API.
type CaddyController struct {
	store Store
	cfg   Config

	// adminDo dispatches HTTP requests to the Caddy admin API. Overridden in
	// tests with a recording stub; production uses caddyAdminDo (net/http).
	adminDo func(ctx context.Context, addr, method, path string, body []byte) (int, []byte, error)

	mu    sync.Mutex
	locks map[string]*sync.Mutex // per-host serialization

	// cleanupMu guards cleanupFailed, which records the last best-effort
	// cleanup outcome per host so the periodic reconcile loop logs a
	// transition rather than every tick. See logCleanupTransition.
	cleanupMu     sync.Mutex
	cleanupFailed map[string]bool
}

// NewCaddyController builds a controller. st serves both spec storage and
// template lookups, so ingress declarations are always read fresh from the
// store (no stale boot-time template snapshot). Caddy itself is managed by
// the operator; podman-api only manages its own route namespace via the admin API.
func NewCaddyController(st Store, cfg Config) *CaddyController {
	if cfg.AdminAddr == "" {
		cfg.AdminAddr = "localhost:2019"
	}
	return &CaddyController{
		store:         st,
		cfg:           cfg,
		adminDo:       caddyAdminDo,
		locks:         map[string]*sync.Mutex{},
		cleanupFailed: map[string]bool{},
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

// logCleanupTransition reports the outcome of a best-effort cleanup, but only
// when it differs from the last outcome recorded for this host.
//
// The unconditional log this replaces was an ERROR-shaped line every
// -ingress-interval, forever, for a host that runs no Caddy at all: the
// cleanup DELETEs our server namespace whenever a host derives zero routes,
// and a host with no admin endpoint refuses that connection every single time.
// It can never succeed and nothing is broken by its failing, so repeating it
// is pure alert fatigue — but silencing it outright would hide a real ingress
// failure on a host that does run Caddy, which is why this is gated on the
// transition rather than removed.
//
// Both directions are reported: the first failure after a success (or after
// nothing at all), and the first success after a failure. A host that has only
// ever succeeded logs nothing, which is the common case.
func (c *CaddyController) logCleanupTransition(host, adminAddr string, err error) {
	c.cleanupMu.Lock()
	if c.cleanupFailed == nil {
		c.cleanupFailed = map[string]bool{}
	}
	wasFailing, seen := c.cleanupFailed[host]
	c.cleanupFailed[host] = err != nil
	c.cleanupMu.Unlock()

	switch {
	case err != nil && (!seen || !wasFailing):
		log.Printf("ingress: best-effort cleanup on %s (admin %s) failed; not logging again until it changes: %v",
			host, adminAddr, err)
	case err == nil && seen && wasFailing:
		log.Printf("ingress: best-effort cleanup on %s (admin %s) succeeded again", host, adminAddr)
	}
}

// forgetCleanupState drops a host's recorded cleanup outcome. Only tests need
// it today; it exists so the map cannot be mistaken for something that must be
// pruned on a host-list reload — it is keyed by host id and bounded by the
// number of hosts that have ever been reconciled.
func (c *CaddyController) forgetCleanupState(host string) {
	c.cleanupMu.Lock()
	delete(c.cleanupFailed, host)
	c.cleanupMu.Unlock()
}

// Compile-time guarantees.
var (
	_ Controller = Disabled{}
	_ Controller = (*CaddyController)(nil)
)

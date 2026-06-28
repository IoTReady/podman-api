package ingress

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"time"
)

// Reconcile makes host's Caddy proxy match the store-derived routes. It is
// serialized per host and safe to call repeatedly.
func (c *CaddyController) Reconcile(ctx context.Context, host string) error {
	lock := c.hostLock(host)
	lock.Lock()
	defer lock.Unlock()

	routes, err := c.deriveRoutes(ctx, host)
	if err != nil {
		return err
	}
	adminAddr := c.resolveAdminAddr(host)

	// No routes: best-effort cleanup of our server namespace in Caddy. If
	// Caddy is unreachable (not yet started, restarting) that is not an error
	// when there is nothing to serve.
	if len(routes) == 0 {
		if err := c.deleteServer(ctx, adminAddr); err != nil {
			log.Printf("ingress: best-effort cleanup on %s (admin %s): %v", host, adminAddr, err)
		}
		return nil
	}

	// Wait for the admin API — one round-trip for a running Caddy, polling
	// (up to 20×300ms) while it restarts.
	if err := c.waitForAdmin(ctx, adminAddr); err != nil {
		return err
	}
	return c.pushRoutes(ctx, adminAddr, routes)
}

// pushRoutes applies routes to the Caddy server via the admin API.
// TLS automation (ACME email, issuers) is operator-managed via Caddyfile;
// automatic_https on the server config is enough to trigger issuance.
func (c *CaddyController) pushRoutes(ctx context.Context, adminAddr string, routes []Route) error {
	srv := caddyServer{
		Listen:         []string{":80", ":443"},
		Routes:         routesToCaddyJSON(routes),
		AutomaticHTTPS: &struct{}{},
	}
	srvJSON, err := json.Marshal(srv)
	if err != nil {
		return fmt.Errorf("ingress: marshal server config: %w", err)
	}
	if code, respBody, err := c.adminDo(ctx, adminAddr, http.MethodPut, "/config/apps/http/servers/podman_api", srvJSON); err != nil {
		return fmt.Errorf("ingress: admin PUT server: %w", err)
	} else if code >= 300 {
		return fmt.Errorf("ingress: admin PUT server: status %d: %s", code, respBody)
	}
	return nil
}

// deleteServer removes the podman_api server from Caddy's config when routes
// go to zero (so Caddy stops listening/serving for our namespace).
func (c *CaddyController) deleteServer(ctx context.Context, adminAddr string) error {
	code, body, err := c.adminDo(ctx, adminAddr, http.MethodDelete, "/config/apps/http/servers/podman_api", nil)
	if err != nil {
		return fmt.Errorf("ingress: admin DELETE server: %w", err)
	}
	// 404 means the server was already gone — that's fine.
	if code >= 300 && code != http.StatusNotFound {
		return fmt.Errorf("ingress: admin DELETE server: status %d: %s", code, body)
	}
	return nil
}

// waitForAdmin polls the Caddy admin API's /config/ endpoint until it
// responds with 200 or the context is cancelled.
func (c *CaddyController) waitForAdmin(ctx context.Context, adminAddr string) error {
	const (
		maxAttempts  = 20
		pollInterval = 300 * time.Millisecond
	)
	var lastErr error
	for i := 0; i < maxAttempts; i++ {
		code, _, err := c.adminDo(ctx, adminAddr, http.MethodGet, "/config/", nil)
		if err == nil && code == http.StatusOK {
			return nil
		}
		lastErr = err
		select {
		case <-ctx.Done():
			return fmt.Errorf("ingress: admin API at %s not ready: %w", adminAddr, ctx.Err())
		case <-time.After(pollInterval):
		}
	}
	if lastErr != nil {
		return fmt.Errorf("ingress: admin API at %s not ready: %w", adminAddr, lastErr)
	}
	return fmt.Errorf("ingress: admin API at %s not ready after %d attempts", adminAddr, maxAttempts)
}

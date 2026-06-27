package ingress

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/iotready/podman-api/internal/podman"
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

	// Nothing to serve and no proxy yet: don't stand up an idle pod.
	// If a pod already exists, fall through so a drop-to-zero removes the server.
	if len(routes) == 0 {
		if _, err := c.client.PodInspect(ctx, host, caddyPodName); errors.Is(err, podman.ErrNotFound) {
			return nil
		} else if err != nil {
			return fmt.Errorf("ingress: inspect caddy pod: %w", err)
		}
		return c.deleteServer(ctx, adminAddr)
	}

	created, err := c.ensureProxy(ctx, host)
	if err != nil {
		return err
	}
	if created {
		// Fresh pod: wait for the admin API to become ready before pushing routes.
		if err := c.waitForAdmin(ctx, adminAddr); err != nil {
			return err
		}
	}

	return c.pushRoutes(ctx, adminAddr, routes)
}

// pushRoutes applies routes to the Caddy server via the admin API. It also
// sets the ACME email in the TLS automation config if configured.
func (c *CaddyController) pushRoutes(ctx context.Context, adminAddr string, routes []Route) error {
	// Build and push the server config with all current routes. This is
	// done first so tests that look for the first PUT call see the routes
	// payload rather than the TLS email update.
	srv := caddyServer{
		Listen:         []string{":80", ":443"},
		Routes:         routesToCaddyJSON(routes),
		AutomaticHTTPS: &struct{}{},
	}
	srvJSON, err := json.Marshal(srv)
	if err != nil {
		return fmt.Errorf("ingress: marshal server config: %w", err)
	}
	code, respBody, err := c.adminDo(ctx, adminAddr, http.MethodPut, "/config/apps/http/servers/podman_api", srvJSON)
	if err != nil {
		return fmt.Errorf("ingress: admin PUT server: %w", err)
	}
	if code >= 300 {
		return fmt.Errorf("ingress: admin PUT server: status %d: %s", code, respBody)
	}

	// Set ACME email in global TLS automation if configured. This is
	// idempotent — Caddy treats a PUT to an existing path as a replace.
	if c.cfg.ACMEEmail != "" {
		emailJSON, _ := json.Marshal(c.cfg.ACMEEmail)
		code, body, err := c.adminDo(ctx, adminAddr, http.MethodPut, "/config/apps/tls/automation/email", emailJSON)
		if err != nil {
			return fmt.Errorf("ingress: admin set TLS email: %w", err)
		}
		if code >= 300 {
			return fmt.Errorf("ingress: admin set TLS email: status %d: %s", code, body)
		}
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
// responds with 200 or the context is cancelled. Used after creating a
// fresh Caddy pod to ensure it's ready before we push routes.
func (c *CaddyController) waitForAdmin(ctx context.Context, adminAddr string) error {
	const (
		maxAttempts  = 20
		pollInterval = 300 * time.Millisecond
	)
	for i := 0; i < maxAttempts; i++ {
		code, _, err := c.adminDo(ctx, adminAddr, http.MethodGet, "/config/", nil)
		if err == nil && code == http.StatusOK {
			return nil
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("ingress: admin API at %s not ready: %w", adminAddr, ctx.Err())
		case <-time.After(pollInterval):
		}
	}
	return fmt.Errorf("ingress: admin API at %s not ready after %d attempts", adminAddr, maxAttempts)
}

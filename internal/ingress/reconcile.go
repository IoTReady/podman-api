package ingress

import (
	"context"
	"fmt"
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
	caddyfile, err := RenderCaddyfile(c.cfg.ACMEEmail, routes)
	if err != nil {
		return fmt.Errorf("ingress: render caddyfile: %w", err)
	}

	created, err := c.ensureProxy(ctx, host, caddyfile)
	if err != nil {
		return err
	}
	if created {
		// The fresh pod boots reading the seeded Caddyfile; nothing to reload.
		return nil
	}

	// Existing pod: push the new config and reload with no downtime.
	if err := c.client.CopyToContainer(ctx, host, caddyContainer, caddyConfigDir, caddyConfigFile, []byte(caddyfile)); err != nil {
		return fmt.Errorf("ingress: copy caddyfile: %w", err)
	}
	cfgPath := caddyConfigDir + "/" + caddyConfigFile
	if res, err := c.client.ContainerExec(ctx, host, caddyContainer,
		[]string{"caddy", "validate", "--config", cfgPath, "--adapter", "caddyfile"}); err != nil {
		return fmt.Errorf("ingress: exec validate: %w", err)
	} else if res.ExitCode != 0 {
		return fmt.Errorf("ingress: caddy validate failed (exit %d): %s", res.ExitCode, res.Output)
	}
	if res, err := c.client.ContainerExec(ctx, host, caddyContainer,
		[]string{"caddy", "reload", "--config", cfgPath}); err != nil {
		return fmt.Errorf("ingress: exec reload: %w", err)
	} else if res.ExitCode != 0 {
		return fmt.Errorf("ingress: caddy reload failed (exit %d): %s", res.ExitCode, res.Output)
	}
	return nil
}

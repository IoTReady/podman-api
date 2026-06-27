package ingress

import (
	"context"
	"errors"
	"fmt"
	"log"

	"github.com/iotready/podman-api/internal/podman"
)

const (
	// caddyPodName is the globally-unique name of the per-host Caddy system pod.
	caddyPodName = "podman-api-ingress-caddy"
	// caddyDataVolume is the named volume backing ACME/cert data (must persist
	// across restarts so Let's Encrypt certs survive pod recreations).
	caddyDataVolume = "podman-api-caddy-data"
	// caddyAdminPort is the Caddy admin API port exposed as a hostPort so the
	// controller can reach it from the control plane.
	//
	// SECURITY: the admin API is unauthenticated and can replace Caddy's entire
	// running config. It is published on the host (see caddyPodYAML) so the
	// controller can reach it at the host's address, which means :2019 must be
	// kept on a trusted/private network (e.g. a tailnet) or firewalled to the
	// control plane — anyone who can reach it controls the host's TLS.
	caddyAdminPort = 2019
	// caddySchemaLabel/caddySchema mark the manifest generation of the managed
	// Caddy pod. ensureProxy recreates any pod missing the current value — e.g.
	// one created by a pre-admin-API release, which neither publishes :2019 nor
	// serves the admin API the controller now depends on.
	caddySchemaLabel = "podman-api.ingress.schema"
	caddySchema      = "admin-api"
)

// caddySeedCaddyfile renders the minimal global Caddyfile used to bootstrap a
// fresh managed Caddy pod. It enables the admin API on 0.0.0.0:2019 (so the
// controller can reach it from outside the pod), sets the ACME email if
// provided, and configures Caddy to auto-save runtime config to /data so
// `caddy run --resume` picks it up on restart.
//
// Routes are NOT seeded here; they are pushed via the admin API immediately
// after the pod is ready (see Reconcile).
func caddySeedCaddyfile(acmeEmail string) string {
	if acmeEmail != "" {
		return fmt.Sprintf("{\n\tadmin 0.0.0.0:%d\n\temail %s\n}\n", caddyAdminPort, acmeEmail)
	}
	return fmt.Sprintf("{\n\tadmin 0.0.0.0:%d\n}\n", caddyAdminPort)
}

// caddyPodYAML renders the kube manifest for the managed Caddy system pod.
// The pod:
//   - Seeds a minimal Caddyfile (admin-only, no routes) via CADDY_SEED env.
//     The boot script writes it only when absent so a live autosave from the
//     admin API survives container restarts (Caddy writes autosave to /data).
//   - Exposes :2019 as hostPort so the controller can call the admin API from
//     the control plane.
//   - Exposes :80 and :443 for HTTP/HTTPS traffic.
//   - Mounts a persistent /data volume for ACME cert storage.
func caddyPodYAML(image, acmeEmail string) string {
	seed := caddySeedCaddyfile(acmeEmail)
	cfgPath := "/etc/caddy/Caddyfile"
	// Boot: write seed only if no file exists (preserves admin-API autosave),
	// then run caddy. --resume picks up the autosaved JSON config from /data
	// on subsequent starts; fallback to the Caddyfile only on first boot.
	boot := fmt.Sprintf(
		`[ -f %s ] || printf '%%s' "$CADDY_SEED" > %s; exec caddy run --resume --config %s --adapter caddyfile`,
		cfgPath, cfgPath, cfgPath)
	return fmt.Sprintf(`apiVersion: v1
kind: Pod
metadata:
  name: %s
  labels:
    podman-api.ingress: caddy
    %s: %s
spec:
  containers:
    - name: caddy
      image: %s
      command: ["sh", "-c", %q]
      env:
        - name: CADDY_SEED
          value: %q
      ports:
        - containerPort: 80
          hostPort: 80
        - containerPort: 443
          hostPort: 443
        - containerPort: %d
          hostPort: %d
      volumeMounts:
        - name: data
          mountPath: /data
  volumes:
    - name: data
      persistentVolumeClaim:
        claimName: %s
`, caddyPodName, caddySchemaLabel, caddySchema, image, boot, seed, caddyAdminPort, caddyAdminPort, caddyDataVolume)
}

// ensureProxy makes the network + Caddy pod exist (and current) on host. It
// returns created=true when it (re)creates the pod and created=false when an
// up-to-date pod is already present. A pod from a pre-admin-API release is
// recreated with the current manifest — it lacks the published :2019 / admin
// API the controller now depends on; the persistent /data volume (and its ACME
// certs) survives the replace.
func (c *CaddyController) ensureProxy(ctx context.Context, host string) (bool, error) {
	if pod, err := c.client.PodInspect(ctx, host, caddyPodName); err == nil {
		if pod.Labels[caddySchemaLabel] == caddySchema {
			return false, nil // present and current
		}
		log.Printf("ingress: recreating stale caddy pod on %s (missing %s=%s)", host, caddySchemaLabel, caddySchema)
	} else if !errors.Is(err, podman.ErrNotFound) {
		return false, fmt.Errorf("ingress: inspect caddy pod: %w", err)
	}
	if err := c.client.NetworkEnsure(ctx, host, c.cfg.Network); err != nil {
		return false, fmt.Errorf("ingress: ensure network: %w", err)
	}
	if err := c.client.VolumeCreate(ctx, host, caddyDataVolume); err != nil {
		return false, fmt.Errorf("ingress: create data volume: %w", err)
	}
	// replace=true so a stale pod is torn down and recreated; for a brand-new
	// host there is nothing to replace.
	if err := c.client.PlayKube(ctx, host, caddyPodYAML(c.cfg.CaddyImage, c.cfg.ACMEEmail), true, c.cfg.Network); err != nil {
		return false, fmt.Errorf("ingress: play caddy pod: %w", err)
	}
	return true, nil
}

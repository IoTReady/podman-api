package ingress

import (
	"context"
	"errors"
	"fmt"

	"github.com/iotready/podman-api/internal/podman"
)

const (
	// caddyPodName is the globally-unique name of the per-host Caddy system pod.
	// It cannot collide with app pods, which are "<template>-<slug>".
	caddyPodName = "podman-api-ingress-caddy"
	// caddyContainer is the container name `podman kube play` assigns: the repo
	// convention (see instance.Service.Logs) is "<pod-name>-<container-name>",
	// so the pod's `name: caddy` container is reachable for exec/cp as
	// "podman-api-ingress-caddy-caddy", NOT the bare "caddy".
	caddyContainer = caddyPodName + "-caddy"
	// caddyConfigDir is where the Caddyfile lives inside the container.
	caddyConfigDir = "/etc/caddy"
	// caddyConfigFile is the Caddyfile name inside caddyConfigDir.
	caddyConfigFile = "Caddyfile"
	// caddyConfigVolume / caddyDataVolume are the named volumes backing the
	// config dir and the ACME/cert data (data must persist across restarts).
	caddyConfigVolume = "podman-api-caddy-config"
	caddyDataVolume   = "podman-api-caddy-data"
)

// caddyPodYAML renders the kube manifest for the Caddy system pod. PVC
// claimNames map to podman named volumes. hostPort publishes :80/:443 to the
// host (rootless: requires net.ipv4.ip_unprivileged_port_start <= 80).
//
// The container seeds its own config on first boot: a small sh wrapper writes
// caddyfile (passed through the CADDY_SEED env) to the config volume only when
// the file is absent, then execs caddy. On restart the (persistent) volume
// already holds the latest config — including anything a live `caddy reload`
// wrote — so the seed never clobbers it. This avoids the volume-import REST API,
// which podman only serves at >= 5.6.0; the env+wrapper works on any version.
func caddyPodYAML(image, caddyfile string) string {
	cfgPath := caddyConfigDir + "/" + caddyConfigFile
	boot := fmt.Sprintf(
		`[ -f %s ] || printf '%%s' "$CADDY_SEED" > %s; exec caddy run --config %s --adapter caddyfile`,
		cfgPath, cfgPath, cfgPath)
	return fmt.Sprintf(`apiVersion: v1
kind: Pod
metadata:
  name: %s
  labels:
    podman-api.ingress: caddy
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
      volumeMounts:
        - name: config
          mountPath: %s
        - name: data
          mountPath: /data
  volumes:
    - name: config
      persistentVolumeClaim:
        claimName: %s
    - name: data
      persistentVolumeClaim:
        claimName: %s
`, caddyPodName, image, boot, caddyfile, caddyConfigDir, caddyConfigVolume, caddyDataVolume)
}

// ensureProxy makes the network + Caddy pod exist on host. When it creates the
// pod it passes initialCaddyfile as the boot seed (see caddyPodYAML) so Caddy
// starts with a valid config, and returns created=true; when the pod already
// exists it does nothing and returns created=false. Reconcile uses created to
// decide whether a live cp+reload is needed.
func (c *CaddyController) ensureProxy(ctx context.Context, host, initialCaddyfile string) (bool, error) {
	if _, err := c.client.PodInspect(ctx, host, caddyPodName); err == nil {
		return false, nil // already present
	} else if !errors.Is(err, podman.ErrNotFound) {
		return false, fmt.Errorf("ingress: inspect caddy pod: %w", err)
	}
	if err := c.client.NetworkEnsure(ctx, host, c.cfg.Network); err != nil {
		return false, fmt.Errorf("ingress: ensure network: %w", err)
	}
	if err := c.client.VolumeCreate(ctx, host, caddyConfigVolume); err != nil {
		return false, fmt.Errorf("ingress: create config volume: %w", err)
	}
	if err := c.client.VolumeCreate(ctx, host, caddyDataVolume); err != nil {
		return false, fmt.Errorf("ingress: create data volume: %w", err)
	}
	// The Caddy pod seeds its own config from initialCaddyfile on first boot
	// (see caddyPodYAML), so we don't need the volume-import API (podman >=5.6.0).
	if err := c.client.PlayKube(ctx, host, caddyPodYAML(c.cfg.CaddyImage, initialCaddyfile), false, c.cfg.Network); err != nil {
		return false, fmt.Errorf("ingress: play caddy pod: %w", err)
	}
	return true, nil
}

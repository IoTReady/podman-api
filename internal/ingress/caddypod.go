package ingress

import (
	"archive/tar"
	"bytes"
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
func caddyPodYAML(image string) string {
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
      args: ["caddy", "run", "--config", "%s/%s", "--adapter", "caddyfile"]
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
`, caddyPodName, image, caddyConfigDir, caddyConfigFile, caddyConfigDir, caddyConfigVolume, caddyDataVolume)
}

// tarFile builds an uncompressed tar containing one file at `name`.
func tarFile(name string, content []byte) (*bytes.Buffer, error) {
	var buf bytes.Buffer
	tw := tar.NewWriter(&buf)
	if err := tw.WriteHeader(&tar.Header{Name: name, Mode: 0o644, Size: int64(len(content))}); err != nil {
		return nil, err
	}
	if _, err := tw.Write(content); err != nil {
		return nil, err
	}
	if err := tw.Close(); err != nil {
		return nil, err
	}
	return &buf, nil
}

// ensureProxy makes the network + Caddy pod exist on host. When it creates the
// pod it seeds the config volume with initialCaddyfile (so Caddy boots with a
// valid config) and returns created=true; when the pod already exists it does
// nothing and returns created=false. Reconcile uses created to decide whether a
// live cp+reload is needed.
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
	// Seed the config volume so Caddy's `caddy run --config` finds a valid file
	// on first boot. VolumeImport unpacks an uncompressed tar into the volume.
	seed, err := tarFile(caddyConfigFile, []byte(initialCaddyfile))
	if err != nil {
		return false, err
	}
	if err := c.client.VolumeImport(ctx, host, caddyConfigVolume, seed); err != nil {
		return false, fmt.Errorf("ingress: seed config volume: %w", err)
	}
	if err := c.client.PlayKube(ctx, host, caddyPodYAML(c.cfg.CaddyImage), false, c.cfg.Network); err != nil {
		return false, fmt.Errorf("ingress: play caddy pod: %w", err)
	}
	return true, nil
}

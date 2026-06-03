package podman

import (
	"context"
	"errors"
	"io"
)

// Client is the contract every consumer of podman speaks. The real
// implementation calls libpod via SSH-tunnelled or unix-socket connections;
// tests use the in-memory fake under ./fake.
type Client interface {
	// Pods
	PlayKube(ctx context.Context, hostID, yaml string, replace bool) error
	PodInspect(ctx context.Context, hostID, name string) (Pod, error)
	PodList(ctx context.Context, hostID string, labelFilters map[string]string) ([]Pod, error)
	PodStart(ctx context.Context, hostID, name string) error
	PodStop(ctx context.Context, hostID, name string) error
	PodRestart(ctx context.Context, hostID, name string) error
	PodRemove(ctx context.Context, hostID, name string, force bool) error

	// Secrets
	SecretCreate(ctx context.Context, hostID, name string, value []byte) error
	SecretList(ctx context.Context, hostID string) ([]Secret, error)
	SecretInspect(ctx context.Context, hostID, name string) (Secret, error)
	SecretRemove(ctx context.Context, hostID, name string) error

	// Volumes
	VolumeInspect(ctx context.Context, hostID, name string) (Volume, error)
	VolumeRemove(ctx context.Context, hostID, name string, force bool) error
	// VolumeExport streams the named volume's contents from host as an
	// uncompressed tar. The caller must Close the returned reader.
	VolumeExport(ctx context.Context, hostID, name string) (io.ReadCloser, error)
	// VolumeImport unpacks an uncompressed tar (as produced by VolumeExport)
	// into the named volume on host. The volume must already exist.
	VolumeImport(ctx context.Context, hostID, name string, r io.Reader) error

	// Logs
	ContainerLogs(ctx context.Context, hostID, container string, opts LogOptions) (<-chan LogLine, error)

	// Images
	ImagePull(ctx context.Context, hostID, ref string) error

	// Health
	Ping(ctx context.Context, hostID string) error
	Version(ctx context.Context, hostID string) (string, error)
	UsedHostPorts(ctx context.Context, hostID string) ([]PortMapping, error)

	// Host
	HostInfo(ctx context.Context, hostID string) (HostInfo, error)
}

// LogOptions are the knobs for ContainerLogs.
type LogOptions struct {
	Tail   int    // 0 = all
	Since  string // RFC3339 or duration like "5m"; "" = beginning
	Follow bool
}

// ErrNotFound is returned when a pod, container, secret, or volume isn't present.
var ErrNotFound = errors.New("podman: not found")

package podman

import (
	"context"
	"errors"
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

	// Logs
	ContainerLogs(ctx context.Context, hostID, container string, opts LogOptions) (<-chan LogLine, error)

	// Images
	ImagePull(ctx context.Context, hostID, ref string) error

	// Health
	Ping(ctx context.Context, hostID string) error
	Version(ctx context.Context, hostID string) (string, error)
	UsedHostPorts(ctx context.Context, hostID string) ([]PortMapping, error)
}

// LogOptions are the knobs for ContainerLogs.
type LogOptions struct {
	Tail   int    // 0 = all
	Since  string // RFC3339 or duration like "5m"; "" = beginning
	Follow bool
}

// ErrNotFound is returned when a pod, container, secret, or volume isn't present.
var ErrNotFound = errors.New("podman: not found")

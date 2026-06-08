package podman

import (
	"context"
	"errors"
	"io"

	"github.com/iotready/podman-api/internal/config"
)

// Client is the contract every consumer of podman speaks. The real
// implementation calls libpod via SSH-tunnelled or unix-socket connections;
// tests use the in-memory fake under ./fake.
type Client interface {
	// Pods
	PlayKube(ctx context.Context, hostID, yaml string, replace bool, networks ...string) error
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
	// VolumeCreate creates an empty named volume on host. Creating a volume that
	// already exists is a no-op (no error).
	VolumeCreate(ctx context.Context, hostID, name string) error

	// Networks
	// NetworkEnsure creates the named network if absent; creating one that
	// already exists is a no-op (no error).
	NetworkEnsure(ctx context.Context, hostID, name string) error

	// Exec
	// ContainerExec runs cmd in the named running container and returns its
	// exit code and combined stdout+stderr. A non-zero exit code is NOT an
	// error; only a transport/podman failure returns a non-nil error.
	ContainerExec(ctx context.Context, hostID, container string, cmd []string) (ExecResult, error)
	// CopyToContainer writes content as a single file `name` into directory
	// `destDir` inside the running container (e.g. destDir="/etc/caddy",
	// name="Caddyfile"). destDir must already exist in the container.
	CopyToContainer(ctx context.Context, hostID, container, destDir, name string, content []byte) error

	// Logs
	ContainerLogs(ctx context.Context, hostID, container string, opts LogOptions) (<-chan LogLine, error)

	// Images
	ImagePull(ctx context.Context, hostID, ref string) error

	// Prune
	// ImagePrune removes unused images. all=false removes only dangling layers;
	// all=true also removes tagged images not used by any container.
	ImagePrune(ctx context.Context, hostID string, all bool) (PruneReport, error)
	// ContainerPrune removes stopped (exited) containers.
	ContainerPrune(ctx context.Context, hostID string) (PruneReport, error)
	// BuildCachePrune removes dangling build cache.
	BuildCachePrune(ctx context.Context, hostID string) (PruneReport, error)
	// VolumePrune removes unused (unattached) volumes. filters are libpod volume
	// prune filters (e.g. {"label!": {"podman-api.protect=true"}}) so callers can
	// protect volumes; never removes in-use volumes.
	VolumePrune(ctx context.Context, hostID string, filters map[string][]string) (PruneReport, error)

	// Health
	Ping(ctx context.Context, hostID string) error
	Version(ctx context.Context, hostID string) (string, error)
	UsedHostPorts(ctx context.Context, hostID string) ([]PortMapping, error)

	// Host
	HostInfo(ctx context.Context, hostID string) (HostInfo, error)
	// Knows reports whether hostID is a registered host this client can reach.
	// The host set is updated at construction and whenever SetHosts is called
	// (e.g. after a SIGHUP host-config reload).
	Knows(hostID string) bool
	// SetHosts replaces the client's host map at runtime. New hosts become
	// connectable; removed hosts are dropped (and their cached connections
	// cleaned up); changed connection params (addr/socket/ssh_key) invalidate
	// the cached context so the next call reopens the connection.
	SetHosts(hosts []config.Host)
}

// ExecResult is the outcome of ContainerExec.
type ExecResult struct {
	ExitCode int
	Output   string // combined stdout+stderr
}

// PruneReport summarizes one prune operation: the ids/names removed and the
// bytes reclaimed (sum of per-item sizes).
type PruneReport struct {
	Items     []string
	Reclaimed int64
}

// LogOptions are the knobs for ContainerLogs.
type LogOptions struct {
	Tail   int    // 0 = all
	Since  string // RFC3339 or duration like "5m"; "" = beginning
	Follow bool
}

// ErrNotFound is returned when a pod, container, secret, or volume isn't present.
var ErrNotFound = errors.New("podman: not found")

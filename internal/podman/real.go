package podman

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/containers/podman/v5/pkg/bindings"
	"github.com/containers/podman/v5/pkg/bindings/containers"
	"github.com/containers/podman/v5/pkg/bindings/images"
	"github.com/containers/podman/v5/pkg/bindings/play"
	"github.com/containers/podman/v5/pkg/bindings/pods"
	"github.com/containers/podman/v5/pkg/bindings/secrets"
	"github.com/containers/podman/v5/pkg/bindings/system"
	"github.com/containers/podman/v5/pkg/bindings/volumes"
	"github.com/containers/podman/v5/pkg/domain/entities"

	"github.com/iotready/podman-api/internal/config"
)

// Real is the production podman.Client implementation backed by libpod
// over SSH (production) or a local unix socket (dev).
//
// Per-host context.Contexts are created lazily and cached. The bindings
// library stores the underlying connection on the context, so callers must
// pass the cached context (returned by ctxFor) to every libpod call.
//
// Uses github.com/containers/podman/v5 v5.8.2.
// bindings.NewConnection signature: func(ctx context.Context, uri string) (context.Context, error)
type Real struct {
	hosts map[string]config.Host

	mu  sync.Mutex
	ctx map[string]context.Context // hostID -> connection-bearing ctx
}

// NewReal validates host configs and registers them. Connections are not
// opened here; first use opens them.
func NewReal(hosts []config.Host) (*Real, error) {
	r := &Real{hosts: map[string]config.Host{}, ctx: map[string]context.Context{}}
	for _, h := range hosts {
		if h.ID == "" {
			return nil, fmt.Errorf("host with empty id")
		}
		if _, dup := r.hosts[h.ID]; dup {
			return nil, fmt.Errorf("duplicate host id %q", h.ID)
		}
		r.hosts[h.ID] = h
	}
	return r, nil
}

// Knows reports whether the host is registered.
func (r *Real) Knows(id string) bool {
	_, ok := r.hosts[id]
	return ok
}

// URIFor returns the libpod URI for hostID. unix-only when addr=="unix".
func (r *Real) URIFor(id string) (string, error) {
	h, ok := r.hosts[id]
	if !ok {
		return "", fmt.Errorf("unknown host %q", id)
	}
	if h.Addr == "unix" {
		return "unix://" + h.Socket, nil
	}
	return "ssh://" + h.Addr + h.Socket, nil
}

// ctxFor returns a libpod-ready context for hostID, opening the connection
// on first use.
func (r *Real) ctxFor(parent context.Context, id string) (context.Context, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if c, ok := r.ctx[id]; ok {
		return c, nil
	}
	h, ok := r.hosts[id]
	if !ok {
		return nil, fmt.Errorf("unknown host %q", id)
	}
	uri, err := r.URIFor(id)
	if err != nil {
		return nil, err
	}
	var c context.Context
	if h.Addr != "unix" && h.SSHKey != "" {
		// SSH host with explicit key file. The fourth arg (`machine`) is false
		// for non-machine connections per the bindings API.
		c, err = bindings.NewConnectionWithIdentity(parent, uri, h.SSHKey, false)
	} else {
		c, err = bindings.NewConnection(parent, uri)
	}
	if err != nil {
		return nil, fmt.Errorf("connect to host %q: %w", id, err)
	}
	r.ctx[id] = c
	return c, nil
}

func (r *Real) Ping(ctx context.Context, id string) error {
	c, err := r.ctxFor(ctx, id)
	if err != nil {
		return err
	}
	_, err = system.Info(c, &system.InfoOptions{})
	return err
}

func (r *Real) Version(ctx context.Context, id string) (string, error) {
	c, err := r.ctxFor(ctx, id)
	if err != nil {
		return "", err
	}
	info, err := system.Info(c, &system.InfoOptions{})
	if err != nil {
		return "", err
	}
	return info.Version.Version, nil
}

func (r *Real) PlayKube(ctx context.Context, id, raw string, replace bool) error {
	c, err := r.ctxFor(ctx, id)
	if err != nil {
		return err
	}
	tmp, err := os.CreateTemp("", "play-kube-*.yaml")
	if err != nil {
		return err
	}
	defer os.Remove(tmp.Name())
	if _, err := tmp.WriteString(raw); err != nil {
		return err
	}
	tmp.Close()

	opts := &play.KubeOptions{}
	if replace {
		t := true
		opts.Replace = &t
	}
	_, err = play.Kube(c, tmp.Name(), opts)
	return err
}

func (r *Real) PodInspect(ctx context.Context, id, name string) (Pod, error) {
	c, err := r.ctxFor(ctx, id)
	if err != nil {
		return Pod{}, err
	}
	rep, err := pods.Inspect(c, name, &pods.InspectOptions{})
	if err != nil {
		if isNotFound(err) {
			return Pod{}, ErrNotFound
		}
		return Pod{}, err
	}
	return podFromInspect(rep), nil
}

func (r *Real) PodList(ctx context.Context, id string, filters map[string]string) ([]Pod, error) {
	c, err := r.ctxFor(ctx, id)
	if err != nil {
		return nil, err
	}
	opts := &pods.ListOptions{}
	if len(filters) > 0 {
		f := map[string][]string{}
		for k, v := range filters {
			f["label"] = append(f["label"], k+"="+v)
		}
		opts.Filters = f
	}
	reps, err := pods.List(c, opts)
	if err != nil {
		return nil, err
	}
	out := make([]Pod, 0, len(reps))
	for _, rep := range reps {
		out = append(out, podFromList(rep))
	}
	return out, nil
}

func (r *Real) PodStart(ctx context.Context, id, name string) error {
	c, err := r.ctxFor(ctx, id)
	if err != nil {
		return err
	}
	_, err = pods.Start(c, name, &pods.StartOptions{})
	return mapNotFound(err)
}

func (r *Real) PodStop(ctx context.Context, id, name string) error {
	c, err := r.ctxFor(ctx, id)
	if err != nil {
		return err
	}
	_, err = pods.Stop(c, name, &pods.StopOptions{})
	return mapNotFound(err)
}

func (r *Real) PodRestart(ctx context.Context, id, name string) error {
	c, err := r.ctxFor(ctx, id)
	if err != nil {
		return err
	}
	_, err = pods.Restart(c, name, &pods.RestartOptions{})
	return mapNotFound(err)
}

func (r *Real) PodRemove(ctx context.Context, id, name string, force bool) error {
	c, err := r.ctxFor(ctx, id)
	if err != nil {
		return err
	}
	opts := &pods.RemoveOptions{}
	if force {
		t := true
		opts.Force = &t
	}
	_, err = pods.Remove(c, name, opts)
	return mapNotFound(err)
}

func (r *Real) SecretCreate(ctx context.Context, id, name string, value []byte) error {
	c, err := r.ctxFor(ctx, id)
	if err != nil {
		return err
	}
	opts := &secrets.CreateOptions{Name: &name}
	_, err = secrets.Create(c, bytes.NewReader(value), opts)
	return err
}

func (r *Real) SecretList(ctx context.Context, id string) ([]Secret, error) {
	c, err := r.ctxFor(ctx, id)
	if err != nil {
		return nil, err
	}
	reps, err := secrets.List(c, &secrets.ListOptions{})
	if err != nil {
		return nil, err
	}
	out := make([]Secret, 0, len(reps))
	for _, s := range reps {
		out = append(out, Secret{Name: s.Spec.Name, CreatedAt: s.CreatedAt})
	}
	return out, nil
}

func (r *Real) SecretInspect(ctx context.Context, id, name string) (Secret, error) {
	c, err := r.ctxFor(ctx, id)
	if err != nil {
		return Secret{}, err
	}
	rep, err := secrets.Inspect(c, name, &secrets.InspectOptions{})
	if err != nil {
		return Secret{}, mapNotFound(err)
	}
	return Secret{Name: rep.Spec.Name, CreatedAt: rep.CreatedAt}, nil
}

func (r *Real) SecretRemove(ctx context.Context, id, name string) error {
	c, err := r.ctxFor(ctx, id)
	if err != nil {
		return err
	}
	return mapNotFound(secrets.Remove(c, name))
}

func (r *Real) VolumeInspect(ctx context.Context, id, name string) (Volume, error) {
	c, err := r.ctxFor(ctx, id)
	if err != nil {
		return Volume{}, err
	}
	rep, err := volumes.Inspect(c, name, &volumes.InspectOptions{})
	if err != nil {
		return Volume{}, mapNotFound(err)
	}
	v := Volume{Name: rep.Name}
	// Size is not always populated; leave at 0 if missing.
	return v, nil
}

func (r *Real) VolumeRemove(ctx context.Context, id, name string, force bool) error {
	c, err := r.ctxFor(ctx, id)
	if err != nil {
		return err
	}
	opts := &volumes.RemoveOptions{}
	if force {
		t := true
		opts.Force = &t
	}
	return mapNotFound(volumes.Remove(c, name, opts))
}

// --- helpers ---

func isNotFound(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "no such pod") ||
		strings.Contains(msg, "no such container") ||
		strings.Contains(msg, "no such secret") ||
		strings.Contains(msg, "no such volume") ||
		strings.Contains(msg, "not found")
}

func mapNotFound(err error) error {
	if isNotFound(err) {
		return ErrNotFound
	}
	return err
}

func podFromInspect(p *entities.PodInspectReport) Pod {
	out := Pod{
		ID:     p.ID,
		Name:   p.Name,
		Status: p.State,
		Labels: p.Labels,
	}
	if !p.Created.IsZero() {
		out.Created = p.Created
	}
	for _, c := range p.Containers {
		out.Containers = append(out.Containers, Container{
			ID:     c.ID,
			Name:   c.Name,
			Status: c.State,
		})
	}
	return out
}

func podFromList(p *entities.ListPodsReport) Pod {
	out := Pod{
		ID:     p.Id,
		Name:   p.Name,
		Status: p.Status,
		Labels: p.Labels,
	}
	if !p.Created.IsZero() {
		out.Created = p.Created
	}
	for _, c := range p.Containers {
		out.Containers = append(out.Containers, Container{
			ID:     c.Id,
			Name:   c.Names,
			Status: c.Status,
		})
	}
	return out
}

func (r *Real) ContainerLogs(ctx context.Context, id, container string, opts LogOptions) (<-chan LogLine, error) {
	c, err := r.ctxFor(ctx, id)
	if err != nil {
		return nil, err
	}
	stdoutCh := make(chan string, 64)
	stderrCh := make(chan string, 64)
	out := make(chan LogLine, 64)

	go func() {
		defer close(out)
		for {
			select {
			case line, ok := <-stdoutCh:
				if !ok {
					stdoutCh = nil
				} else {
					out <- LogLine{Container: container, Stream: "stdout", Line: line, Time: time.Now()}
				}
			case line, ok := <-stderrCh:
				if !ok {
					stderrCh = nil
				} else {
					out <- LogLine{Container: container, Stream: "stderr", Line: line, Time: time.Now()}
				}
			}
			if stdoutCh == nil && stderrCh == nil {
				return
			}
		}
	}()

	tail := ""
	if opts.Tail > 0 {
		tail = strconv.Itoa(opts.Tail)
	}
	follow := opts.Follow
	logsOpts := &containers.LogOptions{
		Stdout: boolPtr(true), Stderr: boolPtr(true),
		Follow: &follow, Tail: &tail, Since: &opts.Since,
	}
	go func() {
		_ = containers.Logs(c, container, logsOpts, stdoutCh, stderrCh)
		close(stdoutCh)
		close(stderrCh)
	}()
	return out, nil
}

func (r *Real) ImagePull(ctx context.Context, id, ref string) error {
	c, err := r.ctxFor(ctx, id)
	if err != nil {
		return err
	}
	_, err = images.Pull(c, ref, &images.PullOptions{})
	return err
}

func (r *Real) UsedHostPorts(ctx context.Context, id string) ([]PortMapping, error) {
	c, err := r.ctxFor(ctx, id)
	if err != nil {
		return nil, err
	}
	all := true
	conts, err := containers.List(c, &containers.ListOptions{All: &all})
	if err != nil {
		return nil, err
	}
	var out []PortMapping
	for _, ct := range conts {
		for _, p := range ct.Ports {
			out = append(out, PortMapping{
				HostIP:        p.HostIP,
				HostPort:      int(p.HostPort),
				ContainerPort: int(p.ContainerPort),
				Protocol:      p.Protocol,
			})
		}
	}
	return out, nil
}

func boolPtr(b bool) *bool { return &b }

// Compile-time guarantee that Real satisfies the Client interface.
var _ Client = (*Real)(nil)

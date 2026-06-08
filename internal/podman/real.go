package podman

import (
	"archive/tar"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"math"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/containers/podman/v5/libpod/define"
	handlers "github.com/containers/podman/v5/pkg/api/handlers"
	"github.com/containers/podman/v5/pkg/bindings"
	"github.com/containers/podman/v5/pkg/bindings/containers"
	"github.com/containers/podman/v5/pkg/bindings/images"
	network "github.com/containers/podman/v5/pkg/bindings/network"
	"github.com/containers/podman/v5/pkg/bindings/play"
	"github.com/containers/podman/v5/pkg/bindings/pods"
	"github.com/containers/podman/v5/pkg/bindings/secrets"
	"github.com/containers/podman/v5/pkg/bindings/system"
	"github.com/containers/podman/v5/pkg/bindings/volumes"
	"github.com/containers/podman/v5/pkg/domain/entities"
	"github.com/containers/podman/v5/pkg/domain/entities/reports"
	dockerContainer "github.com/docker/docker/api/types/container"
	nettypes "go.podman.io/common/libnetwork/types"

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

	mu       sync.Mutex
	ctx      map[string]context.Context // hostID -> connection-bearing ctx
	verified map[string]bool            // hostID -> passed the MinPodmanVersion check

	// versionProbe overrides the version lookup in tests; nil means
	// system.Info over the supplied connection ctx.
	versionProbe func(context.Context) (string, error)
}

// NewReal validates host configs and registers them. Connections are not
// opened here; first use opens them.
func NewReal(hosts []config.Host) (*Real, error) {
	r := &Real{hosts: map[string]config.Host{}, ctx: map[string]context.Context{}, verified: map[string]bool{}}
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
	r.mu.Lock()
	defer r.mu.Unlock()
	_, ok := r.hosts[id]
	return ok
}

// SetHosts replaces the client's host map at runtime, diff-invalidating cached
// connections so that:
//   - newly added hosts become connectable on first use (lazy ctxFor);
//   - removed hosts are dropped (cached context garbage-collected);
//   - hosts whose addr/socket/ssh_key changed get their cached connection and
//     verified flag cleared so the next call reopens with the new params;
//   - unchanged hosts keep their live cached connection.
func (r *Real) SetHosts(hosts []config.Host) {
	newMap := make(map[string]config.Host, len(hosts))
	for _, h := range hosts {
		if h.ID != "" {
			newMap[h.ID] = h
		}
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	// Remove hosts that were deleted.
	for id := range r.hosts {
		if _, keep := newMap[id]; !keep {
			delete(r.hosts, id)
			delete(r.ctx, id)
			delete(r.verified, id)
		}
	}

	// Add or update hosts.
	for id, h := range newMap {
		if old, exists := r.hosts[id]; exists && !hostConnEq(old, h) {
			// Connection params changed: invalidate cached state.
			delete(r.ctx, id)
			delete(r.verified, id)
		}
		r.hosts[id] = h
	}
}

// hostConnEq reports whether two host configs share the same connection
// parameters (address, socket path, SSH key). Other fields (Drain, Labels,
// Prune, etc.) do NOT affect the cached podman connection.
func hostConnEq(a, b config.Host) bool {
	return a.Addr == b.Addr && a.Socket == b.Socket && a.SSHKey == b.SSHKey
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
// on first use. The connection context is rooted at context.Background() so
// it outlives any individual request context — per-request cancellation must
// not kill the cached long-lived connection.
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
	// Use context.Background() — not the caller's per-request context — so
	// the cached connection context is never cancelled by a request ending.
	// Individual operations still honour per-call cancellation because
	// DoRequest creates http.NewRequestWithContext(ctx, ...) from the
	// per-call context passed into each method (PodInspect, PodList, etc.).
	connBase := context.Background()
	var c context.Context
	if h.Addr != "unix" && h.SSHKey != "" {
		// SSH host with explicit key file. The fourth arg (`machine`) is false
		// for non-machine connections per the bindings API.
		c, err = bindings.NewConnectionWithIdentity(connBase, uri, h.SSHKey, false)
	} else {
		c, err = bindings.NewConnection(connBase, uri)
	}
	if err != nil {
		return nil, fmt.Errorf("connect to host %q: %w", id, err)
	}
	r.ctx[id] = c
	return c, nil
}

// probeVersion fetches the podman version over an established connection ctx.
func (r *Real) probeVersion(c context.Context) (string, error) {
	if r.versionProbe != nil {
		return r.versionProbe(c)
	}
	info, err := system.Info(c, &system.InfoOptions{})
	if err != nil {
		return "", err
	}
	return info.Version.Version, nil
}

// ensureVerified enforces MinPodmanVersion once per host per process. On
// failure the host stays unverified (and the connection stays cached), so a
// host whose podman is upgraded in place starts passing without a restart.
// The probe runs outside the mutex; a concurrent duplicate probe is harmless.
func (r *Real) ensureVerified(c context.Context, id string) error {
	r.mu.Lock()
	ok := r.verified[id]
	r.mu.Unlock()
	if ok {
		return nil
	}
	v, err := r.probeVersion(c)
	if err != nil {
		return fmt.Errorf("verify host %q podman version: %w", id, err)
	}
	if err := checkVersion(id, v); err != nil {
		return err
	}
	r.mu.Lock()
	r.verified[id] = true
	r.mu.Unlock()
	return nil
}

// opCtxFor is ctxFor plus the MinPodmanVersion gate. Operation methods must
// call this instead of ctxFor; diagnostics (Ping, Version, HostInfo) use raw
// ctxFor so GET /hosts can still display an unsupported host's version (#85).
func (r *Real) opCtxFor(parent context.Context, id string) (context.Context, error) {
	c, err := r.ctxFor(parent, id)
	if err != nil {
		return nil, err
	}
	if err := r.ensureVerified(c, id); err != nil {
		return nil, err
	}
	return c, nil
}

// preflightTimeout bounds each host's boot-time connect+version probe.
// var (not const) so tests can shrink it.
var preflightTimeout = 10 * time.Second

// Preflight enforces MinPodmanVersion at boot. ALL reachable hosts are checked
// and any below the floor are collected; the returned error aggregates every
// offender via errors.Join so operators see every problem in a single boot
// attempt (main treats it as fatal — the daemon refuses to start). An
// unreachable or slow host is logged and left unverified; the check re-runs on
// its first successful connect (opCtxFor), so a down-at-boot old host still
// cannot sneak in. See #85.
func (r *Real) Preflight(ctx context.Context) error {
	var errs []error
	for id := range r.hosts {
		if err := r.preflightHost(ctx, id); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

func (r *Real) preflightHost(ctx context.Context, id string) error {
	type result struct {
		version string
		err     error
	}
	ch := make(chan result, 1)
	// The dial cannot take a deadline: ctxFor deliberately roots cached
	// connections at context.Background() (per-request cancellation must not
	// kill them), so the attempt is bounded externally. On timeout the
	// goroutine is abandoned; if it completes later it merely caches a usable
	// connection for first use — caching grants no unverified access, it only
	// avoids a second dial (opCtxFor still runs the version gate).
	go func() {
		c, err := r.ctxFor(ctx, id)
		if err != nil {
			ch <- result{err: err}
			return
		}
		v, err := r.probeVersion(c)
		ch <- result{version: v, err: err}
	}()
	select {
	case res := <-ch:
		if res.err != nil {
			log.Printf("preflight: host %q unreachable, podman version check deferred to first use: %v", id, res.err)
			return nil
		}
		if err := checkVersion(id, res.version); err != nil {
			return err
		}
		r.mu.Lock()
		r.verified[id] = true
		r.mu.Unlock()
		log.Printf("preflight: host %q podman %s ok (>= %s)", id, res.version, MinPodmanVersion)
		return nil
	case <-time.After(preflightTimeout):
		log.Printf("preflight: host %q did not answer within %s, podman version check deferred to first use", id, preflightTimeout)
		return nil
	case <-ctx.Done():
		log.Printf("preflight: host %q check cancelled, podman version check deferred to first use: %v", id, ctx.Err())
		return nil
	}
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
	return r.probeVersion(c)
}

func (r *Real) PlayKube(ctx context.Context, id, raw string, replace bool, networks ...string) error {
	c, err := r.opCtxFor(ctx, id)
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
	if len(networks) > 0 {
		opts.Network = &networks
	}
	_, err = play.Kube(c, tmp.Name(), opts)
	return err
}

func (r *Real) PodInspect(ctx context.Context, id, name string) (Pod, error) {
	c, err := r.opCtxFor(ctx, id)
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
	out := podFromInspect(rep)
	// Enrich each container with full inspect data (image, ports, env, etc.).
	for i := range out.Containers {
		full, err := containers.Inspect(c, out.Containers[i].ID, &containers.InspectOptions{})
		if err == nil {
			enrichContainer(&out.Containers[i], full)
		}
	}
	return out, nil
}

// enrichContainer fills in Container fields that are not available from the
// pod inspect report alone.
func enrichContainer(c *Container, ins *define.InspectContainerData) {
	c.Image = ins.ImageDigest
	if c.Image == "" {
		c.Image = ins.Image
	}
	c.ImageTag = ins.ImageName
	if ins.State != nil && !ins.State.StartedAt.IsZero() {
		c.StartedAt = ins.State.StartedAt
	}
	if ins.State != nil && ins.State.Health != nil {
		c.Health = ins.State.Health.Status
	}
	c.RestartCount = int(ins.RestartCount)
	if ins.HostConfig != nil {
		// PortBindings maps "<containerPort>/<protocol>" -> []HostPort, so
		// the container port and protocol live in the map key.
		for key, ports := range ins.HostConfig.PortBindings {
			cp, proto := splitPortKey(key)
			for _, b := range ports {
				hp, _ := strconv.Atoi(b.HostPort)
				c.Ports = append(c.Ports, PortMapping{
					HostIP:        b.HostIP,
					HostPort:      hp,
					ContainerPort: cp,
					Protocol:      proto,
				})
			}
		}
	}
	if ins.Config != nil && ins.Config.Env != nil {
		c.Env = make(map[string]string, len(ins.Config.Env))
		for _, e := range ins.Config.Env {
			eq := strings.IndexByte(e, '=')
			if eq > 0 {
				c.Env[e[:eq]] = e[eq+1:]
			}
		}
	}
}

func (r *Real) PodList(ctx context.Context, id string, filters map[string]string) ([]Pod, error) {
	c, err := r.opCtxFor(ctx, id)
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
		p := podFromList(rep)
		// Enrich each container with full inspect data so list and get return
		// the same shape (image_tag, started_at, ports, env, etc.). Skip on
		// per-container error — partial data beats failing the whole list.
		for i := range p.Containers {
			full, err := containers.Inspect(c, p.Containers[i].ID, &containers.InspectOptions{})
			if err == nil {
				enrichContainer(&p.Containers[i], full)
			}
		}
		out = append(out, p)
	}
	return out, nil
}

func (r *Real) PodStart(ctx context.Context, id, name string) error {
	c, err := r.opCtxFor(ctx, id)
	if err != nil {
		return err
	}
	_, err = pods.Start(c, name, &pods.StartOptions{})
	return mapNotFound(err)
}

func (r *Real) PodStop(ctx context.Context, id, name string) error {
	c, err := r.opCtxFor(ctx, id)
	if err != nil {
		return err
	}
	_, err = pods.Stop(c, name, &pods.StopOptions{})
	return mapNotFound(err)
}

func (r *Real) PodRestart(ctx context.Context, id, name string) error {
	c, err := r.opCtxFor(ctx, id)
	if err != nil {
		return err
	}
	_, err = pods.Restart(c, name, &pods.RestartOptions{})
	return mapNotFound(err)
}

func (r *Real) PodRemove(ctx context.Context, id, name string, force bool) error {
	c, err := r.opCtxFor(ctx, id)
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
	c, err := r.opCtxFor(ctx, id)
	if err != nil {
		return err
	}
	opts := &secrets.CreateOptions{Name: &name}
	_, err = secrets.Create(c, bytes.NewReader(value), opts)
	return err
}

func (r *Real) SecretList(ctx context.Context, id string) ([]Secret, error) {
	c, err := r.opCtxFor(ctx, id)
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
	c, err := r.opCtxFor(ctx, id)
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
	c, err := r.opCtxFor(ctx, id)
	if err != nil {
		return err
	}
	return mapNotFound(secrets.Remove(c, name))
}

func (r *Real) VolumeInspect(ctx context.Context, id, name string) (Volume, error) {
	c, err := r.opCtxFor(ctx, id)
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
	c, err := r.opCtxFor(ctx, id)
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

// VolumeExport streams a volume's contents as an uncompressed tar. The returned
// reader is the live HTTP response body; the caller must Close it. We issue the
// REST request directly rather than using the high-level volumes.Export binding
// because that binding copies into an io.Writer, whereas our contract must hand
// back a live io.ReadCloser for pipe streaming.
func (r *Real) VolumeExport(ctx context.Context, id, name string) (io.ReadCloser, error) {
	c, err := r.opCtxFor(ctx, id)
	if err != nil {
		return nil, err
	}
	conn, err := bindings.GetClient(c)
	if err != nil {
		return nil, err
	}
	resp, err := conn.DoRequest(c, nil, http.MethodGet, "/volumes/%s/export", nil, nil, name)
	if err != nil {
		return nil, err
	}
	if !resp.IsSuccess() {
		defer resp.Body.Close()
		// Process(nil) drains the body and returns podman's error for non-2xx.
		return nil, mapNotFound(resp.Process(nil))
	}
	return resp.Body, nil
}

// VolumeImport unpacks an uncompressed tar into an existing volume on the host.
func (r *Real) VolumeImport(ctx context.Context, id, name string, src io.Reader) error {
	c, err := r.opCtxFor(ctx, id)
	if err != nil {
		return err
	}
	return mapNotFound(volumes.Import(c, name, src))
}

// VolumeCreate creates an empty named volume. An already-existing name is
// treated as success so migrate's create-then-copy step is idempotent on retry.
func (r *Real) VolumeCreate(ctx context.Context, id, name string) error {
	c, err := r.opCtxFor(ctx, id)
	if err != nil {
		return err
	}
	if _, err := volumes.Create(c, entities.VolumeCreateOptions{Name: name}, nil); err != nil {
		if strings.Contains(strings.ToLower(err.Error()), "already exists") {
			return nil
		}
		return err
	}
	return nil
}

// NetworkEnsure creates the named network if absent, with aardvark DNS enabled.
//
// DNS must be on: the `podman network create` CLI defaults it true, but the REST
// API does not, and ingress backend routing resolves pods by name on this
// network — without DNS the proxy can't reach the backend (502).
//
// An existing network is only accepted if its DNS is already on. A network left
// by a pre-DNS build has it off, and DNS can't be flipped on an existing network
// via the API, so we fail with the one-time fix instead of silently keeping it
// disabled (which IgnoreIfExists would do).
func (r *Real) NetworkEnsure(ctx context.Context, id, name string) error {
	c, err := r.opCtxFor(ctx, id)
	if err != nil {
		return err
	}
	if exists, err := network.Exists(c, name, nil); err != nil {
		return err
	} else if exists {
		rep, err := network.Inspect(c, name, nil)
		if err != nil {
			return err
		}
		if !rep.DNSEnabled {
			return fmt.Errorf("network %q exists with DNS disabled (from an older build); remove it and retry: podman network rm %s", name, name)
		}
		return nil
	}
	// IgnoreIfExists guards the create against a concurrent ensure racing in
	// after the Exists check above.
	ignore := true
	_, err = network.CreateWithOptions(c, &nettypes.Network{Name: name, DNSEnabled: true},
		&network.ExtraCreateOptions{IgnoreIfExists: &ignore})
	return err
}

func (r *Real) ContainerExec(ctx context.Context, id, container string, cmd []string) (ExecResult, error) {
	c, err := r.opCtxFor(ctx, id)
	if err != nil {
		return ExecResult{}, err
	}
	sessionID, err := containers.ExecCreate(c, container, &handlers.ExecCreateConfig{
		ExecOptions: dockerContainer.ExecOptions{
			Cmd:          cmd,
			AttachStdout: true,
			AttachStderr: true,
		},
	})
	if err != nil {
		return ExecResult{}, mapNotFound(err)
	}
	var buf bytes.Buffer
	var w io.Writer = &buf
	attach := true
	if err := containers.ExecStartAndAttach(c, sessionID, &containers.ExecStartAndAttachOptions{
		OutputStream: &w,
		ErrorStream:  &w,
		AttachOutput: &attach,
		AttachError:  &attach,
	}); err != nil {
		return ExecResult{}, err
	}
	ins, err := containers.ExecInspect(c, sessionID, &containers.ExecInspectOptions{})
	if err != nil {
		return ExecResult{}, err
	}
	return ExecResult{ExitCode: ins.ExitCode, Output: buf.String()}, nil
}

func (r *Real) CopyToContainer(ctx context.Context, id, container, destDir, name string, content []byte) error {
	c, err := r.opCtxFor(ctx, id)
	if err != nil {
		return err
	}
	var tarBuf bytes.Buffer
	tw := tar.NewWriter(&tarBuf)
	if err := tw.WriteHeader(&tar.Header{
		Name: name,
		Mode: 0o644,
		Size: int64(len(content)),
	}); err != nil {
		return err
	}
	if _, err := tw.Write(content); err != nil {
		return err
	}
	if err := tw.Close(); err != nil {
		return err
	}
	// CopyFromArchive copies INTO the container (PUT /containers/{id}/archive).
	copyFn, err := containers.CopyFromArchive(c, container, destDir, &tarBuf)
	if err != nil {
		return mapNotFound(err)
	}
	return copyFn()
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

// ContainerLogs streams log lines from a container. Cancellation propagates
// bidirectionally: if the caller's ctx is cancelled the underlying
// containers.Logs call is cancelled (via mergedCtx), and if the producer
// finishes naturally the bridge goroutine exits cleanly.
//
// The connection context (c) is long-lived and must not be cancelled per
// request; mergedCtx derives from c but can be independently cancelled so
// the streaming call is torn down without killing the cached connection.
func (r *Real) ContainerLogs(ctx context.Context, id, container string, opts LogOptions) (<-chan LogLine, error) {
	c, err := r.opCtxFor(ctx, id)
	if err != nil {
		return nil, err
	}

	// mergedCtx is derived from the connection context but independently
	// cancellable. Cancelling it tears down the containers.Logs HTTP stream
	// without affecting the cached connection context c.
	mergedCtx, cancel := context.WithCancel(c)

	// Bridge goroutine: propagate caller cancellation to mergedCtx, and also
	// exit when mergedCtx itself finishes (natural completion or cancel()).
	go func() {
		select {
		case <-ctx.Done():
		case <-mergedCtx.Done():
		}
		cancel()
	}()

	stdoutCh := make(chan string, 64)
	stderrCh := make(chan string, 64)
	out := make(chan LogLine, 64)

	go func() {
		defer close(out)
		defer cancel() // signal the bridge and producer to stop when fan-in exits
		for {
			select {
			case <-ctx.Done():
				return
			case line, ok := <-stdoutCh:
				if !ok {
					stdoutCh = nil
				} else {
					select {
					case out <- LogLine{Container: container, Stream: "stdout", Line: line, Time: time.Now()}:
					case <-ctx.Done():
						return
					}
				}
			case line, ok := <-stderrCh:
				if !ok {
					stderrCh = nil
				} else {
					select {
					case out <- LogLine{Container: container, Stream: "stderr", Line: line, Time: time.Now()}:
					case <-ctx.Done():
						return
					}
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
		Follow: &follow, Tail: &tail,
	}
	// Only set Since if non-empty: libpod rejects an empty value with
	// "unable to interpret time value".
	if opts.Since != "" {
		logsOpts.Since = &opts.Since
	}
	go func() {
		_ = containers.Logs(mergedCtx, container, logsOpts, stdoutCh, stderrCh)
		close(stdoutCh)
		close(stderrCh)
	}()
	return out, nil
}

func (r *Real) ImagePull(ctx context.Context, id, ref string) error {
	c, err := r.opCtxFor(ctx, id)
	if err != nil {
		return err
	}
	_, err = images.Pull(c, ref, &images.PullOptions{})
	return err
}

func (r *Real) UsedHostPorts(ctx context.Context, id string) ([]PortMapping, error) {
	c, err := r.opCtxFor(ctx, id)
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
		containerName := ""
		if len(ct.Names) > 0 {
			containerName = ct.Names[0]
		}
		for _, p := range ct.Ports {
			out = append(out, PortMapping{
				HostIP:        p.HostIP,
				HostPort:      int(p.HostPort),
				ContainerPort: int(p.ContainerPort),
				Protocol:      p.Protocol,
				Pod:           ct.PodName,
				Container:     containerName,
			})
		}
	}
	return out, nil
}

// HostInfo returns a point-in-time resource snapshot for a host. CPU/mem/disk
// come from libpod `info`; reclaimable from `system df`; loadavg is a
// best-effort read of /proc/loadavg (the one metric libpod does not expose).
// Any sub-metric that cannot be obtained is left at its zero/nil value rather
// than failing the whole call; only a failed `info` call (host unreachable)
// returns an error.
func (r *Real) HostInfo(ctx context.Context, id string) (HostInfo, error) {
	c, err := r.ctxFor(ctx, id)
	if err != nil {
		return HostInfo{}, err
	}
	info, err := system.Info(c, &system.InfoOptions{})
	if err != nil {
		return HostInfo{}, err
	}
	out := HostInfo{}
	if info.Host != nil {
		out.CPUs = info.Host.CPUs
		out.MemTotal = info.Host.MemTotal
		out.MemFree = info.Host.MemFree
		if info.Host.MemTotal > 0 {
			out.MemUsedPct = float64(info.Host.MemTotal-info.Host.MemFree) / float64(info.Host.MemTotal) * 100
		}
		if u := info.Host.CPUUtilization; u != nil {
			// libpod reports utilization cumulative since boot, not instantaneous.
			sinceBoot := u.UserPercent + u.SystemPercent
			out.CPUPct = &sinceBoot
		}
	}
	if info.Store != nil {
		out.Disk.Total = int64(info.Store.GraphRootAllocated)
		out.Disk.Used = int64(info.Store.GraphRootUsed)
		out.Disk.Free = out.Disk.Total - out.Disk.Used
		if out.Disk.Free < 0 {
			out.Disk.Free = 0
		}
	}
	if df, err := system.DiskUsage(c, &system.DiskOptions{}); err == nil && df != nil {
		var reclaimable int64
		for _, v := range df.Volumes {
			reclaimable += v.ReclaimableSize
		}
		out.Disk.Reclaimable = reclaimable
	}
	if la := r.hostLoadAvg(ctx, id); la != nil {
		out.LoadAvg = la
	}
	return out, nil
}

// hostLoadAvg reads /proc/loadavg for a host, returning the 1/5/15-minute
// averages, or nil if it cannot be read. For a unix (local) host it reads the
// daemon's own /proc/loadavg; for an SSH host it execs `cat /proc/loadavg`
// over a short-lived SSH session bounded by ctx. Any error yields nil so the
// metric is absent.
func (r *Real) hostLoadAvg(ctx context.Context, id string) *[3]float64 {
	r.mu.Lock()
	h, ok := r.hosts[id]
	r.mu.Unlock()
	if !ok {
		return nil
	}
	var raw string
	if h.Addr == "unix" {
		b, err := os.ReadFile("/proc/loadavg")
		if err != nil {
			return nil
		}
		raw = string(b)
	} else {
		out, err := sshReadLoadAvg(ctx, h)
		if err != nil {
			return nil
		}
		raw = out
	}
	return parseLoadAvg(raw)
}

// parseLoadAvg extracts the first three space-separated floats from a
// /proc/loadavg line ("0.42 0.37 0.31 1/512 12345").
func parseLoadAvg(raw string) *[3]float64 {
	fields := strings.Fields(raw)
	if len(fields) < 3 {
		return nil
	}
	var la [3]float64
	for i := 0; i < 3; i++ {
		v, err := strconv.ParseFloat(fields[i], 64)
		if err != nil {
			return nil
		}
		la[i] = v
	}
	return &la
}

func boolPtr(b bool) *bool { return &b }

// splitPortKey parses a libpod PortBindings key like "5432/tcp" into
// (containerPort, protocol). Missing protocol defaults to "tcp".
func splitPortKey(k string) (int, string) {
	slash := strings.IndexByte(k, '/')
	if slash < 0 {
		port, _ := strconv.Atoi(k)
		return port, "tcp"
	}
	port, _ := strconv.Atoi(k[:slash])
	return port, k[slash+1:]
}

// sumPrune folds libpod's per-item prune reports into our PruneReport. Items with
// a non-nil Err are skipped from the reclaimed total but still surfaced as ids.
func sumPrune(reps []*reports.PruneReport) PruneReport {
	var out PruneReport
	for _, r := range reps {
		if r == nil {
			continue
		}
		out.Items = append(out.Items, r.Id)
		// r.Size is uint64; guard the int64 conversion so an implausibly huge
		// item can't wrap Reclaimed negative.
		if r.Err == nil && r.Size <= math.MaxInt64 {
			out.Reclaimed += int64(r.Size)
		}
	}
	return out
}

func (r *Real) ImagePrune(ctx context.Context, id string, all bool) (PruneReport, error) {
	c, err := r.opCtxFor(ctx, id)
	if err != nil {
		return PruneReport{}, err
	}
	reps, err := images.Prune(c, new(images.PruneOptions).WithAll(all))
	if err != nil {
		return PruneReport{}, err
	}
	return sumPrune(reps), nil
}

func (r *Real) ContainerPrune(ctx context.Context, id string) (PruneReport, error) {
	c, err := r.opCtxFor(ctx, id)
	if err != nil {
		return PruneReport{}, err
	}
	reps, err := containers.Prune(c, new(containers.PruneOptions))
	if err != nil {
		return PruneReport{}, err
	}
	return sumPrune(reps), nil
}

func (r *Real) BuildCachePrune(ctx context.Context, id string) (PruneReport, error) {
	c, err := r.opCtxFor(ctx, id)
	if err != nil {
		return PruneReport{}, err
	}
	// Build cache is pruned through the images prune endpoint with the
	// build-cache flag set (libpod has no standalone build-cache binding in v5).
	reps, err := images.Prune(c, new(images.PruneOptions).WithBuildCache(true))
	if err != nil {
		return PruneReport{}, err
	}
	return sumPrune(reps), nil
}

func (r *Real) VolumePrune(ctx context.Context, id string, filters map[string][]string) (PruneReport, error) {
	c, err := r.opCtxFor(ctx, id)
	if err != nil {
		return PruneReport{}, err
	}
	opts := new(volumes.PruneOptions)
	if len(filters) > 0 {
		opts = opts.WithFilters(filters)
	}
	reps, err := volumes.Prune(c, opts)
	if err != nil {
		return PruneReport{}, err
	}
	return sumPrune(reps), nil
}

// Compile-time guarantee that Real satisfies the Client interface.
var _ Client = (*Real)(nil)

package instance

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/iotready/podman-api/internal/config"
	"github.com/iotready/podman-api/internal/podman"
	"github.com/iotready/podman-api/internal/render"
)

// Sentinel errors mapped by the API layer to JSON error codes.
var (
	ErrUnknownHost       = errors.New("unknown host")
	ErrUnknownTemplate   = errors.New("unknown template")
	ErrInstanceNotFound  = errors.New("instance not found")
	ErrInstanceExists    = errors.New("instance already exists")
	ErrHostSecretMissing = errors.New("required host secret missing")
	ErrImagePull         = errors.New("image pull failed")
)

// ApplyOptions controls the side effects of Apply beyond the request body.
type ApplyOptions struct {
	Replace  bool // if false and the pod exists, return ErrInstanceExists
	SkipPull bool // if true, do not pre-pull container images (CI / local-only refs)
}

// ApplyRequest is the body of POST /instances and PUT /instances/{...}.
type ApplyRequest struct {
	Template   string            `json:"template"`
	Slug       string            `json:"slug"`
	Parameters map[string]any    `json:"parameters"`
	Secrets    map[string]string `json:"secrets"`
}

// DeleteOptions controls cleanup beyond the pod itself.
type DeleteOptions struct {
	PruneVolumes bool
	PruneSecrets bool
}

// Service orchestrates instance operations against podman hosts.
type Service struct {
	client     podman.Client
	hosts      map[string]config.Host
	templates  map[string]config.Template
	secretEnvs map[string]map[string]bool // template id -> set of secret-sourced env var names

	mu    sync.Mutex
	locks map[string]*sync.Mutex // key = host|template|slug
}

func NewService(client podman.Client, hosts []config.Host, tmpls []config.Template) *Service {
	s := &Service{
		client:     client,
		hosts:      map[string]config.Host{},
		templates:  map[string]config.Template{},
		secretEnvs: map[string]map[string]bool{},
		locks:      map[string]*sync.Mutex{},
	}
	for _, h := range hosts {
		s.hosts[h.ID] = h
	}
	for _, t := range tmpls {
		s.templates[t.Meta.ID] = t
		s.secretEnvs[t.Meta.ID] = secretEnvNames(t.Body)
	}
	return s
}

func (s *Service) instanceLock(host, tmpl, slug string) *sync.Mutex {
	key := host + "|" + tmpl + "|" + slug
	s.mu.Lock()
	defer s.mu.Unlock()
	m, ok := s.locks[key]
	if !ok {
		m = &sync.Mutex{}
		s.locks[key] = m
	}
	return m
}

func (s *Service) lookup(host, tmpl string) (config.Template, error) {
	if _, ok := s.hosts[host]; !ok {
		return config.Template{}, ErrUnknownHost
	}
	t, ok := s.templates[tmpl]
	if !ok {
		return config.Template{}, ErrUnknownTemplate
	}
	return t, nil
}

func podName(tmpl, slug string) string { return tmpl + "-" + slug }

func instanceSecretName(tmpl, slug, name string) string {
	return tmpl + "-" + slug + "-" + name
}

// Apply creates or replaces an instance. If opts.Replace is false and the
// pod exists, returns ErrInstanceExists. Unless opts.SkipPull is set, every
// container image referenced in the rendered Pod spec is pulled before the
// manifest is played — this surfaces registry errors fast and avoids the
// opaque timeout from play kube's implicit pull.
func (s *Service) Apply(ctx context.Context, host string, req ApplyRequest, opts ApplyOptions) error {
	tmpl, err := s.lookup(host, req.Template)
	if err != nil {
		return err
	}
	if err := render.Validate(tmpl.Meta, req.Parameters, req.Secrets); err != nil {
		return fmt.Errorf("validate: %w", err)
	}

	lock := s.instanceLock(host, req.Template, req.Slug)
	lock.Lock()
	defer lock.Unlock()

	// Pre-check: per-host secrets exist.
	for _, name := range tmpl.Meta.Secrets.PerHostReferenced {
		if _, err := s.client.SecretInspect(ctx, host, name); err != nil {
			if errors.Is(err, podman.ErrNotFound) {
				return fmt.Errorf("%w: %s", ErrHostSecretMissing, name)
			}
			return fmt.Errorf("inspect host secret %q: %w", name, err)
		}
	}

	// Strict-create: 409 if pod exists.
	if !opts.Replace {
		if _, err := s.client.PodInspect(ctx, host, podName(req.Template, req.Slug)); err == nil {
			return ErrInstanceExists
		} else if !errors.Is(err, podman.ErrNotFound) {
			return fmt.Errorf("inspect pod: %w", err)
		}
	}

	yaml, err := render.Render(rawTemplate(tmpl), req.Parameters)
	if err != nil {
		return fmt.Errorf("render: %w", err)
	}

	// Pre-pull images BEFORE writing any secrets. A bad image ref then leaves
	// no orphan secrets behind; secrets that already exist (rotation case) are
	// only touched once we know the manifest will play.
	if !opts.SkipPull {
		for _, img := range containerImages(yaml) {
			if err := s.client.ImagePull(ctx, host, img); err != nil {
				return fmt.Errorf("%w: %s: %v", ErrImagePull, img, err)
			}
		}
	}

	// Push per-instance secrets, then zero the local copies.
	for k, v := range req.Secrets {
		name := instanceSecretName(req.Template, req.Slug, k)
		// Idempotency: if it already exists, remove and recreate (rotation).
		if _, err := s.client.SecretInspect(ctx, host, name); err == nil {
			if err := s.client.SecretRemove(ctx, host, name); err != nil {
				return fmt.Errorf("remove existing secret %q: %w", name, err)
			}
		}
		if err := s.client.SecretCreate(ctx, host, name, wrapAsKubeSecret(name, []byte(v))); err != nil {
			return fmt.Errorf("create secret %q: %w", name, err)
		}
	}
	for k := range req.Secrets {
		req.Secrets[k] = "" // best-effort zero
	}

	if err := s.client.PlayKube(ctx, host, yaml, opts.Replace); err != nil {
		return fmt.Errorf("play kube: %w", err)
	}
	return nil
}

// rawTemplate reconstructs a complete template source from a parsed Template.
// Render needs the full source because ParseMeta strips the meta block before
// handing the body to text/template.
func rawTemplate(t config.Template) string {
	return "# template-meta:\n#   id: " + t.Meta.ID + "\n#   parameters:\n#     required: []\n---\n" + t.Body
}

// Get returns the observed shape for an instance.
func (s *Service) Get(ctx context.Context, host, tmpl, slug string) (Observed, error) {
	if _, err := s.lookup(host, tmpl); err != nil {
		return Observed{}, err
	}
	p, err := s.client.PodInspect(ctx, host, podName(tmpl, slug))
	if err != nil {
		if errors.Is(err, podman.ErrNotFound) {
			return Observed{}, ErrInstanceNotFound
		}
		return Observed{}, err
	}
	t := s.templates[tmpl]
	var vols []podman.Volume
	for _, v := range t.Meta.Volumes {
		name := tmpl + "-" + slug + "-" + v.Name
		if vv, err := s.client.VolumeInspect(ctx, host, name); err == nil {
			vols = append(vols, vv)
		}
	}
	return Normalize(p, tmpl, slug, vols, s.secretEnvs[tmpl]), nil
}

// List returns all instances of a given template on a host.
func (s *Service) List(ctx context.Context, host, tmpl string) ([]Observed, error) {
	if _, err := s.lookup(host, tmpl); err != nil {
		return nil, err
	}
	pods, err := s.client.PodList(ctx, host, map[string]string{"podman-api/template": tmpl})
	if err != nil {
		return nil, err
	}
	out := make([]Observed, 0, len(pods))
	for _, p := range pods {
		slug := p.Labels["podman-api/slug"]
		out = append(out, Normalize(p, tmpl, slug, nil, s.secretEnvs[tmpl]))
	}
	return out, nil
}

func (s *Service) Start(ctx context.Context, host, tmpl, slug string) error {
	return s.lifecycle(ctx, host, tmpl, slug, s.client.PodStart)
}
func (s *Service) Stop(ctx context.Context, host, tmpl, slug string) error {
	return s.lifecycle(ctx, host, tmpl, slug, s.client.PodStop)
}
func (s *Service) Restart(ctx context.Context, host, tmpl, slug string) error {
	return s.lifecycle(ctx, host, tmpl, slug, s.client.PodRestart)
}

func (s *Service) lifecycle(ctx context.Context, host, tmpl, slug string,
	op func(context.Context, string, string) error) error {
	if _, err := s.lookup(host, tmpl); err != nil {
		return err
	}
	lock := s.instanceLock(host, tmpl, slug)
	lock.Lock()
	defer lock.Unlock()
	if err := op(ctx, host, podName(tmpl, slug)); err != nil {
		if errors.Is(err, podman.ErrNotFound) {
			return ErrInstanceNotFound
		}
		return err
	}
	return nil
}

// Upgrade replaces the pod with a new image. The pull happens inside Apply
// (which scans the rendered manifest and pulls every container image), so a
// bad image ref still fails fast — without a duplicate pre-pull here.
func (s *Service) Upgrade(ctx context.Context, host string, req ApplyRequest, image string) error {
	if image == "" {
		return errors.New("upgrade requires an image")
	}
	// Shallow-copy parameters to avoid mutating the caller's map.
	params := make(map[string]any, len(req.Parameters)+1)
	for k, v := range req.Parameters {
		params[k] = v
	}
	params["image"] = image
	req.Parameters = params
	return s.Apply(ctx, host, req, ApplyOptions{Replace: true})
}

// Delete removes the pod and optionally its volumes and per-instance secrets.
func (s *Service) Delete(ctx context.Context, host, tmpl, slug string, opts DeleteOptions) error {
	if _, err := s.lookup(host, tmpl); err != nil {
		return err
	}
	lock := s.instanceLock(host, tmpl, slug)
	lock.Lock()
	defer lock.Unlock()

	if err := s.client.PodRemove(ctx, host, podName(tmpl, slug), true); err != nil {
		if errors.Is(err, podman.ErrNotFound) {
			return ErrInstanceNotFound
		}
		return err
	}

	t := s.templates[tmpl]
	if opts.PruneSecrets {
		for _, name := range t.Meta.Secrets.PerInstance {
			full := instanceSecretName(tmpl, slug, name)
			_ = s.client.SecretRemove(ctx, host, full)
		}
	}
	if opts.PruneVolumes {
		for _, v := range t.Meta.Volumes {
			full := tmpl + "-" + slug + "-" + v.Name
			_ = s.client.VolumeRemove(ctx, host, full, true)
		}
	}
	return nil
}

// Ping checks reachability of a host.
func (s *Service) Ping(ctx context.Context, host string) error {
	if _, ok := s.hosts[host]; !ok {
		return ErrUnknownHost
	}
	return s.client.Ping(ctx, host)
}

// Version returns the podman version string for a host.
func (s *Service) Version(ctx context.Context, host string) (string, error) {
	if _, ok := s.hosts[host]; !ok {
		return "", ErrUnknownHost
	}
	return s.client.Version(ctx, host)
}

// Hosts returns the configured hosts (read-only view for the API).
func (s *Service) Hosts() []config.Host {
	out := make([]config.Host, 0, len(s.hosts))
	for _, h := range s.hosts {
		out = append(out, h)
	}
	return out
}

// Templates returns the loaded templates' metadata (read-only view).
func (s *Service) Templates() []config.Template {
	out := make([]config.Template, 0, len(s.templates))
	for _, t := range s.templates {
		out = append(out, t)
	}
	return out
}

// PortsInUse returns all currently-bound host ports on hostID.
func (s *Service) PortsInUse(ctx context.Context, host string) ([]podman.PortMapping, error) {
	if _, ok := s.hosts[host]; !ok {
		return nil, ErrUnknownHost
	}
	return s.client.UsedHostPorts(ctx, host)
}

// HostSecrets lists secrets on a host.
func (s *Service) HostSecrets(ctx context.Context, host string) ([]podman.Secret, error) {
	if _, ok := s.hosts[host]; !ok {
		return nil, ErrUnknownHost
	}
	return s.client.SecretList(ctx, host)
}

// PutHostSecret creates-or-rotates a host secret. We "rotate" by removing
// then recreating, since podman secrets are immutable.
func (s *Service) PutHostSecret(ctx context.Context, host, name string, value []byte) error {
	if _, ok := s.hosts[host]; !ok {
		return ErrUnknownHost
	}
	if _, err := s.client.SecretInspect(ctx, host, name); err == nil {
		if err := s.client.SecretRemove(ctx, host, name); err != nil {
			return err
		}
	}
	return s.client.SecretCreate(ctx, host, name, wrapAsKubeSecret(name, value))
}

func (s *Service) DeleteHostSecret(ctx context.Context, host, name string) error {
	if _, ok := s.hosts[host]; !ok {
		return ErrUnknownHost
	}
	err := s.client.SecretRemove(ctx, host, name)
	if errors.Is(err, podman.ErrNotFound) {
		return nil
	}
	return err
}

// InstanceVolumes returns the named volumes the API believes belong to this instance.
// Volumes that don't exist on the host are omitted (no error).
func (s *Service) InstanceVolumes(ctx context.Context, host, tmpl, slug string) ([]podman.Volume, error) {
	t, err := s.lookup(host, tmpl)
	if err != nil {
		return nil, err
	}
	var out []podman.Volume
	for _, v := range t.Meta.Volumes {
		name := tmpl + "-" + slug + "-" + v.Name
		if vv, err := s.client.VolumeInspect(ctx, host, name); err == nil {
			out = append(out, vv)
		}
	}
	return out, nil
}

// DeleteVolume removes a named volume on a host. Idempotent.
func (s *Service) DeleteVolume(ctx context.Context, host, name string, force bool) error {
	if _, ok := s.hosts[host]; !ok {
		return ErrUnknownHost
	}
	err := s.client.VolumeRemove(ctx, host, name, force)
	if errors.Is(err, podman.ErrNotFound) {
		return nil
	}
	return err
}

// Logs returns a channel of log lines from one container in an instance.
func (s *Service) Logs(ctx context.Context, host, tmpl, slug, container string, opts podman.LogOptions) (<-chan podman.LogLine, error) {
	if _, err := s.lookup(host, tmpl); err != nil {
		return nil, err
	}
	if _, err := s.client.PodInspect(ctx, host, podName(tmpl, slug)); err != nil {
		if errors.Is(err, podman.ErrNotFound) {
			return nil, ErrInstanceNotFound
		}
		return nil, err
	}
	cname := podName(tmpl, slug) + "-" + container
	return s.client.ContainerLogs(ctx, host, cname, opts)
}

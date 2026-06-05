package instance

import (
	"context"
	"errors"
	"fmt"
	"io"
	"maps"
	"slices"
	"sync"
	"sync/atomic"

	"github.com/iotready/podman-api/internal/config"
	"github.com/iotready/podman-api/internal/ingress"
	"github.com/iotready/podman-api/internal/podman"
	"github.com/iotready/podman-api/internal/render"
	"github.com/iotready/podman-api/internal/store"
)

// Sentinel errors mapped by the API layer to JSON error codes.
var (
	ErrUnknownHost       = errors.New("unknown host")
	ErrUnknownTemplate   = errors.New("unknown template")
	ErrInstanceNotFound  = errors.New("instance not found")
	ErrInstanceExists    = errors.New("instance already exists")
	ErrHostSecretMissing = errors.New("required host secret missing")
	ErrImagePull         = errors.New("image pull failed")
	ErrHostDraining      = errors.New("host is draining")
	ErrPortConflict      = errors.New("required host port already in use")
	ErrSameHost          = errors.New("source and destination host are the same")
	ErrStoreDisabled     = errors.New("migrate requires the state store")
	ErrVolumeIntegrity   = errors.New("volume copy failed integrity check")
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
	Domains    []string          `json:"domains,omitempty"`
}

// DeleteOptions controls cleanup beyond the pod itself.
type DeleteOptions struct {
	PruneVolumes bool
	PruneSecrets bool
}

// Service orchestrates instance operations against podman hosts.
type Service struct {
	client     podman.Client
	hosts      atomic.Pointer[map[string]config.Host] // hot-swappable on SIGHUP
	templates  map[string]config.Template
	secretEnvs map[string]map[string]bool // template id -> set of secret-sourced env var names
	store      store.Store                // nil = store disabled (stateless proxy behaviour)
	ingress    ingress.Controller         // never nil; ingress.Disabled{} when off
	ingressNet string                     // shared ingress network; "" when ingress disabled

	verifyVolumes bool // verify each migrated volume's content before reaping the source

	mu    sync.Mutex
	locks map[string]*sync.Mutex // key = host|template|slug
}

func NewService(client podman.Client, hosts []config.Host, tmpls []config.Template) *Service {
	s := &Service{
		client:        client,
		templates:     map[string]config.Template{},
		secretEnvs:    map[string]map[string]bool{},
		locks:         map[string]*sync.Mutex{},
		verifyVolumes: true,
	}
	s.ingress = ingress.Disabled{}
	s.SetHosts(hosts)
	for _, t := range tmpls {
		s.templates[t.Meta.ID] = t
		s.secretEnvs[t.Meta.ID] = secretEnvNames(t.Body)
	}
	return s
}

// SetHosts atomically replaces the live host set. Used by main on SIGHUP to
// pick up edits to hosts/*.yaml (e.g. flipping drain) without restart.
func (s *Service) SetHosts(hosts []config.Host) {
	m := make(map[string]config.Host, len(hosts))
	for _, h := range hosts {
		m[h.ID] = h
	}
	s.hosts.Store(&m)
}

// SetStore wires the optional desired-state store. A nil store (the default)
// disables persistence and the daemon behaves as a stateless proxy. Called by
// main after construction, mirroring SetHosts. Unlike SetHosts it is NOT a
// concurrent hot-swap: it must be called at startup, before the server begins
// accepting requests.
func (s *Service) SetStore(st store.Store) { s.store = st }

// SetIngress enables ingress reconciliation. network is the shared podman
// network app pods join; passing a real controller marks ingress enabled so
// Apply will accept domains. Call with ingress.Disabled{} and "" to disable.
func (s *Service) SetIngress(c ingress.Controller, network string) {
	s.ingress = c
	s.ingressNet = network
}

func (s *Service) ingressEnabled() bool { return s.ingressNet != "" }

// validateIngress enforces the ingress rules for a request carrying domains
// BEFORE the pod is played or the spec is persisted: ingress must be enabled,
// the template must declare ingress:, and each domain must be unclaimed by any
// other instance on the host. Enforcing them up front keeps an invalid spec out
// of the store; otherwise it would poison deriveRoutes and fail every later
// reconcile on the host. A request with no domains is always allowed.
func (s *Service) validateIngress(ctx context.Context, host string, req ApplyRequest, tmpl config.Template) error {
	if len(req.Domains) == 0 {
		return nil
	}
	if !s.ingressEnabled() {
		return fmt.Errorf("instance %s/%s declares domains but ingress is disabled", req.Template, req.Slug)
	}
	if tmpl.Meta.Ingress == nil {
		return fmt.Errorf("instance %s/%s declares domains but template %q has no ingress", req.Template, req.Slug, req.Template)
	}
	if s.store == nil {
		return nil
	}
	keys, err := s.store.ListSpecKeys(ctx, host)
	if err != nil {
		return fmt.Errorf("ingress: check domain uniqueness on %s: %w", host, err)
	}
	want := make(map[string]bool, len(req.Domains))
	for _, d := range req.Domains {
		want[d] = true
	}
	for _, k := range keys {
		if k.Template == req.Template && k.Slug == req.Slug {
			continue // the instance being (re)applied — its own domains don't conflict
		}
		other, err := s.store.GetSpec(ctx, host, k.Template, k.Slug)
		if err != nil {
			return fmt.Errorf("ingress: check domain uniqueness on %s: %w", host, err)
		}
		for _, d := range other.Domains {
			if want[d] {
				return fmt.Errorf("ingress: domain %q already claimed by %s/%s on %s", d, k.Template, k.Slug, host)
			}
		}
	}
	return nil
}

func (s *Service) hostsSnap() map[string]config.Host {
	p := s.hosts.Load()
	if p == nil {
		return nil
	}
	return *p
}

func (s *Service) host(id string) (config.Host, bool) {
	h, ok := s.hostsSnap()[id]
	return h, ok
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
	if _, ok := s.host(host); !ok {
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

	// Validate ingress rules BEFORE playing the pod or persisting the spec, so a
	// rejected request never leaves a poison spec in the store — a persisted spec
	// that violates these rules would fail every later reconcile on the host.
	// deriveRoutes re-checks the same rules at reconcile time as a backstop.
	if err := s.validateIngress(ctx, host, req, tmpl); err != nil {
		return err
	}

	// Pre-check: per-host secrets exist.
	for _, name := range tmpl.Meta.Secrets.PerHostReferenced {
		if _, err := s.client.SecretInspect(ctx, host, name); err != nil {
			if errors.Is(err, podman.ErrNotFound) {
				return fmt.Errorf("%w: %s", ErrHostSecretMissing, name)
			}
			return fmt.Errorf("inspect host secret %q: %w", name, err)
		}
	}

	// Drain check: a draining host refuses *create-shaped* Apply. We treat
	// "create-shaped" as either Replace=false, or Replace=true against a pod
	// that doesn't exist yet (which would otherwise sneak past the gate).
	// In-place upgrades of existing pods, lifecycle ops, and reads are
	// unaffected — drain is about not accepting new tenants.
	hostCfg, _ := s.host(host) // existence already verified by lookup
	podExists := false
	if _, err := s.client.PodInspect(ctx, host, podName(req.Template, req.Slug)); err == nil {
		podExists = true
	} else if !errors.Is(err, podman.ErrNotFound) {
		return fmt.Errorf("inspect pod: %w", err)
	}
	if !podExists && hostCfg.Drain {
		return ErrHostDraining
	}
	if !opts.Replace && podExists {
		return ErrInstanceExists
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

	// Snapshot secrets (zeroed below) and parameters before persisting, so the
	// stored spec is independent of the caller's request struct.
	var secretsCopy map[string]string
	var paramsCopy map[string]any
	var domainsCopy []string
	if s.store != nil {
		secretsCopy = maps.Clone(req.Secrets)
		paramsCopy = maps.Clone(req.Parameters)
		domainsCopy = slices.Clone(req.Domains)
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

	var networks []string
	if s.ingressEnabled() && tmpl.Meta.Ingress != nil {
		networks = []string{s.ingressNet}
	}
	if err := s.client.PlayKube(ctx, host, yaml, opts.Replace, networks...); err != nil {
		return fmt.Errorf("play kube: %w", err)
	}
	if s.store != nil {
		sp := store.Spec{
			Host:       host,
			Template:   req.Template,
			Slug:       req.Slug,
			Parameters: paramsCopy,
			Secrets:    secretsCopy,
			Domains:    domainsCopy,
		}
		if err := s.store.PutSpec(ctx, sp); err != nil {
			return fmt.Errorf("persist spec: %w", err)
		}
	}
	if s.ingressEnabled() {
		if err := s.ingress.Reconcile(ctx, host); err != nil {
			return fmt.Errorf("ingress reconcile: %w", err)
		}
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

// ListAllInstances returns every podman-api-managed pod on a host across all
// known templates. The result is the union of List(host, t) for each loaded
// template id, so a pod for a template the daemon doesn't know about is
// silently omitted.
func (s *Service) ListAllInstances(ctx context.Context, host string) ([]Observed, error) {
	if _, ok := s.host(host); !ok {
		return nil, ErrUnknownHost
	}
	var out []Observed
	for tmplID := range s.templates {
		pods, err := s.client.PodList(ctx, host, map[string]string{"podman-api/template": tmplID})
		if err != nil {
			return nil, fmt.Errorf("list %s: %w", tmplID, err)
		}
		for _, p := range pods {
			slug := p.Labels["podman-api/slug"]
			out = append(out, Normalize(p, tmplID, slug, nil, s.secretEnvs[tmplID]))
		}
	}
	return out, nil
}

// InstanceCount returns the total number of podman-api-managed pods on a
// host across all known templates. Used by /hosts to surface drain decisions.
func (s *Service) InstanceCount(ctx context.Context, host string) (int, error) {
	all, err := s.ListAllInstances(ctx, host)
	if err != nil {
		return 0, err
	}
	return len(all), nil
}

// HostCounts returns the number of managed instances and the total number of
// their containers on a host, in a single ListAllInstances sweep.
func (s *Service) HostCounts(ctx context.Context, host string) (instances, containers int, err error) {
	all, err := s.ListAllInstances(ctx, host)
	if err != nil {
		return 0, 0, err
	}
	for _, obs := range all {
		containers += len(obs.Containers)
	}
	return len(all), containers, nil
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

	podExisted := true
	if err := s.client.PodRemove(ctx, host, podName(tmpl, slug), true); err != nil {
		if !errors.Is(err, podman.ErrNotFound) {
			return err
		}
		// The pod is already gone. We still honour any prune request below so a
		// caller can reap secrets/volumes orphaned by an earlier prune-less
		// delete — delete is an idempotent reconcile toward "gone".
		podExisted = false
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
	// Reconcile away the desired-state row. This runs even when the pod was
	// already gone (so a stale spec doesn't linger); ErrNotFound — never stored,
	// or an idempotent double-delete — is not an error. Note this happens before
	// the not-found guard below, so a pod-gone Delete still cleans up the spec
	// even though it reports ErrInstanceNotFound to the caller.
	if s.store != nil {
		if err := s.store.DeleteSpec(ctx, host, tmpl, slug); err != nil && !errors.Is(err, store.ErrNotFound) {
			return fmt.Errorf("delete spec: %w", err)
		}
	}
	// Reconcile ingress so the deleted instance's routes are dropped. Placed
	// alongside the spec deletion (before the not-found guard below) so the
	// common successful-delete path always reconciles, and a redundant delete of
	// an already-gone pod still converges the proxy toward "route removed".
	if s.ingressEnabled() {
		if err := s.ingress.Reconcile(ctx, host); err != nil {
			return fmt.Errorf("ingress reconcile: %w", err)
		}
	}
	// If the pod was already gone and the caller asked for no pruning, there was
	// nothing to delete — report not-found as before. When a prune was
	// requested we treat the call as a successful reconcile.
	if !podExisted && !opts.PruneSecrets && !opts.PruneVolumes {
		return ErrInstanceNotFound
	}
	return nil
}

// Ping checks reachability of a host.
func (s *Service) Ping(ctx context.Context, host string) error {
	if _, ok := s.host(host); !ok {
		return ErrUnknownHost
	}
	return s.client.Ping(ctx, host)
}

// Version returns the podman version string for a host.
func (s *Service) Version(ctx context.Context, host string) (string, error) {
	if _, ok := s.host(host); !ok {
		return "", ErrUnknownHost
	}
	return s.client.Version(ctx, host)
}

// Hosts returns the configured hosts (read-only view for the API).
func (s *Service) Hosts() []config.Host {
	out := make([]config.Host, 0, len(s.hostsSnap()))
	for _, h := range s.hostsSnap() {
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

// HostLoad returns a point-in-time resource snapshot for a host.
func (s *Service) HostLoad(ctx context.Context, host string) (podman.HostInfo, error) {
	if _, ok := s.host(host); !ok {
		return podman.HostInfo{}, ErrUnknownHost
	}
	return s.client.HostInfo(ctx, host)
}

// PortsInUse returns all currently-bound host ports on hostID.
func (s *Service) PortsInUse(ctx context.Context, host string) ([]podman.PortMapping, error) {
	if _, ok := s.host(host); !ok {
		return nil, ErrUnknownHost
	}
	return s.client.UsedHostPorts(ctx, host)
}

// HostSecrets lists secrets on a host.
func (s *Service) HostSecrets(ctx context.Context, host string) ([]podman.Secret, error) {
	if _, ok := s.host(host); !ok {
		return nil, ErrUnknownHost
	}
	return s.client.SecretList(ctx, host)
}

// PutHostSecret creates-or-rotates a host secret on the host, then (when the
// store is enabled and persist is true) records the value so a later
// migrate/evacuate can re-provision it on a destination. We "rotate" by
// removing then recreating, since podman secrets are immutable. Push happens
// before persist: we never store a value we failed to apply to the host.
func (s *Service) PutHostSecret(ctx context.Context, host, name string, value []byte, persist bool) error {
	if _, ok := s.host(host); !ok {
		return ErrUnknownHost
	}
	if _, err := s.client.SecretInspect(ctx, host, name); err == nil {
		if err := s.client.SecretRemove(ctx, host, name); err != nil {
			return err
		}
	}
	if err := s.client.SecretCreate(ctx, host, name, wrapAsKubeSecret(name, value)); err != nil {
		return err
	}
	if s.store != nil && persist {
		if err := s.store.PutHostSecret(ctx, host, name, value); err != nil {
			return fmt.Errorf("persist host secret: %w", err)
		}
	}
	return nil
}

func (s *Service) DeleteHostSecret(ctx context.Context, host, name string) error {
	if _, ok := s.host(host); !ok {
		return ErrUnknownHost
	}
	if err := s.client.SecretRemove(ctx, host, name); err != nil && !errors.Is(err, podman.ErrNotFound) {
		return err
	}
	if s.store != nil {
		if err := s.store.DeleteHostSecret(ctx, host, name); err != nil {
			return fmt.Errorf("delete persisted host secret: %w", err)
		}
	}
	return nil
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
		vv, err := s.client.VolumeInspect(ctx, host, name)
		if errors.Is(err, podman.ErrNotFound) {
			continue // a declared volume may legitimately not exist yet — skip it
		}
		if err != nil {
			// Do NOT swallow transient errors: callers (migrate/evacuate) reap the
			// source after copying this set, so a silently-dropped volume means
			// data loss. Fail loud instead. (#50)
			return nil, fmt.Errorf("inspect volume %q: %w", name, err)
		}
		out = append(out, vv)
	}
	return out, nil
}

// DeleteVolume removes a named volume on a host. Idempotent.
func (s *Service) DeleteVolume(ctx context.Context, host, name string, force bool) error {
	if _, ok := s.host(host); !ok {
		return ErrUnknownHost
	}
	err := s.client.VolumeRemove(ctx, host, name, force)
	if errors.Is(err, podman.ErrNotFound) {
		return nil
	}
	return err
}

// SetVerifyVolumes toggles post-copy volume integrity verification during
// migrate. Default true; set false (via -migrate-verify-volumes=false) to skip
// the extra source+dest re-export per volume.
func (s *Service) SetVerifyVolumes(v bool) { s.verifyVolumes = v }

// volumeManifest exports a host's volume and fingerprints its tar stream.
func (s *Service) volumeManifest(ctx context.Context, host, name string) (Manifest, error) {
	rc, err := s.client.VolumeExport(ctx, host, name)
	if err != nil {
		return nil, err
	}
	defer rc.Close()
	return buildManifest(rc)
}

// CopyVolume streams a named volume's contents from one host to another through
// an in-process pipe — the data crosses the daemon's network (two connections)
// but never its disk. The destination volume must already exist. The source is
// only ever read, so a failed copy leaves it untouched (migrate relies on this).
func (s *Service) CopyVolume(ctx context.Context, fromHost, toHost, name string) error {
	rc, err := s.client.VolumeExport(ctx, fromHost, name)
	if err != nil {
		return fmt.Errorf("export volume %q from %s: %w", name, fromHost, err)
	}
	defer rc.Close()

	pr, pw := io.Pipe()
	copyDone := make(chan struct{})
	go func() {
		_, cerr := io.Copy(pw, rc)
		// nil cerr closes the pipe with EOF (clean); an error closes it so the
		// importer's read fails too.
		pw.CloseWithError(cerr)
		close(copyDone)
	}()

	importErr := s.client.VolumeImport(ctx, toHost, name, pr)
	// Unblock the copy goroutine if the importer stopped reading early, then
	// wait for it so we never leak it. CloseWithError(nil) == Close, harmless
	// after a fully-consumed stream.
	pr.CloseWithError(importErr)
	<-copyDone

	// Note: a mid-stream source-read failure propagates through the pipe and
	// surfaces here as importErr, so the message names the destination rather
	// than the source. io.Pipe couples the two errors (a failed source read and
	// a rejecting importer both yield the same pipe error), so cleanly
	// distinguishing the locus needs a read-tracking wrapper — deferred to the
	// migrate handler (#34), which is this primitive's first caller.
	if importErr != nil {
		return fmt.Errorf("import volume %q to %s: %w", name, toHost, importErr)
	}
	return nil
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

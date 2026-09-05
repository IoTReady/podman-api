package instance

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"maps"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/iotready/podman-api/extension"
	"github.com/iotready/podman-api/internal/config"
	"github.com/iotready/podman-api/internal/ingress"
	"github.com/iotready/podman-api/internal/podman"
	"github.com/iotready/podman-api/internal/render"
	"github.com/iotready/podman-api/internal/store"
)

// Sentinel errors mapped by the API layer to JSON error codes.
var (
	ErrUnknownHost       = errors.New("unknown host")
	ErrHostAlreadyExists = errors.New("host already exists")
	ErrHostHasBackups    = errors.New("host has backups; rename would orphan their blob storage")
	ErrUnknownTemplate   = errors.New("unknown template")
	ErrInstanceNotFound  = errors.New("instance not found")
	ErrInstanceExists    = errors.New("instance already exists")
	ErrHostSecretMissing = errors.New("required host secret missing")
	ErrImagePull         = errors.New("image pull failed")
	ErrHostDraining      = errors.New("host is draining")
	ErrPortConflict      = errors.New("required host port already in use")
	// ErrNetworkNameConflict marks the DNS-name collision firstNameConflictFor
	// reports, so the API layer can answer 409 network_name_conflict instead of
	// 500 internal. It is a conflict with another instance's claim, not a
	// malformed request: the caller fixes it by renaming or by keeping the two
	// instances off the network, never by re-sending the same body (#288 review).
	ErrNetworkNameConflict = errors.New("shared-network DNS name already claimed")
	ErrSameHost            = errors.New("source and destination host are the same")
	ErrStoreDisabled       = errors.New("migrate requires the state store")
	ErrVolumeIntegrity     = errors.New("volume copy failed integrity check")

	ErrBackupNotFound      = errors.New("backup not found")
	ErrBackupNotRestorable = errors.New("backup is not restorable")
	ErrBackupBusy          = errors.New("backup has a backup or restore in flight")
	ErrBackupsDisabled     = errors.New("backups require a blob store (-backup-dir)")
	ErrInvalidBackupScope  = errors.New("backup scope names a volume that cannot be backed up")
)

// BackupMarkerNone is the one marker literal the core interprets. A volume
// declaring `backup: none` is never exported by a backup, on any path. Every
// other marker string stays opaque — the grammar (cadence, mode) belongs to a
// commercial BackupScheduler, and the core ascribes it no meaning.
//
// It is an alias of render.BackupMarkerNone (itself an alias of the canonical
// extension.BackupMarkerNone) rather than its own literal, so the definitions
// cannot drift.
const BackupMarkerNone = render.BackupMarkerNone

// IsBackupMarkerNone reports whether a raw marker is the `none` veto. Use this
// rather than `== BackupMarkerNone`: the comparison folds case and trims
// whitespace so a near-miss stored before the registration validator existed
// still vetoes, instead of failing open and exporting the volume. See
// extension.IsBackupMarkerNone.
func IsBackupMarkerNone(marker string) bool { return render.IsBackupMarkerNone(marker) }

// ApplyOptions controls the side effects of Apply beyond the request body.
type ApplyOptions struct {
	Replace  bool // if false and the pod exists, return ErrInstanceExists
	SkipPull bool // if true, do not pre-pull container images (CI / local-only refs)
	// AllowMissingSecrets relaxes the "every PerInstance secret must be present"
	// validation rule for this Apply. It is used by the secret-rotation path:
	// rotation overlays new values onto a stored spec and re-applies it, and must
	// not be blocked just because a PerInstance secret was already unset in that
	// spec (e.g. a template that gained a secret after the instance was deployed).
	// The "unknown secret" check still applies. Deploys never set this.
	AllowMissingSecrets bool
	// RestoreIntent, when non-nil, requests a one-shot point-in-time restore for
	// this Apply: it is handed to the SidecarInjector but is NOT persisted into
	// the stored spec, so the reconcile path never replays it. Only the
	// point-in-time restore trigger sets this; ordinary deploys leave it nil.
	RestoreIntent *extension.RestoreIntent
	// SupersededSlug names an instance of the SAME template on the same host
	// that this apply replaces and whose spec is about to be deleted — the old
	// slug of a rename. Its claims on host-scoped namespaces (ingress domains,
	// shared-network DNS names) are therefore not conflicts: they are being
	// handed over, not contended.
	//
	// Rename applies the new slug BEFORE deleting the old spec (so a failed
	// apply can roll back to a still-registered instance), which means the old
	// instance is present in the store during the new one's validation. Without
	// this, renaming any instance carrying a domain or an alias is impossible.
	// Only rename sets it.
	SupersededSlug string
}

// ApplyRequest is the body of POST /instances and PUT /instances/{...}.
type ApplyRequest struct {
	Template   string            `json:"template"`
	Slug       string            `json:"slug"`
	Parameters map[string]any    `json:"parameters"`
	Secrets    map[string]string `json:"secrets"`
	Domains    []string          `json:"domains,omitempty"`
	// Networks are EXTRA shared-network memberships for this instance alone,
	// unioned with (never replacing) the ones its template declares (#270). It
	// is how one template serves two groups that must not share a network, and
	// how an existing instance joins a new network without a template edit that
	// would move every other instance of it too.
	//
	// Union, not override: an instance cannot drop the connectivity its
	// template guarantees. Opting out of a template network is a template-split
	// problem, deliberately not solved here.
	//
	// Validated exactly as a template's own `networks:` are
	// (render.ValidateNetworkList) and persisted on the spec, so boot converge
	// replays this instance's own set rather than re-deriving it.
	Networks []render.Network `json:"networks,omitempty"`
}

// DeleteOptions controls cleanup beyond the pod itself.
type DeleteOptions struct {
	PruneVolumes bool
	PruneSecrets bool
}

// Store is the persistence surface the instance Service needs: the desired-state
// spec/host-secret store plus the template catalog. main wires a single
// store.DB, which satisfies this; tests pass a store.Memory. The Service always
// has a store — callers MUST SetStore before use.
type Store interface {
	store.Store
	store.TemplateStore
	store.BackupStore
}

// Service orchestrates instance operations against podman hosts.
type Service struct {
	client     podman.Client
	hosts      atomic.Pointer[map[string]config.Host] // hot-swappable on SIGHUP
	store      Store                                  // template catalog + desired-state store; set via SetStore before use
	ingress    ingress.Controller                     // never nil; ingress.Disabled{} when off
	ingressNet string                                 // shared ingress network; "" when ingress disabled
	blobs      BlobStore                              // backup artifact store; set via SetBlobStore (nil → backups disabled)
	sidecar    SidecarInjector                        // sidecar injector; set via SetSidecarInjector (nil → no injection)
	instCache  *instanceCache                         // per-host read cache over ListAllInstances (default 3s TTL)
	statsCache *statsCache                            // per-host container resource samples (poller-fed)
	volCache   *volumeUsageCache                      // per-host volume sizing (slow sampler-fed)

	verifyVolumes bool // verify each migrated volume's content before reaping the source

	mu        sync.Mutex
	locks     map[string]*sync.Mutex // key = host|template|slug
	hostLocks map[string]*sync.Mutex // key = host; serializes host-wide domain claims (#82)
	// tmplMu serializes template mutations (create/update/clone/delete) so each
	// check-then-act is atomic (#61). Apply takes it as a *read* lock only around
	// its final template-existence recheck + PutSpec, so a concurrent
	// DeleteTemplate (write lock) cannot delete a template between Apply's recheck
	// and its spec persist — closing the delete-vs-Apply orphan race (#61 review-2).
	tmplMu sync.RWMutex
}

func NewService(client podman.Client, hosts []config.Host) *Service {
	s := &Service{
		client:        client,
		locks:         map[string]*sync.Mutex{},
		hostLocks:     map[string]*sync.Mutex{},
		verifyVolumes: true,
	}
	s.ingress = ingress.Disabled{}
	s.SetHosts(hosts)
	s.instCache = newInstanceCache(3 * time.Second)
	s.statsCache = newStatsCache()
	s.volCache = newVolumeUsageCache()
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

// SetStore wires the template catalog + desired-state store. The store is
// mandatory — every template lookup and spec persist goes through it — so main
// must call this at startup, before the server begins accepting requests
// (tests pass a store.Memory). Unlike SetHosts it is NOT a concurrent hot-swap.
func (s *Service) SetStore(st Store) { s.store = st }

// SetBlobStore wires the backup artifact store. Backups/restores are refused
// (ErrBackupsDisabled) until this is set; main always sets it.
func (s *Service) SetBlobStore(bs BlobStore) { s.blobs = bs }

// SetSidecarInjector wires a sidecar injector that is called after template
// rendering but before the pod YAML is applied.
func (s *Service) SetSidecarInjector(si SidecarInjector) { s.sidecar = si }

// SetInstanceCacheTTL replaces the per-host ListAllInstances cache with one of
// the given TTL. ttl == 0 disables caching (live passthrough). Wire from the
// server if the operator wants a non-default window; the default is 3s.
func (s *Service) SetInstanceCacheTTL(ttl time.Duration) {
	s.instCache = newInstanceCache(ttl)
}

// invalidateInstances drops the cached instance list for a host so the next
// ListAllInstances re-sweeps. Called by mutating methods.
func (s *Service) invalidateInstances(host string) {
	s.instCache.invalidate(host)
}

// SetIngress enables ingress reconciliation. network is the shared podman
// network app pods join; passing a real controller marks ingress enabled so
// Apply will accept domains. Call with ingress.Disabled{} and "" to disable.
func (s *Service) SetIngress(c ingress.Controller, network string) {
	s.ingress = c
	s.ingressNet = network
}

func (s *Service) ingressEnabled() bool { return s.ingressNet != "" }

// ensureNetworks creates every podman network the instance's pod must join on
// host, and returns their names in join order: the shared ingress network first
// (when ingress is enabled AND the template declares ingress), then the
// template's own `networks:` declarations (#243).
//
// The two are independent: a template with no ingress still joins its declared
// networks, and it does so on a server started without -ingress — that is the
// whole point of the seam, letting two instances reach each other by pod DNS
// name with no host port published at all.
//
// Ensuring happens before the caller plays the pod, because podman fails a
// `kube play` onto a network that does not exist yet — the first deploy on a
// fresh host would otherwise fail. The result is de-duplicated, so a template
// that names the ingress network explicitly does not produce a duplicate
// --network (which podman rejects).
//
// This is the ONE place the join list is assembled; both the apply path and the
// boot-converge path call it, so they cannot drift on what a pod is attached to.
func (s *Service) ensureNetworks(ctx context.Context, hostID string, m render.Meta) ([]string, error) {
	joins := s.joinNetworks(m)
	out := make([]string, 0, len(joins))
	for _, j := range joins {
		if err := s.client.NetworkEnsure(ctx, hostID, j.Name); err != nil {
			return nil, fmt.Errorf("ensure network %q on %s: %w", j.Name, hostID, err)
		}
		out = append(out, networkArg(j))
	}
	return out, nil
}

// joinNetworks resolves the exact set of networks an instance of a template
// joins, in join order, with each network's aliases merged. It is pure: no host
// IO, so the apply-time uniqueness check (validateNetworkAliases) can ask "what
// would this pod be called on which networks" without touching podman, and
// cannot drift from what ensureNetworks actually plays.
//
// De-duplication merges rather than drops: a template that names the ingress
// network explicitly, with aliases, keeps those aliases instead of losing them
// to the bare ingress entry that leads the list.
func (s *Service) joinNetworks(m render.Meta) []render.Network {
	var want []render.Network
	if s.ingressEnabled() && m.Ingress != nil {
		want = append(want, render.Network{Name: s.ingressNet})
	}
	want = append(want, m.Networks...)

	out := make([]render.Network, 0, len(want))
	at := make(map[string]int, len(want))
	for _, n := range want {
		i, dup := at[n.Name]
		if !dup {
			at[n.Name] = len(out)
			out = append(out, render.Network{Name: n.Name, Aliases: append([]string(nil), n.Aliases...)})
			continue
		}
		for _, a := range n.Aliases {
			if !slices.Contains(out[i].Aliases, a) {
				out[i].Aliases = append(out[i].Aliases, a)
			}
		}
	}
	return out
}

// metaWithNetworks returns m with extra unioned into its declared networks, so
// every consumer downstream — ensureNetworks, joinNetworks, the claim machinery
// — keeps taking a single render.Meta and cannot drift on what an instance is
// attached to (#270). It mirrors metaWithContainerNames: a derived view of a
// template's meta for one instance, never a stored template edit.
//
// Appending duplicates is safe and load-bearing: joinNetworks de-duplicates by
// name and MERGES aliases, so an override naming a network the template already
// declares adds its aliases to that join instead of producing a second one.
func metaWithNetworks(m render.Meta, extra []render.Network) render.Meta {
	if len(extra) == 0 {
		return m
	}
	out := m
	out.Networks = append(slices.Clone(m.Networks), extra...)
	return out
}

// networkArg formats one membership as podman's --network value. The
// "name:alias=a,alias=b" spelling is parsed server-side by kube play
// (specgen.ParseNetworkFlag → parseBridgeNetworkOptions), so aliases need no
// separate API call — they ride along with the join.
func networkArg(n render.Network) string {
	if len(n.Aliases) == 0 {
		return n.Name
	}
	return n.Name + ":alias=" + strings.Join(n.Aliases, ",alias=")
}

// validateIngress enforces the ingress rules for a request carrying domains
// BEFORE the pod is played or the spec is persisted: ingress must be enabled,
// the template must declare ingress:, and each domain must be unclaimed by any
// other instance on the host — bar the one this apply supersedes, if any (see
// ApplyOptions.SupersededSlug). Enforcing them up front keeps an invalid spec out
// of the store; otherwise it would poison deriveRoutes and fail every later
// reconcile on the host. A request with no domains is always allowed.
func (s *Service) validateIngress(ctx context.Context, host string, req ApplyRequest, tmpl store.Template, opts ApplyOptions) error {
	if len(req.Domains) == 0 {
		return nil
	}
	if !s.ingressEnabled() {
		return fmt.Errorf("instance %s/%s declares domains but ingress is disabled", req.Template, req.Slug)
	}
	if tmpl.Meta.Ingress == nil {
		return fmt.Errorf("instance %s/%s declares domains but template %q has no ingress", req.Template, req.Slug, req.Template)
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
		if k.Template == req.Template && (k.Slug == req.Slug || k.Slug == opts.SupersededSlug) {
			// The instance being (re)applied, or the one it supersedes (rename):
			// neither holds a domain against it.
			continue
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

// netClaim is one instance's claim on shared-network DNS names: which networks
// its pod joins and, for each, the names it answers to on that network.
type netClaim struct {
	template string
	slug     string
	joins    []render.Network
	// containers are the template's LITERAL container names. podman aliases the
	// pod by each of them on every network it joins, so they are claims the
	// template never wrote down — see claimedNames (#272).
	containers []string
	// ingressNet is the shared ingress network's name, or "" when ingress is
	// disabled. It is the ONE network container names are not enforced on, and
	// it is matched by IDENTITY rather than by how the membership was declared —
	// see claimedNames.
	ingressNet string
}

// claimName is one DNS name a pod answers to on a network, with where it came
// from — the kind is only ever used to explain a collision, since an operator
// looking at `aliases:` cannot otherwise tell why "db" was already taken.
type claimName struct {
	name string
	kind string
}

const (
	kindPodName   = "pod DNS name"
	kindAlias     = "declared alias"
	kindContainer = "container name"
)

// newClaim builds the claim an instance of tmpl/slug makes under meta.
func (s *Service) newClaim(tmpl, slug string, m render.Meta) netClaim {
	return netClaim{
		template:   tmpl,
		slug:       slug,
		joins:      s.joinNetworks(m),
		containers: m.ContainerNames,
		ingressNet: s.ingressNet,
	}
}

// claimedNames returns every DNS name the claim registers on network net: the
// pod's own podman DNS name, the template's declared aliases, and — on every
// network but the ingress one — its literal container names. It reports false
// when the claim does not join that network at all.
//
// The container names are there because podman puts them there: kube play adds
// every container name in the played YAML as an alias on every joined network
// (podman 5.8.2, pkg/domain/infra/abi/play.go: "Add the original container names
// from the kube yaml as aliases"). Two instances of a template whose container
// is named "db" therefore both answer to "db", which is the same silent
// arbitrary resolution the declared-alias rule exists to prevent (#272). Only
// LITERAL names count: a name carrying a parameter ("db-{{.slug}}") is a
// different name on every instance and claims nothing shared.
//
// The shared ingress network is deliberately EXEMPT. Every ingress template
// joins it — it is not an opt-in namespace claim the way `networks:` is — the
// ingress controller addresses pods by pod DNS name, and enforcing container
// names there would refuse the second instance of every web template on the
// host. Declared aliases and pod names are still enforced there, as before.
//
// The exemption is keyed on the network's IDENTITY, not on how the pod came to
// join it. A template may name the ingress network in its own `networks:` block
// — that is the supported way to declare an alias on it, and joinNetworks
// merges rather than drops — so an exemption keyed on "the template did not
// declare this network" would drop away for exactly that legal configuration
// and refuse the second instance of such a web template on its container name.
func (c netClaim) claimedNames(net string) ([]claimName, bool) {
	for _, j := range c.joins {
		if j.Name != net {
			continue
		}
		out := make([]claimName, 0, len(j.Aliases)+len(c.containers)+1)
		out = append(out, claimName{podName(c.template, c.slug), kindPodName})
		for _, a := range j.Aliases {
			out = append(out, claimName{a, kindAlias})
		}
		if net != c.ingressNet {
			for _, n := range c.containers {
				out = append(out, claimName{n, kindContainer})
			}
		}
		return out, true
	}
	return nil, false
}

// firstNameConflictFor returns the first DNS name on which subject collides with
// one of peers, or nil if subject's names are all free.
//
// Podman does not police this: two pods claiming "mariadb" on frappe-shared both
// register, and resolution picks one arbitrarily. That failure is silent and
// looks like an application bug, so the daemon refuses to create it — the same
// way validateIngress refuses a domain already claimed.
//
// Both directions of clash are caught, because a name can be claimed two ways:
//
//   - alias vs alias — the common case. Note aliases come from the TEMPLATE, so
//     this necessarily forbids a second instance of an aliased template on the
//     same host and network. That is the intended reading of "the database on
//     this network": one instance answers to it.
//   - alias vs pod DNS name — an alias equal to another instance's
//     "<template>-<slug>" would shadow that instance, and a new instance whose
//     pod name collides with an existing alias would be shadowed by it.
//
// It is deliberately SUBJECT-SCOPED, not "is this host conflict-free": a clash
// between two OTHER instances is not this subject's problem, and reporting it
// here would block every unrelated deploy on the host until someone cleaned it
// up. That state is reachable and transient by design — rename holds the old and
// new spec simultaneously for up to verifyTimeout — so a host-wide reading would
// fail unrelated applies for minutes at a time, on any network, and would also
// contradict converge, which tolerates the same state and merely warns
// (#269 re-review finding 1).
//
// It is pure, so the three callers that must agree — apply
// (validateNetworkAliases), template edit (checkTemplateNetworkConflicts) and
// boot converge (warnOnNetworkNameConflict) — cannot drift on what counts as a
// conflict.
func firstNameConflictFor(subject netClaim, peers []netClaim) error {
	for _, j := range subject.joins {
		mine, _ := subject.claimedNames(j.Name)
		claimed := make(map[string]string, len(mine))
		for _, n := range mine {
			// A name claimed two ways by the SAME pod (an alias that repeats a
			// container name) is not a conflict; keep the first kind seen.
			if _, dup := claimed[n.name]; !dup {
				claimed[n.name] = n.kind
			}
		}
		for _, p := range peers {
			theirs, joined := p.claimedNames(j.Name)
			if !joined {
				continue
			}
			for _, n := range theirs {
				mineKind, clash := claimed[n.name]
				if !clash {
					continue
				}
				err := fmt.Errorf("%w: networks: DNS name %q on network %q is claimed by both %s/%s (%s) and %s/%s (%s)",
					ErrNetworkNameConflict, n.name, j.Name, subject.template, subject.slug, mineKind, p.template, p.slug, n.kind)
				if mineKind == kindContainer || n.kind == kindContainer {
					// The container-name half of a collision is invisible in the
					// template meta — podman registers it — so the message has to
					// say where it came from and how to get out of it.
					err = fmt.Errorf("%w; podman registers every container name in a pod as an alias on each network it joins, so give the container a per-instance name (e.g. `name: %s-{{.slug}}`) or keep the two instances off this network", err, n.name)
				}
				return err
			}
		}
	}
	return nil
}

// appliedNetworks returns the per-instance networks stored for one instance, or
// nil when the instance has no spec. It reads the ListSpecNetworks projection
// rather than GetSpec so it never decrypts: callers on the recovery paths (see
// Upgrade) must not be broken by an unreadable secrets blob.
//
// The projection is host-wide, which is one extra row scan per call — acceptable
// on the low-frequency upgrade path, and the whole point is that no key is
// needed. A store error is returned: silently dropping the networks here would
// reintroduce the very detach the carry-forward exists to prevent.
func (s *Service) appliedNetworks(ctx context.Context, host, tmpl, slug string) ([]render.Network, error) {
	rows, err := s.store.ListSpecNetworks(ctx, host)
	if err != nil {
		return nil, fmt.Errorf("load instance networks: %w", err)
	}
	for _, r := range rows {
		if r.Template == tmpl && r.Slug == slug {
			return slices.Clone(r.Networks), nil
		}
	}
	return nil, nil
}

// hostClaims builds the DNS-name claims of every instance on host. metaFor lets
// a caller substitute a template's meta — the template-edit check asks "what
// would the claims be if this edit landed" without persisting it first.
//
// It decrypts no specs: SpecNetworks carries template+slug — all a pod name and
// (via the template) an alias set needs — plus each instance's own extra
// networks, which live in no template (#270). Templates are cached, so a host of
// N instances costs one ListSpecNetworks plus one GetTemplate per DISTINCT
// template.
//
// An instance whose template is gone is skipped: its pod's membership is
// unknowable from here, and a lingering orphan must not block every apply.
func (s *Service) hostClaims(ctx context.Context, host string, metaFor map[string]render.Meta) ([]netClaim, error) {
	keys, err := s.store.ListSpecNetworks(ctx, host)
	if err != nil {
		return nil, err
	}
	cache := make(map[string]render.Meta, len(metaFor))
	maps.Copy(cache, metaFor)
	out := make([]netClaim, 0, len(keys))
	for _, k := range keys {
		meta, ok := cache[k.Template]
		if !ok {
			t, err := s.store.GetTemplate(ctx, k.Template)
			if errors.Is(err, store.ErrNotFound) {
				continue
			}
			if err != nil {
				return nil, err
			}
			meta = metaWithContainerNames(t)
			cache[k.Template] = meta
		}
		// Per-INSTANCE networks are unioned after the per-template lookup, never
		// into the cache: two instances of one template can carry different
		// overrides, and caching one instance's would attribute it to the rest.
		out = append(out, s.newClaim(k.Template, k.Slug, metaWithNetworks(meta, k.Networks)))
	}
	return out, nil
}

// validateNetworkAliases refuses an apply whose pod would answer to a DNS name
// another instance on the host already claims on a shared network, BEFORE the
// pod is played or the spec is persisted (#269) — the same "keep the invalid
// state out of the store" reasoning validateIngress applies to domains.
//
// Peers are measured against their template's CURRENT meta, which is what the
// next reconcile will play them with, not necessarily what they are running now
// — the same divergence networksChanged warns about at edit time.
//
// opts.SupersededSlug names an instance being replaced by this apply (rename),
// whose claims are therefore about to be released; see ApplyOptions.
func (s *Service) validateNetworkAliases(ctx context.Context, host string, req ApplyRequest, tmpl store.Template, opts ApplyOptions) error {
	meta := metaWithNetworks(metaWithContainerNames(tmpl), req.Networks)
	mine := s.newClaim(req.Template, req.Slug, meta)
	if len(mine.joins) == 0 {
		return nil
	}
	// The substituted meta deliberately carries the container names but NOT this
	// request's per-instance networks: it stands in for OTHER instances of the
	// same template, whose own memberships come from their own SpecKey.
	peers, err := s.hostClaims(ctx, host, map[string]render.Meta{req.Template: metaWithContainerNames(tmpl)})
	if err != nil {
		return fmt.Errorf("networks: check alias uniqueness on %s: %w", host, err)
	}
	if err := firstNameConflictFor(mine, excluding(peers, req.Template, req.Slug, opts.SupersededSlug)); err != nil {
		return fmt.Errorf("%w on %s", err, host)
	}
	return nil
}

// excluding drops the named instances of tmpl from claims: the instance being
// applied (it does not conflict with itself) and, when set, the one it
// supersedes. An empty slug matches nothing.
func excluding(claims []netClaim, tmpl string, slugs ...string) []netClaim {
	out := make([]netClaim, 0, len(claims))
	for _, c := range claims {
		if c.template == tmpl && slices.Contains(slugs, c.slug) {
			continue
		}
		out = append(out, c)
	}
	return out
}

// warnOnNetworkNameConflict logs when the instance about to be converged shares
// a shared-network DNS name with another instance on the host. The boot path
// plays it anyway — see the call site — so this is the operator's only signal
// that two pods are answering to one name.
//
// Never fails the converge: a store hiccup here must not keep an instance down.
//
// subject is the converging instance's own effective meta (template + ITS
// per-instance networks); tmplMeta is the template's meta WITHOUT them, used to
// stand in for other instances of the same template. Keeping the two apart is
// load-bearing, exactly as it is in validateNetworkAliases: hostClaims seeds its
// template cache from tmplMeta, so folding the subject's per-instance networks
// into it would attribute this instance's memberships to every peer sharing its
// template and report a conflict on a network no peer has joined (#270 review).
func (s *Service) warnOnNetworkNameConflict(ctx context.Context, host, tmpl, slug string, subject, tmplMeta render.Meta) {
	if len(subject.Networks) == 0 {
		return
	}
	// NOTE this costs a ListSpecKeys plus a GetTemplate per distinct template,
	// per converged instance — O(N) host scans over a boot of N instances, and
	// one log line per instance for a conflict that involves two. It also costs
	// O(N) TEMPLATE RENDERS: every template row whose stored ContainerNames is
	// nil is re-derived from its body by metaWithContainerNames on each of those
	// scans (two renders plus a YAML decode each), and a nil is not only a legacy
	// row — a template whose container names are all parameter-dependent, which
	// is the shape this check recommends, stores nil for as long as it exists.
	// Cheap enough at current fleet sizes; hoist the claim set to the caller if
	// that changes.
	claims, err := s.hostClaims(ctx, host, map[string]render.Meta{tmpl: tmplMeta})
	if err != nil {
		return
	}
	if err := firstNameConflictFor(s.newClaim(tmpl, slug, subject), excluding(claims, tmpl, slug)); err != nil {
		log.Printf("WARNING: converge %s/%s on %s: %v; podman will resolve it arbitrarily", tmpl, slug, host, err)
	}
}

// checkTemplateNetworkConflicts refuses a template edit that would make two
// LIVE instances claim one DNS name on one network (#269 review finding 2).
//
// Apply-time validation alone is not enough, because an edit introduces the
// conflict everywhere at once without going through apply: two instances of a
// template that gains an alias would each be refused every later apply — and
// applyLocked is also the upgrade, secret-rotation and parameter-update path, so
// both instances become permanently un-upgradeable, each error naming the other,
// with no in-product way out but reverting the template. Refusing the edit keeps
// that state unreachable.
//
// It only inspects hosts, never podman. It runs for EVERY template, including
// one that declares no networks of its own: an instance can join a network its
// template never names (#270), so a container-name edit on a network-less
// template can still collide — see the note in the body.
func (s *Service) checkTemplateNetworkConflicts(ctx context.Context, t store.Template) error {
	// No `if len(t.Meta.Networks) == 0 { return nil }` shortcut: it was sound
	// only while every membership came from the template. An instance can now
	// join a network its template never names (#270), so a template declaring
	// none can still introduce a cross-instance collision — a container-name
	// edit claims a name on every non-ingress network the pod joins (#272), and
	// those networks may all be per-instance. Skipping the scan would let such
	// an edit through and leave both instances permanently un-appliable (this is
	// also the upgrade/rotate/parameter-update path), which is precisely the
	// unreachable state this function exists to preserve. The scan is one
	// ListSpecNetworks plus a GetTemplate per distinct template per host, on the
	// rare template-write path (#270 review).
	for _, h := range s.hostsSnap() {
		claims, err := s.hostClaims(ctx, h.ID, map[string]render.Meta{t.Meta.ID: metaWithContainerNames(t)})
		if err != nil {
			// A host that cannot be read cannot be cleared either. Fail the edit
			// rather than let a conflict through on a store blip.
			return fmt.Errorf("networks: check alias uniqueness on %s: %w", h.ID, err)
		}
		// Only THIS template's instances are subjects: a pre-existing clash
		// between two unrelated instances is not this edit's doing, and failing
		// the edit for it would make an unrelated mess un-editable.
		for _, c := range claims {
			if c.template != t.Meta.ID {
				continue
			}
			if err := firstNameConflictFor(c, excluding(claims, c.template, c.slug)); err != nil {
				return fmt.Errorf("%w on %s", err, h.ID)
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

// hasTemplate reports whether a template id is present in the catalog. It
// distinguishes a genuinely-absent template (store.ErrNotFound → (false, nil))
// from a transient store error ((false, err)): the caller (ReconcileMigrate)
// makes a terminal "template gone" decision on absence, so a recoverable lookup
// failure must NOT be mistaken for absence.
func (s *Service) hasTemplate(ctx context.Context, id string) (bool, error) {
	_, err := s.store.GetTemplate(ctx, id)
	if errors.Is(err, store.ErrNotFound) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return true, nil
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

// hostLock serializes the host-wide domain-uniqueness check and the spec
// persist that claims those domains. Without it, two Applies for *different*
// instances on one host hold *different* instanceLocks, so both can pass
// validateIngress before either persists and both claim the same domain. Apply
// takes it (before the instanceLock — a consistent order so the two never
// deadlock) only for domain-carrying requests. (#82)
func (s *Service) hostLock(host string) *sync.Mutex {
	s.mu.Lock()
	defer s.mu.Unlock()
	m, ok := s.hostLocks[host]
	if !ok {
		m = &sync.Mutex{}
		s.hostLocks[host] = m
	}
	return m
}

func (s *Service) lookup(ctx context.Context, host, tmpl string) (store.Template, error) {
	if _, ok := s.host(host); !ok {
		return store.Template{}, ErrUnknownHost
	}
	t, err := s.store.GetTemplate(ctx, tmpl)
	if errors.Is(err, store.ErrNotFound) {
		return store.Template{}, ErrUnknownTemplate
	}
	if err != nil {
		return store.Template{}, fmt.Errorf("lookup template %q: %w", tmpl, err)
	}
	return t, nil
}

func podName(tmpl, slug string) string { return tmpl + "-" + slug }

// volumeName is the single definition of a managed volume's name on a host:
// the template's declared (short) volume name namespaced by instance. Every
// producer AND every consumer must go through it — since #209 the name is also
// a Prometheus label that podman_api_volume_size_bytes joins on, so drift
// between two construction sites no longer fails loudly (a missing volume, an
// erroring operation) but silently: the join misses and the panel renders
// empty, with nothing logged anywhere. See #211.
func volumeName(tmpl, slug, short string) string { return volumeNamePrefix(tmpl, slug) + short }

// volumeNamePrefix is volumeName's instance-scoped prefix, for the one caller
// (rename) that must strip it off an existing name to recover the short name.
func volumeNamePrefix(tmpl, slug string) string { return podName(tmpl, slug) + "-" }

// appliedVolumeNames returns the (short) volume names a template declares, for
// recording in store.Spec.AppliedVolumes at Apply time (#257). Always non-nil
// so an empty declaration round-trips as "known: no volumes" rather than
// "unknown" (nil) — see the Spec field doc.
func appliedVolumeNames(vols []render.Volume) []string {
	names := make([]string, 0, len(vols))
	for _, v := range vols {
		names = append(names, v.Name)
	}
	return names
}

// appliedVolumeMeta returns, for each declared volume, its backup marker and
// exclude patterns keyed by short name — for recording in
// store.Spec.AppliedVolumeMeta at Apply time (#256 review round-4). Always
// non-nil, mirroring appliedVolumeNames, so an empty declaration round-trips
// as "known: no volumes" rather than "unknown".
func appliedVolumeMeta(vols []render.Volume) map[string]store.AppliedVolumeMarker {
	meta := make(map[string]store.AppliedVolumeMarker, len(vols))
	for _, v := range vols {
		meta[v.Name] = store.AppliedVolumeMarker{Backup: v.Backup, Exclude: v.Exclude}
	}
	return meta
}

func instanceSecretName(tmpl, slug, name string) string {
	return extension.InstanceSecretName(tmpl, slug, name)
}

// applyImagePullTimeout bounds the ENTIRE pre-pull phase inside applyLocked
// (Apply/UpgradeImage) — one shared deadline for every image in the rendered
// pod spec (containers and initContainers), not a fresh budget per image —
// independent of the -image-pull-timeout flag (podman.SetImagePullTimeout).
// Apply/UpgradeImage hold the real instance's
// lock (and hostLock for domain-carrying requests) across this call — the
// extended -image-pull-timeout budget (2h default) is meant for the migrate
// preflight path, which runs under a separate migrate-serialization lock
// keyed on a sentinel pseudo-host, not the real instance/host lock (#238).
// Applying the extended budget here too would let one slow pull block every
// other request against the instance (and the host's domain checks) for up
// to 2h instead of the pre-#238 10-minute worst case.
const applyImagePullTimeout = 10 * time.Minute

// Apply creates or replaces an instance. If opts.Replace is false and the pod
// exists, returns ErrInstanceExists. Unless opts.SkipPull is set, every container
// image referenced in the rendered Pod spec is pulled before the manifest is
// played. Apply acquires the per-host lock (domain-carrying requests only, taken
// before the instance lock — a consistent order so the two never deadlock) and the
// per-instance lock, then runs applyLocked.
func (s *Service) Apply(ctx context.Context, host string, req ApplyRequest, opts ApplyOptions) error {
	// Domain claims are checked host-wide (validateIngress) but only become
	// durable at PutSpec far below; the per-instance lock does not serialize two
	// different instances racing for the same domain on one host. Hold a per-host
	// lock for domain-carrying requests so the check→persist claim is atomic.
	// Because the spec is persisted AFTER PlayKube (to avoid leaving a poison
	// spec when the play fails), this lock necessarily spans the image pull and
	// pod play too: two *ingress* deploys to the same host serialize end-to-end,
	// not just over the store access. That's acceptable for a single-operator
	// system — non-ingress and different-host deploys take no host lock and stay
	// fully concurrent. Taken before the instanceLock (consistent order → no
	// deadlock) and only when the request can create a host-wide claim.
	//
	// Per-instance networks (#270) are the second such claim: validateNetworkAliases
	// is the same check→persist pair, and the claim only becomes durable at
	// PutSpec far below, so two concurrent domain-less applies each requesting
	// the same network + alias would both read an unclaimed host and both
	// succeed — the duplicate DNS name #269 exists to prevent, now reachable
	// from a scripted rollout because the aliases are request data rather than a
	// property of the template. (A collision between two TEMPLATE-declared alias
	// sets can still race the same way; that predates this change and needs the
	// template lookup Apply does not do before locking.)
	if len(req.Domains) > 0 || len(req.Networks) > 0 {
		hl := s.hostLock(host)
		hl.Lock()
		defer hl.Unlock()
	}
	lock := s.instanceLock(host, req.Template, req.Slug)
	lock.Lock()
	defer lock.Unlock()
	return s.applyLocked(ctx, host, req, opts)
}

// ApplyAndObserve creates or replaces an instance (via Apply), waits for
// container healthchecks to pass (up to deployVerifyTimeout), then returns the
// observed state. On readiness timeout the operation still succeeds but
// Observed.Warnings carries a human-readable message.
func (s *Service) ApplyAndObserve(ctx context.Context, host string, req ApplyRequest, opts ApplyOptions) (Observed, error) {
	if err := s.Apply(ctx, host, req, opts); err != nil {
		return Observed{}, err
	}
	readyErr := s.waitReady(ctx, host, req.Template, req.Slug, readyOpts{
		timeout:     deployVerifyTimeout,
		stableCount: deployVerifyStableCount,
	})
	obs, err := s.Get(ctx, host, req.Template, req.Slug)
	if err != nil {
		return Observed{}, err
	}
	if w := readinessWarning(readyErr); w != "" {
		obs.Warnings = append(obs.Warnings, w)
	}
	return obs, nil
}

// applyLocked is the lock-free core of Apply: it performs the full create/replace
// (validate → play → persist → ingress) assuming the caller already holds the
// per-instance lock (and, for domain-carrying requests, the per-host lock).
// Splitting it out lets the read-modify-write callers (RotateInstanceSecrets,
// UpgradeImage) hold a single lock across GetSpec + re-apply without re-entering
// Apply's non-reentrant lock. Apply is the public, lock-acquiring entry point.
func (s *Service) applyLocked(ctx context.Context, host string, req ApplyRequest, opts ApplyOptions) error {
	// Any create/replace changes this host's instance list; drop the cache on
	// return (after the mutation completes) so the next read is fresh. Deferred
	// so all success and error exits invalidate — an extra sweep on failure is
	// harmless, a stale hit after success is not.
	defer s.invalidateInstances(host)

	tmpl, err := s.lookup(ctx, host, req.Template)
	if err != nil {
		return err
	}
	// Fill any omitted parameters from their ParamDef.Default before validation
	// and render, so callers can omit optional params for a one-click deploy.
	// Caller-supplied values always win; only absent keys are filled.
	req.Parameters = render.ApplyDefaults(tmpl.Meta, req.Parameters)
	// slug is NOT settable through req.Parameters: if the parameters already
	// carry a "slug" key it is pinned back to the canonical req.Slug, the same
	// way migrate.go and rename.go pin it before re-applying. slug is a
	// declared parameter in the bundled templates and drives metadata.name,
	// secretKeyRef.name and claimName, so a "slug" parameter disagreeing with
	// req.Slug would render — and, with Replace, destructively tear down and
	// replace — a *different* pod, while everything else here (the per-instance
	// lock, the podExists/drain gates, instanceSecretName, validateIngress, the
	// persisted spec) keeps using req.Slug. The wrong instance goes down under
	// no lock and the persisted spec describes a pod no later
	// Start/Stop/Delete can find. Every apply path funnels through here, so
	// this pin closes it for all callers, not just the HTTP handlers (which
	// also reject a disagreeing parameters.slug up front — defence in depth).
	//
	// The pin is conditional (only fires when "slug" is already a key) for the
	// same reason as the one in UpdateInstanceParameters: nothing requires a
	// template to declare "slug" as a parameter, and an unconditional write
	// would inject an undeclared key that render.Validate below rejects with
	// `unknown parameter "slug"` on every such template. A caller trying to
	// *introduce* slug where the template doesn't declare it is still rejected
	// by render.Validate, which is the correct outcome. (#201)
	if _, ok := req.Parameters["slug"]; ok {
		req.Parameters["slug"] = req.Slug // canonical slug always wins; pod name must match podName()
	}
	validate := render.Validate
	if opts.AllowMissingSecrets {
		validate = render.ValidateAllowMissingSecrets
	}
	if err := validate(tmpl.Meta, req.Parameters, req.Secrets); err != nil {
		return fmt.Errorf("validate: %w", err)
	}
	// Reject a secret-bearing deploy on a key-less store BEFORE any host mutation.
	// PutSpec (far below) would reject the same request with ErrSecretsNeedKey, but
	// only after secrets were created and the pod played — leaving an orphaned pod
	// and secrets with no spec row. Fail fast here so nothing is mutated. (#61)
	if len(req.Secrets) > 0 && !s.store.SecretsEnabled() {
		return store.ErrSecretsNeedKey
	}
	// Per-instance networks are policed by the same validator as a template's
	// own declarations, before anything is mutated: a malformed name would
	// otherwise reach NetworkEnsure with the pod half-deployed, and a bad one
	// persisted on the spec would fail every later boot converge.
	if err := render.ValidateNetworkList(req.Networks, "networks"); err != nil {
		return fmt.Errorf("validate: %w", err)
	}
	// Validate ingress rules BEFORE playing the pod or persisting the spec, so a
	// rejected request never leaves a poison spec in the store — a persisted spec
	// that violates these rules would fail every later reconcile on the host.
	// deriveRoutes re-checks the same rules at reconcile time as a backstop.
	if err := s.validateIngress(ctx, host, req, tmpl, opts); err != nil {
		return err
	}
	// Same reasoning for shared-network DNS names (#269): a duplicate alias is
	// silent at the podman layer, so it must be refused before the pod is played.
	if err := s.validateNetworkAliases(ctx, host, req, tmpl, opts); err != nil {
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

	yaml, err := render.RenderAndValidate(tmpl.Body, req.Parameters)
	if err != nil {
		return fmt.Errorf("render: %w", err)
	}

	var injectorSecrets []store.InjectorSecret
	if s.sidecar != nil {
		inj, err := s.sidecar.InjectSidecars(ctx, yaml, toExtMeta(tmpl.Meta), req.Parameters, host, req.Slug, opts.RestoreIntent)
		if err != nil {
			return fmt.Errorf("sidecar inject: %w", err)
		}
		yaml = inj.YAML
		for _, sec := range inj.Secrets {
			injectorSecrets = append(injectorSecrets, store.InjectorSecret{Name: sec.Name, Key: sec.Key, Value: sec.Value})
		}
	}
	// A registered injector may also declare host ports its sidecar(s) need
	// exclusively (extension.HostPortRequirer) — ports the rendered pod YAML
	// itself never expresses as a hostPort (e.g. a rootless IPSEC sidecar's
	// IKE traffic, translated through pasta's host-side socket beneath the
	// sidecar's own process). Check those BEFORE any host mutation: a pod
	// whose sidecar can never establish because the port is already held by
	// another process on this host (native or another instance) is a fail
	// silently-forever bug, not a fail-fast one, without this check.
	if reqPorts, ok := s.sidecar.(extension.HostPortRequirer); ok {
		want, err := reqPorts.RequiredHostPorts(req.Parameters)
		if err != nil {
			return fmt.Errorf("sidecar required host ports: %w", err)
		}
		if len(want) > 0 {
			used, err := s.client.UsedHostPorts(ctx, host)
			if err != nil {
				return fmt.Errorf("ports in use: %w", err)
			}
			selfPod := podName(req.Template, req.Slug)
			busy := make(map[string]bool, len(used))
			for _, p := range used {
				// Apply uses Replace: true, so on a re-apply the instance's own
				// pod is still up and still publishing at this point in the
				// check — exclude it, or an instance that both publishes a
				// hostPort and declares that same port via RequiredHostPorts
				// can never be re-applied (#295). Ports held by any OTHER pod
				// still count as busy.
				if p.Pod == selfPod {
					continue
				}
				busy[p.Protocol+"/"+strconv.Itoa(p.HostPort)] = true
			}
			// UsedHostPorts only sees ports podman itself published for a
			// container it manages — it has ZERO visibility into a plain
			// host-level process (a native systemd service, e.g. strongSwan's
			// charon holding udp/500+4500 on vedanta, the actual #130 repro).
			// Cross-check each required protocol's live /proc/net/<proto>[6]
			// binding too, so a non-podman bind is caught just as reliably.
			// One HostBoundPorts call per distinct protocol in `want` (at
			// most 2: tcp/udp), not one per port.
			hostBound := make(map[string]map[int]bool, 2)
			for _, p := range want {
				if _, done := hostBound[p.Protocol]; done {
					continue
				}
				bound, err := s.client.HostBoundPorts(ctx, host, p.Protocol)
				if err != nil {
					return fmt.Errorf("host bound ports (%s): %w", p.Protocol, err)
				}
				set := make(map[int]bool, len(bound))
				for _, port := range bound {
					set[port] = true
				}
				hostBound[p.Protocol] = set
			}
			for _, p := range want {
				switch {
				case busy[p.Protocol+"/"+strconv.Itoa(p.Port)]:
					return fmt.Errorf("%w: %s/%d (bound by a podman-managed container)", ErrPortConflict, p.Protocol, p.Port)
				case hostBound[p.Protocol][p.Port]:
					return fmt.Errorf("%w: %s/%d (bound on the host outside podman)", ErrPortConflict, p.Protocol, p.Port)
				}
			}
		}
	}
	// Reject injector-declared secrets on a key-less store BEFORE any host
	// mutation. This mirrors the pre-injection check for template-declared
	// secrets above — if the injector added secrets but the store cannot encrypt
	// them, abort now so no orphan secrets or pods are left behind.
	if len(injectorSecrets) > 0 && !s.store.SecretsEnabled() {
		return store.ErrSecretsNeedKey
	}

	// Pre-pull images BEFORE writing any secrets. A bad image ref then leaves
	// no orphan secrets behind; secrets that already exist (rotation case) are
	// only touched once we know the manifest will play.
	if !opts.SkipPull {
		pullCtx, cancel := context.WithTimeout(ctx, applyImagePullTimeout)
		for _, img := range containerImages(yaml) {
			log.Printf("apply: pull image %s on %s", img, host)
			if err := s.client.ImagePull(pullCtx, host, img); err != nil {
				cancel()
				return fmt.Errorf("%w: %s: %v", ErrImagePull, img, err)
			}
		}
		cancel()
	}

	// Snapshot secrets (zeroed below) and parameters before persisting, so the
	// stored spec is independent of the caller's request struct.
	secretsCopy := maps.Clone(req.Secrets)
	paramsCopy := maps.Clone(req.Parameters)
	domainsCopy := slices.Clone(req.Domains)

	// Push per-instance template-declared secrets, then zero the local copies.
	// The K8s Secret data key is the namespaced name, matching the convention
	// used by every bundled template's secretKeyRef.key field.
	for k, v := range req.Secrets {
		name := instanceSecretName(req.Template, req.Slug, k)
		log.Printf("apply: secret %s on %s", name, host)
		if _, err := s.client.SecretInspect(ctx, host, name); err == nil {
			if err := s.client.SecretRemove(ctx, host, name); err != nil {
				return fmt.Errorf("remove existing secret %q: %w", name, err)
			}
		}
		if err := s.client.SecretCreate(ctx, host, name, wrapAsKubeSecret(name, name, []byte(v))); err != nil {
			return fmt.Errorf("create secret %q: %w", name, err)
		}
	}
	for k := range req.Secrets {
		req.Secrets[k] = "" // best-effort zero
	}

	// Push injector-declared secrets. The K8s Secret data key is the injector's
	// declared Key (not the secret name), so the injected sidecar's
	// secretKeyRef.key resolves correctly.
	for _, sec := range injectorSecrets {
		name := instanceSecretName(req.Template, req.Slug, sec.Name)
		log.Printf("apply: injector secret %s on %s", name, host)
		if _, err := s.client.SecretInspect(ctx, host, name); err == nil {
			if err := s.client.SecretRemove(ctx, host, name); err != nil {
				return fmt.Errorf("remove existing injector secret %q: %w", name, err)
			}
		}
		if err := s.client.SecretCreate(ctx, host, name, wrapAsKubeSecret(name, sec.Key, []byte(sec.Value))); err != nil {
			return fmt.Errorf("create injector secret %q: %w", name, err)
		}
	}

	// The join set is the template's declarations unioned with this instance's
	// own (#270) — assembled once, here, and reused for the log line below.
	netMeta := metaWithNetworks(tmpl.Meta, req.Networks)
	networks, err := s.ensureNetworks(ctx, host, netMeta)
	if err != nil {
		return err
	}
	if joins := s.joinNetworks(netMeta); len(joins) > 0 {
		// Log the network NAMES, not the join arguments: what was ensured is
		// "frappe-shared", not "frappe-shared:alias=mariadb".
		names := make([]string, 0, len(joins))
		for _, j := range joins {
			names = append(names, j.Name)
		}
		log.Printf("apply: ensured networks %v on %s", names, host)
	}
	log.Printf("apply: play kube on %s", host)
	if err := s.client.PlayKube(ctx, host, yaml, opts.Replace, networks...); err != nil {
		return fmt.Errorf("play kube: %w", err)
	}
	sp := store.Spec{
		Host:            host,
		Template:        req.Template,
		Slug:            req.Slug,
		Parameters:      paramsCopy,
		Secrets:         secretsCopy,
		InjectorSecrets: injectorSecrets,
		Domains:         domainsCopy,
		// AppliedVolumes records exactly what THIS apply declared, not what
		// exists on the host — same relationship InjectorSecrets has to the
		// injector's declared output. #257: this is what later lets
		// CheckBackupable tell a template rename (declared differently now,
		// but still present under the name it was applied with) from real
		// loss (present under neither name).
		AppliedVolumes: appliedVolumeNames(tmpl.Meta.Volumes),
		// AppliedVolumeMeta captures each volume's marker/exclude patterns
		// alongside its name, so a LATER template rename (which leaves the
		// CURRENT declared map with no entry under the old short name) does not
		// silently drop a `backup: none` veto or exclude patterns for a volume
		// that has not been re-applied since (#256 review round-4, blocking
		// finding).
		AppliedVolumeMeta: appliedVolumeMeta(tmpl.Meta.Volumes),
		// AppliedNetworks records this instance's own extra memberships so boot
		// converge can replay them: they exist in no template, so a converge
		// that re-derived the join list from tmpl.Meta alone would silently
		// detach the pod from them on the next reboot (#270).
		AppliedNetworks: slices.Clone(req.Networks),
	}
	// Recheck the template still exists, then persist the spec — both under the
	// template read lock so a concurrent DeleteTemplate (write lock) cannot slip
	// its in-use scan + delete between our recheck and our PutSpec. Ordering
	// guarantee w.r.t. DeleteTemplate: either we PutSpec first (then Delete's
	// in-use scan sees this spec and blocks with ErrTemplateInUse), or Delete
	// completes first (then this recheck finds the template gone and we fail
	// before persisting). Either way no spec ever references a deleted template.
	//
	// Lock order: tmplMu.RLock is taken AFTER the hostLock/instanceLock this Apply
	// already holds; DeleteTemplate takes only tmplMu.Lock (no host/instance
	// locks), so the lock sets are disjoint and there is no deadlock cycle.
	//
	// Residual window (accepted): if the template is deleted DURING the play above
	// (before this recheck), we will have played the pod yet fail here — a narrow
	// orphan-pod window of the same class as a mid-deploy crash. We deliberately
	// do not hold a lock across the slow image-pull/play to close it.
	s.tmplMu.RLock()
	if _, err := s.store.GetTemplate(ctx, req.Template); err != nil {
		s.tmplMu.RUnlock()
		if errors.Is(err, store.ErrNotFound) {
			return ErrUnknownTemplate // template deleted mid-deploy
		}
		return fmt.Errorf("recheck template: %w", err)
	}
	err = s.store.PutSpec(ctx, sp)
	s.tmplMu.RUnlock()
	if err != nil {
		return fmt.Errorf("persist spec: %w", err)
	}
	if s.ingressEnabled() {
		if err := s.ingress.Reconcile(ctx, host); err != nil {
			return fmt.Errorf("ingress reconcile: %w", err)
		}
	}
	return nil
}

// Get returns the observed shape for an instance.
func (s *Service) Get(ctx context.Context, host, tmpl, slug string) (Observed, error) {
	t, err := s.lookup(ctx, host, tmpl)
	if err != nil {
		return Observed{}, err
	}
	p, err := s.client.PodInspect(ctx, host, podName(tmpl, slug))
	if err != nil {
		if errors.Is(err, podman.ErrNotFound) {
			return Observed{}, ErrInstanceNotFound
		}
		return Observed{}, err
	}
	var vols []podman.Volume
	for _, v := range t.Meta.Volumes {
		name := volumeName(tmpl, slug, v.Name)
		if vv, err := s.client.VolumeInspect(ctx, host, name); err == nil {
			vols = append(vols, vv)
		}
	}
	sp, haveSpec, specErr := s.instanceSpec(ctx, host, tmpl, slug)
	var vals map[string]bool
	if haveSpec {
		vals = secretValues(sp)
	}
	obs := Normalize(p, tmpl, slug, vols, secretEnvNames(t.Body), vals)
	if specErr != nil {
		obs.EnvSummary = nil
		obs.Warnings = append(obs.Warnings, envSummaryWithheldWarning(specErr))
	}
	if haveSpec {
		// The stored parameters an operator can read back before a
		// read-modify-write of a shared value (#200). An unreadable or absent
		// spec leaves the field out entirely rather than emitting null.
		obs.Parameters = PublicParameters(t.Meta, sp.Parameters, vals)
	}
	return obs, nil
}

// envSummaryWithheldWarning is the Observed.Warnings entry used when an
// instance's stored spec cannot be read. Without the spec we cannot know which
// env values are secret, so env_summary is withheld rather than returned
// possibly-unredacted (#198). The rest of the observation is unaffected.
func envSummaryWithheldWarning(err error) string {
	return fmt.Sprintf("env_summary withheld: instance secrets unreadable (%v)", err)
}

// instanceSpec loads one instance's stored spec, the source of both the
// env_summary redaction inputs and the reported parameters. The bool reports
// whether a spec was found.
//
// A missing spec is NOT an error: the pod simply has no recorded state (never
// applied by this core, or its spec was deleted while it ran), and withholding
// env_summary for it would regress every such instance for no security gain. A
// corrupt or undecryptable spec IS an error — the caller withholds env_summary
// rather than risk returning secret material.
func (s *Service) instanceSpec(ctx context.Context, host, tmpl, slug string) (store.Spec, bool, error) {
	sp, err := s.store.GetSpec(ctx, host, tmpl, slug)
	if errors.Is(err, store.ErrNotFound) {
		return store.Spec{}, false, nil
	}
	if err != nil {
		return store.Spec{}, false, err
	}
	return sp, true, nil
}

// specSecrets is one instance's redaction input for a host-wide sweep: either
// its known secret values, or the error that made them unknowable.
type specSecrets struct {
	vals map[string]bool
	err  error
}

// hostSecretValues loads the secret values of every stored instance on host in
// one pass, so a list sweep does not issue a store read per pod inside its
// worker fan-out. Instances absent from the returned map have no stored spec
// and are redacted by name alone.
//
// A failure to enumerate the host's specs is returned as the second value, NOT
// folded into the map: a map miss means "no stored spec" (name-pass only), so
// returning an empty map on enumeration failure would silently revert the whole
// host to name-only redaction. Callers apply the returned error to every
// instance on the host instead, which fails closed.
func (s *Service) hostSecretValues(ctx context.Context, host string) (map[store.SpecKey]specSecrets, error) {
	keys, err := s.store.ListSpecKeys(ctx, host)
	if err != nil {
		return nil, err
	}
	out := map[store.SpecKey]specSecrets{}
	for _, k := range keys {
		sp, gerr := s.store.GetSpec(ctx, host, k.Template, k.Slug)
		if errors.Is(gerr, store.ErrNotFound) {
			// The spec was deleted between ListSpecKeys and this read. Match
			// instanceSpec: no recorded spec is not an error, so skip
			// the key and let it fall through to name-only redaction, rather
			// than recording it as an unreadable-spec error.
			continue
		}
		if gerr != nil {
			out[k] = specSecrets{err: gerr}
			continue
		}
		out[k] = specSecrets{vals: secretValues(sp)}
	}
	return out, nil
}

// applySecretRedaction finishes one observation: it withholds env_summary when
// this instance's secrets were unreadable, or when the whole host sweep failed.
func applySecretRedaction(obs Observed, ss specSecrets, sweepErr error) Observed {
	err := sweepErr
	if err == nil {
		err = ss.err
	}
	if err != nil {
		obs.EnvSummary = nil
		obs.Warnings = append(obs.Warnings, envSummaryWithheldWarning(err))
	}
	return obs
}

// List returns all instances of a given template on a host.
func (s *Service) List(ctx context.Context, host, tmpl string) ([]Observed, error) {
	t, err := s.lookup(ctx, host, tmpl)
	if err != nil {
		return nil, err
	}
	pods, err := s.client.PodList(ctx, host, map[string]string{"podman-api/template": tmpl})
	if err != nil {
		return nil, err
	}
	secretEnvs := secretEnvNames(t.Body)
	vals, sweepErr := s.hostSecretValues(ctx, host)
	out := make([]Observed, 0, len(pods))
	for _, p := range pods {
		slug := p.Labels["podman-api/slug"]
		ss := vals[store.SpecKey{Template: tmpl, Slug: slug}]
		obs := Normalize(p, tmpl, slug, nil, secretEnvs, ss.vals)
		out = append(out, applySecretRedaction(obs, ss, sweepErr))
	}
	return out, nil
}

// ListAllInstances returns every podman-api-managed pod on a host across all
// known templates. The result is the union of List(host, t) for each catalog
// template id, so a pod for a template the daemon doesn't know about is
// silently omitted.
//
// Volumes on each result are declared names only — the sweep makes no podman
// call per volume, so an entry does not mean the volume exists and SizeBytes is
// always 0. See ObservedVolume; Get is the authority on existence and size.
func (s *Service) ListAllInstances(ctx context.Context, host string) ([]Observed, error) {
	obs, _, err := s.instCache.getWithMeta(host, func() ([]Observed, error) {
		return s.listAllInstancesLive(ctx, host)
	})
	return obs, err
}

// ListAllInstancesWithMeta is ListAllInstances plus freshness metadata (when the
// data was captured and whether the host was reachable on the last refresh), for
// UI callers that render a staleness cue.
func (s *Service) ListAllInstancesWithMeta(ctx context.Context, host string) ([]Observed, Freshness, error) {
	return s.instCache.getWithMeta(host, func() ([]Observed, error) {
		return s.listAllInstancesLive(ctx, host)
	})
}

// EnableWarmInventory switches the instance cache into warm mode so entries are
// served without expiry and kept fresh by the inventory poller. Call once at
// startup, before serving traffic, when the poller is enabled.
func (s *Service) EnableWarmInventory() { s.instCache.setWarm(true) }

// InventorySnapshot returns the warm cache's current contents for every host
// that has an entry, without triggering a fetch. It is meaningful only when the
// inventory poller is running — with the lazy cache, entries appear only after a
// read, so a snapshot would under-report.
func (s *Service) InventorySnapshot() map[string]HostInventory {
	return s.instCache.snapshot()
}

// RefreshHostStats samples every container's resource usage on host and stores
// it for the metrics collector. One podman call per host.
//
// On error the host's samples are dropped, so its series go absent rather than
// freezing at their last value. The caller must NOT treat a failure here as the
// host being unreachable — reachability is the inventory refresh's to decide.
func (s *Service) RefreshHostStats(ctx context.Context, host string) error {
	st, err := s.client.ContainerStats(ctx, host)
	if err != nil {
		s.statsCache.drop(host)
		return err
	}
	m := make(map[string]podman.ContainerStats, len(st))
	for _, c := range st {
		m[c.Name] = c
	}
	s.statsCache.put(host, HostStats{Containers: m, FetchedAt: time.Now()})
	return nil
}

// DropHostStats discards the host's cached samples without sampling it. It is
// for the caller that decides NOT to call RefreshHostStats at all — e.g. the
// poller skipping a host whose inventory refresh just failed — which would
// otherwise leave the last-known sample in the cache forever, freezing the
// host's series instead of retiring them. Same contract as a failed refresh
// (see statsCache): absent beats frozen for cumulative counters.
//
// No context and no I/O: it costs a tick nothing.
func (s *Service) DropHostStats(host string) { s.statsCache.drop(host) }

// StatsSnapshot returns the cached container samples for every host. Never
// fetches: it runs on the scrape path.
func (s *Service) StatsSnapshot() map[string]HostStats { return s.statsCache.snapshot() }

// RefreshHostVolumeUsage sizes every volume on host via one `system df` call.
// Podman walks the whole store to answer, so this belongs on a slow cadence
// with its own timeout. On error the previous sizing is retained.
func (s *Service) RefreshHostVolumeUsage(ctx context.Context, host string) error {
	sizes, err := s.client.VolumeUsage(ctx, host)
	if err != nil {
		return err
	}
	s.volCache.put(host, HostVolumeUsage{Sizes: sizes, FetchedAt: time.Now()})
	return nil
}

// VolumeUsageSnapshot returns the cached volume sizing for every host. Never
// fetches: it runs on the scrape path.
func (s *Service) VolumeUsageSnapshot() map[string]HostVolumeUsage {
	return s.volCache.snapshot()
}

// RefreshHost performs a live inventory sweep of host and stores it in the warm
// cache. On failure it marks the host's last-known-good entry unreachable
// (keeping the data) and returns the error for the caller to log. This is the
// only proactive cache populator; the background poller calls it per tick.
func (s *Service) RefreshHost(ctx context.Context, host string) error {
	gen := s.instCache.beginRefresh(host)
	obs, err := s.listAllInstancesLive(ctx, host)
	if err != nil {
		s.instCache.markUnreachable(host, gen)
		return err
	}
	s.instCache.put(host, gen, obs, time.Now())
	return nil
}

// listAllInstancesLive performs the live podman sweep (no caching). See
// ListAllInstances for the cached entry point.
func (s *Service) listAllInstancesLive(ctx context.Context, host string) ([]Observed, error) {
	if _, ok := s.host(host); !ok {
		return nil, ErrUnknownHost
	}
	tmpls, err := s.store.ListTemplates(ctx)
	if err != nil {
		return nil, fmt.Errorf("list templates: %w", err)
	}
	vals, sweepErr := s.hostSecretValues(ctx, host)

	type result struct {
		obs []Observed
		err error
	}
	const maxWorkers = 8
	sem := make(chan struct{}, maxWorkers)
	results := make([]result, len(tmpls))
	var wg sync.WaitGroup
	for i, t := range tmpls {
		wg.Add(1)
		sem <- struct{}{}
		go func(i int, t store.Template) {
			defer wg.Done()
			defer func() { <-sem }()
			tmplID := t.Meta.ID
			pods, err := s.client.PodList(ctx, host, map[string]string{"podman-api/template": tmplID})
			if err != nil {
				results[i] = result{err: fmt.Errorf("list %s: %w", tmplID, err)}
				return
			}
			secretEnvs := secretEnvNames(t.Body)
			var part []Observed
			for _, p := range pods {
				slug := p.Labels["podman-api/slug"]
				ss := vals[store.SpecKey{Template: tmplID, Slug: slug}]
				// Names only, derived from template meta — the same construction
				// Get uses. No podman call: the sweep must not pay for volume
				// inspection, and sizes come from the volume-usage cache.
				var vols []podman.Volume
				for _, v := range t.Meta.Volumes {
					vols = append(vols, podman.Volume{Name: volumeName(tmplID, slug, v.Name)})
				}
				obs := Normalize(p, tmplID, slug, vols, secretEnvs, ss.vals)
				part = append(part, applySecretRedaction(obs, ss, sweepErr))
			}
			results[i] = result{obs: part}
		}(i, t)
	}
	wg.Wait()

	var out []Observed
	for _, r := range results {
		if r.err != nil {
			return nil, r.err // first error in template order, matching prior serial semantics
		}
		out = append(out, r.obs...)
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

// Start starts a stopped instance and waits for container healthchecks to pass
// (up to deployVerifyTimeout). On readiness timeout the call still succeeds and
// Observed.Warnings carries a human-readable message.
func (s *Service) Start(ctx context.Context, host, tmpl, slug string) (Observed, error) {
	if err := s.lifecycle(ctx, host, tmpl, slug, s.client.PodStart); err != nil {
		return Observed{}, err
	}
	readyErr := s.waitReady(ctx, host, tmpl, slug, readyOpts{
		timeout:     deployVerifyTimeout,
		stableCount: deployVerifyStableCount,
	})
	obs, err := s.Get(ctx, host, tmpl, slug)
	if err != nil {
		return Observed{}, err
	}
	if w := readinessWarning(readyErr); w != "" {
		obs.Warnings = append(obs.Warnings, w)
	}
	return obs, nil
}
func (s *Service) Stop(ctx context.Context, host, tmpl, slug string) error {
	return s.lifecycle(ctx, host, tmpl, slug, s.client.PodStop)
}
func (s *Service) Restart(ctx context.Context, host, tmpl, slug string) error {
	return s.lifecycle(ctx, host, tmpl, slug, s.client.PodRestart)
}

func (s *Service) lifecycle(ctx context.Context, host, tmpl, slug string,
	op func(context.Context, string, string) error) error {
	defer s.invalidateInstances(host)
	if _, err := s.lookup(ctx, host, tmpl); err != nil {
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
	// This request is rebuilt from the caller's body, which carries no networks.
	// Applying it as-is would detach the pod from every network it joined
	// per-instance and persist that loss (#270 review) — silently, with the
	// instance still Running. Carry the stored set forward unless the caller
	// stated one. Read outside the instance lock Apply takes below: a concurrent
	// apply could make it stale, which is the same staleness every other field
	// of a caller-supplied request already has, and far better than a certain
	// detach.
	//
	// Read through the non-decrypting projection, NOT GetSpec: this route exists
	// to re-apply an instance from a caller-supplied parameters+secrets body, and
	// is the way out when the stored spec cannot be decrypted (no key, rotated
	// key, corrupt row). Making it depend on a decryptable spec would turn an
	// optional carry-forward field into a hard failure on exactly the recovery
	// path (#288 re-review).
	if req.Networks == nil {
		nets, err := s.appliedNetworks(ctx, host, req.Template, req.Slug)
		if err != nil {
			return err
		}
		req.Networks = nets
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

// UpgradeImage performs an image-only upgrade: it loads the instance's stored
// spec (parameters + secrets), overrides the "image" parameter, and re-applies
// with Replace. Existing secrets and parameters are reused as-is — the operator
// supplies only the new image; rotating a secret is a separate operation. Like
// RotateInstanceSecrets it sets AllowMissingSecrets, so a template that gained a
// required per-instance secret after the instance was deployed does not block an
// image upgrade of that already-running instance (the missing secret was already
// missing; the upgrade never worsens the pod).
// Returns ErrInstanceNotFound when no spec is stored for the instance.
//
// The load (GetSpec) and re-apply (applyLocked) happen atomically under the
// per-instance lock: the image override is a read-modify-write of the stored
// parameters, so holding the lock across both halves keeps a concurrent
// rotation/upgrade of the same instance from reading the pre-commit spec and
// dropping this update. It takes only the instance lock (no host lock): the
// upgrade re-applies the instance's own already-persisted domains unchanged,
// which validateIngress excludes from its uniqueness check, so it can never
// create a new cross-instance domain claim and needs no per-host lock. (If a
// future edit let this method *change* domains, the missing host lock would
// become a real bug — the no-hostLock safety rests on domains being unchanged.)
// (#114)
func (s *Service) UpgradeImage(ctx context.Context, host, tmpl, slug, image string) error {
	if image == "" {
		return errors.New("upgrade requires an image")
	}
	lock := s.instanceLock(host, tmpl, slug)
	lock.Lock()
	defer lock.Unlock()
	spec, err := s.store.GetSpec(ctx, host, tmpl, slug)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return ErrInstanceNotFound
		}
		return fmt.Errorf("load spec: %w", err)
	}
	params := maps.Clone(spec.Parameters)
	if params == nil {
		params = map[string]any{}
	}
	params["image"] = image
	return s.applyLocked(ctx, host, ApplyRequest{
		Template:   tmpl,
		Slug:       slug,
		Parameters: params,
		Secrets:    spec.Secrets,
		Domains:    spec.Domains,
		// Carry the instance's own extra networks (#270) forward: they live
		// on the spec, not the template, so a re-apply that omitted them would
		// detach the pod from every network it joined per-instance.
		Networks: slices.Clone(spec.AppliedNetworks),
	}, ApplyOptions{Replace: true, AllowMissingSecrets: true})
}

// RotateInstanceSecrets overlays newSecrets onto the instance's stored
// per-instance secrets and re-applies (Replace=true), restarting the pod. Names
// absent from newSecrets keep their existing value — callers are write-only and
// never see current values. An empty newSecrets is rejected so a blank submit
// does not pointlessly restart the instance. Returns ErrInstanceNotFound when no
// spec is stored, or the store's error (incl. store.ErrSpecCorrupt or
// store.ErrSecretsUndecryptable) when the spec cannot be read.
//
// The load (GetSpec) and re-apply (applyLocked) happen atomically under the
// per-instance lock: rotation is a read-modify-write of the stored secrets, so
// holding the lock across both halves keeps a concurrent rotation/upgrade of the
// same instance from reading the pre-commit spec and dropping this update. It
// takes only the instance lock (no host lock): rotation re-applies the
// instance's own already-persisted domains unchanged, which validateIngress
// excludes from its uniqueness check, so it can never create a new
// cross-instance domain claim and needs no per-host lock. (If a future edit let
// this method *change* domains, the missing host lock would become a real bug —
// the no-hostLock safety rests on domains being unchanged.) (#114)
func (s *Service) RotateInstanceSecrets(ctx context.Context, host, tmpl, slug string, newSecrets map[string]string) error {
	if len(newSecrets) == 0 {
		return errors.New("no secrets to rotate")
	}
	lock := s.instanceLock(host, tmpl, slug)
	lock.Lock()
	defer lock.Unlock()
	spec, err := s.store.GetSpec(ctx, host, tmpl, slug)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return ErrInstanceNotFound
		}
		return fmt.Errorf("load spec: %w", err)
	}
	merged := maps.Clone(spec.Secrets)
	if merged == nil {
		merged = map[string]string{}
	}
	for k, v := range newSecrets {
		merged[k] = v
	}
	return s.applyLocked(ctx, host, ApplyRequest{
		Template:   tmpl,
		Slug:       slug,
		Parameters: spec.Parameters,
		Secrets:    merged,
		Domains:    spec.Domains,
		// Carry the instance's own extra networks (#270) forward: they live
		// on the spec, not the template, so a re-apply that omitted them would
		// detach the pod from every network it joined per-instance.
		Networks: slices.Clone(spec.AppliedNetworks),
	}, ApplyOptions{Replace: true, AllowMissingSecrets: true})
}

// UpdateInstanceParameters overlays newParams onto the instance's stored render
// parameters and re-applies (Replace=true), restarting the pod. Names absent
// from newParams keep their existing value; a parameter cannot be deleted this
// way (that still needs a full PUT). An empty newParams is rejected so a blank
// submit does not pointlessly restart the instance. Returns ErrInstanceNotFound
// when no spec is stored, or the store's error (incl. store.ErrSpecCorrupt or
// store.ErrSecretsUndecryptable) when the spec cannot be read.
//
// The stored per-instance secrets are reused as-is and never need to be
// supplied by the caller — they are write-only plaintext the operator cannot
// read back, which is exactly why a parameter change had no API route before
// (pro#74). Like UpgradeImage it sets AllowMissingSecrets, so a template that
// gained a required per-instance secret after the instance was deployed does not
// block a parameter change to that already-running instance (the missing secret
// was already missing; the update never worsens the pod).
//
// slug is NOT settable through newParams: if the stored spec already has a
// "slug" entry, it is pinned back to the canonical slug after the overlay
// below, the same way migrate.go and rename.go pin it before re-applying.
// Without this, a caller-supplied "slug" parameter (a declared template
// parameter, so it passes render.Validate) would render a manifest for a
// *different* pod while this method still holds and reports success under the
// original slug's lock — the wrong pod gets replaced (destructively, if it
// already exists) and the persisted spec for the original slug ends up
// describing a pod no later Start/Stop/Delete can find. The HTTP handler also
// rejects a "slug" key up front (defence in depth); this pin makes the method
// itself safe for any caller.
//
// The pin is conditional (only fires when "slug" is already a key in merged)
// because nothing requires a template to declare "slug" as a parameter at all
// — a singleton template can hardcode metadata.name and never mention it. On
// such a template an unconditional write would inject an undeclared "slug"
// key and render.Validate would reject every PATCH with
// `unknown parameter "slug"`. A caller trying to *introduce* slug on a
// template that doesn't declare it is still rejected by render.Validate, which
// is the correct outcome — the safety property above is unaffected.
//
// applyLocked discards the stored spec's InjectorSecrets and rebuilds the list
// by re-running the sidecar injector, then persists the fresh list — injector-
// declared secrets are re-derived on this path, not preserved, unlike the boot-
// converge path in spec_reconcile.go.
//
// Replace: true means podman tears the pod down before creating the new one,
// and the spec is only persisted after a successful play; a parameter change
// that renders a manifest podman refuses to play returns an error with the pod
// gone and the stored spec still describing the pre-update state — recovery is
// boot converge or a manual re-apply.
//
// The load (GetSpec) and re-apply (applyLocked) happen atomically under the
// per-instance lock: the overlay is a read-modify-write of the stored
// parameters, so holding the lock across both halves keeps a concurrent
// rotation/upgrade of the same instance from reading the pre-commit spec and
// dropping this update. It takes only the instance lock (no host lock): the
// update re-applies the instance's own already-persisted domains unchanged,
// which validateIngress excludes from its uniqueness check, so it can never
// create a new cross-instance domain claim and needs no per-host lock. Domains
// are not derivable from parameters — they come from ApplyRequest.Domains, which
// this method does not accept. (If a future edit let this method *change*
// domains, the missing host lock would become a real bug — the no-hostLock
// safety rests on domains being unchanged.) (#114)
func (s *Service) UpdateInstanceParameters(ctx context.Context, host, tmpl, slug string, newParams map[string]any) error {
	if len(newParams) == 0 {
		return errors.New("no parameters to update")
	}
	lock := s.instanceLock(host, tmpl, slug)
	lock.Lock()
	defer lock.Unlock()
	spec, err := s.store.GetSpec(ctx, host, tmpl, slug)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return ErrInstanceNotFound
		}
		return fmt.Errorf("load spec: %w", err)
	}
	merged := maps.Clone(spec.Parameters)
	if merged == nil {
		merged = map[string]any{}
	}
	for k, v := range newParams {
		merged[k] = v
	}
	if _, ok := merged["slug"]; ok {
		merged["slug"] = slug // canonical slug always wins; pod name must match podName()
	}
	return s.applyLocked(ctx, host, ApplyRequest{
		Template:   tmpl,
		Slug:       slug,
		Parameters: merged,
		Secrets:    maps.Clone(spec.Secrets),
		Domains:    spec.Domains,
		// Carry the instance's own extra networks (#270) forward: they live
		// on the spec, not the template, so a re-apply that omitted them would
		// detach the pod from every network it joined per-instance.
		Networks: slices.Clone(spec.AppliedNetworks),
	}, ApplyOptions{Replace: true, AllowMissingSecrets: true})
}

// StoredSpec returns the persisted spec (parameters, secrets, domains) for an
// existing instance, so the UI edit form can pre-populate parameters and merge
// secrets before re-applying. The caller must NOT render or log the returned
// secrets — they are write-only merge inputs, intended to be overlaid with form
// values and re-persisted via ApplyAndObserve(Replace: true).
func (s *Service) StoredSpec(ctx context.Context, host, tmpl, slug string) (store.Spec, error) {
	return s.store.GetSpec(ctx, host, tmpl, slug)
}

// InstanceSecretState reports, per stored per-instance secret name, that a value
// is present — presence only, never the value (the secret model is write-only).
// Names a template declares but the instance never set are simply absent from the
// map. Returns ErrInstanceNotFound when no spec is stored, or the store's error
// (incl. store.ErrSpecCorrupt or store.ErrSecretsUndecryptable) when the spec
// cannot be read.
func (s *Service) InstanceSecretState(ctx context.Context, host, tmpl, slug string) (map[string]bool, error) {
	spec, err := s.store.GetSpec(ctx, host, tmpl, slug)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return nil, ErrInstanceNotFound
		}
		return nil, fmt.Errorf("load spec: %w", err)
	}
	set := make(map[string]bool, len(spec.Secrets))
	for name := range spec.Secrets {
		set[name] = true
	}
	return set, nil
}

// pruneInstanceResources best-effort removes an instance's per-instance secrets
// and/or named volumes on a host. Both names are deterministic from
// template+slug, so leaving them behind risks a future deploy of the same slug
// silently reusing stale data (play kube reuses an existing named volume) or
// stale on-disk credentials. This is an idempotent reconcile toward "gone";
// callers that need durability handle the spec row separately. Failures other
// than podman.ErrNotFound are logged (not-found is the expected steady state
// for an idempotent reconcile and stays quiet) but never returned — a caller
// asking to prune must not have the rest of Delete fail underneath it.
func (s *Service) pruneInstanceResources(ctx context.Context, host, tmpl, slug string, secrets, volumes bool, injectorSecrets []store.InjectorSecret) {
	t, err := s.store.GetTemplate(ctx, tmpl)
	haveTemplate := err == nil
	// Template gone from the catalog: t.Meta.Secrets.PerInstance and
	// t.Meta.Volumes can't be derived, but injectorSecrets comes from the
	// stored spec, not the template, so it is still prunable below.
	//
	// Logged unless the template is genuinely absent, for the same reason the
	// podman failures below are: a transient store error here silently skips
	// BOTH declared loops while the caller still gets its 204 — which is
	// exactly the #214 failure mode, on a template that still exists. Absence
	// is the one case that is ordinary and stays quiet.
	if err != nil && !errors.Is(err, store.ErrNotFound) {
		log.Printf("prune: get template %s (skipping declared secrets/volumes on %s): %v", tmpl, host, err)
	}

	if secrets {
		if haveTemplate {
			for _, name := range t.Meta.Secrets.PerInstance {
				full := instanceSecretName(tmpl, slug, name)
				if err := s.client.SecretRemove(ctx, host, full); err != nil && !errors.Is(err, podman.ErrNotFound) {
					log.Printf("prune: remove secret %s on %s: %v", full, host, err)
				}
				// The wrapped-secret volume `podman kube play` creates when a
				// secret: volume mount resolves it holds the DECODED PLAINTEXT
				// credential on disk (_data/<name>, root-owned) — not template
				// data. Gated on `secrets`, not `volumes`: prune_secrets=true is
				// an operator asking for the key to be gone, and gating the key's
				// own on-disk copy behind a second flag would just recreate the
				// false all-clear this fix exists to close. It also widens no
				// blast radius — this is the same name SecretRemove above just
				// targeted.
				//
				// Note the doc comment above says play kube reuses an existing
				// named volume. That is true of *the volume*, not of its
				// contents: kube play re-materialises a secret wrapper volume
				// from the secret on every play, verified on podman 5.8.2 (old
				// value → pod rm → secret rm + recreate → re-play → new value).
				// So rotating a leaked credential really does replace it, and
				// no apply-path change is needed here.
				if err := s.client.VolumeRemove(ctx, host, full, true); err != nil && !errors.Is(err, podman.ErrNotFound) {
					log.Printf("prune: remove secret wrapper volume %s on %s: %v", full, host, err)
				}
			}
		}
		for _, sec := range injectorSecrets {
			full := instanceSecretName(tmpl, slug, sec.Name)
			if err := s.client.SecretRemove(ctx, host, full); err != nil && !errors.Is(err, podman.ErrNotFound) {
				log.Printf("prune: remove injector secret %s on %s: %v", full, host, err)
			}
			// Same rationale as above: an injector secret mounted as a volume
			// gets the same wrapper-volume treatment from podman.
			if err := s.client.VolumeRemove(ctx, host, full, true); err != nil && !errors.Is(err, podman.ErrNotFound) {
				log.Printf("prune: remove injector secret wrapper volume %s on %s: %v", full, host, err)
			}
		}
	}
	if volumes && haveTemplate {
		for _, v := range t.Meta.Volumes {
			name := volumeName(tmpl, slug, v.Name)
			if err := s.client.VolumeRemove(ctx, host, name, true); err != nil && !errors.Is(err, podman.ErrNotFound) {
				log.Printf("prune: remove volume %s on %s: %v", name, host, err)
			}
		}
	}
}

// Delete removes the pod and optionally its volumes and per-instance secrets.
func (s *Service) Delete(ctx context.Context, host, tmpl, slug string, opts DeleteOptions) error {
	defer s.invalidateInstances(host)

	if _, err := s.lookup(ctx, host, tmpl); err != nil {
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

	var injectorSecrets []store.InjectorSecret
	// A GetSpec failure leaves injectorSecrets empty, so the prune below is
	// never even asked to remove them — VPN PSKs, WireGuard private keys — and
	// reports nothing wrong. Same silence-is-a-bug reasoning as
	// pruneInstanceResources itself; a missing spec is ordinary and stays quiet.
	if spec, err := s.store.GetSpec(ctx, host, tmpl, slug); err == nil {
		injectorSecrets = spec.InjectorSecrets
	} else if !errors.Is(err, store.ErrNotFound) {
		log.Printf("prune: get spec %s/%s on %s (injector secrets will not be pruned): %v", tmpl, slug, host, err)
	}
	s.pruneInstanceResources(ctx, host, tmpl, slug, opts.PruneSecrets, opts.PruneVolumes, injectorSecrets)
	// Reconcile away the desired-state row. This runs even when the pod was
	// already gone (so a stale spec doesn't linger); ErrNotFound — never stored,
	// or an idempotent double-delete — is not an error. Note this happens before
	// the not-found guard below, so a pod-gone Delete still cleans up the spec
	// even though it reports ErrInstanceNotFound to the caller.
	if err := s.store.DeleteSpec(ctx, host, tmpl, slug); err != nil && !errors.Is(err, store.ErrNotFound) {
		return fmt.Errorf("delete spec: %w", err)
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

// CanRenameHost reports whether RenameHost(oldID, newID) would be accepted,
// without changing anything. It runs exactly the checks RenameHost does — live
// host list, then the store's refusal cases — and returns the same errors.
//
// It exists purely to answer the most common refusal, ErrHostHasBackups,
// without paying for a destructive config rewrite plus a revert inside
// RenameHost. It is advisory and must never be load-bearing: it takes no lock,
// so its answer can be stale by the time RenameHost runs. RenameHost re-runs
// checkRenameHosts inside the host lock and the store re-checks its own
// refusals inside the migration transaction — those, not this, are the source
// of truth.
func (s *Service) CanRenameHost(ctx context.Context, oldID, newID string) error {
	if err := s.checkRenameHosts(oldID, newID); err != nil {
		return err
	}
	return mapRenameHostStoreErr(s.store.CheckHostRename(ctx, oldID, newID))
}

// checkRenameHosts validates a rename against the live host list only.
func (s *Service) checkRenameHosts(oldID, newID string) error {
	hosts := s.Hosts()
	found := false
	for _, h := range hosts {
		if h.ID == oldID {
			found = true
			break
		}
	}
	if !found {
		return ErrUnknownHost
	}
	for _, h := range hosts {
		if h.ID == newID {
			return ErrHostAlreadyExists
		}
	}
	return nil
}

// mapRenameHostStoreErr translates the store's rename sentinels into the
// service-level ones the API layer classifies.
func mapRenameHostStoreErr(err error) error {
	switch {
	case err == nil:
		return nil
	case errors.Is(err, store.ErrHostRenameConflict):
		return ErrHostAlreadyExists
	case errors.Is(err, store.ErrHostRenameHasBackups):
		return ErrHostHasBackups
	default:
		return err
	}
}

// RenameHost migrates a host's store rows (specs, host secrets, backups) from
// oldID to newID, rewrites the on-disk host config via rewriteFile, and on
// success updates the live host set so the new id is resolvable immediately —
// no restart or SIGHUP required. Returns
// ErrUnknownHost if oldID isn't a currently-configured host,
// ErrHostAlreadyExists if newID is already a currently-configured host or
// collides with a store.ErrHostRenameConflict (stale rows under an id no
// host currently claims), and ErrHostHasBackups if oldID has any backups
// (S3 blob re-keying is out of scope; see
// docs/superpowers/specs/2026-08-10-host-rename-design.md).
//
// rewriteFile mutates the on-disk host config (hosts/*.yaml) and returns a
// revert closure that restores it verbatim. It is a parameter rather than
// something the caller does around this call so that the ENTIRE mutating
// sequence — config rewrite, store migration, revert-on-failure — happens
// inside the one lock acquisition below. It must be non-nil.
//
// That is not a tidiness preference; two callers each doing their own rewrite
// outside the lock is a split-brain generator. Two concurrent renames of h1
// (h1->h2 and h1->h3) would both read the file before either wrote it, so one
// write is silently lost; then both call in here, one wins the host lock and
// commits h2 to the store, and the LOSER — which fails the post-lock
// checkRenameHosts below with ErrUnknownHost — reverts the file to the `id: h1`
// bytes IT captured, AFTER the winner committed. The config file then says h1
// forever while the store and the live host set say h2, reachable from two
// ordinary concurrent requests. With rewriteFile in here the loser never
// rewrites anything: it fails the check before reaching this step.
//
// Locking: the rename is serialized against every concurrent Apply for oldID by
// taking the host lock AND the per-instance lock of every instance the store
// currently knows about on oldID, held across the whole body (config rewrite,
// store migration, revert, host-list swap, client propagation, cache eviction).
// Callers must not take any of these locks around this call. Without that, an Apply
// that passed its own locks before the rename started can land its
// PutSpec(host: oldID, …) AFTER the `UPDATE specs SET host = newID` has run:
// the row is then permanently orphaned under an id no live host claims, and the
// instance is unreachable through the API even though its pod is running.
// The locks are taken in a fixed order — hostLock first, then instance locks
// sorted by their composite key — matching Apply's own hostLock-before-
// instanceLock order, so no cycle is possible (two operations on *different*
// hosts share no locks at all).
//
// One race this does NOT close, honestly: an instance that does not exist yet.
// An Apply for a (template, slug) that was absent when ListSpecKeys below took
// its snapshot creates its own fresh instanceLock (instanceLock lazily creates
// one for any key), which RenameHost never knew to acquire and therefore does
// not hold — so that apply is not serialized against the rename and can still
// orphan its spec under oldID. It is a strictly narrower window than the bug
// this closes (both calls must interleave within the single ListSpecKeys
// snapshot window, and only for an instance being created for the first time,
// versus any existing instance at any point during the whole rename), but it is
// a real residual gap, not a closed one.
func (s *Service) RenameHost(ctx context.Context, oldID, newID string, rewriteFile func() (revert func() error, err error)) error {
	hl := s.hostLock(oldID)
	hl.Lock()
	defer hl.Unlock()

	// Re-checked here (not just by the caller-side CanRenameHost advisory
	// check) inside the host lock: two concurrent RenameHost(oldID -> ...)
	// calls can both pass validation against the same pre-lock Hosts()
	// snapshot, then serialize on hl. Without re-checking after acquiring
	// the lock, the loser would proceed anyway once unblocked — oldID is no
	// longer live, store.RenameHost matches zero rows (already renamed by
	// the winner), and the function would return nil: a success response
	// for a rename that did not happen. checkRenameHosts only reads
	// s.Hosts() and takes no lock of its own, so calling it here is safe.
	//
	// This does NOT serialize two renames converging on the same newID from
	// two DIFFERENT old hosts (each takes a different hostLock) — that case
	// stays covered by the store's ErrHostRenameConflict and is out of
	// scope here.
	if err := s.checkRenameHosts(oldID, newID); err != nil {
		return err
	}

	keys, err := s.store.ListSpecKeys(ctx, oldID)
	if err != nil {
		return fmt.Errorf("list instances on %q: %w", oldID, err)
	}
	slices.SortFunc(keys, func(a, b store.SpecKey) int {
		if c := strings.Compare(a.Template, b.Template); c != 0 {
			return c
		}
		return strings.Compare(a.Slug, b.Slug)
	})
	for _, k := range keys {
		il := s.instanceLock(oldID, k.Template, k.Slug)
		il.Lock()
		defer il.Unlock()
	}

	// The config rewrite goes here — after the definitive checks and inside every
	// lock, but before the store migration, so a failure to write the file leaves
	// the store untouched and there is nothing to revert.
	revert, err := rewriteFile()
	if err != nil {
		return err
	}

	if err := mapRenameHostStoreErr(s.store.RenameHost(ctx, oldID, newID)); err != nil {
		if rerr := revert(); rerr != nil {
			log.Printf("rename host %s -> %s: store migration failed (%v) AND reverting the host config failed (%v) — the config file is now inconsistent with the store, fix by hand", oldID, newID, err, rerr)
		}
		return err
	}

	hosts := s.Hosts()
	for i := range hosts {
		if hosts[i].ID == oldID {
			hosts[i].ID = newID
		}
	}
	s.SetHosts(hosts)
	// The podman client keeps its OWN host map (podman.Real.hosts), so without
	// this every podman operation against newID fails `unknown host` — and the
	// stale oldID keeps resolving — until a SIGHUP. SIGHUP's reload does the
	// same two calls; see server.applyHosts, which additionally refreshes the
	// background pollers' host list (the API handler triggers that via the
	// hosts reloader after this returns).
	s.client.SetHosts(hosts)

	// Evict every per-host cache keyed by the old id. Not migrated to newID on
	// purpose: they repopulate on the next read/poll now that newID is
	// reachable, whereas a stale oldID entry would be exported as a metric
	// under a host that no longer exists for the lifetime of the process.
	s.instCache.invalidate(oldID)
	s.statsCache.drop(oldID)
	s.volCache.drop(oldID)
	return nil
}

// Templates returns the catalog's templates (read-only view). A store error is
// propagated so callers can surface it (e.g. an HTTP 500) rather than rendering
// an empty catalog as if it succeeded.
func (s *Service) Templates(ctx context.Context) ([]store.Template, error) {
	return s.store.ListTemplates(ctx)
}

// Template returns one catalog template by ID (read-only view), or
// store.ErrNotFound. A point lookup for callers that need a single template,
// avoiding Templates()' full-catalog list + scan.
func (s *Service) Template(ctx context.Context, id string) (store.Template, error) {
	return s.store.GetTemplate(ctx, id)
}

// HostLoad returns a point-in-time resource snapshot for a host.
func (s *Service) HostLoad(ctx context.Context, host string) (podman.HostInfo, error) {
	if _, ok := s.host(host); !ok {
		return podman.HostInfo{}, ErrUnknownHost
	}
	return s.client.HostInfo(ctx, host)
}

// HostUptime returns hostID's current kernel uptime (see
// podman.Client.HostUptime). Used by the inventory poller to detect a host
// reboot and trigger ReconcileSpecsOnHost for just that host; unlike HostLoad
// it is not exposed over the API.
func (s *Service) HostUptime(ctx context.Context, host string) (time.Duration, bool, error) {
	if _, ok := s.host(host); !ok {
		return 0, false, ErrUnknownHost
	}
	return s.client.HostUptime(ctx, host)
}

// RefreshHostLoadAvg samples the host's load averages into the podman client's
// cache, so a later HostLoad serves them without paying for the read (see
// podman.Client.SampleLoadAvg). Called by the inventory poller; not exposed
// over the API — the values it warms surface through HostLoad like any other.
//
// sampled is false when nothing was read and the outcome says nothing about
// the host (see podman.Client.SampleLoadAvg); the poller uses it to leave its
// last real verdict standing rather than inventing one.
func (s *Service) RefreshHostLoadAvg(ctx context.Context, host string) (sampled bool, err error) {
	if _, ok := s.host(host); !ok {
		// Not an outcome either: the host list the poller walked is one reload
		// behind this map.
		return false, nil
	}
	return s.client.SampleLoadAvg(ctx, host)
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

// PutHostSecret creates-or-rotates a host secret on the host, then (when
// persist is true) records the value so a later migrate/evacuate can
// re-provision it on a destination. We "rotate" by removing then recreating,
// since podman secrets are immutable. Push happens before persist: we never
// store a value we failed to apply to the host. The store write is a non-atomic
// tail — if it fails the host already holds the new value while the store lags;
// the caller's retry re-rotates and re-persists idempotently, so the divergence
// is self-healing.
func (s *Service) PutHostSecret(ctx context.Context, host, name string, value []byte, persist bool) error {
	if _, ok := s.host(host); !ok {
		return ErrUnknownHost
	}
	if _, err := s.client.SecretInspect(ctx, host, name); err == nil {
		if err := s.client.SecretRemove(ctx, host, name); err != nil {
			return err
		}
	}
	if err := s.client.SecretCreate(ctx, host, name, wrapAsKubeSecret(name, name, value)); err != nil {
		return err
	}
	if persist {
		if err := s.store.PutHostSecret(ctx, host, name, value); err != nil {
			return fmt.Errorf("persist host secret: %w", err)
		}
	}
	return nil
}

// DeleteHostSecret removes a host secret from the host and from the store. Like
// PutHostSecret, the store write is a non-atomic tail: a store-delete failure
// surfaces after the host removal succeeded, but a retry skips the already-gone
// host secret and re-deletes the store row, so the divergence is self-healing.
func (s *Service) DeleteHostSecret(ctx context.Context, host, name string) error {
	if _, ok := s.host(host); !ok {
		return ErrUnknownHost
	}
	if err := s.client.SecretRemove(ctx, host, name); err != nil && !errors.Is(err, podman.ErrNotFound) {
		return err
	}
	if err := s.store.DeleteHostSecret(ctx, host, name); err != nil {
		return fmt.Errorf("delete persisted host secret: %w", err)
	}
	return nil
}

// InstanceVolumes returns the named volumes the API believes belong to this instance.
// Volumes that don't exist on the host are omitted (no error).
func (s *Service) InstanceVolumes(ctx context.Context, host, tmpl, slug string) ([]podman.Volume, error) {
	t, err := s.lookup(ctx, host, tmpl)
	if err != nil {
		return nil, err
	}
	var out []podman.Volume
	for _, v := range t.Meta.Volumes {
		name := volumeName(tmpl, slug, v.Name)
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
	// Unfiltered on purpose: backup exclude patterns (#248) apply only to
	// backupVolume. This path fingerprints a volume for verification, not a
	// backup; a filtered fingerprint would false-mismatch against an
	// unfiltered destination.
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
//
// CopyVolume builds and returns a Manifest from the source export stream so the
// caller can verify the destination against the exact same snapshot, without
// re-exporting the source (which would risk a false mismatch if the source is
// still settling — see #153).
func (s *Service) CopyVolume(ctx context.Context, fromHost, toHost, name string) (Manifest, error) {
	// Unfiltered on purpose: backup exclude patterns (#248) apply only to
	// backupVolume. This path removes the source once the copy lands, so a
	// dropped entry would have no second copy to recover from.
	rc, err := s.client.VolumeExport(ctx, fromHost, name)
	if err != nil {
		return nil, fmt.Errorf("export volume %q from %s: %w", name, fromHost, err)
	}
	defer rc.Close()

	pr, pw := io.Pipe()
	copyDone := make(chan error, 1)
	var srcManifest Manifest

	go func() {
		tr := io.TeeReader(rc, pw)
		m, err := buildManifest(tr)
		if err == nil {
			srcManifest = m
		}
		pw.CloseWithError(err)
		copyDone <- err
	}()

	importErr := s.client.VolumeImport(ctx, toHost, name, pr)
	pr.CloseWithError(importErr)

	copyErr := <-copyDone
	if importErr != nil {
		return nil, fmt.Errorf("import volume %q to %s: %w", name, toHost, importErr)
	}
	if copyErr != nil {
		return nil, fmt.Errorf("copy volume %q: build manifest: %w", name, copyErr)
	}
	return srcManifest, nil
}

// Logs returns a channel of log lines from one container in an instance.
func (s *Service) Logs(ctx context.Context, host, tmpl, slug, container string, opts podman.LogOptions) (<-chan podman.LogLine, error) {
	if _, err := s.lookup(ctx, host, tmpl); err != nil {
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

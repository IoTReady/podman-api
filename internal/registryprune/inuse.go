// Package registryprune implements in-use-aware garbage collection of the
// fleet's container registry.
package registryprune

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"sort"
	"strings"
	"time"
	"unicode"

	"github.com/iotready/podman-api/internal/config"
	"github.com/iotready/podman-api/internal/imgregistry"
	"github.com/iotready/podman-api/internal/instance"
	"github.com/iotready/podman-api/internal/store"
)

// ErrUnsafeToPrune aborts a prune run. Every ambiguity in the in-use
// computation resolves to this error rather than to a smaller protected set: a
// digest wrongly believed unused is deleted forever and takes an instance's
// recreatability with it (#64 — 11 of 24 live Engine instances lost the
// manifests they would have needed to be rebuilt). A run that refuses to start
// costs a cycle; a run that under-protects costs a customer.
var ErrUnsafeToPrune = errors.New("unsafe to prune")

// InUseSet is the set of manifest digests that must never be deleted, keyed by
// digest across ALL repositories. Blobs are shared between repos, so a digest
// protected anywhere is protected everywhere — deliberately broader than
// necessary, because that is the conservative direction.
type InUseSet struct {
	// Valid is true only on a set BuildInUseSet returned without error. The
	// zero value, and every aborted return, is false. Check it before acting:
	// an aborted set answers Has() false for every digest in the registry, so
	// a caller that logs the error and carries on would delete all of it.
	Valid   bool
	Digests map[string]struct{}
	// NonDigestObserved names running containers whose observed image was not
	// digest-form, i.e. podman gave us an image ID rather than a manifest
	// digest and the entry we derived from it can never match anything in the
	// registry. Such a container is protected only by its ImageTag and by its
	// spec, so the gap must be recorded (a job step) rather than left silent.
	// Entries are "host/slug/container: <image>".
	NonDigestObserved []string
	// ForeignSkipped lists the image references skipped as belonging to some
	// other registry, deduped. A run that skips everything is a misconfigured
	// registry host, and must be visible rather than silent.
	ForeignSkipped []string
}

// Has reports whether digest is protected. digest is normalised the same way
// the set's own entries were, so a caller comparing an upper-case hex ref
// cannot slip past a lower-case entry.
func (s InUseSet) Has(digest string) bool {
	if s.Digests == nil {
		return false
	}
	_, ok := s.Digests[strings.ToLower(strings.TrimSpace(digest))]
	return ok
}

// Len is the number of protected digests.
func (s InUseSet) Len() int { return len(s.Digests) }

// HostEnumerator yields the fleet's CURRENT host set. BuildInUseSet takes this
// rather than a []string because a partial host list under-protects silently:
// the hosts that were passed still produce a non-zero fleet-wide count, so no
// fail-closed rule fires, and every digest pinned only on a missing host is
// deleted. That is the #64 failure mode through a door the rules do not watch.
//
// INVARIANT: an implementation must return every configured host or fail. It
// must be the same live view the server reloads on SIGHUP (*instance.Service
// satisfies it), never a snapshot captured by the caller.
type HostEnumerator interface {
	Hosts() []config.Host
}

// InventorySource is the observed half: the warm inventory cache, satisfied by
// *instance.Service.
type InventorySource interface {
	ListAllInstancesWithMeta(ctx context.Context, host string) ([]instance.Observed, instance.Freshness, error)
}

// SpecSource is the desired half: the spec store, satisfied by store.Store.
type SpecSource interface {
	ListSpecKeys(ctx context.Context, host string) ([]store.SpecKey, error)
	GetSpec(ctx context.Context, host, template, slug string) (store.Spec, error)
}

// ManifestResolver is the read-only slice of imgregistry.Client this package
// needs. It deliberately excludes Delete: the component that decides what to
// protect must not be able to remove anything, so no bug here can turn into a
// deletion.
type ManifestResolver interface {
	Catalog(ctx context.Context) ([]string, error)
	Manifest(ctx context.Context, repo, ref string) (imgregistry.Manifest, error)
}

// Config parameterises BuildInUseSet.
type Config struct {
	// RegistryHosts are every spelling of the registry being pruned as it may
	// appear in an image reference (e.g. "reg.example:5000", "reg.example",
	// "100.64.0.23:5000"). They are what separate our refs from foreign ones;
	// an empty list aborts, because without one a host-qualified foreign ref
	// cannot be told from one of ours.
	RegistryHosts []string
	// MaxSnapshotAge bounds how stale a host's inventory snapshot may be. Must
	// be positive — a zero value is treated as unconfigured and aborts, rather
	// than silently disabling the staleness check.
	MaxSnapshotAge time.Duration
	// Now is a test seam; defaults to time.Now.
	Now func() time.Time
}

// bareImageIDRe matches podman's algorithm-less image ID form (what
// InspectContainerData.Image carries when ImageDigest is empty).
var bareImageIDRe = regexp.MustCompile(`^[a-f0-9]{64}$`)

// digestAlgoHexLen maps every algorithm go-digest registers (algorithm.go:34 —
// SHA256, SHA384, SHA512) to the exact hex length its own validator requires.
// These are the only algorithms InspectContainerData.ImageDigest
// (image.Digest().String()) can carry, so this is the complete set of bare
// digests we are willing to trust without parsing.
//
// imgregistry.IsDigestRef is deliberately not enough on its own here: its
// pattern accepts any lowercase-alphanumeric run as the algorithm and any hex
// run of 32 or more, so an unqualified "<repo>:<40-hex git sha>" — a common
// tagging convention — matches it and would be claimed as a digest before the
// ref was ever parsed, protecting a junk key that matches nothing while the
// manifest the instance actually needs went unprotected and got deleted.
var digestAlgoHexLen = map[string]int{"sha256": 64, "sha384": 96, "sha512": 128}

// digestShapedRe matches "<algorithm>:<hex>" with no repository path, i.e. the
// shape a bare digest has. Whether it IS a digest depends on the algorithm and
// hex length; see classifyBareDigest.
var digestShapedRe = regexp.MustCompile(`^([a-z0-9]+(?:[+._-][a-z0-9]+)*):([a-f0-9]{32,})$`)

// bareDigestKind is what classifyBareDigest concluded about a reference.
type bareDigestKind int

const (
	// notBareDigest: the ref is not digest-shaped, or its hex run is not a
	// length any digest algorithm produces — an unqualified "<repo>:<40-hex git
	// sha>" lands here and is resolved through the registry like any other tag.
	notBareDigest bareDigestKind = iota
	// trustedBareDigest: a registered algorithm at its own required length.
	trustedBareDigest
	// suspectBareDigest: canonically digest-shaped (the hex run is exactly the
	// length of one of the registered algorithms) but the algorithm/length pair
	// is not one podman can emit. This MUST be an error, not a fall-through:
	// ordinary parsing turns "sha384:<96 hex>" into repository name "sha384",
	// which misses the catalog and is then marked Covered AND Foreign — a
	// silent skip that the non-digest fatal check cannot see either. Probed on
	// the pre-fix code: sha384/v1/md5 refs all returned err=nil, Valid=true,
	// and appeared only as a ForeignSkipped entry.
	suspectBareDigest
)

// canonicalDigestHexLen reports whether n is the hex length of some registered
// digest algorithm.
func canonicalDigestHexLen(n int) bool {
	for _, want := range digestAlgoHexLen {
		if n == want {
			return true
		}
	}
	return false
}

// classifyBareDigest decides how to treat a reference with no repository path.
func classifyBareDigest(ref string) bareDigestKind {
	m := digestShapedRe.FindStringSubmatch(ref)
	if m == nil || !imgregistry.IsDigestRef(ref) {
		return notBareDigest
	}
	algo, hex := m[1], m[2]
	if want, ok := digestAlgoHexLen[algo]; ok {
		if len(hex) == want {
			return trustedBareDigest
		}
		// A registered algorithm at the wrong length is a corrupt digest, never
		// a repository called "sha256".
		return suspectBareDigest
	}
	if canonicalDigestHexLen(len(hex)) {
		return suspectBareDigest
	}
	return notBareDigest
}

// publicRegistryHosts are registry hosts that are, by construction, never ours.
// They bound the wrong-registry-host rule below: see its comment.
var publicRegistryHosts = map[string]struct{}{
	"docker.io":            {},
	"index.docker.io":      {},
	"registry-1.docker.io": {},
	"quay.io":              {},
	"ghcr.io":              {},
	"gcr.io":               {},
	"registry.k8s.io":      {},
	"k8s.gcr.io":           {},
	"public.ecr.aws":       {},
	"mcr.microsoft.com":    {},
	"registry.gitlab.com":  {},
	"codeberg.org":         {},
}

// BuildInUseSet computes the fleet-wide set of digests that must be protected
// from deletion. It is the union of two sources:
//
//   - observed: every running container's image AND its image tag, from the
//     warm inventory. Both are needed: podman reports a manifest digest when it
//     has one and an image ID when it does not, and only the tag can be
//     resolved back to a manifest in the latter case;
//   - desired: every image-bearing value in every stored spec's parameters
//     ("image", "pg_image", any "*_image"), resolved tag->digest through the
//     registry. This half is what makes a stopped instance — or one whose spec
//     pins something nothing is currently running — still recreatable. The
//     legacy registry-gc.sh has no equivalent.
//
// Any doubt aborts with ErrUnsafeToPrune and an empty set; see the doc on that
// error.
func BuildInUseSet(ctx context.Context, hostSrc HostEnumerator, inv InventorySource, specs SpecSource, reg ManifestResolver, cfg Config) (InUseSet, error) {
	now := cfg.Now
	if now == nil {
		now = time.Now
	}
	if cfg.MaxSnapshotAge <= 0 {
		return InUseSet{}, fmt.Errorf("%w: max snapshot age is not configured", ErrUnsafeToPrune)
	}
	if len(cfg.RegistryHosts) == 0 {
		return InUseSet{}, fmt.Errorf("%w: no registry host is configured, so a foreign image reference cannot be told from one of ours", ErrUnsafeToPrune)
	}
	ourHosts := make([]string, 0, len(cfg.RegistryHosts))
	for _, raw := range cfg.RegistryHosts {
		h, err := normalizeRegistryHost(raw)
		if err != nil {
			// Rejected, never accepted-and-hoped: a registry host that does not
			// compare equal to our own refs classifies every one of them as
			// foreign, and the fleet-wide-zero rule does not save us because
			// digest-pinned containers still populate the set. The run would
			// proceed and delete every manifest reachable only through a tag.
			return InUseSet{}, fmt.Errorf("%w: registry host %q: %v", ErrUnsafeToPrune, raw, err)
		}
		ourHosts = append(ourHosts, h)
	}
	// Every configured host must be swept. Drain is deliberately ignored: a
	// drained host refuses NEW instances but still runs the pods it has, so its
	// images are as in-use as any other host's.
	var hosts []string
	for _, h := range hostSrc.Hosts() {
		hosts = append(hosts, h.ID)
	}

	// The catalog bounds which refs this run could ever delete. A ref whose
	// repo is not in it belongs to some other registry (docker.io/…), so there
	// is nothing here to protect it from — but a catalog we cannot read tells
	// us nothing at all, so that aborts.
	repos, err := reg.Catalog(ctx)
	if err != nil {
		return InUseSet{}, fmt.Errorf("%w: registry catalog: %v", ErrUnsafeToPrune, err)
	}
	known := make(map[string]struct{}, len(repos))
	for _, r := range repos {
		known[r] = struct{}{}
	}

	digests := make(map[string]struct{})
	var nonDigest []string
	foreign := make(map[string]struct{})
	var qualifiedSeen, qualifiedMatched int

	for _, host := range hosts {
		// (a) observed.
		obs, fresh, err := inv.ListAllInstancesWithMeta(ctx, host)
		if err != nil {
			return InUseSet{}, fmt.Errorf("%w: inventory for host %q: %v", ErrUnsafeToPrune, host, err)
		}
		if !fresh.Reachable {
			return InUseSet{}, fmt.Errorf("%w: host %q unreachable; its snapshot under-reports what is in use", ErrUnsafeToPrune, host)
		}
		if !fresh.HasData {
			return InUseSet{}, fmt.Errorf("%w: host %q has no inventory snapshot", ErrUnsafeToPrune, host)
		}
		if fresh.FetchedAt.IsZero() || now().Sub(fresh.FetchedAt) > cfg.MaxSnapshotAge {
			return InUseSet{}, fmt.Errorf("%w: host %q snapshot is older than %s", ErrUnsafeToPrune, host, cfg.MaxSnapshotAge)
		}
		for _, o := range obs {
			for _, c := range o.Containers {
				if c.IsInfra {
					// Every pod has an infra container, and on podman 5.8.2 it
					// reports no image, image digest or image name at all
					// (verified live across engine-1/2/infra on 2026-08-07: 78
					// of 78). It runs a locally-built pause image that is in no
					// registry, so there is nothing to protect — but treating
					// it as an unresolvable ref would abort every run on every
					// real host.
					//
					// The flag comes from the POD's own InfraID, not from the
					// container's name or the absence of an image. Neither of
					// those discriminates: `podman kube play` names containers
					// <pod>-<containerName>, so a template declaring one called
					// "infra" matches the name, and an app container whose
					// inspect failed (PodList swallows that) is equally
					// image-less — and that is precisely the case the
					// fail-closed rule below exists to catch.
					continue
				}
				// podman gives us two views of a running container's image, and
				// each covers a hole in the other. Image is ImageDigest when
				// podman has one, but falls back to an image ID, which is NOT a
				// manifest digest and can never match anything in the registry.
				// ImageTag is the full reference — what registry-gc.sh
				// protected. Union both; over-protection is free.
				got, err := resolveRef(ctx, reg, known, ourHosts, c.Image)
				if err != nil {
					return InUseSet{}, fmt.Errorf("%w: running container %s/%s/%s: %v", ErrUnsafeToPrune, host, o.Slug, c.Name, err)
				}
				covered := got.Covered
				countQualified(got, &qualifiedSeen, &qualifiedMatched)
				if got.Digest != "" {
					digests[got.Digest] = struct{}{}
				}
				if got.Foreign {
					foreign[strings.TrimSpace(c.Image)] = struct{}{}
				}
				gotTag, terr := resolveRef(ctx, reg, known, ourHosts, c.ImageTag)
				switch {
				case terr == nil:
					countQualified(gotTag, &qualifiedSeen, &qualifiedMatched)
					if gotTag.Digest != "" {
						digests[gotTag.Digest] = struct{}{}
					}
					if gotTag.Foreign {
						foreign[strings.TrimSpace(c.ImageTag)] = struct{}{}
					}
				case errors.Is(terr, imgregistry.ErrUnreachable):
					// Never "already gone".
					return InUseSet{}, fmt.Errorf("%w: resolving image tag of %s/%s/%s: %v", ErrUnsafeToPrune, host, o.Slug, c.Name, terr)
				default:
					// An absent or since-re-pushed tag adds nothing. Tolerated
					// ONLY when the image itself is digest-form, where the
					// digest is already protected and aborting would deadlock
					// the job on every rolled instance. When it is not, the
					// container has no protection at all and the fatal check
					// below fires.
				}
				if !covered {
					// The image is an image ID, not a manifest digest, so the
					// entry derived from it can never match anything in the
					// registry. Resolving ImageTag is not a rescue: if the tag
					// has MOVED it resolves cleanly to a different digest while
					// this container's own manifest stays unprotected, with no
					// error anywhere. Fatal — recorded, then aborted after the
					// sweep so an operator sees every offending container at
					// once rather than one per run.
					msg := fmt.Sprintf("%s/%s/%s: observed image %q is an image ID, not a manifest digest",
						host, o.Slug, c.Name, c.Image)
					if terr != nil {
						msg += fmt.Sprintf("; its tag %q did not resolve either (%v)", c.ImageTag, terr)
					}
					nonDigest = append(nonDigest, msg)
				}
			}
		}

		// (b) desired.
		keys, err := specs.ListSpecKeys(ctx, host)
		if err != nil {
			return InUseSet{}, fmt.Errorf("%w: listing specs on host %q: %v", ErrUnsafeToPrune, host, err)
		}
		for _, k := range keys {
			sp, err := specs.GetSpec(ctx, host, k.Template, k.Slug)
			if err != nil {
				return InUseSet{}, fmt.Errorf("%w: reading spec %s/%s/%s: %v", ErrUnsafeToPrune, host, k.Template, k.Slug, err)
			}
			for name, v := range sp.Parameters {
				if !isImageParam(name) {
					continue
				}
				s, ok := v.(string)
				if !ok {
					return InUseSet{}, fmt.Errorf("%w: spec %s/%s/%s parameter %q is %T, not an image reference",
						ErrUnsafeToPrune, host, k.Template, k.Slug, name, v)
				}
				if strings.TrimSpace(s) == "" {
					// An empty value pins no image; there is no digest hiding
					// behind it.
					continue
				}
				got, err := resolveRef(ctx, reg, known, ourHosts, s)
				if err != nil {
					return InUseSet{}, fmt.Errorf("%w: spec %s/%s/%s parameter %q: %v",
						ErrUnsafeToPrune, host, k.Template, k.Slug, name, err)
				}
				countQualified(got, &qualifiedSeen, &qualifiedMatched)
				if got.Digest != "" {
					digests[got.Digest] = struct{}{}
				}
				if got.Foreign {
					foreign[strings.TrimSpace(s)] = struct{}{}
				}
			}
		}
	}

	// A running container whose image is an image ID rather than a manifest
	// digest may have its real manifest unprotected, and the moved-tag case is
	// undetectable from here. That is the #64 failure mode, so it is fatal
	// rather than advisory — the field stays populated so an operator can see
	// WHICH containers caused it. Verified safe on this fleet: 123 of 123
	// running app containers carry a digest (engine-1/2/infra, 2026-08-07).
	if len(nonDigest) > 0 {
		return InUseSet{NonDigestObserved: nonDigest}, fmt.Errorf(
			"%w: %d running container(s) report an image ID rather than a manifest digest: %s",
			ErrUnsafeToPrune, len(nonDigest), strings.Join(nonDigest, "; "))
	}

	// A configured registry host that is well-formed but WRONG — stale after a
	// registry move, or a typo — makes every one of our refs foreign, and one
	// digest-pinned container is enough to keep the fleet-wide count non-zero,
	// so nothing else fires. Closing the class: if anything was host-qualified
	// and none of it was ours, the configuration is wrong, not the fleet. A
	// sweep with no qualified refs at all is not evidence either way.
	//
	// The tally deliberately EXCLUDES refs qualified to a well-known public
	// registry (docker.io, quay.io, …). Without that exclusion a fleet whose
	// only host-qualified refs are third-party sidecars — bare-digest observed
	// images plus a "docker.io/library/postgres:16" parameter, which is a
	// perfectly ordinary and correct configuration — aborts every run forever,
	// and nothing an operator can do short of editing a spec clears it. A
	// public-registry ref is not evidence about our own registry's spelling in
	// either direction, so it belongs in neither side of the ratio. Every
	// PRIVATE qualified ref still counts, which is where a stale or mistyped
	// host actually shows up.
	if qualifiedSeen > 0 && qualifiedMatched == 0 {
		return InUseSet{}, fmt.Errorf(
			"%w: no host-qualified image reference matched a configured registry host %v, out of %d qualified references seen",
			ErrUnsafeToPrune, cfg.RegistryHosts, qualifiedSeen)
	}

	// Matches registry-gc.sh: a fleet-wide zero means the inputs are wrong, not
	// that the whole registry is garbage.
	if len(digests) == 0 {
		return InUseSet{}, fmt.Errorf("%w: fleet-wide in-use digest count is zero", ErrUnsafeToPrune)
	}
	skipped := make([]string, 0, len(foreign))
	for r := range foreign {
		skipped = append(skipped, r)
	}
	sort.Strings(skipped)
	return InUseSet{Valid: true, Digests: digests, NonDigestObserved: nonDigest, ForeignSkipped: skipped}, nil
}

// isImageParam reports whether a render parameter name carries an image
// reference: "image" itself, or any "*_image" (pg_image, sidecar_image, …).
func isImageParam(name string) bool {
	n := strings.ToLower(name)
	return n == "image" || strings.HasSuffix(n, "_image")
}

// resolveRef turns one image reference into the manifest digest it must
// protect.
//
// It returns "" (protect nothing, no error) only when the reference provably
// belongs somewhere this run cannot delete from: another registry, or a
// repository ours does not host. Every other failure is an error, and every
// error aborts the run — a ref quietly skipped is exactly the false negative
// this package exists to prevent.
//
// refOutcome is what one image reference resolved to.
type refOutcome struct {
	// Digest is the manifest digest to protect, or "" when there is nothing
	// here to protect.
	Digest string
	// Covered reports whether the answer is trustworthy: true when a real
	// manifest digest was obtained, and true when the ref provably is not ours
	// (nothing to protect, nothing missed). False only for podman's
	// algorithm-less image ID, whose value cannot match any registry manifest.
	Covered bool
	// Foreign marks a ref skipped because it belongs to another registry, or to
	// a repository this one does not host.
	Foreign bool
	// Qualified marks a ref that carried a registry-host component at all, and
	// HostMatched whether that component is one of ours. A sweep in which some
	// ref was qualified and NONE matched means the configured host is wrong.
	Qualified   bool
	HostMatched bool
}

func resolveRef(ctx context.Context, reg ManifestResolver, known map[string]struct{}, registryHosts []string, ref string) (refOutcome, error) {
	var out refOutcome
	ref = strings.TrimSpace(ref)
	if ref == "" {
		return refOutcome{}, errors.New("image reference is empty")
	}
	// podman's algorithm-less image ID (InspectContainerData.Image, used when
	// ImageDigest is empty). Normalising it to sha256 form can only
	// over-protect, which is free — but it is NOT a manifest digest, so it
	// cannot count as coverage.
	if bareImageIDRe.MatchString(strings.ToLower(ref)) {
		return refOutcome{Digest: "sha256:" + strings.ToLower(ref)}, nil
	}
	// A BARE digest with no repository. This is what podman actually puts in
	// ObservedContainer.Image: InspectContainerData.ImageDigest is
	// image.Digest().String() (libpod/container_inspect.go), and
	// enrichContainer copies it verbatim. Measured on engine-1, 2026-08-07:
	// ImageDigest="sha256:42283567cae4…" against
	// ImageName="100.64.0.23:5000/jioworldcentre/web:2026.06.27-218c436".
	// Parsing it as a reference yields repo "sha256", which is in no catalog,
	// so it would be discarded as foreign and the observed half would protect
	// nothing at all.
	//
	// Gated on a KNOWN digest algorithm at its own length, not on digest-shape
	// alone — see digestAlgoHexLen.
	switch classifyBareDigest(strings.ToLower(ref)) {
	case trustedBareDigest:
		return refOutcome{Digest: strings.ToLower(ref), Covered: true}, nil
	case suspectBareDigest:
		// Never fall through to splitRef: it would become a bogus repository
		// name, miss the catalog, and be skipped as foreign with no error.
		return refOutcome{}, fmt.Errorf(
			"reference %q is digest-shaped but is not a digest any registered algorithm (sha256/sha384/sha512) produces", ref)
	}

	name, tag, digest := splitRef(ref)
	if h := hostComponent(name); h != "" {
		// Only refs that COULD have been ours count toward the
		// wrong-registry-host tally. A ref qualified to a well-known public
		// registry says nothing about how our own registry is spelled, so
		// counting it would turn a perfectly correct configuration into a
		// permanent abort — see the rule's own comment in BuildInUseSet.
		if _, public := publicRegistryHosts[strings.ToLower(h)]; !public {
			out.Qualified = true
			out.HostMatched = isOurHost(h, registryHosts)
		}
	}
	if digest != "" {
		// Already pinned: no registry round-trip, and nothing the registry
		// could say would make us protect less.
		if !imgregistry.IsDigestRef(digest) {
			return refOutcome{}, fmt.Errorf("malformed digest in reference %q", ref)
		}
		out.Digest, out.Covered = digest, true
		return out, nil
	}

	repo, ours := repoFromName(name, registryHosts)
	if !ours {
		// Host-qualified to some other registry. It must NOT reach the catalog
		// gate: stripping the host would derive a bare repo name that our
		// registry may well also host ("postgres"), so the ref would be
		// queried against us, 404, and abort every run until someone edited a
		// spec — a self-inflicted deadlock.
		out.Covered, out.Foreign = true, true
		return out, nil
	}
	if repo == "" {
		return refOutcome{}, fmt.Errorf("cannot derive a repository from reference %q", ref)
	}
	if _, ok := known[repo]; !ok {
		// Not a repository this registry hosts — nothing here to delete, so
		// nothing to protect.
		out.Covered, out.Foreign = true, true
		return out, nil
	}
	if tag == "" {
		tag = "latest"
	}
	if !imgregistry.ValidRef(tag) || !imgregistry.ValidRepoName(repo) {
		return refOutcome{}, fmt.Errorf("invalid repository/tag in reference %q", ref)
	}
	m, err := reg.Manifest(ctx, repo, tag)
	if err != nil {
		// Includes ErrNotFound: a tag we cannot resolve is a digest we cannot
		// protect. Never "skip it".
		return refOutcome{}, fmt.Errorf("resolving %s:%s: %w", repo, tag, err)
	}
	if m.Digest == "" {
		return refOutcome{}, fmt.Errorf("registry returned no digest for %s:%s", repo, tag)
	}
	if !imgregistry.IsDigestRef(strings.ToLower(m.Digest)) {
		return refOutcome{}, fmt.Errorf("registry returned a malformed digest %q for %s:%s", m.Digest, repo, tag)
	}
	out.Digest, out.Covered = strings.ToLower(m.Digest), true
	return out, nil
}

// splitRef breaks an image reference into its name portion (possibly
// host-qualified), its tag, and its digest. Digest is lower-cased; at most one
// of tag/digest is set.
func splitRef(ref string) (name, tag, digest string) {
	if i := strings.LastIndex(ref, "@"); i != -1 {
		return ref[:i], "", strings.ToLower(ref[i+1:])
	}
	if slash, colon := strings.LastIndex(ref, "/"), strings.LastIndex(ref, ":"); colon > slash {
		// A ":" after the last "/" is a tag; a ":" inside a host:port prefix is
		// not.
		return ref[:colon], ref[colon+1:], ""
	}
	return ref, "", ""
}

// repoFromName strips OUR registry's host component from an image name to get
// the bare repository name Catalog() groups tags under (imgregistry's Catalog
// returns bare names like "engine", never host-qualified).
//
// ours is false when the name is qualified with a DIFFERENT registry host —
// the caller must then treat the ref as foreign and never test it against our
// catalog. An unqualified name is assumed to be ours (that is the shape our own
// Catalog returns), and is still catalog-gated by the caller. The
// host-vs-path-element test is the standard Docker heuristic, matching
// internal/ui's repoFromImage and scripts/roll.py.
func repoFromName(name string, registryHosts []string) (repo string, ours bool) {
	if name == "" {
		return "", true
	}
	h := hostComponent(name)
	if h == "" {
		return name, true
	}
	if name == h {
		// A bare host with no repository path. Nothing is addressable either
		// way, and the caller turns repo "" into "cannot derive a repository" —
		// an error, not a silent skip. Reporting ours=false here would skip it
		// silently instead, which is why both arms agree on true.
		return "", true
	}
	if !isOurHost(h, registryHosts) {
		return "", false
	}
	return name[len(h)+1:], true
}

// hostComponent returns the registry-host component of an image name, or ""
// when it has none.
func hostComponent(name string) string {
	slash := strings.Index(name, "/")
	if slash == -1 {
		if looksLikeRegistryHost(name) {
			return name
		}
		return ""
	}
	if first := name[:slash]; looksLikeRegistryHost(first) {
		return first
	}
	return ""
}

func isOurHost(h string, registryHosts []string) bool {
	for _, r := range registryHosts {
		if strings.EqualFold(h, r) {
			return true
		}
	}
	return false
}

func looksLikeRegistryHost(s string) bool {
	return s == "localhost" || strings.ContainsAny(s, ".:")
}

// normalizeRegistryHost canonicalises one configured spelling of our registry
// into the bare host[:port] form that appears in an image reference, and
// rejects anything that is not one.
//
// This is load-bearing, not tidying. The comparison it feeds decides whether a
// ref is ours; a value that never compares equal — a trailing slash, a stray
// space from an env var, or the "http://host:5000" BaseURL shape
// imgregistry.NewHTTPClient takes — silently classifies EVERY one of our refs
// as foreign. The fleet-wide-zero rule does not catch it, because
// digest-pinned containers still populate the set, so the run proceeds and
// deletes every manifest that was only reachable through a tag.
func normalizeRegistryHost(raw string) (string, error) {
	h := strings.ToLower(strings.TrimSpace(raw))
	if i := strings.Index(h, "://"); i != -1 {
		h = h[i+len("://"):]
	}
	h = strings.TrimRight(h, "/")
	h = strings.TrimSpace(h)
	if h == "" {
		return "", errors.New("empty")
	}
	if strings.ContainsAny(h, "/") {
		return "", errors.New("must be a bare host[:port], with no path")
	}
	if strings.ContainsFunc(h, unicode.IsSpace) {
		return "", errors.New("must not contain whitespace")
	}
	if strings.ContainsAny(h, "@?#") {
		return "", errors.New("must be a bare host[:port], with no userinfo, query or fragment")
	}
	// The property matching actually depends on. A value failing this test can
	// never be the host component of a reference — hostComponent would not even
	// recognise it as a host — so every one of our refs would be classified
	// foreign, silently.
	if !looksLikeRegistryHost(h) {
		return "", errors.New(`must contain "." or ":", or be "localhost"; otherwise it can never be the host component of an image reference`)
	}
	return h, nil
}

// countQualified accumulates the host-qualified/host-matched tallies behind the
// wrong-registry-host rule.
func countQualified(o refOutcome, seen, matched *int) {
	if !o.Qualified {
		return
	}
	*seen++
	if o.HostMatched {
		*matched++
	}
}

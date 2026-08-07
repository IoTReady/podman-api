// Package registryprune implements in-use-aware garbage collection of the
// fleet's container registry.
package registryprune

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"time"

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
	Digests map[string]struct{}
	// NonDigestObserved names running containers whose observed image was not
	// digest-form, i.e. podman gave us an image ID rather than a manifest
	// digest and the entry we derived from it can never match anything in the
	// registry. Such a container is protected only by its ImageTag and by its
	// spec, so the gap must be recorded (a job step) rather than left silent.
	// Entries are "host/slug/container: <image>".
	NonDigestObserved []string
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
	// RegistryHost is the host[:port] of the registry being pruned, as it
	// appears in image references (e.g. "reg.example:5000"). It is what
	// separates our refs from foreign ones; empty aborts, because without it
	// a host-qualified foreign ref cannot be told from one of ours.
	RegistryHost string
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
	if strings.TrimSpace(cfg.RegistryHost) == "" {
		return InUseSet{}, fmt.Errorf("%w: registry host is not configured, so a foreign image reference cannot be told from one of ours", ErrUnsafeToPrune)
	}
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
				// podman gives us two views of a running container's image, and
				// each covers a hole in the other. Image is ImageDigest when
				// podman has one, but falls back to an image ID, which is NOT a
				// manifest digest and can never match anything in the registry.
				// ImageTag is the full reference — what registry-gc.sh
				// protected. Union both; over-protection is free.
				d, covered, err := resolveRef(ctx, reg, known, cfg.RegistryHost, c.Image)
				if err != nil {
					return InUseSet{}, fmt.Errorf("%w: running container %s/%s/%s: %v", ErrUnsafeToPrune, host, o.Slug, c.Name, err)
				}
				if d != "" {
					digests[d] = struct{}{}
				}
				td, _, terr := resolveRef(ctx, reg, known, cfg.RegistryHost, c.ImageTag)
				switch {
				case terr == nil:
					if td != "" {
						digests[td] = struct{}{}
					}
				case errors.Is(terr, imgregistry.ErrUnreachable):
					// Never "already gone".
					return InUseSet{}, fmt.Errorf("%w: resolving image tag of %s/%s/%s: %v", ErrUnsafeToPrune, host, o.Slug, c.Name, terr)
				default:
					// An absent or since-re-pushed tag adds nothing. It is not
					// worth aborting on: when the image itself is digest-form
					// the digest is already protected, and when it is not, the
					// gap is recorded below.
				}
				if !covered {
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
				d, _, err := resolveRef(ctx, reg, known, cfg.RegistryHost, s)
				if err != nil {
					return InUseSet{}, fmt.Errorf("%w: spec %s/%s/%s parameter %q: %v",
						ErrUnsafeToPrune, host, k.Template, k.Slug, name, err)
				}
				if d != "" {
					digests[d] = struct{}{}
				}
			}
		}
	}

	// Matches registry-gc.sh: a fleet-wide zero means the inputs are wrong, not
	// that the whole registry is garbage.
	if len(digests) == 0 {
		return InUseSet{}, fmt.Errorf("%w: fleet-wide in-use digest count is zero", ErrUnsafeToPrune)
	}
	return InUseSet{Digests: digests, NonDigestObserved: nonDigest}, nil
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
// covered reports whether the answer is trustworthy: true when a real manifest
// digest was obtained, and true when the ref provably is not ours (nothing to
// protect, nothing missed). It is false only for podman's algorithm-less image
// ID, where the value returned cannot match any registry manifest and the
// caller must record the gap.
func resolveRef(ctx context.Context, reg ManifestResolver, known map[string]struct{}, registryHost, ref string) (digestOut string, covered bool, err error) {
	ref = strings.TrimSpace(ref)
	if ref == "" {
		return "", false, errors.New("image reference is empty")
	}
	// podman's algorithm-less image ID (InspectContainerData.Image, used when
	// ImageDigest is empty). Normalising it to sha256 form can only
	// over-protect, which is free — but it is NOT a manifest digest, so it
	// cannot count as coverage.
	if bareImageIDRe.MatchString(strings.ToLower(ref)) {
		return "sha256:" + strings.ToLower(ref), false, nil
	}

	name, tag, digest := splitRef(ref)
	if digest != "" {
		// Already pinned: no registry round-trip, and nothing the registry
		// could say would make us protect less.
		if !imgregistry.IsDigestRef(digest) {
			return "", false, fmt.Errorf("malformed digest in reference %q", ref)
		}
		return digest, true, nil
	}

	repo, ours := repoFromName(name, registryHost)
	if !ours {
		// Host-qualified to some other registry. It must NOT reach the catalog
		// gate: stripping the host would derive a bare repo name that our
		// registry may well also host ("postgres"), so the ref would be
		// queried against us, 404, and abort every run until someone edited a
		// spec — a self-inflicted deadlock.
		return "", true, nil
	}
	if repo == "" {
		return "", false, fmt.Errorf("cannot derive a repository from reference %q", ref)
	}
	if _, ok := known[repo]; !ok {
		// Not a repository this registry hosts — nothing here to delete, so
		// nothing to protect.
		return "", true, nil
	}
	if tag == "" {
		tag = "latest"
	}
	if !imgregistry.ValidRef(tag) || !imgregistry.ValidRepoName(repo) {
		return "", false, fmt.Errorf("invalid repository/tag in reference %q", ref)
	}
	m, err := reg.Manifest(ctx, repo, tag)
	if err != nil {
		// Includes ErrNotFound: a tag we cannot resolve is a digest we cannot
		// protect. Never "skip it".
		return "", false, fmt.Errorf("resolving %s:%s: %w", repo, tag, err)
	}
	if m.Digest == "" {
		return "", false, fmt.Errorf("registry returned no digest for %s:%s", repo, tag)
	}
	if !imgregistry.IsDigestRef(strings.ToLower(m.Digest)) {
		return "", false, fmt.Errorf("registry returned a malformed digest %q for %s:%s", m.Digest, repo, tag)
	}
	return strings.ToLower(m.Digest), true, nil
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
func repoFromName(name, registryHost string) (repo string, ours bool) {
	if name == "" {
		return "", true
	}
	isOurHost := func(h string) bool { return strings.EqualFold(h, registryHost) }
	slash := strings.Index(name, "/")
	if slash == -1 {
		if looksLikeRegistryHost(name) {
			// A bare host with no repository path: nothing addressable either
			// way.
			return "", isOurHost(name)
		}
		return name, true
	}
	if first := name[:slash]; looksLikeRegistryHost(first) {
		if !isOurHost(first) {
			return "", false
		}
		return name[slash+1:], true
	}
	return name, true
}

func looksLikeRegistryHost(s string) bool {
	return s == "localhost" || strings.ContainsAny(s, ".:")
}

package server

import (
	"flag"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/iotready/podman-api/internal/imgregistry"
	"github.com/iotready/podman-api/internal/instance"
	"github.com/iotready/podman-api/internal/registryprune"
	"github.com/iotready/podman-api/internal/store"
)

// registryPruneConfig is the resolved -registry-prune-* flag set.
//
// Everything here defaults to off. The feature is absent unless Enabled is set
// AND a registry client exists; blob GC is absent on top of that unless a
// registry pod is named. That layering is deliberate: Stage A (unlinking
// manifests) is recoverable until the blobs are swept, so an operator can enable
// the two halves separately.
type registryPruneConfig struct {
	Enabled           bool
	Interval          time.Duration
	DryRun            bool
	MaxDeletesPerRepo int
	MaxSnapshotAge    time.Duration
	ExtraHosts        string

	Host              string
	RegistryPod       string
	RegistryContainer string
	GCPod             string
	StoragePath       string
	SizePath          string

	// GCImage, GCConfigPath, GCMountPath and GCTimeout all default to empty/
	// zero, in which case BlobGC applies its own defaults (registry:2,
	// /etc/docker/registry/config.yml, /var/lib/registry, 30m — see
	// registryprune.BlobGC's default* constants). They become load-bearing
	// only when the registry image changes: a different base, or the
	// registry:3 line, can move ConfigPath, and pinning Image by digest is
	// exactly what an operator wants after a supply-chain scare (#227).
	GCImage      string
	GCConfigPath string
	// GCMountPath is DELIBERATELY separate from SizePath: see BlobGC.MountPath
	// vs SizePath's own doc comment. Coincide only by default; must stay able
	// to diverge.
	GCMountPath string
	GCTimeout   time.Duration
}

// defaultGCPodName is the one-shot blob-GC pod's name. It must never be able to
// equal the registry's own pod name; see buildRegistryPrune and
// registryprune.BlobGC.validate, which both refuse the collision. The registry
// pod flag deliberately defaults to EMPTY rather than to a plausible name, so
// the two cannot coincide by default however this constant is edited.
const defaultGCPodName = "registry-blob-gc"

// registryPruneFlags declares the -registry-prune-* flags on fs.
func registryPruneFlags(fs *flag.FlagSet) *registryPruneConfig {
	c := &registryPruneConfig{}
	fs.BoolVar(&c.Enabled, "registry-prune-enabled", false,
		"enable the scheduled, in-use-aware container-registry garbage collector; requires -registry-address. Off means the scheduler never starts and the job kind is not registered")
	fs.DurationVar(&c.Interval, "registry-prune-interval", 24*time.Hour,
		"cadence of scheduled registry prune runs (only when -registry-prune-enabled); <=0 disables the scheduler")
	fs.BoolVar(&c.DryRun, "registry-prune-dry-run", true,
		"classify and report without deleting anything. DEFAULTS TO TRUE: enabling the scheduler must never, on its own, delete a manifest — a wrongly deleted manifest is unrecoverable and takes an instance's rebuildability with it (#64). Set false only after reading a dry run's job steps")
	fs.IntVar(&c.MaxDeletesPerRepo, "registry-prune-max-deletes-per-repo", 100,
		"per-repo tripwire: a repository with more delete candidates than this is skipped and recorded, while every other repository still proceeds. Must be positive")
	fs.DurationVar(&c.MaxSnapshotAge, "registry-prune-max-snapshot-age", 10*time.Minute,
		"how stale a host's inventory snapshot may be before a prune run aborts; must be positive. Keep it comfortably above -inventory-refresh-interval")
	fs.StringVar(&c.ExtraHosts, "registry-prune-extra-hosts", "",
		"comma-separated ADDITIONAL spellings of the registry as it may appear in an image reference (e.g. an IP as well as a DNS name). The address from -registry-address is always included; a spelling missing from this list is treated as a foreign registry")

	fs.StringVar(&c.Host, "registry-prune-host", "",
		"host ID (from hosts/*.yaml) carrying the registry pod; required for blob garbage collection")
	fs.StringVar(&c.RegistryPod, "registry-prune-registry-pod", "",
		"podman pod name of the registry instance. EMPTY (the default) disables blob garbage collection entirely: manifests are unlinked and the blobs are left recoverable")
	fs.StringVar(&c.RegistryContainer, "registry-prune-registry-container", "",
		"registry container name, used only to measure storage size (du) before and after GC; empty disables sizing and the bytes-reclaimed metric")
	fs.StringVar(&c.GCPod, "registry-prune-gc-pod", defaultGCPodName,
		"name of the one-shot garbage-collect pod. It MUST NOT be -registry-prune-registry-pod: that pod is played with replace=true and force-removed afterwards, so pointing it at the registry would destroy the registry instance")
	fs.StringVar(&c.StoragePath, "registry-prune-storage-path", "",
		"absolute path of the registry storage directory ON THE HOST, hostPath-mounted into the GC pod; required when -registry-prune-registry-pod is set")
	fs.StringVar(&c.SizePath, "registry-prune-size-path", "",
		"the storage directory as seen INSIDE the registry container (its rootdirectory), used only for du; empty uses /var/lib/registry")

	fs.StringVar(&c.GCImage, "registry-prune-gc-image", "",
		"container image for the one-shot blob-GC pod (e.g. to pin by digest); empty uses registry:2")
	fs.StringVar(&c.GCConfigPath, "registry-prune-gc-config-path", "",
		"path to the registry config.yml as seen INSIDE the GC pod's container; empty uses /etc/docker/registry/config.yml. Moves if the GC image's base changes")
	fs.StringVar(&c.GCMountPath, "registry-prune-gc-mount-path", "",
		"where -registry-prune-storage-path is mounted INSIDE the GC pod; empty uses /var/lib/registry. Distinct from -registry-prune-size-path, which is the path inside the REGISTRY container, not the GC pod")
	fs.DurationVar(&c.GCTimeout, "registry-prune-gc-timeout", 0,
		"how long to wait for the one-shot blob-GC pod to finish; must be positive if set. Empty/zero uses BlobGC's own 30m default")
	return c
}

// buildRegistryPrune resolves the flag set into a job handler, or nil when the
// feature is off.
//
// (nil, nil) means "absent": the caller registers no job kind and starts no
// scheduler. An error means the operator asked for the feature and described it
// in a way that could only misbehave — those fail startup rather than being
// discovered a tick later, or worse, not discovered at all.
//
// reg is the CONCRETE *imgregistry.HTTPClient, not the imgregistry.Client
// interface, and that is the whole point of the parameter's type. classifyAll
// builds both the delete plan and the cross-repo protected-digest set from
// ResolveTags; a listing served from imgregistry.CachingClient's 5-minute TTL —
// warmed by anything that browsed the repo, an operator's UI registry page
// included — can omit a protected tag pushed minutes ago. Reason it through:
// digest D is tagged only "feat-x", 31 days old, so it classifies feat-stale and
// is deletable. Someone re-tags D as the release "2026.08.07" and pushes it three
// minutes ago. A cached listing still reports [feat-x], nameProtected never sees
// the CalVer tag, and D enters the plan. The pre-delete backstop cannot rescue it
// either — nothing has deployed the release yet, so it is in no in-use set. The
// manifest behind a just-cut release tag is deleted and the tag left dangling.
//
// This used to be a runtime type assertion against *CachingClient, which a third
// decorator would have walked straight past. As a concrete parameter type the
// compiler refuses any wrapper at all, so the property holds for decorators
// nobody has written yet. The cache buys the prune nothing regardless: it walks
// every repo once per run, on an interval, and pays the ~21s cold cost either way.
//
// What this does NOT close is the hazard itself, only the cache as a source of
// it. A run still lists each repo once and deletes minutes later, so the
// residual window between a repo's listing and its delete pass exists
// regardless of the TTL removed above — ~21s measured for `engine`, the
// fleet's largest repo, is an indication of the cost, not a bound on the
// whole run. A digest classified sha-orphan at listing time and re-tagged as
// a release before the delete pass is still deleted. This is inherited from
// registry-gc.sh, which has
// the same shape, and it is accepted rather than fixed: the blast radius is a
// dangling tag whose manifest can be re-pushed, not unrecoverable data. A real
// fix means re-resolving each candidate immediately before deleting it.
// inventoryInterval is -inventory-refresh-interval, passed through so
// buildRegistryPrune can cross-check it against -registry-prune-max-snapshot-age
// (#229) — see registrySnapshotAgeBudgetError.
func buildRegistryPrune(cfg registryPruneConfig, registryBase string, reg *imgregistry.HTTPClient,
	svc *instance.Service, specs registryprune.SpecSource, pod registryprune.PodRunner,
	metrics registryprune.Metrics, inventoryInterval time.Duration) (*registryprune.Handler, error) {

	if !cfg.Enabled {
		return nil, nil
	}
	// Same rule as the registry browser: no client, no feature. Deliberately a
	// warning rather than a startup error — the flag is a request to enable
	// something that has no registry to act on, which is a misconfiguration
	// worth shouting about but not one worth refusing to boot over.
	if reg == nil {
		log.Printf("WARNING: -registry-prune-enabled is set but no registry client exists (-registry-address is empty); registry prune is disabled")
		return nil, nil
	}
	if cfg.MaxDeletesPerRepo <= 0 {
		return nil, fmt.Errorf("registry prune: -registry-prune-max-deletes-per-repo must be positive (it is the per-repo tripwire), got %d", cfg.MaxDeletesPerRepo)
	}
	if cfg.MaxSnapshotAge <= 0 {
		return nil, fmt.Errorf("registry prune: -registry-prune-max-snapshot-age must be positive, got %s", cfg.MaxSnapshotAge)
	}
	if verr := registrySnapshotAgeBudgetError(cfg.MaxSnapshotAge, inventoryInterval); verr != nil {
		return nil, verr
	}

	// Validated at STARTUP, not on the first run. A malformed spelling can never
	// compare equal to the host component of any of our refs, so BuildInUseSet
	// aborts every run with ErrUnsafeToPrune — fail-closed, but invisible for up
	// to a whole interval unless someone opens the job page.
	hosts := registryPruneHosts(registryBase, cfg.ExtraHosts)
	for _, rh := range hosts {
		if verr := registryprune.ValidateRegistryHost(rh); verr != nil {
			return nil, fmt.Errorf("registry prune: registry host %q (from -registry-address/-registry-prune-extra-hosts): %w", rh, verr)
		}
	}

	h := &registryprune.Handler{
		Registry: reg,
		// The LIVE host view, not a snapshot: BuildInUseSet takes an
		// enumerator precisely so a caller cannot hand it a stale subset, and
		// *instance.Service re-reads the hosts the API itself serves from,
		// including after a SIGHUP reload. A host missing from the sweep
		// silently un-protects every digest pinned only there.
		Hosts:     svc,
		Inventory: svc,
		Specs:     specs,
		Metrics:   metrics,
		Config: registryprune.Config{
			// Derived from the SAME base URL the imgregistry client was
			// constructed with, never re-specified: a spelling that does not
			// compare equal to our own refs classifies every one of them as
			// foreign and un-protects every tag-pinned image fleet-wide.
			// registryprune normalises the "http://host:port" shape itself.
			RegistryHosts:  hosts,
			MaxSnapshotAge: cfg.MaxSnapshotAge,
		},
	}

	if registryPruneBlobGCEnabled(cfg) {
		if pod == nil {
			return nil, fmt.Errorf("registry prune: blob GC needs a podman client")
		}
		if cfg.Host == "" {
			return nil, fmt.Errorf("registry prune: -registry-prune-registry-pod is set but -registry-prune-host is empty; blob GC cannot name the host to stop the registry on")
		}
		if !strings.HasPrefix(cfg.StoragePath, "/") {
			return nil, fmt.Errorf("registry prune: -registry-prune-storage-path %q must be an absolute host path when -registry-prune-registry-pod is set", cfg.StoragePath)
		}
		if cfg.GCTimeout < 0 {
			return nil, fmt.Errorf("registry prune: -registry-prune-gc-timeout must be positive (zero uses the 30m default), got %s", cfg.GCTimeout)
		}
		gcPod := strings.TrimSpace(cfg.GCPod)
		if gcPod == "" {
			gcPod = defaultGCPodName
		}
		// The whole-registry-destroyed guard, checked at STARTUP as well as in
		// BlobGC.validate. The GC pod is played with replace=true and
		// force-removed afterwards; if the two names coincide the registry
		// instance is destroyed, not stopped, and nothing in the core rebuilds
		// it. Refusing to boot is the cheap end of that trade.
		//
		// EqualFold + TrimSpace, not ==: podman pod names are matched
		// case-insensitively enough in practice that " Registry-Main" reaching
		// this guard as "not equal" would be a reasoning step nobody should
		// have to take, and the trims are free.
		regPod := strings.TrimSpace(cfg.RegistryPod)
		if strings.EqualFold(gcPod, regPod) {
			return nil, fmt.Errorf("registry prune: -registry-prune-gc-pod %q is the registry's own pod name %q: playing it would replace and then destroy the registry instance", gcPod, regPod)
		}
		h.BlobGC = &registryprune.BlobGC{
			Podman:            pod,
			HostID:            cfg.Host,
			RegistryPod:       regPod,
			RegistryContainer: cfg.RegistryContainer,
			StoragePath:       cfg.StoragePath,
			SizePath:          strings.TrimSpace(cfg.SizePath),
			PodName:           gcPod,
			Image:             strings.TrimSpace(cfg.GCImage),
			ConfigPath:        strings.TrimSpace(cfg.GCConfigPath),
			MountPath:         strings.TrimSpace(cfg.GCMountPath),
			Timeout:           cfg.GCTimeout,
		}
	}
	return h, nil
}

// registrySnapshotAgeBudgetError fails startup when a prune run can never
// complete: BuildInUseSet aborts with ErrUnsafeToPrune whenever a host's
// inventory snapshot is older than -registry-prune-max-snapshot-age, and
// between two poller ticks a snapshot's age climbs from ~0 up to
// -inventory-refresh-interval. Set MaxSnapshotAge at or below that interval
// and every run aborts by construction — the feature reports itself enabled
// and never once completes, which is exactly the silent-non-execution shape
// #219 exists to eliminate.
//
// Mirrors statsBudgetWarning's cross-check pattern (server.go, #212) but
// fails the boot rather than only warning: unlike a stretched poll cadence
// (degradation), an unusable prune schedule is a feature that is silently
// entirely absent.
//
// inventoryInterval <= 0 means the poller is disabled; this check is then
// silent, because it has nothing meaningful to compare MaxSnapshotAge
// against — the poller's own cadence does not exist.
func registrySnapshotAgeBudgetError(maxSnapshotAge, inventoryInterval time.Duration) error {
	if inventoryInterval <= 0 {
		return nil
	}
	if maxSnapshotAge > inventoryInterval {
		return nil
	}
	return fmt.Errorf("registry prune: -registry-prune-max-snapshot-age (%s) is not greater than "+
		"-inventory-refresh-interval (%s): a host's snapshot age reaches the interval between "+
		"refreshes, so every prune run would abort as unsafe (stale snapshot). Raise "+
		"-registry-prune-max-snapshot-age above -inventory-refresh-interval, or lower the interval",
		maxSnapshotAge, inventoryInterval)
}

// registryPruneBlobGCEnabled reports whether Stage B is configured. Defined
// once so buildRegistryPrune (which builds the BlobGC) and registryPrunePayload
// (which sets SkipBlobGC) can never disagree about whether Stage B exists.
func registryPruneBlobGCEnabled(cfg registryPruneConfig) bool {
	return strings.TrimSpace(cfg.RegistryPod) != ""
}

// buildRegistryPruneScheduler wires the ticker. Separate from the handler so
// the interval's pass-through is testable: an Interval that silently arrives as
// zero makes Scheduler.tick return immediately, and the feature logs itself as
// enabled while never running.
func buildRegistryPruneScheduler(cfg registryPruneConfig, db store.JobStore) *registryprune.Scheduler {
	return &registryprune.Scheduler{
		Store:    db,
		Interval: cfg.Interval,
		Now:      time.Now,
		Payload:  func() registryprune.Payload { return registryPrunePayload(cfg) },
	}
}

// registryPruneHosts is every spelling of our registry, leading with the base
// URL the imgregistry client itself was built from.
func registryPruneHosts(base, extra string) []string {
	out := []string{base}
	for _, e := range strings.Split(extra, ",") {
		if e = strings.TrimSpace(e); e != "" {
			out = append(out, e)
		}
	}
	return out
}

// registryPrunePayload is the payload the scheduler enqueues, evaluated once
// per enqueue.
//
// Note that the flag set is NOT re-read on SIGHUP — only hosts/*.yaml and the
// operator file are reloaded — so re-evaluating per enqueue buys nothing today
// beyond keeping the policy in one place. Changing -registry-prune-dry-run
// requires a restart.
func registryPrunePayload(cfg registryPruneConfig) registryprune.Payload {
	pol := registryprune.DefaultPolicy()
	pol.MaxDeletesPerRepo = cfg.MaxDeletesPerRepo
	return registryprune.Payload{
		Policy: pol,
		DryRun: cfg.DryRun,
		// SkipBlobGC is not a separate flag: leaving -registry-prune-registry-pod
		// empty already leaves Handler.BlobGC nil, which is the same outcome
		// with one fewer way to be half-configured.
		SkipBlobGC: !registryPruneBlobGCEnabled(cfg),
	}
}

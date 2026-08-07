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
	return c
}

// buildRegistryPrune resolves the flag set into a job handler, or nil when the
// feature is off.
//
// (nil, nil) means "absent": the caller registers no job kind and starts no
// scheduler. An error means the operator asked for the feature and described it
// in a way that could only misbehave — those fail startup rather than being
// discovered a tick later, or worse, not discovered at all.
func buildRegistryPrune(cfg registryPruneConfig, registryBase string, reg imgregistry.Client,
	svc *instance.Service, db store.DB, pod registryprune.PodRunner,
	metrics registryprune.Metrics) (*registryprune.Handler, error) {

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

	h := &registryprune.Handler{
		Registry: reg,
		// The LIVE host view, not a snapshot: BuildInUseSet takes an
		// enumerator precisely so a caller cannot hand it a stale subset, and
		// *instance.Service re-reads the hosts the API itself serves from,
		// including after a SIGHUP reload. A host missing from the sweep
		// silently un-protects every digest pinned only there.
		Hosts:     svc,
		Inventory: svc,
		Specs:     db,
		Metrics:   metrics,
		Config: registryprune.Config{
			// Derived from the SAME base URL the imgregistry client was
			// constructed with, never re-specified: a spelling that does not
			// compare equal to our own refs classifies every one of them as
			// foreign and un-protects every tag-pinned image fleet-wide.
			// registryprune normalises the "http://host:port" shape itself.
			RegistryHosts:  registryPruneHosts(registryBase, cfg.ExtraHosts),
			MaxSnapshotAge: cfg.MaxSnapshotAge,
		},
	}

	if cfg.RegistryPod != "" {
		if pod == nil {
			return nil, fmt.Errorf("registry prune: blob GC needs a podman client")
		}
		if cfg.Host == "" {
			return nil, fmt.Errorf("registry prune: -registry-prune-registry-pod is set but -registry-prune-host is empty; blob GC cannot name the host to stop the registry on")
		}
		if !strings.HasPrefix(cfg.StoragePath, "/") {
			return nil, fmt.Errorf("registry prune: -registry-prune-storage-path %q must be an absolute host path when -registry-prune-registry-pod is set", cfg.StoragePath)
		}
		gcPod := cfg.GCPod
		if gcPod == "" {
			gcPod = defaultGCPodName
		}
		// The whole-registry-destroyed guard, checked at STARTUP as well as in
		// BlobGC.validate. The GC pod is played with replace=true and
		// force-removed afterwards; if the two names coincide the registry
		// instance is destroyed, not stopped, and nothing in the core rebuilds
		// it. Refusing to boot is the cheap end of that trade.
		if gcPod == cfg.RegistryPod {
			return nil, fmt.Errorf("registry prune: -registry-prune-gc-pod %q is the registry's own pod name: playing it would replace and then destroy the registry instance", gcPod)
		}
		h.BlobGC = &registryprune.BlobGC{
			Podman:            pod,
			HostID:            cfg.Host,
			RegistryPod:       cfg.RegistryPod,
			RegistryContainer: cfg.RegistryContainer,
			StoragePath:       cfg.StoragePath,
			SizePath:          cfg.SizePath,
			PodName:           gcPod,
		}
	}
	return h, nil
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

// registryPrunePayload is the payload the scheduler enqueues. Evaluated per
// enqueue so a SIGHUP-reloaded policy would be picked up by the next run.
func registryPrunePayload(cfg registryPruneConfig) registryprune.Payload {
	pol := registryprune.DefaultPolicy()
	pol.MaxDeletesPerRepo = cfg.MaxDeletesPerRepo
	return registryprune.Payload{
		Policy: pol,
		DryRun: cfg.DryRun,
		// SkipBlobGC is not a separate flag: leaving -registry-prune-registry-pod
		// empty already leaves Handler.BlobGC nil, which is the same outcome
		// with one fewer way to be half-configured.
		SkipBlobGC: cfg.RegistryPod == "",
	}
}

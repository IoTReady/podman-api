package server

import (
	"testing"
	"time"

	"github.com/iotready/podman-api/internal/obs"
	"github.com/iotready/podman-api/internal/podman/fake"
	"github.com/iotready/podman-api/internal/registryprune"
	"github.com/iotready/podman-api/internal/store"
	"github.com/prometheus/client_golang/prometheus"
)

// This file exists because of a whole class of bug the guard-condition tests
// cannot see: a flag that parses correctly, logs correctly, and is then never
// copied into the struct that consumes it. Every case below was verified by
// deleting the field assignment in server/registryprune.go and watching THIS
// test fail — the suite as it stood survived all of them.
//
// The DryRun row is the one that matters most: dropping `DryRun: cfg.DryRun`
// turns -registry-prune-dry-run=true (the default) into a real, irreversible
// delete run, and nothing else in the process would say so.

// fullyConfigured is every flag set to a NON-DEFAULT value, so a field that is
// silently dropped falls back to something this test can tell apart from what
// was asked for.
func fullyConfigured(t *testing.T) *registryPruneConfig {
	t.Helper()
	return parsedRegistryPruneFlags(t,
		"-registry-prune-enabled",
		"-registry-prune-interval=6h",
		"-registry-prune-dry-run=false",
		"-registry-prune-max-deletes-per-repo=37",
		"-registry-prune-max-snapshot-age=90s",
		"-registry-prune-extra-hosts=100.64.0.23:5000, reg.alias:5000",
		"-registry-prune-host=otp-infra-1",
		"-registry-prune-registry-pod=registry-main",
		"-registry-prune-registry-container=registry-main-registry",
		"-registry-prune-gc-pod=blob-gc-one-shot",
		"-registry-prune-storage-path=/srv/registry",
		"-registry-prune-size-path=/var/lib/registry/docker",
		"-registry-prune-gc-image=registry@sha256:deadbeef",
		"-registry-prune-gc-config-path=/etc/registry/gc-config.yml",
		"-registry-prune-gc-mount-path=/mnt/registry-storage",
		"-registry-prune-gc-timeout=45m",
	)
}

func TestRegistryPrunePayload_EveryFieldIsCarried(t *testing.T) {
	cfg := fullyConfigured(t)

	t.Run("dry run", func(t *testing.T) {
		// Both directions, because the failure that matters is the default
		// (true) silently arriving as false and deleting for real.
		if got := registryPrunePayload(*cfg); got.DryRun {
			t.Fatal("DryRun = true, want false: -registry-prune-dry-run=false was set")
		}
		on := parsedRegistryPruneFlags(t, "-registry-prune-enabled")
		if got := registryPrunePayload(*on); !got.DryRun {
			t.Fatal("DryRun = false with the flag left at its default: an enabled server would DELETE FOR REAL")
		}
	})

	t.Run("tripwire", func(t *testing.T) {
		if got := registryPrunePayload(*cfg).Policy.MaxDeletesPerRepo; got != 37 {
			t.Fatalf("MaxDeletesPerRepo = %d, want 37 (a drop falls back to DefaultPolicy's 100 and the flag does nothing)", got)
		}
	})

	t.Run("policy is otherwise the default ruleset", func(t *testing.T) {
		p := registryPrunePayload(*cfg).Policy
		if len(p.ProtectedExact) == 0 || p.CalVer == nil {
			t.Fatal("policy lost its protected-name rules; validatePolicy would abort every run")
		}
		if p.DeleteUnrecognised {
			t.Fatal("DeleteUnrecognised must stay false (#66)")
		}
	})

	t.Run("skip blob gc follows the registry pod", func(t *testing.T) {
		if registryPrunePayload(*cfg).SkipBlobGC {
			t.Fatal("SkipBlobGC = true with a registry pod configured")
		}
		off := parsedRegistryPruneFlags(t, "-registry-prune-enabled")
		if !registryPrunePayload(*off).SkipBlobGC {
			t.Fatal("SkipBlobGC = false with no registry pod: Stage B would be attempted unconfigured")
		}
	})
}

func TestBuildRegistryPruneScheduler_CarriesTheInterval(t *testing.T) {
	s := buildRegistryPruneScheduler(*fullyConfigured(t), store.NewMemory())
	if s.Interval != 6*time.Hour {
		t.Fatalf("Interval = %s, want 6h: at zero, tick() returns immediately and the feature never runs while logging itself enabled", s.Interval)
	}
	if s.Store == nil {
		t.Fatal("Store is nil: nothing can be enqueued")
	}
	if s.Payload == nil {
		t.Fatal("Payload is nil: tick() returns immediately")
	}
	if s.Now == nil {
		t.Fatal("Now is nil")
	}
	if s.Payload().Policy.MaxDeletesPerRepo != 37 {
		t.Fatal("the scheduler's payload is not the configured one")
	}
}

func TestBuildRegistryPrune_EveryFieldIsCarried(t *testing.T) {
	cfg := *fullyConfigured(t)
	svc := testSvc()
	db := store.NewMemory()
	pod := fake.New()
	metrics := obs.NewRegistryPruneMetrics(prometheus.NewRegistry())

	h, err := buildRegistryPrune(cfg, "http://reg.example:5000", testRegistryClient(), svc, db, pod, metrics, defaultTestInventoryInterval)
	if err != nil {
		t.Fatal(err)
	}

	if h.Registry == nil {
		t.Fatal("Registry is nil")
	}
	// Identity, not just non-nil: BuildInUseSet takes an enumerator so a caller
	// cannot hand it a stale host subset, and this is the assertion that the
	// live service — the same view GET /hosts serves — is what it got.
	if h.Hosts != registryprune.HostEnumerator(svc) {
		t.Error("Hosts is not the live *instance.Service")
	}
	if h.Inventory != registryprune.InventorySource(svc) {
		t.Error("Inventory is not the live *instance.Service")
	}
	if h.Specs != registryprune.SpecSource(db) {
		t.Error("Specs is not the spec store")
	}
	// A dropped Metrics leaves noopMetrics: every registry_prune series stays
	// flat forever, and #220's alerting is built on them.
	if h.Metrics != registryprune.Metrics(metrics) {
		t.Error("Metrics is not the collector it was given")
	}
	if h.Config.MaxSnapshotAge != 90*time.Second {
		t.Errorf("MaxSnapshotAge = %s, want 90s (a zero value aborts every run)", h.Config.MaxSnapshotAge)
	}
	wantHosts := []string{"http://reg.example:5000", "100.64.0.23:5000", "reg.alias:5000"}
	if len(h.Config.RegistryHosts) != len(wantHosts) {
		t.Fatalf("RegistryHosts = %v, want %v", h.Config.RegistryHosts, wantHosts)
	}
	for i, want := range wantHosts {
		if h.Config.RegistryHosts[i] != want {
			t.Errorf("RegistryHosts[%d] = %q, want %q", i, h.Config.RegistryHosts[i], want)
		}
	}

	g := h.BlobGC
	if g == nil {
		t.Fatal("BlobGC is nil despite a fully configured Stage B")
	}
	for _, c := range []struct{ name, got, want string }{
		{"HostID", g.HostID, "otp-infra-1"},
		{"RegistryPod", g.RegistryPod, "registry-main"},
		{"RegistryContainer", g.RegistryContainer, "registry-main-registry"},
		{"StoragePath", g.StoragePath, "/srv/registry"},
		// SizePath is measured inside the REGISTRY container and MountPath
		// inside the GC pod; a dropped SizePath silently falls back to
		// /var/lib/registry and sizes a path that may not exist there.
		{"SizePath", g.SizePath, "/var/lib/registry/docker"},
		{"PodName", g.PodName, "blob-gc-one-shot"},
		// #227: Image, ConfigPath and MountPath were previously hardcoded
		// defaults with no flag reaching them. A registry image change (a
		// different base, or the registry:3 line) can move ConfigPath, and
		// pinning Image by digest is exactly what an operator wants after a
		// supply-chain scare.
		{"Image", g.Image, "registry@sha256:deadbeef"},
		{"ConfigPath", g.ConfigPath, "/etc/registry/gc-config.yml"},
		{"MountPath", g.MountPath, "/mnt/registry-storage"},
	} {
		if c.got != c.want {
			t.Errorf("BlobGC.%s = %q, want %q", c.name, c.got, c.want)
		}
	}
	if g.Timeout != 45*time.Minute {
		t.Errorf("BlobGC.Timeout = %s, want 45m", g.Timeout)
	}
	if g.Podman == nil {
		t.Error("BlobGC.Podman is nil: the stage cannot stop the registry")
	}
}

// There is deliberately no "refuses a CachingClient" test any more: since
// buildRegistryPrune takes the concrete *imgregistry.HTTPClient, such a test
// cannot COMPILE. The property moved from a runtime assertion this suite had to
// police to one the compiler enforces for every decorator, including ones nobody
// has written yet.

func TestBuildRegistryPrune_RejectsAMalformedRegistryHost(t *testing.T) {
	// Startup, not the first run. Fail-closed either way, but a run that aborts
	// with ErrUnsafeToPrune is invisible for a whole interval unless someone
	// opens the job page.
	if _, err := buildRegistryPrune(*enabledConfig(t), "http://reg.example:5000/v2/", testRegistryClient(), testSvc(), nil, nil, nil, defaultTestInventoryInterval); err == nil {
		t.Fatal("want an error for a registry base carrying a path")
	}
	c := *parsedRegistryPruneFlags(t, "-registry-prune-enabled", "-registry-prune-extra-hosts=not a host")
	if _, err := buildRegistryPrune(c, "http://reg.example:5000", testRegistryClient(), testSvc(), nil, nil, nil, defaultTestInventoryInterval); err == nil {
		t.Fatal("want an error for a malformed extra host")
	}
}

package server

import (
	"flag"
	"testing"
	"time"

	"github.com/iotready/podman-api/internal/imgregistry"
	"github.com/iotready/podman-api/internal/instance"
	"github.com/iotready/podman-api/internal/podman/fake"
	"github.com/iotready/podman-api/internal/registryprune"
)

// defaultTestInventoryInterval is the fixture value passed as
// buildRegistryPrune's inventoryInterval parameter everywhere the interaction
// with -registry-prune-max-snapshot-age is not itself under test. It matches
// -inventory-refresh-interval's own default (server.go), and every fixture
// config here leaves -registry-prune-max-snapshot-age comfortably above it.
const defaultTestInventoryInterval = 30 * time.Second

func parsedRegistryPruneFlags(t *testing.T, args ...string) *registryPruneConfig {
	t.Helper()
	fs := flag.NewFlagSet("test", flag.ContinueOnError)
	c := registryPruneFlags(fs)
	if err := fs.Parse(args); err != nil {
		t.Fatal(err)
	}
	return c
}

// The feature must be entirely absent unless an operator asks for it, and even
// then must not delete anything until a second, separate opt-in.
func TestRegistryPruneFlags_DefaultsAreOff(t *testing.T) {
	c := parsedRegistryPruneFlags(t)
	if c.Enabled {
		t.Error("-registry-prune-enabled must default to false")
	}
	if !c.DryRun {
		t.Error("-registry-prune-dry-run must default to true: enabling the scheduler must not, on its own, delete a manifest")
	}
	if c.RegistryPod != "" {
		t.Errorf("-registry-prune-registry-pod must default to empty (blob GC off), got %q", c.RegistryPod)
	}
	if c.StoragePath != "" {
		t.Errorf("-registry-prune-storage-path must default to empty, got %q", c.StoragePath)
	}
	if c.MaxDeletesPerRepo <= 0 {
		t.Errorf("-registry-prune-max-deletes-per-repo must default positive (the tripwire), got %d", c.MaxDeletesPerRepo)
	}
	// #227: all four default to empty/zero so BlobGC's OWN defaults (registry:2,
	// /etc/docker/registry/config.yml, /var/lib/registry, 30m) apply unless an
	// operator overrides them.
	if c.GCImage != "" {
		t.Errorf("-registry-prune-gc-image must default to empty, got %q", c.GCImage)
	}
	if c.GCConfigPath != "" {
		t.Errorf("-registry-prune-gc-config-path must default to empty, got %q", c.GCConfigPath)
	}
	if c.GCMountPath != "" {
		t.Errorf("-registry-prune-gc-mount-path must default to empty, got %q", c.GCMountPath)
	}
	if c.GCTimeout != 0 {
		t.Errorf("-registry-prune-gc-timeout must default to zero, got %s", c.GCTimeout)
	}
}

// The whole-registry-destroyed guard, at the flag layer. If the two names
// coincide the sequence is: stop the registry, PlayKube(replace=true) over its
// own pod name, then PodRemove(force=true) — the registry instance is gone, not
// down, and nothing in the core rebuilds it.
func TestRegistryPruneFlags_GCPodDefaultIsNotRegistryPodDefault(t *testing.T) {
	c := parsedRegistryPruneFlags(t)
	if c.GCPod == c.RegistryPod {
		t.Fatalf("GC pod default %q equals registry pod default %q: playing it would replace and then destroy the registry", c.GCPod, c.RegistryPod)
	}
}

func testRegistryClient() *imgregistry.HTTPClient {
	return imgregistry.NewHTTPClient("http://reg.example:5000", imgregistry.Auth{})
}

func testSvc() *instance.Service { return instance.NewService(fake.New(), nil) }

func enabledConfig(t *testing.T) *registryPruneConfig {
	t.Helper()
	return parsedRegistryPruneFlags(t, "-registry-prune-enabled")
}

func TestBuildRegistryPrune_DisabledByDefault(t *testing.T) {
	h, err := buildRegistryPrune(*parsedRegistryPruneFlags(t), "http://reg.example:5000",
		testRegistryClient(), testSvc(), nil, nil, nil, defaultTestInventoryInterval)
	if err != nil {
		t.Fatal(err)
	}
	if h != nil {
		t.Fatal("registry prune must be absent unless -registry-prune-enabled is set")
	}
}

// A nil registry client disables the feature outright, exactly like the
// registry browser: there is nothing to prune and nothing to talk to.
func TestBuildRegistryPrune_NilRegistryClientDisables(t *testing.T) {
	h, err := buildRegistryPrune(*enabledConfig(t), "", nil, testSvc(), nil, nil, nil, defaultTestInventoryInterval)
	if err != nil {
		t.Fatal(err)
	}
	if h != nil {
		t.Fatal("a nil registry client must fully disable registry prune")
	}
}

func TestBuildRegistryPrune_EnabledBuildsAHandler(t *testing.T) {
	h, err := buildRegistryPrune(*enabledConfig(t), "http://reg.example:5000",
		testRegistryClient(), testSvc(), nil, nil, nil, defaultTestInventoryInterval)
	if err != nil {
		t.Fatal(err)
	}
	if h == nil {
		t.Fatal("want a handler when enabled with a registry client")
	}
	if h.BlobGC != nil {
		t.Error("blob GC must stay off until a registry pod name is configured")
	}
}

// The ref-matching set must come from the SAME base URL the client talks to. A
// mismatch classifies every one of our own refs as foreign and silently
// un-protects every tag-pinned image in the fleet.
func TestBuildRegistryPrune_RegistryHostsDeriveFromTheClientBase(t *testing.T) {
	const base = "http://reg.example:5000"
	h, err := buildRegistryPrune(*enabledConfig(t), base, testRegistryClient(), testSvc(), nil, nil, nil, defaultTestInventoryInterval)
	if err != nil {
		t.Fatal(err)
	}
	if len(h.Config.RegistryHosts) == 0 || h.Config.RegistryHosts[0] != base {
		t.Fatalf("RegistryHosts = %v, want it to lead with the client's own base %q", h.Config.RegistryHosts, base)
	}
	if h.Config.MaxSnapshotAge <= 0 {
		t.Error("MaxSnapshotAge must be positive, or every run aborts")
	}
}

func TestBuildRegistryPrune_RejectsGCPodEqualToRegistryPod(t *testing.T) {
	c := *parsedRegistryPruneFlags(t,
		"-registry-prune-enabled",
		"-registry-prune-host=otp-infra-1",
		"-registry-prune-registry-pod=registry-main",
		"-registry-prune-gc-pod=registry-main",
		"-registry-prune-storage-path=/srv/registry",
	)
	if _, err := buildRegistryPrune(c, "http://reg.example:5000", testRegistryClient(), testSvc(), nil, fake.New(), nil, defaultTestInventoryInterval); err == nil {
		t.Fatal("want an error when the GC pod name is the registry's own pod name")
	}
}

// The strengthened guard, pinned. All three mutations (EqualFold back to ==,
// and dropping either TrimSpace) previously survived the whole suite, so the
// hardening could silently revert. Each row below fails under exactly one of
// them.
func TestBuildRegistryPrune_CollisionGuardIgnoresCaseAndSurroundingSpace(t *testing.T) {
	for _, tc := range []struct{ name, registryPod, gcPod string }{
		{"differing case", "Registry-Main", "registry-main"},
		{"differing case, other way", "registry-main", "REGISTRY-MAIN"},
		{"leading space on the GC pod", "registry-main", "  registry-main"},
		{"trailing space on the registry pod", "registry-main  ", "registry-main"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := *parsedRegistryPruneFlags(t,
				"-registry-prune-enabled",
				"-registry-prune-host=otp-infra-1",
				"-registry-prune-registry-pod="+tc.registryPod,
				"-registry-prune-gc-pod="+tc.gcPod,
				"-registry-prune-storage-path=/srv/registry",
			)
			if _, err := buildRegistryPrune(c, "http://reg.example:5000", testRegistryClient(), testSvc(), nil, fake.New(), nil, defaultTestInventoryInterval); err == nil {
				t.Fatalf("accepted GC pod %q against registry pod %q: playing it would replace and then destroy the registry", tc.gcPod, tc.registryPod)
			}
		})
	}
}

// The trimmed names are what reach BlobGC, so the package's own backstop
// compares the same strings this guard did.
func TestBuildRegistryPrune_TrimsPodNamesBeforeTheyReachBlobGC(t *testing.T) {
	c := *parsedRegistryPruneFlags(t,
		"-registry-prune-enabled",
		"-registry-prune-host=otp-infra-1",
		"-registry-prune-registry-pod=  registry-main  ",
		"-registry-prune-gc-pod=  blob-gc-one-shot  ",
		"-registry-prune-storage-path=/srv/registry",
	)
	h, err := buildRegistryPrune(c, "http://reg.example:5000", testRegistryClient(), testSvc(), nil, fake.New(), nil, defaultTestInventoryInterval)
	if err != nil {
		t.Fatal(err)
	}
	if h.BlobGC.RegistryPod != "registry-main" {
		t.Errorf("RegistryPod = %q, want it trimmed", h.BlobGC.RegistryPod)
	}
	if h.BlobGC.PodName != "blob-gc-one-shot" {
		t.Errorf("PodName = %q, want it trimmed", h.BlobGC.PodName)
	}
}

// A whitespace-only -registry-prune-gc-image/-gc-config-path/-gc-mount-path
// is not the empty string BlobGC.image()/configPath()/mountPath() check for a
// default fallback, so it would otherwise be used verbatim and fail opaquely
// deep inside the GC pod rather than defaulting the way an actually-empty
// flag does. Trim here, matching GCPod two lines above in buildRegistryPrune.
func TestBuildRegistryPrune_TrimsGCImageConfigAndMountPath(t *testing.T) {
	c := *parsedRegistryPruneFlags(t,
		"-registry-prune-enabled",
		"-registry-prune-host=otp-infra-1",
		"-registry-prune-registry-pod=registry-main",
		"-registry-prune-storage-path=/srv/registry",
		"-registry-prune-gc-image=   ",
		"-registry-prune-gc-config-path=   ",
		"-registry-prune-gc-mount-path=   ",
		"-registry-prune-size-path=   ",
	)
	h, err := buildRegistryPrune(c, "http://reg.example:5000", testRegistryClient(), testSvc(), nil, fake.New(), nil, defaultTestInventoryInterval)
	if err != nil {
		t.Fatal(err)
	}
	if h.BlobGC.Image != "" {
		t.Errorf("Image = %q, want it trimmed to empty so BlobGC.image() falls back to its own default", h.BlobGC.Image)
	}
	if h.BlobGC.ConfigPath != "" {
		t.Errorf("ConfigPath = %q, want it trimmed to empty so BlobGC.configPath() falls back to its own default", h.BlobGC.ConfigPath)
	}
	if h.BlobGC.MountPath != "" {
		t.Errorf("MountPath = %q, want it trimmed to empty so BlobGC.mountPath() falls back to its own default", h.BlobGC.MountPath)
	}
	if h.BlobGC.SizePath != "" {
		t.Errorf("SizePath = %q, want it trimmed to empty so BlobGC.sizePath() falls back to its own default", h.BlobGC.SizePath)
	}
}

func TestBuildRegistryPrune_RegistryPodWithoutStoragePathIsRejected(t *testing.T) {
	c := *parsedRegistryPruneFlags(t,
		"-registry-prune-enabled",
		"-registry-prune-host=otp-infra-1",
		"-registry-prune-registry-pod=registry-main",
	)
	if _, err := buildRegistryPrune(c, "http://reg.example:5000", testRegistryClient(), testSvc(), nil, fake.New(), nil, defaultTestInventoryInterval); err == nil {
		t.Fatal("want an error: a blob GC with no storage path can never run")
	}
}

// #227: a negative GC timeout can only misbehave (BlobGC.timeout() treats
// anything <= 0 as "use the 30m default", so a negative value silently means
// something other than what was asked for) and is rejected at startup rather
// than discovered later.
func TestBuildRegistryPrune_RejectsNegativeGCTimeout(t *testing.T) {
	c := *parsedRegistryPruneFlags(t,
		"-registry-prune-enabled",
		"-registry-prune-host=otp-infra-1",
		"-registry-prune-registry-pod=registry-main",
		"-registry-prune-storage-path=/srv/registry",
		"-registry-prune-gc-timeout=-1m",
	)
	if _, err := buildRegistryPrune(c, "http://reg.example:5000", testRegistryClient(), testSvc(), nil, fake.New(), nil, defaultTestInventoryInterval); err == nil {
		t.Fatal("want an error for a negative -registry-prune-gc-timeout")
	}
}

func TestBuildRegistryPrune_BlobGCWiredWhenFullyConfigured(t *testing.T) {
	c := *parsedRegistryPruneFlags(t,
		"-registry-prune-enabled",
		"-registry-prune-host=otp-infra-1",
		"-registry-prune-registry-pod=registry-main",
		"-registry-prune-registry-container=registry-main-registry",
		"-registry-prune-storage-path=/srv/registry",
	)
	h, err := buildRegistryPrune(c, "http://reg.example:5000", testRegistryClient(), testSvc(), nil, fake.New(), nil, defaultTestInventoryInterval)
	if err != nil {
		t.Fatal(err)
	}
	if h.BlobGC == nil {
		t.Fatal("want blob GC wired")
	}
	if h.BlobGC.PodName == h.BlobGC.RegistryPod {
		t.Fatal("GC pod name must never equal the registry pod name")
	}
}

// #229: -registry-prune-max-snapshot-age set at or below
// -inventory-refresh-interval makes every prune run abort on a stale snapshot
// by construction between refreshes, while the feature reports itself as
// enabled and never completes a run. That must fail startup, not run silently
// broken.
func TestBuildRegistryPrune_RejectsMaxSnapshotAgeAtOrBelowInventoryInterval(t *testing.T) {
	for _, tc := range []struct {
		name           string
		maxSnapshotAge string
		interval       time.Duration
	}{
		{"equal", "60s", 60 * time.Second},
		{"below", "30s", 60 * time.Second},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := *parsedRegistryPruneFlags(t,
				"-registry-prune-enabled",
				"-registry-prune-max-snapshot-age="+tc.maxSnapshotAge,
			)
			if _, err := buildRegistryPrune(c, "http://reg.example:5000", testRegistryClient(), testSvc(), nil, nil, nil, tc.interval); err == nil {
				t.Fatalf("want an error: -registry-prune-max-snapshot-age=%s is not above the %s inventory interval, "+
					"so every run would abort with a stale snapshot", tc.maxSnapshotAge, tc.interval)
			}
		})
	}
}

// The inverse: comfortably above the interval must build cleanly.
func TestBuildRegistryPrune_AcceptsMaxSnapshotAgeAboveInventoryInterval(t *testing.T) {
	c := *parsedRegistryPruneFlags(t,
		"-registry-prune-enabled",
		"-registry-prune-max-snapshot-age=10m",
	)
	if _, err := buildRegistryPrune(c, "http://reg.example:5000", testRegistryClient(), testSvc(), nil, nil, nil, 30*time.Second); err != nil {
		t.Fatalf("10m max-snapshot-age against a 30s inventory interval should build cleanly: %v", err)
	}
}

// The inventory poller disabled (interval <= 0) is silent: there is nothing
// for this check to compare MaxSnapshotAge against.
func TestBuildRegistryPrune_SilentWhenInventoryPollerDisabled(t *testing.T) {
	c := *parsedRegistryPruneFlags(t, "-registry-prune-enabled")
	if _, err := buildRegistryPrune(c, "http://reg.example:5000", testRegistryClient(), testSvc(), nil, nil, nil, 0); err != nil {
		t.Fatalf("an inventory interval of 0 (poller disabled) must not trip the cross-check: %v", err)
	}
}

func TestJobRegistry_RegistryPruneAbsentWhenNotWired(t *testing.T) {
	reg, recs := buildJobRegistry(testSvc(), nil, nil, 1, nil, nil, nil)
	if _, ok := reg[registryprune.JobKind]; ok {
		t.Fatal("registry-prune must be absent from the job registry when the feature is off")
	}
	if _, ok := recs[registryprune.JobKind]; ok {
		t.Fatal("registry-prune must have no reconciler")
	}
}

func TestJobRegistry_RegistryPruneRegisteredWhenWired(t *testing.T) {
	h, err := buildRegistryPrune(*enabledConfig(t), "http://reg.example:5000",
		testRegistryClient(), testSvc(), nil, nil, nil, defaultTestInventoryInterval)
	if err != nil {
		t.Fatal(err)
	}
	reg, recs := buildJobRegistry(testSvc(), nil, nil, 1, nil, nil, h)
	if _, ok := reg[registryprune.JobKind]; !ok {
		t.Fatal("registry-prune must be registered when the feature is enabled")
	}
	if _, ok := recs[registryprune.JobKind]; ok {
		t.Fatal("registry-prune must have no reconciler: a failed run is retried by the scheduler")
	}
}

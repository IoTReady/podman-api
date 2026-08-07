package server

import (
	"flag"
	"testing"

	"github.com/iotready/podman-api/internal/imgregistry"
	"github.com/iotready/podman-api/internal/instance"
	"github.com/iotready/podman-api/internal/podman/fake"
	"github.com/iotready/podman-api/internal/registryprune"
)

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

func testRegistryClient() imgregistry.Client {
	return imgregistry.NewHTTPClient("http://reg.example:5000", imgregistry.Auth{})
}

func testSvc() *instance.Service { return instance.NewService(fake.New(), nil) }

func enabledConfig(t *testing.T) *registryPruneConfig {
	t.Helper()
	return parsedRegistryPruneFlags(t, "-registry-prune-enabled")
}

func TestBuildRegistryPrune_DisabledByDefault(t *testing.T) {
	h, err := buildRegistryPrune(*parsedRegistryPruneFlags(t), "http://reg.example:5000",
		testRegistryClient(), testSvc(), nil, nil, nil)
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
	h, err := buildRegistryPrune(*enabledConfig(t), "", nil, testSvc(), nil, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if h != nil {
		t.Fatal("a nil registry client must fully disable registry prune")
	}
}

func TestBuildRegistryPrune_EnabledBuildsAHandler(t *testing.T) {
	h, err := buildRegistryPrune(*enabledConfig(t), "http://reg.example:5000",
		testRegistryClient(), testSvc(), nil, nil, nil)
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
	h, err := buildRegistryPrune(*enabledConfig(t), base, testRegistryClient(), testSvc(), nil, nil, nil)
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
	if _, err := buildRegistryPrune(c, "http://reg.example:5000", testRegistryClient(), testSvc(), nil, fake.New(), nil); err == nil {
		t.Fatal("want an error when the GC pod name is the registry's own pod name")
	}
}

func TestBuildRegistryPrune_RegistryPodWithoutStoragePathIsRejected(t *testing.T) {
	c := *parsedRegistryPruneFlags(t,
		"-registry-prune-enabled",
		"-registry-prune-host=otp-infra-1",
		"-registry-prune-registry-pod=registry-main",
	)
	if _, err := buildRegistryPrune(c, "http://reg.example:5000", testRegistryClient(), testSvc(), nil, fake.New(), nil); err == nil {
		t.Fatal("want an error: a blob GC with no storage path can never run")
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
	h, err := buildRegistryPrune(c, "http://reg.example:5000", testRegistryClient(), testSvc(), nil, fake.New(), nil)
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
		testRegistryClient(), testSvc(), nil, nil, nil)
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

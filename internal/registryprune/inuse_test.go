package registryprune

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/iotready/podman-api/internal/config"
	"github.com/iotready/podman-api/internal/imgregistry"
	"github.com/iotready/podman-api/internal/instance"
	"github.com/iotready/podman-api/internal/store"
)

// --- fakes -----------------------------------------------------------------

type invEntry struct {
	obs   []instance.Observed
	fresh instance.Freshness
	err   error
}

type fakeInv struct{ hosts map[string]invEntry }

func (f *fakeInv) ListAllInstancesWithMeta(_ context.Context, host string) ([]instance.Observed, instance.Freshness, error) {
	e, ok := f.hosts[host]
	if !ok {
		return nil, instance.Freshness{}, fmt.Errorf("no such host %q", host)
	}
	return e.obs, e.fresh, e.err
}

type fakeSpecs struct {
	keys    map[string][]store.SpecKey
	specs   map[string]store.Spec // "host/template/slug"
	listErr error
	getErr  error
}

func (f *fakeSpecs) ListSpecKeys(_ context.Context, host string) ([]store.SpecKey, error) {
	if f.listErr != nil {
		return nil, f.listErr
	}
	return f.keys[host], nil
}

func (f *fakeSpecs) GetSpec(_ context.Context, host, template, slug string) (store.Spec, error) {
	if f.getErr != nil {
		return store.Spec{}, f.getErr
	}
	s, ok := f.specs[host+"/"+template+"/"+slug]
	if !ok {
		return store.Spec{}, store.ErrNotFound
	}
	return s, nil
}

type fakeReg struct {
	catalog    []string
	catalogErr error
	// manifests is keyed "repo:ref".
	manifests map[string]string // -> digest
	manErr    map[string]error
	calls     int
}

func (f *fakeReg) Catalog(context.Context) ([]string, error) {
	if f.catalogErr != nil {
		return nil, f.catalogErr
	}
	return f.catalog, nil
}

func (f *fakeReg) Manifest(_ context.Context, repo, ref string) (imgregistry.Manifest, error) {
	f.calls++
	k := repo + ":" + ref
	if err, ok := f.manErr[k]; ok {
		return imgregistry.Manifest{}, err
	}
	d, ok := f.manifests[k]
	if !ok {
		return imgregistry.Manifest{}, fmt.Errorf("manifest %s: %w", k, imgregistry.ErrNotFound)
	}
	return imgregistry.Manifest{Digest: d}, nil
}

// --- helpers ---------------------------------------------------------------

const (
	digA = "sha256:aaaa000000000000000000000000000000000000000000000000000000000001"
	digB = "sha256:bbbb000000000000000000000000000000000000000000000000000000000002"
	digC = "sha256:cccc000000000000000000000000000000000000000000000000000000000003"
)

var fixedNow = time.Date(2026, 8, 7, 12, 0, 0, 0, time.UTC)

// fakeHosts is a HostEnumerator. Production passes *instance.Service so the
// list can never be a stale subset.
type fakeHosts struct{ ids []string }

func (f fakeHosts) Hosts() []config.Host {
	out := make([]config.Host, 0, len(f.ids))
	for _, id := range f.ids {
		out = append(out, config.Host{ID: id})
	}
	return out
}

func hostsOf(ids ...string) fakeHosts { return fakeHosts{ids: ids} }

func freshOK() instance.Freshness {
	return instance.Freshness{FetchedAt: fixedNow.Add(-time.Minute), Reachable: true, HasData: true}
}

const testRegistryHost = "reg.example:5000"

func baseCfg() Config {
	return Config{
		RegistryHost:   testRegistryHost,
		MaxSnapshotAge: 10 * time.Minute,
		Now:            func() time.Time { return fixedNow },
	}
}

func obs(tmpl, slug string, images ...string) instance.Observed {
	o := instance.Observed{Template: tmpl, Slug: slug}
	for i, img := range images {
		o.Containers = append(o.Containers, instance.ObservedContainer{
			Name:  fmt.Sprintf("c%d", i),
			Image: img,
		})
	}
	return o
}

func mustAbort(t *testing.T, set InUseSet, err error) {
	t.Helper()
	if !errors.Is(err, ErrUnsafeToPrune) {
		t.Fatalf("want ErrUnsafeToPrune, got %v", err)
	}
	if len(set.Digests) != 0 {
		t.Fatalf("aborting run must return an empty set, got %d digests: %v", len(set.Digests), set.Digests)
	}
	if len(set.NonDigestObserved) != 0 {
		t.Fatalf("aborting run must return no observations, got %v", set.NonDigestObserved)
	}
}

func hasDigest(t *testing.T, set InUseSet, d string) {
	t.Helper()
	if _, ok := set.Digests[d]; !ok {
		t.Fatalf("digest %s missing from in-use set %v", d, set.Digests)
	}
}

// --- the most important test in the plan ------------------------------------

// A digest reachable only through a stored spec — with NOTHING running — must
// be protected. This is the #64 recreatability regression: 11 of 24 live
// instances lost the manifests they would have needed to be recreated.
func TestBuildInUseSet_SpecOnlyDigestIsProtected(t *testing.T) {
	inv := &fakeInv{hosts: map[string]invEntry{
		"engine-1": {obs: nil, fresh: freshOK()},
	}}
	specs := &fakeSpecs{
		keys:  map[string][]store.SpecKey{"engine-1": {{Template: "engine", Slug: "acme"}}},
		specs: map[string]store.Spec{"engine-1/engine/acme": {Parameters: map[string]any{"image": "reg.example:5000/engine:v9"}}},
	}
	reg := &fakeReg{catalog: []string{"engine"}, manifests: map[string]string{"engine:v9": digA}}

	set, err := BuildInUseSet(context.Background(), hostsOf("engine-1"), inv, specs, reg, baseCfg())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	hasDigest(t, set, digA)
}

func TestBuildInUseSet_UnionsObservedAndSpecSources(t *testing.T) {
	inv := &fakeInv{hosts: map[string]invEntry{
		"engine-1": {obs: []instance.Observed{obs("engine", "acme", "reg.example:5000/engine@"+digA)}, fresh: freshOK()},
	}}
	specs := &fakeSpecs{
		keys: map[string][]store.SpecKey{"engine-1": {{Template: "engine", Slug: "acme"}}},
		specs: map[string]store.Spec{"engine-1/engine/acme": {Parameters: map[string]any{
			"image":    "reg.example:5000/engine:v9",
			"pg_image": "reg.example:5000/postgres:16",
			"port":     31000,
		}}},
	}
	reg := &fakeReg{
		catalog:   []string{"engine", "postgres"},
		manifests: map[string]string{"engine:v9": digB, "postgres:16": digC},
	}

	set, err := BuildInUseSet(context.Background(), hostsOf("engine-1"), inv, specs, reg, baseCfg())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	for _, d := range []string{digA, digB, digC} {
		hasDigest(t, set, d)
	}
	if len(set.Digests) != 3 {
		t.Fatalf("want 3 digests, got %v", set.Digests)
	}
}

func TestBuildInUseSet_DigestPinnedRefNeedsNoRegistryRoundTrip(t *testing.T) {
	inv := &fakeInv{hosts: map[string]invEntry{
		"engine-1": {obs: []instance.Observed{obs("engine", "acme", "reg.example:5000/engine@"+digA)}, fresh: freshOK()},
	}}
	specs := &fakeSpecs{
		keys: map[string][]store.SpecKey{"engine-1": {{Template: "engine", Slug: "acme"}}},
		specs: map[string]store.Spec{"engine-1/engine/acme": {Parameters: map[string]any{
			"image": "reg.example:5000/engine@" + digA,
		}}},
	}
	reg := &fakeReg{catalog: []string{"engine"}}

	set, err := BuildInUseSet(context.Background(), hostsOf("engine-1"), inv, specs, reg, baseCfg())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	hasDigest(t, set, digA)
	if reg.calls != 0 {
		t.Fatalf("digest-pinned refs must not hit the registry, got %d Manifest calls", reg.calls)
	}
}

// A repo with no live instances is not, by itself, a reason to abort — only a
// FLEET-WIDE zero in-use count is.
func TestBuildInUseSet_UnusedRepoDoesNotTripZeroRule(t *testing.T) {
	inv := &fakeInv{hosts: map[string]invEntry{
		"engine-1": {obs: []instance.Observed{obs("engine", "acme", "reg.example:5000/engine@"+digA)}, fresh: freshOK()},
	}}
	specs := &fakeSpecs{}
	reg := &fakeReg{catalog: []string{"engine", "abandoned", "also-unused"}}

	set, err := BuildInUseSet(context.Background(), hostsOf("engine-1"), inv, specs, reg, baseCfg())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	hasDigest(t, set, digA)
}

// A ref that belongs to some other registry entirely (its repo is not in our
// catalog) holds nothing this run could delete, so it is skipped rather than
// aborting. Everything else still resolves.
func TestBuildInUseSet_ForeignRegistryRefIsSkipped(t *testing.T) {
	inv := &fakeInv{hosts: map[string]invEntry{
		"engine-1": {obs: []instance.Observed{obs("engine", "acme", "reg.example:5000/engine@"+digA)}, fresh: freshOK()},
	}}
	specs := &fakeSpecs{
		keys: map[string][]store.SpecKey{"engine-1": {{Template: "engine", Slug: "acme"}}},
		specs: map[string]store.Spec{"engine-1/engine/acme": {Parameters: map[string]any{
			"pg_image": "docker.io/library/postgres:16-alpine",
		}}},
	}
	reg := &fakeReg{catalog: []string{"engine"}}

	set, err := BuildInUseSet(context.Background(), hostsOf("engine-1"), inv, specs, reg, baseCfg())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	hasDigest(t, set, digA)
	if reg.calls != 0 {
		t.Fatalf("foreign repo must not be queried, got %d Manifest calls", reg.calls)
	}
}

// --- fail-closed rules ------------------------------------------------------

func TestBuildInUseSet_AbortsWhenHostUnreachable(t *testing.T) {
	inv := &fakeInv{hosts: map[string]invEntry{
		"engine-1": {obs: []instance.Observed{obs("engine", "acme", "reg.example:5000/engine@"+digA)},
			fresh: instance.Freshness{FetchedAt: fixedNow.Add(-time.Minute), Reachable: false, HasData: true}},
	}}
	set, err := BuildInUseSet(context.Background(), hostsOf("engine-1"), inv, &fakeSpecs{}, &fakeReg{catalog: []string{"engine"}}, baseCfg())
	mustAbort(t, set, err)
}

func TestBuildInUseSet_AbortsWhenSnapshotStale(t *testing.T) {
	inv := &fakeInv{hosts: map[string]invEntry{
		"engine-1": {obs: []instance.Observed{obs("engine", "acme", "reg.example:5000/engine@"+digA)},
			fresh: instance.Freshness{FetchedAt: fixedNow.Add(-time.Hour), Reachable: true, HasData: true}},
	}}
	set, err := BuildInUseSet(context.Background(), hostsOf("engine-1"), inv, &fakeSpecs{}, &fakeReg{catalog: []string{"engine"}}, baseCfg())
	mustAbort(t, set, err)
}

func TestBuildInUseSet_AbortsWhenHostHasNoData(t *testing.T) {
	inv := &fakeInv{hosts: map[string]invEntry{
		"engine-1": {fresh: instance.Freshness{FetchedAt: fixedNow.Add(-time.Minute), Reachable: true, HasData: false}},
	}}
	set, err := BuildInUseSet(context.Background(), hostsOf("engine-1"), inv, &fakeSpecs{}, &fakeReg{catalog: []string{"engine"}}, baseCfg())
	mustAbort(t, set, err)
}

func TestBuildInUseSet_AbortsWhenInventoryErrors(t *testing.T) {
	inv := &fakeInv{hosts: map[string]invEntry{
		"engine-1": {err: errors.New("boom")},
	}}
	set, err := BuildInUseSet(context.Background(), hostsOf("engine-1"), inv, &fakeSpecs{}, &fakeReg{catalog: []string{"engine"}}, baseCfg())
	mustAbort(t, set, err)
}

func TestBuildInUseSet_AbortsWhenFleetWideCountIsZero(t *testing.T) {
	inv := &fakeInv{hosts: map[string]invEntry{
		"engine-1": {obs: nil, fresh: freshOK()},
	}}
	set, err := BuildInUseSet(context.Background(), hostsOf("engine-1"), inv, &fakeSpecs{}, &fakeReg{catalog: []string{"engine"}}, baseCfg())
	mustAbort(t, set, err)
}

// The single most dangerous "convenience": skipping an image ref that will not
// resolve. A skipped ref IS a false negative.
func TestBuildInUseSet_AbortsWhenSpecRefUnresolvable(t *testing.T) {
	inv := &fakeInv{hosts: map[string]invEntry{
		"engine-1": {obs: []instance.Observed{obs("engine", "acme", "reg.example:5000/engine@"+digA)}, fresh: freshOK()},
	}}
	specs := &fakeSpecs{
		keys: map[string][]store.SpecKey{"engine-1": {{Template: "engine", Slug: "acme"}}},
		specs: map[string]store.Spec{"engine-1/engine/acme": {Parameters: map[string]any{
			"image": "reg.example:5000/engine:tag-that-is-gone",
		}}},
	}
	reg := &fakeReg{catalog: []string{"engine"}} // no manifests -> ErrNotFound
	set, err := BuildInUseSet(context.Background(), hostsOf("engine-1"), inv, specs, reg, baseCfg())
	mustAbort(t, set, err)
}

func TestBuildInUseSet_AbortsWhenRegistryUnreachable(t *testing.T) {
	inv := &fakeInv{hosts: map[string]invEntry{
		"engine-1": {obs: []instance.Observed{obs("engine", "acme", "reg.example:5000/engine@"+digA)}, fresh: freshOK()},
	}}
	specs := &fakeSpecs{
		keys: map[string][]store.SpecKey{"engine-1": {{Template: "engine", Slug: "acme"}}},
		specs: map[string]store.Spec{"engine-1/engine/acme": {Parameters: map[string]any{
			"image": "reg.example:5000/engine:v9",
		}}},
	}
	reg := &fakeReg{
		catalog: []string{"engine"},
		manErr:  map[string]error{"engine:v9": fmt.Errorf("nope: %w", imgregistry.ErrUnreachable)},
	}
	set, err := BuildInUseSet(context.Background(), hostsOf("engine-1"), inv, specs, reg, baseCfg())
	mustAbort(t, set, err)
}

func TestBuildInUseSet_AbortsWhenCatalogUnreachable(t *testing.T) {
	inv := &fakeInv{hosts: map[string]invEntry{
		"engine-1": {obs: []instance.Observed{obs("engine", "acme", "reg.example:5000/engine@"+digA)}, fresh: freshOK()},
	}}
	reg := &fakeReg{catalogErr: fmt.Errorf("down: %w", imgregistry.ErrUnreachable)}
	set, err := BuildInUseSet(context.Background(), hostsOf("engine-1"), inv, &fakeSpecs{}, reg, baseCfg())
	mustAbort(t, set, err)
}

func TestBuildInUseSet_AbortsWhenSpecStoreErrors(t *testing.T) {
	inv := &fakeInv{hosts: map[string]invEntry{
		"engine-1": {obs: []instance.Observed{obs("engine", "acme", "reg.example:5000/engine@"+digA)}, fresh: freshOK()},
	}}
	specs := &fakeSpecs{listErr: errors.New("db locked")}
	set, err := BuildInUseSet(context.Background(), hostsOf("engine-1"), inv, specs, &fakeReg{catalog: []string{"engine"}}, baseCfg())
	mustAbort(t, set, err)
}

func TestBuildInUseSet_AbortsWhenSpecMissingForListedKey(t *testing.T) {
	inv := &fakeInv{hosts: map[string]invEntry{
		"engine-1": {obs: []instance.Observed{obs("engine", "acme", "reg.example:5000/engine@"+digA)}, fresh: freshOK()},
	}}
	specs := &fakeSpecs{keys: map[string][]store.SpecKey{"engine-1": {{Template: "engine", Slug: "acme"}}}}
	set, err := BuildInUseSet(context.Background(), hostsOf("engine-1"), inv, specs, &fakeReg{catalog: []string{"engine"}}, baseCfg())
	mustAbort(t, set, err)
}

// A running container whose image reference tells us nothing is a container we
// cannot protect. Abort rather than guess.
func TestBuildInUseSet_AbortsOnObservedContainerWithNoImage(t *testing.T) {
	inv := &fakeInv{hosts: map[string]invEntry{
		"engine-1": {obs: []instance.Observed{obs("engine", "acme", "")}, fresh: freshOK()},
	}}
	set, err := BuildInUseSet(context.Background(), hostsOf("engine-1"), inv, &fakeSpecs{}, &fakeReg{catalog: []string{"engine"}}, baseCfg())
	mustAbort(t, set, err)
}

func TestBuildInUseSet_AbortsWhenMaxSnapshotAgeUnset(t *testing.T) {
	inv := &fakeInv{hosts: map[string]invEntry{
		"engine-1": {obs: []instance.Observed{obs("engine", "acme", "reg.example:5000/engine@"+digA)}, fresh: freshOK()},
	}}
	cfg := baseCfg()
	cfg.MaxSnapshotAge = 0
	set, err := BuildInUseSet(context.Background(), hostsOf("engine-1"), inv, &fakeSpecs{}, &fakeReg{catalog: []string{"engine"}}, cfg)
	mustAbort(t, set, err)
}

func TestBuildInUseSet_AbortsWithNoHosts(t *testing.T) {
	set, err := BuildInUseSet(context.Background(), hostsOf(), &fakeInv{}, &fakeSpecs{}, &fakeReg{}, baseCfg())
	mustAbort(t, set, err)
}

// A bare podman image ID (no algorithm prefix) is normalised to sha256 form and
// protected. Over-protection is free; under-protection is unrecoverable.
func TestBuildInUseSet_BareHexImageIDIsProtected(t *testing.T) {
	hex := "aaaa000000000000000000000000000000000000000000000000000000000001"
	inv := &fakeInv{hosts: map[string]invEntry{
		"engine-1": {obs: []instance.Observed{obs("engine", "acme", hex)}, fresh: freshOK()},
	}}
	set, err := BuildInUseSet(context.Background(), hostsOf("engine-1"), inv, &fakeSpecs{}, &fakeReg{catalog: []string{"engine"}}, baseCfg())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	hasDigest(t, set, "sha256:"+hex)
}

// Digests are compared case-insensitively-normalised so an upper-case hex ref
// cannot slip past a lower-case protected entry.
func TestBuildInUseSet_NormalisesDigestCase(t *testing.T) {
	upper := "SHA256:AAAA000000000000000000000000000000000000000000000000000000000001"
	inv := &fakeInv{hosts: map[string]invEntry{
		"engine-1": {obs: []instance.Observed{obs("engine", "acme", "reg.example:5000/engine@"+upper)}, fresh: freshOK()},
	}}
	set, err := BuildInUseSet(context.Background(), hostsOf("engine-1"), inv, &fakeSpecs{}, &fakeReg{catalog: []string{"engine"}}, baseCfg())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	hasDigest(t, set, digA)
}

// An empty *_image parameter pins nothing — it is not an unresolvable ref.
func TestBuildInUseSet_EmptyImageParameterIsNotAnAbort(t *testing.T) {
	inv := &fakeInv{hosts: map[string]invEntry{
		"engine-1": {obs: []instance.Observed{obs("engine", "acme", "reg.example:5000/engine@"+digA)}, fresh: freshOK()},
	}}
	specs := &fakeSpecs{
		keys: map[string][]store.SpecKey{"engine-1": {{Template: "engine", Slug: "acme"}}},
		specs: map[string]store.Spec{"engine-1/engine/acme": {Parameters: map[string]any{
			"sidecar_image": "",
		}}},
	}
	set, err := BuildInUseSet(context.Background(), hostsOf("engine-1"), inv, specs, &fakeReg{catalog: []string{"engine"}}, baseCfg())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	hasDigest(t, set, digA)
}

// A non-string value under an image-bearing key is not an image ref we can
// reason about; refuse the run rather than ignore it.
func TestBuildInUseSet_AbortsOnNonStringImageParameter(t *testing.T) {
	inv := &fakeInv{hosts: map[string]invEntry{
		"engine-1": {obs: []instance.Observed{obs("engine", "acme", "reg.example:5000/engine@"+digA)}, fresh: freshOK()},
	}}
	specs := &fakeSpecs{
		keys: map[string][]store.SpecKey{"engine-1": {{Template: "engine", Slug: "acme"}}},
		specs: map[string]store.Spec{"engine-1/engine/acme": {Parameters: map[string]any{
			"image": 42,
		}}},
	}
	set, err := BuildInUseSet(context.Background(), hostsOf("engine-1"), inv, specs, &fakeReg{catalog: []string{"engine"}}, baseCfg())
	mustAbort(t, set, err)
}

// An unqualified ref (no registry host component) is assumed to be ours when
// its repo is in the catalog.
func TestBuildInUseSet_UnqualifiedRefResolvesAgainstCatalog(t *testing.T) {
	inv := &fakeInv{hosts: map[string]invEntry{
		"engine-1": {obs: nil, fresh: freshOK()},
	}}
	specs := &fakeSpecs{
		keys:  map[string][]store.SpecKey{"engine-1": {{Template: "engine", Slug: "acme"}}},
		specs: map[string]store.Spec{"engine-1/engine/acme": {Parameters: map[string]any{"image": "engine:v9"}}},
	}
	reg := &fakeReg{catalog: []string{"engine"}, manifests: map[string]string{"engine:v9": digB}}
	set, err := BuildInUseSet(context.Background(), hostsOf("engine-1"), inv, specs, reg, baseCfg())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	hasDigest(t, set, digB)
}

// --- review round: fixes for IMPORTANT 1-5 ----------------------------------

// obsTagged builds an instance whose single container carries both an image and
// an image tag, the way enrichContainer populates them.
func obsTagged(tmpl, slug, image, tag string) instance.Observed {
	return instance.Observed{Template: tmpl, Slug: slug, Containers: []instance.ObservedContainer{
		{Name: "app", Image: image, ImageTag: tag},
	}}
}

// IMPORTANT 1: a host-qualified reference to some OTHER registry must never
// reach the catalog gate. Stripping the host would derive repo "postgres",
// which our registry does host, so the ref would be queried against us,
// 404, and abort every run forever — a self-inflicted deadlock.
func TestBuildInUseSet_ForeignHostQualifiedRefNeverReachesCatalog(t *testing.T) {
	inv := &fakeInv{hosts: map[string]invEntry{
		"engine-1": {obs: []instance.Observed{obs("engine", "acme", testRegistryHost+"/engine@"+digA)}, fresh: freshOK()},
	}}
	specs := &fakeSpecs{
		keys: map[string][]store.SpecKey{"engine-1": {{Template: "engine", Slug: "acme"}}},
		specs: map[string]store.Spec{"engine-1/engine/acme": {Parameters: map[string]any{
			// Same repo NAME as one of ours, different registry.
			"pg_image": "docker.io/postgres:16",
		}}},
	}
	reg := &fakeReg{catalog: []string{"engine", "postgres"}}

	set, err := BuildInUseSet(context.Background(), hostsOf("engine-1"), inv, specs, reg, baseCfg())
	if err != nil {
		t.Fatalf("a foreign registry's ref must not abort the run: %v", err)
	}
	if reg.calls != 0 {
		t.Fatalf("foreign ref must never be queried against our registry, got %d Manifest calls", reg.calls)
	}
	hasDigest(t, set, digA)
}

// A ref qualified with OUR registry host still resolves normally.
func TestBuildInUseSet_OurHostQualifiedRefResolves(t *testing.T) {
	inv := &fakeInv{hosts: map[string]invEntry{"engine-1": {fresh: freshOK()}}}
	specs := &fakeSpecs{
		keys:  map[string][]store.SpecKey{"engine-1": {{Template: "engine", Slug: "acme"}}},
		specs: map[string]store.Spec{"engine-1/engine/acme": {Parameters: map[string]any{"image": testRegistryHost + "/engine:v9"}}},
	}
	reg := &fakeReg{catalog: []string{"engine"}, manifests: map[string]string{"engine:v9": digB}}
	set, err := BuildInUseSet(context.Background(), hostsOf("engine-1"), inv, specs, reg, baseCfg())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	hasDigest(t, set, digB)
}

// Without a configured registry host, ours and theirs cannot be told apart.
func TestBuildInUseSet_AbortsWhenRegistryHostUnset(t *testing.T) {
	inv := &fakeInv{hosts: map[string]invEntry{
		"engine-1": {obs: []instance.Observed{obs("engine", "acme", testRegistryHost+"/engine@"+digA)}, fresh: freshOK()},
	}}
	cfg := baseCfg()
	cfg.RegistryHost = ""
	set, err := BuildInUseSet(context.Background(), hostsOf("engine-1"), inv, &fakeSpecs{}, &fakeReg{catalog: []string{"engine"}}, cfg)
	mustAbort(t, set, err)
}

// IMPORTANT 4: the abort path must not leak a partially built set. Host A
// resolves cleanly BEFORE host B fails, so a `return set, err` bug is visible
// here and nowhere else.
func TestBuildInUseSet_AbortOnLaterHostReturnsNoPartialSet(t *testing.T) {
	for _, tc := range []struct {
		name  string
		bad   instance.Freshness
		badOK bool
	}{
		{name: "unreachable", bad: instance.Freshness{FetchedAt: fixedNow.Add(-time.Minute), Reachable: false, HasData: true}},
		{name: "stale", bad: instance.Freshness{FetchedAt: fixedNow.Add(-time.Hour), Reachable: true, HasData: true}},
		{name: "no-data", bad: instance.Freshness{FetchedAt: fixedNow.Add(-time.Minute), Reachable: true, HasData: false}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			inv := &fakeInv{hosts: map[string]invEntry{
				"engine-1": {obs: []instance.Observed{obs("engine", "acme", testRegistryHost+"/engine@"+digA)}, fresh: freshOK()},
				"engine-2": {obs: []instance.Observed{obs("engine", "beta", testRegistryHost+"/engine@"+digB)}, fresh: tc.bad},
			}}
			set, err := BuildInUseSet(context.Background(), hostsOf("engine-1", "engine-2"), inv,
				&fakeSpecs{}, &fakeReg{catalog: []string{"engine"}}, baseCfg())
			mustAbort(t, set, err)
		})
	}
}

// IMPORTANT 5: production is 3+ hosts; the per-host loop must union all of
// them, from both halves.
func TestBuildInUseSet_UnionsAcrossHosts(t *testing.T) {
	inv := &fakeInv{hosts: map[string]invEntry{
		"engine-1":     {obs: []instance.Observed{obs("engine", "acme", testRegistryHost+"/engine@"+digA)}, fresh: freshOK()},
		"engine-2":     {obs: []instance.Observed{obs("engine", "beta", testRegistryHost+"/engine@"+digB)}, fresh: freshOK()},
		"engine-infra": {fresh: freshOK()},
	}}
	specs := &fakeSpecs{
		keys: map[string][]store.SpecKey{"engine-infra": {{Template: "engine", Slug: "gamma"}}},
		specs: map[string]store.Spec{"engine-infra/engine/gamma": {Parameters: map[string]any{
			"image": testRegistryHost + "/engine:v9",
		}}},
	}
	reg := &fakeReg{catalog: []string{"engine"}, manifests: map[string]string{"engine:v9": digC}}

	set, err := BuildInUseSet(context.Background(), hostsOf("engine-1", "engine-2", "engine-infra"), inv, specs, reg, baseCfg())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	for _, d := range []string{digA, digB, digC} {
		hasDigest(t, set, d)
	}
	if set.Len() != 3 {
		t.Fatalf("want 3 digests across 3 hosts, got %v", set.Digests)
	}
}

// A host the enumerator returns but the inventory does not know about is an
// inconsistency, not something to walk past.
func TestBuildInUseSet_AbortsWhenEnumeratedHostIsUnknownToInventory(t *testing.T) {
	inv := &fakeInv{hosts: map[string]invEntry{
		"engine-1": {obs: []instance.Observed{obs("engine", "acme", testRegistryHost+"/engine@"+digA)}, fresh: freshOK()},
	}}
	set, err := BuildInUseSet(context.Background(), hostsOf("engine-1", "engine-2"), inv,
		&fakeSpecs{}, &fakeReg{catalog: []string{"engine"}}, baseCfg())
	mustAbort(t, set, err)
}

// IMPORTANT 3: when podman reports an image ID rather than a manifest digest,
// the derived sha256:<id> entry can never match a registry manifest. The
// container's ImageTag — which the legacy shell script protected — must be
// resolved and unioned in, and the gap recorded.
func TestBuildInUseSet_ObservedImageTagIsResolvedAndGapRecorded(t *testing.T) {
	hex := "aaaa000000000000000000000000000000000000000000000000000000000001"
	inv := &fakeInv{hosts: map[string]invEntry{
		"engine-1": {obs: []instance.Observed{obsTagged("engine", "acme", hex, testRegistryHost+"/engine:dev")}, fresh: freshOK()},
	}}
	reg := &fakeReg{catalog: []string{"engine"}, manifests: map[string]string{"engine:dev": digB}}

	set, err := BuildInUseSet(context.Background(), hostsOf("engine-1"), inv, &fakeSpecs{}, reg, baseCfg())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	hasDigest(t, set, digB)          // resolved from ImageTag
	hasDigest(t, set, "sha256:"+hex) // over-protection from the image ID
	if len(set.NonDigestObserved) != 1 {
		t.Fatalf("want the non-digest observation recorded, got %v", set.NonDigestObserved)
	}
	if !strings.Contains(set.NonDigestObserved[0], "engine-1") || !strings.Contains(set.NonDigestObserved[0], "acme") {
		t.Fatalf("observation must identify the container, got %q", set.NonDigestObserved[0])
	}
}

// A digest-form observed image is authoritative: nothing is recorded as a gap.
func TestBuildInUseSet_DigestObservedRecordsNoGap(t *testing.T) {
	inv := &fakeInv{hosts: map[string]invEntry{
		"engine-1": {obs: []instance.Observed{obsTagged("engine", "acme", testRegistryHost+"/engine@"+digA, testRegistryHost+"/engine:dev")}, fresh: freshOK()},
	}}
	reg := &fakeReg{catalog: []string{"engine"}, manifests: map[string]string{"engine:dev": digB}}
	set, err := BuildInUseSet(context.Background(), hostsOf("engine-1"), inv, &fakeSpecs{}, reg, baseCfg())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	hasDigest(t, set, digA)
	hasDigest(t, set, digB)
	if len(set.NonDigestObserved) != 0 {
		t.Fatalf("digest-form observation must record no gap, got %v", set.NonDigestObserved)
	}
}

// An ImageTag that no longer exists is tolerated when the observed image is
// itself digest-form — the digest is already protected, and aborting would
// deadlock the job on every rolled instance.
func TestBuildInUseSet_MissingImageTagToleratedWhenImageIsPinned(t *testing.T) {
	inv := &fakeInv{hosts: map[string]invEntry{
		"engine-1": {obs: []instance.Observed{obsTagged("engine", "acme", testRegistryHost+"/engine@"+digA, testRegistryHost+"/engine:gone")}, fresh: freshOK()},
	}}
	set, err := BuildInUseSet(context.Background(), hostsOf("engine-1"), inv, &fakeSpecs{}, &fakeReg{catalog: []string{"engine"}}, baseCfg())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	hasDigest(t, set, digA)
}

// But an unreachable registry while resolving an ImageTag is never tolerated.
func TestBuildInUseSet_AbortsWhenImageTagResolutionIsUnreachable(t *testing.T) {
	inv := &fakeInv{hosts: map[string]invEntry{
		"engine-1": {obs: []instance.Observed{obsTagged("engine", "acme", testRegistryHost+"/engine@"+digA, testRegistryHost+"/engine:dev")}, fresh: freshOK()},
	}}
	reg := &fakeReg{catalog: []string{"engine"}, manErr: map[string]error{"engine:dev": fmt.Errorf("nope: %w", imgregistry.ErrUnreachable)}}
	set, err := BuildInUseSet(context.Background(), hostsOf("engine-1"), inv, &fakeSpecs{}, reg, baseCfg())
	mustAbort(t, set, err)
}

// MINOR: Has is the accessor Task 5 will call; exercise its normalisation.
func TestInUseSet_HasNormalisesInput(t *testing.T) {
	inv := &fakeInv{hosts: map[string]invEntry{
		"engine-1": {obs: []instance.Observed{obs("engine", "acme", testRegistryHost+"/engine@"+digA)}, fresh: freshOK()},
	}}
	set, err := BuildInUseSet(context.Background(), hostsOf("engine-1"), inv, &fakeSpecs{}, &fakeReg{catalog: []string{"engine"}}, baseCfg())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !set.Has(digA) {
		t.Fatal("Has must find the digest it was built from")
	}
	if !set.Has("  " + strings.ToUpper(digA) + "\n") {
		t.Fatal("Has must normalise case and surrounding whitespace")
	}
	if set.Has(digC) {
		t.Fatal("Has must not find an unprotected digest")
	}
	if (InUseSet{}).Has(digA) {
		t.Fatal("the zero set protects nothing")
	}
}

// MINOR: protection is keyed by digest across repos — two repos resolving to
// the same digest collapse to one protected entry, and that entry protects the
// digest wherever it appears.
func TestBuildInUseSet_ProtectionIsCrossRepo(t *testing.T) {
	inv := &fakeInv{hosts: map[string]invEntry{"engine-1": {fresh: freshOK()}}}
	specs := &fakeSpecs{
		keys: map[string][]store.SpecKey{"engine-1": {{Template: "engine", Slug: "acme"}}},
		specs: map[string]store.Spec{"engine-1/engine/acme": {Parameters: map[string]any{
			"image":    testRegistryHost + "/engine:v9",
			"pg_image": testRegistryHost + "/mirror/engine:v9",
		}}},
	}
	reg := &fakeReg{
		catalog:   []string{"engine", "mirror/engine"},
		manifests: map[string]string{"engine:v9": digA, "mirror/engine:v9": digA},
	}
	set, err := BuildInUseSet(context.Background(), hostsOf("engine-1"), inv, specs, reg, baseCfg())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if set.Len() != 1 {
		t.Fatalf("the same digest in two repos is one protected entry, got %v", set.Digests)
	}
	hasDigest(t, set, digA)
}

package registryprune

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

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

func freshOK() instance.Freshness {
	return instance.Freshness{FetchedAt: fixedNow.Add(-time.Minute), Reachable: true, HasData: true}
}

func baseCfg(hosts ...string) Config {
	return Config{
		Hosts:          hosts,
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
		t.Fatalf("aborting run must return an empty set, got %d digests", len(set.Digests))
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

	set, err := BuildInUseSet(context.Background(), inv, specs, reg, baseCfg("engine-1"))
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

	set, err := BuildInUseSet(context.Background(), inv, specs, reg, baseCfg("engine-1"))
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

	set, err := BuildInUseSet(context.Background(), inv, specs, reg, baseCfg("engine-1"))
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

	set, err := BuildInUseSet(context.Background(), inv, specs, reg, baseCfg("engine-1"))
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

	set, err := BuildInUseSet(context.Background(), inv, specs, reg, baseCfg("engine-1"))
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
	set, err := BuildInUseSet(context.Background(), inv, &fakeSpecs{}, &fakeReg{catalog: []string{"engine"}}, baseCfg("engine-1"))
	mustAbort(t, set, err)
}

func TestBuildInUseSet_AbortsWhenSnapshotStale(t *testing.T) {
	inv := &fakeInv{hosts: map[string]invEntry{
		"engine-1": {obs: []instance.Observed{obs("engine", "acme", "reg.example:5000/engine@"+digA)},
			fresh: instance.Freshness{FetchedAt: fixedNow.Add(-time.Hour), Reachable: true, HasData: true}},
	}}
	set, err := BuildInUseSet(context.Background(), inv, &fakeSpecs{}, &fakeReg{catalog: []string{"engine"}}, baseCfg("engine-1"))
	mustAbort(t, set, err)
}

func TestBuildInUseSet_AbortsWhenHostHasNoData(t *testing.T) {
	inv := &fakeInv{hosts: map[string]invEntry{
		"engine-1": {fresh: instance.Freshness{FetchedAt: fixedNow.Add(-time.Minute), Reachable: true, HasData: false}},
	}}
	set, err := BuildInUseSet(context.Background(), inv, &fakeSpecs{}, &fakeReg{catalog: []string{"engine"}}, baseCfg("engine-1"))
	mustAbort(t, set, err)
}

func TestBuildInUseSet_AbortsWhenInventoryErrors(t *testing.T) {
	inv := &fakeInv{hosts: map[string]invEntry{
		"engine-1": {err: errors.New("boom")},
	}}
	set, err := BuildInUseSet(context.Background(), inv, &fakeSpecs{}, &fakeReg{catalog: []string{"engine"}}, baseCfg("engine-1"))
	mustAbort(t, set, err)
}

func TestBuildInUseSet_AbortsWhenFleetWideCountIsZero(t *testing.T) {
	inv := &fakeInv{hosts: map[string]invEntry{
		"engine-1": {obs: nil, fresh: freshOK()},
	}}
	set, err := BuildInUseSet(context.Background(), inv, &fakeSpecs{}, &fakeReg{catalog: []string{"engine"}}, baseCfg("engine-1"))
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
	set, err := BuildInUseSet(context.Background(), inv, specs, reg, baseCfg("engine-1"))
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
	set, err := BuildInUseSet(context.Background(), inv, specs, reg, baseCfg("engine-1"))
	mustAbort(t, set, err)
}

func TestBuildInUseSet_AbortsWhenCatalogUnreachable(t *testing.T) {
	inv := &fakeInv{hosts: map[string]invEntry{
		"engine-1": {obs: []instance.Observed{obs("engine", "acme", "reg.example:5000/engine@"+digA)}, fresh: freshOK()},
	}}
	reg := &fakeReg{catalogErr: fmt.Errorf("down: %w", imgregistry.ErrUnreachable)}
	set, err := BuildInUseSet(context.Background(), inv, &fakeSpecs{}, reg, baseCfg("engine-1"))
	mustAbort(t, set, err)
}

func TestBuildInUseSet_AbortsWhenSpecStoreErrors(t *testing.T) {
	inv := &fakeInv{hosts: map[string]invEntry{
		"engine-1": {obs: []instance.Observed{obs("engine", "acme", "reg.example:5000/engine@"+digA)}, fresh: freshOK()},
	}}
	specs := &fakeSpecs{listErr: errors.New("db locked")}
	set, err := BuildInUseSet(context.Background(), inv, specs, &fakeReg{catalog: []string{"engine"}}, baseCfg("engine-1"))
	mustAbort(t, set, err)
}

func TestBuildInUseSet_AbortsWhenSpecMissingForListedKey(t *testing.T) {
	inv := &fakeInv{hosts: map[string]invEntry{
		"engine-1": {obs: []instance.Observed{obs("engine", "acme", "reg.example:5000/engine@"+digA)}, fresh: freshOK()},
	}}
	specs := &fakeSpecs{keys: map[string][]store.SpecKey{"engine-1": {{Template: "engine", Slug: "acme"}}}}
	set, err := BuildInUseSet(context.Background(), inv, specs, &fakeReg{catalog: []string{"engine"}}, baseCfg("engine-1"))
	mustAbort(t, set, err)
}

// A running container whose image reference tells us nothing is a container we
// cannot protect. Abort rather than guess.
func TestBuildInUseSet_AbortsOnObservedContainerWithNoImage(t *testing.T) {
	inv := &fakeInv{hosts: map[string]invEntry{
		"engine-1": {obs: []instance.Observed{obs("engine", "acme", "")}, fresh: freshOK()},
	}}
	set, err := BuildInUseSet(context.Background(), inv, &fakeSpecs{}, &fakeReg{catalog: []string{"engine"}}, baseCfg("engine-1"))
	mustAbort(t, set, err)
}

func TestBuildInUseSet_AbortsWhenMaxSnapshotAgeUnset(t *testing.T) {
	inv := &fakeInv{hosts: map[string]invEntry{
		"engine-1": {obs: []instance.Observed{obs("engine", "acme", "reg.example:5000/engine@"+digA)}, fresh: freshOK()},
	}}
	cfg := baseCfg("engine-1")
	cfg.MaxSnapshotAge = 0
	set, err := BuildInUseSet(context.Background(), inv, &fakeSpecs{}, &fakeReg{catalog: []string{"engine"}}, cfg)
	mustAbort(t, set, err)
}

func TestBuildInUseSet_AbortsWithNoHosts(t *testing.T) {
	set, err := BuildInUseSet(context.Background(), &fakeInv{}, &fakeSpecs{}, &fakeReg{}, baseCfg())
	mustAbort(t, set, err)
}

// A bare podman image ID (no algorithm prefix) is normalised to sha256 form and
// protected. Over-protection is free; under-protection is unrecoverable.
func TestBuildInUseSet_BareHexImageIDIsProtected(t *testing.T) {
	hex := "aaaa000000000000000000000000000000000000000000000000000000000001"
	inv := &fakeInv{hosts: map[string]invEntry{
		"engine-1": {obs: []instance.Observed{obs("engine", "acme", hex)}, fresh: freshOK()},
	}}
	set, err := BuildInUseSet(context.Background(), inv, &fakeSpecs{}, &fakeReg{catalog: []string{"engine"}}, baseCfg("engine-1"))
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
	set, err := BuildInUseSet(context.Background(), inv, &fakeSpecs{}, &fakeReg{catalog: []string{"engine"}}, baseCfg("engine-1"))
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
	set, err := BuildInUseSet(context.Background(), inv, specs, &fakeReg{catalog: []string{"engine"}}, baseCfg("engine-1"))
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
	set, err := BuildInUseSet(context.Background(), inv, specs, &fakeReg{catalog: []string{"engine"}}, baseCfg("engine-1"))
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
	set, err := BuildInUseSet(context.Background(), inv, specs, reg, baseCfg("engine-1"))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	hasDigest(t, set, digB)
}

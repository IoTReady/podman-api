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
		RegistryHosts:  []string{testRegistryHost},
		MaxSnapshotAge: 10 * time.Minute,
		Now:            func() time.Time { return fixedNow },
	}
}

// obsLive builds an instance carrying the container image shapes podman
// actually produces, measured on engine-1 (podman 5.8.2, 2026-08-07):
//
//	ImageDigest = "sha256:<hex>"            <- BARE, no repository
//	ImageName   = "100.64.0.23:5000/jioworldcentre/web:2026.06.27-218c436"
//	              or "…/repo@sha256:<hex>"
//
// enrichContainer maps ImageDigest->Image and ImageName->ImageTag, so
// ObservedContainer.Image is a bare digest in production and NEVER the
// "host/repo@sha256:…" shape these tests used to assume.
func obsLive(tmpl, slug string, digests ...string) instance.Observed {
	o := instance.Observed{Template: tmpl, Slug: slug}
	for i, d := range digests {
		o.Containers = append(o.Containers, instance.ObservedContainer{
			Name:     fmt.Sprintf("c%d", i),
			Image:    d,
			ImageTag: testRegistryHost + "/engine@" + d,
		})
	}
	return o
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
	// Valid is documented as the one thing a caller checks before deleting, so
	// an abort that left it true would defeat every other guard here.
	if set.Valid {
		t.Fatal("aborting run must return Valid=false; a caller checking only Valid would proceed to delete")
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
		"engine-1": {obs: []instance.Observed{obsLive("engine", "acme", digA)}, fresh: freshOK()},
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
		"engine-1": {obs: []instance.Observed{obsLive("engine", "acme", digA)}, fresh: freshOK()},
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
	if !strings.Contains(err.Error(), "max snapshot age") {
		t.Fatalf("want the unconfigured-max-age abort, not the staleness one: %v", err)
	}
}

func TestBuildInUseSet_AbortsWithNoHosts(t *testing.T) {
	set, err := BuildInUseSet(context.Background(), hostsOf(), &fakeInv{}, &fakeSpecs{}, &fakeReg{}, baseCfg())
	mustAbort(t, set, err)
}

// A bare podman image ID is NOT a manifest digest: the sha256:<id> entry it
// yields can never match anything in the registry, so the running container's
// real manifest is protected by nothing. Fatal, with the container named.
func TestBuildInUseSet_AbortsOnBareHexImageID(t *testing.T) {
	hex := "aaaa000000000000000000000000000000000000000000000000000000000001"
	inv := &fakeInv{hosts: map[string]invEntry{
		"engine-1": {obs: []instance.Observed{obs("engine", "acme", hex)}, fresh: freshOK()},
	}}
	set, err := BuildInUseSet(context.Background(), hostsOf("engine-1"), inv, &fakeSpecs{}, &fakeReg{catalog: []string{"engine"}}, baseCfg())
	mustAbort(t, set, err)
	if len(set.NonDigestObserved) != 1 {
		t.Fatalf("the abort must still name the offending container, got %v", set.NonDigestObserved)
	}
}

// Digests are compared case-insensitively-normalised so an upper-case hex ref
// cannot slip past a lower-case protected entry. The observed image is a BARE
// digest (the measured production shape) with no ImageTag behind it, so the
// only code path that can produce the lower-case entry is the bare-digest
// branch — the "@"-split path is not available to rescue it.
func TestBuildInUseSet_NormalisesDigestCase(t *testing.T) {
	upper := "SHA256:AAAA000000000000000000000000000000000000000000000000000000000001"
	inv := &fakeInv{hosts: map[string]invEntry{
		"engine-1": {obs: []instance.Observed{obs("engine", "acme", upper)}, fresh: freshOK()},
	}}
	set, err := BuildInUseSet(context.Background(), hostsOf("engine-1"), inv, &fakeSpecs{}, &fakeReg{catalog: []string{"engine"}}, baseCfg())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	hasDigest(t, set, digA)
	if len(set.Digests) != 1 {
		t.Fatalf("want exactly the normalised digest, got %v", set.Digests)
	}
}

// An empty *_image parameter pins nothing — it is not an unresolvable ref.
func TestBuildInUseSet_EmptyImageParameterIsNotAnAbort(t *testing.T) {
	inv := &fakeInv{hosts: map[string]invEntry{
		"engine-1": {obs: []instance.Observed{obsLive("engine", "acme", digA)}, fresh: freshOK()},
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
		"engine-1": {obs: []instance.Observed{obsLive("engine", "acme", digA)}, fresh: freshOK()},
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
		"engine-1": {obs: []instance.Observed{obsLive("engine", "acme", digA)}, fresh: freshOK()},
	}}
	cfg := baseCfg()
	cfg.RegistryHosts = nil
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
				"engine-1": {obs: []instance.Observed{obsLive("engine", "acme", digA)}, fresh: freshOK()},
				"engine-2": {obs: []instance.Observed{obsLive("engine", "beta", digB)}, fresh: tc.bad},
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
		"engine-1":     {obs: []instance.Observed{obsLive("engine", "acme", digA)}, fresh: freshOK()},
		"engine-2":     {obs: []instance.Observed{obsLive("engine", "beta", digB)}, fresh: freshOK()},
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
		"engine-1": {obs: []instance.Observed{obsLive("engine", "acme", digA)}, fresh: freshOK()},
	}}
	set, err := BuildInUseSet(context.Background(), hostsOf("engine-1", "engine-2"), inv,
		&fakeSpecs{}, &fakeReg{catalog: []string{"engine"}}, baseCfg())
	mustAbort(t, set, err)
}

// IMPORTANT 3 (round 2): when podman reports an image ID rather than a manifest
// digest, resolving ImageTag is not enough — a tag that has MOVED resolves
// cleanly to the NEW digest while the running container's own manifest stays
// unprotected, with no error anywhere. So a non-digest observation is fatal,
// and the field is still populated for diagnosis.
func TestBuildInUseSet_AbortsWhenObservedImageIsNotAManifestDigest(t *testing.T) {
	hex := "aaaa000000000000000000000000000000000000000000000000000000000001"
	inv := &fakeInv{hosts: map[string]invEntry{
		"engine-1": {obs: []instance.Observed{obsTagged("engine", "acme", hex, testRegistryHost+"/engine:dev")}, fresh: freshOK()},
	}}
	// The tag resolves perfectly well — to a digest that may not be the one
	// this container is running.
	reg := &fakeReg{catalog: []string{"engine"}, manifests: map[string]string{"engine:dev": digB}}

	set, err := BuildInUseSet(context.Background(), hostsOf("engine-1"), inv, &fakeSpecs{}, reg, baseCfg())
	mustAbort(t, set, err)
	if len(set.NonDigestObserved) != 1 {
		t.Fatalf("want the offending container recorded for diagnosis, got %v", set.NonDigestObserved)
	}
	if !strings.Contains(set.NonDigestObserved[0], "engine-1") || !strings.Contains(set.NonDigestObserved[0], "acme") {
		t.Fatalf("observation must identify the container, got %q", set.NonDigestObserved[0])
	}
}

// IMPORTANT I2: an image ID whose tag ALSO cannot be resolved leaves the
// container with no protection at all. Same abort, message naming the tag.
func TestBuildInUseSet_AbortsWhenImageIDAndTagBothUnresolvable(t *testing.T) {
	hex := "aaaa000000000000000000000000000000000000000000000000000000000001"
	inv := &fakeInv{hosts: map[string]invEntry{
		"engine-1": {obs: []instance.Observed{obsTagged("engine", "acme", hex, testRegistryHost+"/engine:deleted")}, fresh: freshOK()},
	}}
	set, err := BuildInUseSet(context.Background(), hostsOf("engine-1"), inv, &fakeSpecs{}, &fakeReg{catalog: []string{"engine"}}, baseCfg())
	mustAbort(t, set, err)
	if len(set.NonDigestObserved) != 1 || !strings.Contains(set.NonDigestObserved[0], "deleted") {
		t.Fatalf("want the unresolvable tag recorded, got %v", set.NonDigestObserved)
	}
}

// A digest-form observed image is authoritative: nothing is recorded as a gap.
func TestBuildInUseSet_DigestObservedRecordsNoGap(t *testing.T) {
	inv := &fakeInv{hosts: map[string]invEntry{
		"engine-1": {obs: []instance.Observed{obsTagged("engine", "acme", digA, testRegistryHost+"/engine:dev")}, fresh: freshOK()},
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
		"engine-1": {obs: []instance.Observed{obsTagged("engine", "acme", digA, testRegistryHost+"/engine:gone")}, fresh: freshOK()},
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
		"engine-1": {obs: []instance.Observed{obsTagged("engine", "acme", digA, testRegistryHost+"/engine:dev")}, fresh: freshOK()},
	}}
	reg := &fakeReg{catalog: []string{"engine"}, manErr: map[string]error{"engine:dev": fmt.Errorf("nope: %w", imgregistry.ErrUnreachable)}}
	set, err := BuildInUseSet(context.Background(), hostsOf("engine-1"), inv, &fakeSpecs{}, reg, baseCfg())
	mustAbort(t, set, err)
}

// MINOR: Has is the accessor Task 5 will call; exercise its normalisation.
func TestInUseSet_HasNormalisesInput(t *testing.T) {
	inv := &fakeInv{hosts: map[string]invEntry{
		"engine-1": {obs: []instance.Observed{obsLive("engine", "acme", digA)}, fresh: freshOK()},
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

// --- review round 2 ---------------------------------------------------------

// tagOnlyFleet is a fleet whose single instance pins nothing by digest: every
// protected digest has to come from resolving a tag, so a registry host that
// misclassifies our own refs as foreign shows up as a zero-count abort rather
// than as silence.
func tagOnlyFleet() (*fakeInv, *fakeSpecs, *fakeReg) {
	inv := &fakeInv{hosts: map[string]invEntry{"engine-1": {fresh: freshOK()}}}
	specs := &fakeSpecs{
		keys:  map[string][]store.SpecKey{"engine-1": {{Template: "engine", Slug: "acme"}}},
		specs: map[string]store.Spec{"engine-1/engine/acme": {Parameters: map[string]any{"image": "reg.example:5000/engine:v9"}}},
	}
	reg := &fakeReg{catalog: []string{"engine"}, manifests: map[string]string{"engine:v9": digA}}
	return inv, specs, reg
}

// CRITICAL C1: a registry host with a trailing slash, surrounding whitespace,
// or a scheme (the shape imgregistry.NewHTTPClient takes) must still match our
// own refs. Getting this wrong classifies every one of our refs as foreign and
// silently deletes every manifest reachable only through a tag.
func TestBuildInUseSet_NormalisesRegistryHostSpelling(t *testing.T) {
	for _, spelling := range []string{
		"reg.example:5000/",
		"  reg.example:5000 ",
		"http://reg.example:5000",
		"https://reg.example:5000/",
		"REG.EXAMPLE:5000",
		"reg.example:5000\t", // trailing whitespace is trimmed, like a space
	} {
		t.Run(spelling, func(t *testing.T) {
			inv, specs, reg := tagOnlyFleet()
			cfg := baseCfg()
			cfg.RegistryHosts = []string{spelling}
			set, err := BuildInUseSet(context.Background(), hostsOf("engine-1"), inv, specs, reg, cfg)
			if err != nil {
				t.Fatalf("registry host %q must be accepted and match our refs: %v", spelling, err)
			}
			hasDigest(t, set, digA)
		})
	}
}

// A registry host that is still not a bare host after normalisation is
// rejected outright, not accepted-and-hoped.
//
// Each case asserts the message of ITS OWN check, not the shared "registry
// host" prefix: rule 4's abort ("no host-qualified image reference matched a
// configured registry host …") contains that prefix too, so a substring
// assertion on it passes even when the check under test has been deleted
// entirely.
func TestBuildInUseSet_RejectsMalformedRegistryHost(t *testing.T) {
	for _, tc := range []struct{ spelling, wantMsg string }{
		{"reg.example:5000/some/path", "with no path"},
		{"reg example:5000", "must not contain whitespace"}, // whitespace INSIDE the host, not around it
		{"reg.example:5000\tx", "must not contain whitespace"},
		{"   ", "empty"},
		{"", "empty"},
		{"http://", "empty"},
	} {
		t.Run(tc.spelling, func(t *testing.T) {
			inv, specs, reg := tagOnlyFleet()
			cfg := baseCfg()
			cfg.RegistryHosts = []string{tc.spelling}
			set, err := BuildInUseSet(context.Background(), hostsOf("engine-1"), inv, specs, reg, cfg)
			mustAbort(t, set, err)
			// Must be rejected AS A CONFIG ERROR, not stumbled into via the
			// fleet-wide-zero rule or rule 4, both of which also fire here.
			if !strings.Contains(err.Error(), "registry host") || !strings.Contains(err.Error(), tc.wantMsg) {
				t.Fatalf("want a registry-host error containing %q, got %v", tc.wantMsg, err)
			}
		})
	}
}

// One bad alias in the list poisons the comparison, so the whole config is
// rejected rather than the bad entry dropped.
func TestBuildInUseSet_RejectsMalformedAliasAmongGoodOnes(t *testing.T) {
	inv, specs, reg := tagOnlyFleet()
	cfg := baseCfg()
	cfg.RegistryHosts = []string{"reg.example:5000", "reg.example/oops"}
	set, err := BuildInUseSet(context.Background(), hostsOf("engine-1"), inv, specs, reg, cfg)
	mustAbort(t, set, err)
	if !strings.Contains(err.Error(), `"reg.example/oops"`) || !strings.Contains(err.Error(), "with no path") {
		t.Fatalf("want the bad alias named with its own path error, got %v", err)
	}
}

// IMPORTANT I1: our registry is spelled several ways across specs written over
// the years (port omitted, IP instead of DNS name). Every alias must protect.
func TestBuildInUseSet_RegistryAliasesAllProtect(t *testing.T) {
	for _, ref := range []string{
		"reg.example:5000/engine:v9",
		"reg.example/engine:v9",
		"100.64.0.23:5000/engine:v9",
	} {
		t.Run(ref, func(t *testing.T) {
			inv := &fakeInv{hosts: map[string]invEntry{"engine-1": {fresh: freshOK()}}}
			specs := &fakeSpecs{
				keys:  map[string][]store.SpecKey{"engine-1": {{Template: "engine", Slug: "acme"}}},
				specs: map[string]store.Spec{"engine-1/engine/acme": {Parameters: map[string]any{"image": ref}}},
			}
			reg := &fakeReg{catalog: []string{"engine"}, manifests: map[string]string{"engine:v9": digA}}
			cfg := baseCfg()
			cfg.RegistryHosts = []string{"reg.example:5000", "reg.example", "100.64.0.23:5000"}
			set, err := BuildInUseSet(context.Background(), hostsOf("engine-1"), inv, specs, reg, cfg)
			if err != nil {
				t.Fatalf("alias %q must be recognised as ours: %v", ref, err)
			}
			hasDigest(t, set, digA)
		})
	}
}

// A foreign skip is recorded, deduped, so a run that skips everything is
// visible in the job output instead of silent.
func TestBuildInUseSet_ForeignSkipsAreRecorded(t *testing.T) {
	inv := &fakeInv{hosts: map[string]invEntry{
		"engine-1": {obs: []instance.Observed{obsLive("engine", "acme", digA)}, fresh: freshOK()},
	}}
	specs := &fakeSpecs{
		keys: map[string][]store.SpecKey{"engine-1": {{Template: "engine", Slug: "acme"}}},
		specs: map[string]store.Spec{"engine-1/engine/acme": {Parameters: map[string]any{
			"pg_image":      "docker.io/postgres:16",
			"sidecar_image": "docker.io/postgres:16", // same ref twice -> one entry
			"other_image":   "quay.io/rclone:latest",
		}}},
	}
	set, err := BuildInUseSet(context.Background(), hostsOf("engine-1"), inv, specs, &fakeReg{catalog: []string{"engine", "postgres"}}, baseCfg())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(set.ForeignSkipped) != 2 {
		t.Fatalf("want 2 deduped foreign skips, got %v", set.ForeignSkipped)
	}
	joined := strings.Join(set.ForeignSkipped, " ")
	if !strings.Contains(joined, "docker.io/postgres:16") || !strings.Contains(joined, "quay.io/rclone:latest") {
		t.Fatalf("foreign skips must name the refs, got %v", set.ForeignSkipped)
	}
}

// Every pod carries an infra container, and on podman 5.8.2 it reports NO
// image, image digest or image name at all (verified live on engine-1/2/infra,
// 2026-08-07: 78 of 78 infra containers). Treating that as an unresolvable
// image would abort every run on every real host, so it is skipped — keyed off
// the pod's own InfraID, which is the only unambiguous signal.
func TestBuildInUseSet_SkipsPodInfraContainers(t *testing.T) {
	o := instance.Observed{Template: "engine", Slug: "acme", Containers: []instance.ObservedContainer{
		{Name: "1fd9c02bf4f0-infra", IsInfra: true},
		{Name: "engine", Image: digA, ImageTag: testRegistryHost + "/engine@" + digA},
	}}
	inv := &fakeInv{hosts: map[string]invEntry{"engine-1": {obs: []instance.Observed{o}, fresh: freshOK()}}}
	set, err := BuildInUseSet(context.Background(), hostsOf("engine-1"), inv, &fakeSpecs{}, &fakeReg{catalog: []string{"engine"}}, baseCfg())
	if err != nil {
		t.Fatalf("a pod infra container must not abort the run: %v", err)
	}
	hasDigest(t, set, digA)
	if len(set.NonDigestObserved) != 0 {
		t.Fatalf("an infra container is expected, not a gap: %v", set.NonDigestObserved)
	}
}

// A container is infra only when the POD says so. A name suffix is not a
// signal: `podman kube play` names containers <pod>-<containerName>, so a
// template declaring a container called "infra" is named identically.
func TestBuildInUseSet_InfraNamedContainerWithAnImageIsNotSkipped(t *testing.T) {
	o := instance.Observed{Template: "engine", Slug: "acme", Containers: []instance.ObservedContainer{
		// Named like an infra container, and NOT flagged as one: a template
		// declaring a container "infra" under `podman kube play` produces
		// exactly this name.
		{Name: "my-infra", Image: digB, ImageTag: testRegistryHost + "/engine@" + digB},
	}}
	inv := &fakeInv{hosts: map[string]invEntry{"engine-1": {obs: []instance.Observed{o}, fresh: freshOK()}}}
	set, err := BuildInUseSet(context.Background(), hostsOf("engine-1"), inv, &fakeSpecs{}, &fakeReg{catalog: []string{"engine"}}, baseCfg())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	hasDigest(t, set, digB)
}

// --- review round 3 ---------------------------------------------------------

// CRITICAL: podman reports ImageDigest as a BARE "sha256:<hex>" with no
// repository (image.Digest().String(), libpod/container_inspect.go), and
// enrichContainer copies it straight into ObservedContainer.Image. Parsing it
// as a reference yields repo "sha256", which is in no catalog, so the running
// container's own manifest was being discarded as "foreign" — the observed
// half protecting nothing at all. This is the exact end-to-end shape probed on
// engine-1: a bare digest, plus a tag that has since MOVED to another digest.
func TestBuildInUseSet_BareDigestObservedImageIsProtected(t *testing.T) {
	inv := &fakeInv{hosts: map[string]invEntry{
		"engine-1": {obs: []instance.Observed{obsTagged("engine", "acme", digA, testRegistryHost+"/engine:dev")}, fresh: freshOK()},
	}}
	reg := &fakeReg{catalog: []string{"engine"}, manifests: map[string]string{"engine:dev": digB}}

	set, err := BuildInUseSet(context.Background(), hostsOf("engine-1"), inv, &fakeSpecs{}, reg, baseCfg())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	hasDigest(t, set, digA) // the manifest this container is ACTUALLY running
	hasDigest(t, set, digB) // and the one its tag now points at
	if len(set.NonDigestObserved) != 0 {
		t.Fatalf("a bare digest is a manifest digest, not a gap: %v", set.NonDigestObserved)
	}
	if len(set.ForeignSkipped) != 0 {
		t.Fatalf("a bare digest belongs to no registry and must not be recorded as foreign: %v", set.ForeignSkipped)
	}
	if reg.calls != 1 {
		t.Fatalf("a bare digest needs no registry round-trip, want 1 call (the tag), got %d", reg.calls)
	}
}

// A well-formed but WRONG registry host — stale after a registry move, or a
// typo — makes every one of our refs foreign. One digest-pinned container is
// enough to defeat the fleet-wide-zero rule, so the class is closed with its
// own rule: if any ref was host-qualified and none matched, abort.
func TestBuildInUseSet_AbortsWhenNoQualifiedRefMatchesRegistryHost(t *testing.T) {
	inv := &fakeInv{hosts: map[string]invEntry{
		"engine-1": {obs: []instance.Observed{obsLive("engine", "acme", digA)}, fresh: freshOK()},
	}}
	specs := &fakeSpecs{
		keys:  map[string][]store.SpecKey{"engine-1": {{Template: "engine", Slug: "acme"}}},
		specs: map[string]store.Spec{"engine-1/engine/acme": {Parameters: map[string]any{"image": testRegistryHost + "/engine:v9"}}},
	}
	cfg := baseCfg()
	cfg.RegistryHosts = []string{"reg2.example:5000"} // well-formed, and wrong
	set, err := BuildInUseSet(context.Background(), hostsOf("engine-1"), inv, specs,
		&fakeReg{catalog: []string{"engine"}, manifests: map[string]string{"engine:v9": digB}}, cfg)
	mustAbort(t, set, err)
	if !strings.Contains(err.Error(), "no host-qualified") {
		t.Fatalf("want the no-match rule, got %v", err)
	}
}

// A fleet whose refs are all unqualified is not evidence of a wrong host, so
// the rule above must not fire when there was nothing to match.
func TestBuildInUseSet_NoQualifiedRefsAtAllDoesNotTripTheMatchRule(t *testing.T) {
	inv := &fakeInv{hosts: map[string]invEntry{"engine-1": {fresh: freshOK()}}}
	specs := &fakeSpecs{
		keys:  map[string][]store.SpecKey{"engine-1": {{Template: "engine", Slug: "acme"}}},
		specs: map[string]store.Spec{"engine-1/engine/acme": {Parameters: map[string]any{"image": "engine:v9"}}},
	}
	reg := &fakeReg{catalog: []string{"engine"}, manifests: map[string]string{"engine:v9": digA}}
	set, err := BuildInUseSet(context.Background(), hostsOf("engine-1"), inv, specs, reg, baseCfg())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	hasDigest(t, set, digA)
}

// A registry host that is syntactically clean but could never appear as the
// host component of a reference is rejected: looksLikeRegistryHost is the
// property matching actually depends on, so anything failing it silently
// classifies every ref as foreign.
//
// The "myreg" case is why this check cannot be left to rule 4: with
// RegistryHosts=["myreg"] and refs spelled "myreg/engine:v9", hostComponent
// refuses to see "myreg" as a host at all, so the ref is never Qualified, rule
// 4 stays silent, the catalog gate misses, and the ref is skipped as foreign —
// while one digest-pinned container elsewhere keeps the fleet-wide count
// non-zero. Hence the per-case message assertions.
func TestBuildInUseSet_RejectsRegistryHostThatCannotAppearInARef(t *testing.T) {
	for _, tc := range []struct{ spelling, wantMsg string }{
		{"myreg", "can never be the host component"},             // no ".", no ":", not localhost
		{"user@reg.example:5000", "userinfo, query or fragment"}, // userinfo
		{"reg.example:5000?x=1", "userinfo, query or fragment"},  // query
		{"reg.example:5000#frag", "userinfo, query or fragment"}, // fragment
	} {
		t.Run(tc.spelling, func(t *testing.T) {
			inv, specs, reg := tagOnlyFleet()
			cfg := baseCfg()
			cfg.RegistryHosts = []string{tc.spelling}
			set, err := BuildInUseSet(context.Background(), hostsOf("engine-1"), inv, specs, reg, cfg)
			mustAbort(t, set, err)
			if !strings.Contains(err.Error(), "registry host") || !strings.Contains(err.Error(), tc.wantMsg) {
				t.Fatalf("want a registry-host error containing %q, got %v", tc.wantMsg, err)
			}
		})
	}
}

// The looksLikeRegistryHost rejection above is load-bearing in a case rule 4
// structurally cannot reach. Without it, "myreg" is accepted, every
// "myreg/engine:v9" ref is skipped as foreign (hostComponent does not see
// "myreg" as a host, so Qualified is never set), and a single digest-pinned
// container keeps the fleet-wide count non-zero — the run proceeds and deletes
// the manifest behind every tag.
func TestBuildInUseSet_UnhostlikeRegistryHostIsNotCaughtByAnyOtherRule(t *testing.T) {
	inv := &fakeInv{hosts: map[string]invEntry{
		// One digest-pinned container: enough to defeat the fleet-wide-zero rule.
		"engine-1": {obs: []instance.Observed{obs("engine", "acme", digB)}, fresh: freshOK()},
	}}
	specs := &fakeSpecs{
		keys:  map[string][]store.SpecKey{"engine-1": {{Template: "engine", Slug: "acme"}}},
		specs: map[string]store.Spec{"engine-1/engine/acme": {Parameters: map[string]any{"image": "myreg/engine:v9"}}},
	}
	reg := &fakeReg{catalog: []string{"engine"}, manifests: map[string]string{"engine:v9": digA}}
	cfg := baseCfg()
	cfg.RegistryHosts = []string{"myreg"}
	set, err := BuildInUseSet(context.Background(), hostsOf("engine-1"), inv, specs, reg, cfg)
	mustAbort(t, set, err)
	if !strings.Contains(err.Error(), "can never be the host component") {
		t.Fatalf("only the looksLikeRegistryHost check can catch this; got %v", err)
	}
}

// "localhost" is a legitimate registry host component.
func TestBuildInUseSet_AcceptsLocalhostRegistryHost(t *testing.T) {
	inv := &fakeInv{hosts: map[string]invEntry{"engine-1": {fresh: freshOK()}}}
	specs := &fakeSpecs{
		keys:  map[string][]store.SpecKey{"engine-1": {{Template: "engine", Slug: "acme"}}},
		specs: map[string]store.Spec{"engine-1/engine/acme": {Parameters: map[string]any{"image": "localhost/engine:v9"}}},
	}
	reg := &fakeReg{catalog: []string{"engine"}, manifests: map[string]string{"engine:v9": digA}}
	cfg := baseCfg()
	cfg.RegistryHosts = []string{"localhost"}
	set, err := BuildInUseSet(context.Background(), hostsOf("engine-1"), inv, specs, reg, cfg)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	hasDigest(t, set, digA)
}

// MINOR (a): a bare registry host with no repository path is not addressable
// and must abort, not be skipped as foreign — the surviving mutation.
func TestBuildInUseSet_AbortsOnBareRegistryHostRef(t *testing.T) {
	inv := &fakeInv{hosts: map[string]invEntry{
		"engine-1": {obs: []instance.Observed{obsLive("engine", "acme", digA)}, fresh: freshOK()},
	}}
	specs := &fakeSpecs{
		keys:  map[string][]store.SpecKey{"engine-1": {{Template: "engine", Slug: "acme"}}},
		specs: map[string]store.Spec{"engine-1/engine/acme": {Parameters: map[string]any{"image": "docker.io"}}},
	}
	set, err := BuildInUseSet(context.Background(), hostsOf("engine-1"), inv, specs,
		&fakeReg{catalog: []string{"engine"}}, baseCfg())
	mustAbort(t, set, err)
	if !strings.Contains(err.Error(), "repository") {
		t.Fatalf("want a cannot-derive-a-repository abort, got %v", err)
	}
}

// MINOR (c): an aborted result must not look like a usable empty set to a
// caller that logs the error and carries on — Has() would answer false for
// every digest in the registry.
func TestInUseSet_ValidOnlyOnSuccess(t *testing.T) {
	inv := &fakeInv{hosts: map[string]invEntry{
		"engine-1": {obs: []instance.Observed{obsLive("engine", "acme", digA)}, fresh: freshOK()},
	}}
	ok, err := BuildInUseSet(context.Background(), hostsOf("engine-1"), inv, &fakeSpecs{}, &fakeReg{catalog: []string{"engine"}}, baseCfg())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !ok.Valid {
		t.Fatal("a successful set must be Valid")
	}
	bad, err := BuildInUseSet(context.Background(), hostsOf(), &fakeInv{}, &fakeSpecs{}, &fakeReg{}, baseCfg())
	if err == nil {
		t.Fatal("want an abort")
	}
	if bad.Valid {
		t.Fatal("an aborted set must not be Valid")
	}
}

// The infra skip keys off the pod's InfraID. An app container that merely
// looks like one — named "*-infra", and image-less because its inspect failed
// (PodList swallows that error) — must still abort.
func TestBuildInUseSet_AbortsOnImagelessAppContainerNamedInfra(t *testing.T) {
	o := instance.Observed{Template: "engine", Slug: "acme", Containers: []instance.ObservedContainer{
		{Name: "1fd9c02bf4f0-infra", IsInfra: true},
		{Name: "engine-acme-infra"}, // declared "infra" in the template; inspect failed
		{Name: "engine", Image: digA, ImageTag: testRegistryHost + "/engine@" + digA},
	}}
	inv := &fakeInv{hosts: map[string]invEntry{"engine-1": {obs: []instance.Observed{o}, fresh: freshOK()}}}
	set, err := BuildInUseSet(context.Background(), hostsOf("engine-1"), inv, &fakeSpecs{}, &fakeReg{catalog: []string{"engine"}}, baseCfg())
	mustAbort(t, set, err)
}

// --- review round 4 ---------------------------------------------------------

// CRITICAL: the bare-digest short-circuit must recognise a MANIFEST digest,
// not merely "something matching imgregistry's digestRe". That pattern's
// algorithm part is any lowercase alphanumeric run, so an unqualified
// "<repo>:<40-hex git sha>" — a very common tagging convention — matches it and
// is claimed as a digest before splitRef ever runs.
//
// Probed against the pre-fix code: err=nil, Valid=true, and the protected set
// was literally map["engine:9b2c3d4e…"] with ZERO registry calls. Nothing else
// fires: the ref is unqualified so rule 4 sees nothing, the map is non-empty so
// the fleet-wide-zero rule is satisfied, and the outcome is Covered so no gap
// is recorded. The manifest that instance actually needs goes unprotected and
// is deleted.
func TestBuildInUseSet_UnqualifiedGitShaTagIsNotAManifestDigest(t *testing.T) {
	const ref = "engine:9b2c3d4e5f60718293a4b5c6d7e8f90123456789" // 40-hex git sha as a TAG
	inv := &fakeInv{hosts: map[string]invEntry{"engine-1": {fresh: freshOK()}}}
	specs := &fakeSpecs{
		keys:  map[string][]store.SpecKey{"engine-1": {{Template: "engine", Slug: "acme"}}},
		specs: map[string]store.Spec{"engine-1/engine/acme": {Parameters: map[string]any{"image": ref}}},
	}
	reg := &fakeReg{catalog: []string{"engine"}, manifests: map[string]string{ref: digA}}

	set, err := BuildInUseSet(context.Background(), hostsOf("engine-1"), inv, specs, reg, baseCfg())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	hasDigest(t, set, digA)
	if reg.calls != 1 {
		t.Fatalf("a tag must be resolved through the registry, got %d Manifest calls", reg.calls)
	}
	if _, ok := set.Digests[ref]; ok {
		t.Fatalf("the reference itself was protected as if it were a digest: %v", set.Digests)
	}
}

// The host-qualified spelling of the same tag was never affected — the "/"
// breaks digestRe before the algorithm part can match. Probed (calls=1,
// digest resolved) rather than assumed, and pinned here so a future
// "simplification" of the gate cannot quietly change it.
func TestBuildInUseSet_QualifiedGitShaTagResolvesThroughTheRegistry(t *testing.T) {
	const tag = "9b2c3d4e5f60718293a4b5c6d7e8f90123456789"
	inv := &fakeInv{hosts: map[string]invEntry{"engine-1": {fresh: freshOK()}}}
	specs := &fakeSpecs{
		keys: map[string][]store.SpecKey{"engine-1": {{Template: "engine", Slug: "acme"}}},
		specs: map[string]store.Spec{"engine-1/engine/acme": {Parameters: map[string]any{
			"image": testRegistryHost + "/engine:" + tag,
		}}},
	}
	reg := &fakeReg{catalog: []string{"engine"}, manifests: map[string]string{"engine:" + tag: digA}}

	set, err := BuildInUseSet(context.Background(), hostsOf("engine-1"), inv, specs, reg, baseCfg())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	hasDigest(t, set, digA)
	if reg.calls != 1 {
		t.Fatalf("want 1 Manifest call, got %d", reg.calls)
	}
}

// A digest whose algorithm is neither sha256 nor sha512 is not something
// podman's ImageDigest (image.Digest().String(), always a registered
// algorithm) can produce, so it must go down the ordinary parsing path rather
// than be trusted as a bare digest.
func TestBuildInUseSet_UnknownDigestAlgorithmIsNotTrustedAsABareDigest(t *testing.T) {
	const ref = "md5:" + "aaaa000000000000000000000000000000000000000000000000000000000001"
	inv := &fakeInv{hosts: map[string]invEntry{"engine-1": {fresh: freshOK()}}}
	specs := &fakeSpecs{
		keys:  map[string][]store.SpecKey{"engine-1": {{Template: "engine", Slug: "acme"}}},
		specs: map[string]store.Spec{"engine-1/engine/acme": {Parameters: map[string]any{"image": ref}}},
	}
	// "md5" is not in the catalog, so the ordinary path classifies it foreign —
	// and the fleet-wide-zero rule then aborts. Either way it must NOT end up
	// protected verbatim.
	reg := &fakeReg{catalog: []string{"engine"}}
	set, _ := BuildInUseSet(context.Background(), hostsOf("engine-1"), inv, specs, reg, baseCfg())
	if _, ok := set.Digests[ref]; ok {
		t.Fatalf("an unknown-algorithm ref was trusted as a manifest digest: %v", set.Digests)
	}
}

// Rule 4 (nothing host-qualified matched our registry) exists to catch a
// configured host that is stale or a typo. A ref qualified to a well-known
// PUBLIC registry is no evidence either way about how our own registry is
// spelled, so counting it turns a correct configuration into a permanent
// block that no operator action short of editing a spec clears.
//
// Probed on the pre-fix code: a bare-digest observed image + one
// "docker.io/library/postgres:16" sidecar parameter gave qualifiedSeen=1,
// qualifiedMatched=0 and aborted.
func TestBuildInUseSet_PublicRegistryRefsAloneDoNotTripTheMatchRule(t *testing.T) {
	inv := &fakeInv{hosts: map[string]invEntry{
		"engine-1": {obs: []instance.Observed{obs("engine", "acme", digA)}, fresh: freshOK()},
	}}
	specs := &fakeSpecs{
		keys: map[string][]store.SpecKey{"engine-1": {{Template: "engine", Slug: "acme"}}},
		specs: map[string]store.Spec{"engine-1/engine/acme": {Parameters: map[string]any{
			"pg_image":  "docker.io/library/postgres:16",
			"ls_image":  "quay.io/litestream/litestream:0.5.15",
			"cbg_image": "codeberg.org/example/thing:1",
		}}},
	}
	reg := &fakeReg{catalog: []string{"engine", "postgres"}}
	set, err := BuildInUseSet(context.Background(), hostsOf("engine-1"), inv, specs, reg, baseCfg())
	if err != nil {
		t.Fatalf("a fleet whose only qualified refs are public must not abort: %v", err)
	}
	hasDigest(t, set, digA)
}

// The carve-out above must not blunt the rule: a ref qualified to a
// NON-public host that does not match our configured one is still the
// misconfiguration signal, and still aborts.
func TestBuildInUseSet_PrivateWrongHostStillTripsTheMatchRule(t *testing.T) {
	inv := &fakeInv{hosts: map[string]invEntry{
		"engine-1": {obs: []instance.Observed{obs("engine", "acme", digA)}, fresh: freshOK()},
	}}
	specs := &fakeSpecs{
		keys: map[string][]store.SpecKey{"engine-1": {{Template: "engine", Slug: "acme"}}},
		specs: map[string]store.Spec{"engine-1/engine/acme": {Parameters: map[string]any{
			"image":    "reg.example:5000/engine:v9",
			"pg_image": "docker.io/library/postgres:16",
		}}},
	}
	cfg := baseCfg()
	cfg.RegistryHosts = []string{"reg2.example:5000"} // well-formed, and wrong
	reg := &fakeReg{catalog: []string{"engine"}, manifests: map[string]string{"engine:v9": digB}}
	set, err := BuildInUseSet(context.Background(), hostsOf("engine-1"), inv, specs, reg, cfg)
	mustAbort(t, set, err)
	if !strings.Contains(err.Error(), "no host-qualified") {
		t.Fatalf("want the no-match rule, got %v", err)
	}
}

// --- review round 5 ---------------------------------------------------------

// N1: the bare-digest gate must agree with its own safety claim. sha384 IS a
// registered go-digest algorithm (algorithm.go:34, alongside SHA256/SHA512),
// so excluding it lost something real; and an unrecognised algorithm did NOT
// "fall through to an abort" — probed on the pre-fix code, "sha384:<96 hex>",
// "v1:<64 hex>" and "md5:<64 hex>" each returned err=nil, Valid=true, and
// showed up only as a ForeignSkipped entry, because splitRef turned the
// algorithm into a repository name that missed the catalog and was then marked
// Covered AND Foreign — invisible to the non-digest fatal check too. Silent
// under-protection.
func TestBuildInUseSet_BareDigestAlgorithmGate(t *testing.T) {
	sha384 := "sha384:" + strings.Repeat("cd", 48) // registered, 96 hex
	sha512 := "sha512:" + strings.Repeat("ef", 64) // registered, 128 hex
	for _, tc := range []struct {
		name, ref string
		// wantProtected is the digest the ref itself must contribute, "" when
		// the ref must abort the run instead.
		wantProtected string
		wantErrMsg    string
	}{
		{name: "sha384_is_registered_and_trusted", ref: sha384, wantProtected: sha384},
		{name: "sha512_is_registered_and_trusted", ref: sha512, wantProtected: sha512},
		{name: "sha256_wrong_length_is_corrupt", ref: "sha256:" + strings.Repeat("ab", 48),
			wantErrMsg: "digest-shaped but is not a digest"},
		{name: "unregistered_algo_at_digest_length", ref: "md5:" + strings.Repeat("bb", 32),
			wantErrMsg: "digest-shaped but is not a digest"},
		{name: "unregistered_algo_v1", ref: "v1:" + strings.Repeat("aa", 32),
			wantErrMsg: "digest-shaped but is not a digest"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// One ordinary digest-pinned container, so the fleet-wide-zero rule
			// can never be what produces (or hides) the result.
			inv := &fakeInv{hosts: map[string]invEntry{
				"engine-1": {obs: []instance.Observed{obs("engine", "acme", digB)}, fresh: freshOK()},
			}}
			specs := &fakeSpecs{
				keys:  map[string][]store.SpecKey{"engine-1": {{Template: "engine", Slug: "acme"}}},
				specs: map[string]store.Spec{"engine-1/engine/acme": {Parameters: map[string]any{"image": tc.ref}}},
			}
			reg := &fakeReg{catalog: []string{"engine"}}
			set, err := BuildInUseSet(context.Background(), hostsOf("engine-1"), inv, specs, reg, baseCfg())
			if tc.wantErrMsg != "" {
				mustAbort(t, set, err)
				if !strings.Contains(err.Error(), tc.wantErrMsg) {
					t.Fatalf("want %q, got %v", tc.wantErrMsg, err)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			hasDigest(t, set, tc.wantProtected)
			for _, f := range set.ForeignSkipped {
				if f == tc.ref {
					t.Fatalf("a registered-algorithm digest was skipped as foreign: %v", set.ForeignSkipped)
				}
			}
		})
	}
}

// The gate must NOT swallow an unqualified "<repo>:<40-hex git sha>": 40 is not
// the hex length of any registered algorithm, so it stays a tag. This is the
// round-4 Critical, re-pinned against the round-5 rewrite of the gate.
func TestBuildInUseSet_GitShaTagSurvivesTheAlgorithmGate(t *testing.T) {
	const ref = "engine:9b2c3d4e5f60718293a4b5c6d7e8f90123456789"
	inv := &fakeInv{hosts: map[string]invEntry{"engine-1": {fresh: freshOK()}}}
	specs := &fakeSpecs{
		keys:  map[string][]store.SpecKey{"engine-1": {{Template: "engine", Slug: "acme"}}},
		specs: map[string]store.Spec{"engine-1/engine/acme": {Parameters: map[string]any{"image": ref}}},
	}
	reg := &fakeReg{catalog: []string{"engine"}, manifests: map[string]string{ref: digA}}
	set, err := BuildInUseSet(context.Background(), hostsOf("engine-1"), inv, specs, reg, baseCfg())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	hasDigest(t, set, digA)
	if reg.calls != 1 {
		t.Fatalf("want the tag resolved through the registry, got %d Manifest calls", reg.calls)
	}
}

// N2: three fail-closed guards that behave correctly but had no test behind
// them — all three survived the mutation battery. The third is load-bearing:
// without it a spec pinning ".../engine@<junk>" puts the junk string in the
// protected set with Covered=true while the real manifest goes unprotected and
// nothing errors — a #64-shaped false negative.
func TestBuildInUseSet_MalformedDigestGuards(t *testing.T) {
	for _, tc := range []struct {
		name, specImage string
		manifests       map[string]string
		wantErrMsg      string
	}{
		{
			name:       "registry_returns_no_digest",
			specImage:  testRegistryHost + "/engine:v9",
			manifests:  map[string]string{"engine:v9": ""},
			wantErrMsg: "registry returned no digest",
		},
		{
			name:       "registry_returns_malformed_digest",
			specImage:  testRegistryHost + "/engine:v9",
			manifests:  map[string]string{"engine:v9": "not-a-digest"},
			wantErrMsg: "registry returned a malformed digest",
		},
		{
			name:       "pinned_ref_carries_junk_after_at",
			specImage:  testRegistryHost + "/engine@junkjunkjunk",
			wantErrMsg: "malformed digest in reference",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// A digest-pinned container keeps the fleet-wide count non-zero, so
			// only the guard under test can produce the abort.
			inv := &fakeInv{hosts: map[string]invEntry{
				"engine-1": {obs: []instance.Observed{obs("engine", "acme", digB)}, fresh: freshOK()},
			}}
			specs := &fakeSpecs{
				keys:  map[string][]store.SpecKey{"engine-1": {{Template: "engine", Slug: "acme"}}},
				specs: map[string]store.Spec{"engine-1/engine/acme": {Parameters: map[string]any{"image": tc.specImage}}},
			}
			reg := &fakeReg{catalog: []string{"engine"}, manifests: tc.manifests}
			set, err := BuildInUseSet(context.Background(), hostsOf("engine-1"), inv, specs, reg, baseCfg())
			mustAbort(t, set, err)
			if !strings.Contains(err.Error(), tc.wantErrMsg) {
				t.Fatalf("want %q, got %v", tc.wantErrMsg, err)
			}
		})
	}
}

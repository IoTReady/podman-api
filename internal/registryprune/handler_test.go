package registryprune

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/iotready/podman-api/internal/imgregistry"
	"github.com/iotready/podman-api/internal/instance"
	"github.com/iotready/podman-api/internal/jobs"
	"github.com/iotready/podman-api/internal/store"
)

// --- a full imgregistry.Client fake ----------------------------------------
//
// Built against the real interface (compile-time asserted below) rather than a
// narrowed local one: the components this handler consumes were shipped with
// four Criticals that fixtures encoding an *assumed* shape passed straight
// over, so the fake must be able to lie only in ways the real client can.

type fakeClient struct {
	// catalogs is returned one entry per Catalog() call, the last entry
	// repeating. A registry whose catalog CHANGES between calls is the TOCTOU
	// this handler must be immune to (CI pushes a repo mid-run).
	catalogs     [][]string
	catalogCalls int
	catalogErr   error

	tags     map[string][]imgregistry.TagGroup
	tagsErr  map[string]error
	tagCalls []string
	// dropped is what ResolveTags reports it could not resolve for a repo —
	// the real client's silent-skip, made visible.
	dropped map[string][]imgregistry.DroppedTag

	manifests map[string]string // "repo:ref" -> digest

	deletes   []string // "repo@digest", in call order
	deleteErr map[string]error
}

var _ imgregistry.Client = (*fakeClient)(nil)

func (f *fakeClient) Catalog(context.Context) ([]string, error) {
	f.catalogCalls++
	if f.catalogErr != nil {
		return nil, f.catalogErr
	}
	if len(f.catalogs) == 0 {
		return nil, nil
	}
	i := f.catalogCalls - 1
	if i >= len(f.catalogs) {
		i = len(f.catalogs) - 1
	}
	return append([]string(nil), f.catalogs[i]...), nil
}

func (f *fakeClient) Tags(_ context.Context, repo string) ([]imgregistry.TagGroup, error) {
	f.tagCalls = append(f.tagCalls, repo)
	return f.tagsFor(repo)
}

func (f *fakeClient) tagsFor(repo string) ([]imgregistry.TagGroup, error) {
	if err, ok := f.tagsErr[repo]; ok {
		return nil, err
	}
	return append([]imgregistry.TagGroup(nil), f.tags[repo]...), nil
}

func (f *fakeClient) ResolveTags(_ context.Context, repo string) (imgregistry.TagListing, error) {
	f.tagCalls = append(f.tagCalls, repo)
	groups, err := f.tagsFor(repo)
	if err != nil {
		return imgregistry.TagListing{}, err
	}
	return imgregistry.TagListing{Groups: groups, Dropped: f.dropped[repo]}, nil
}

func (f *fakeClient) TagCount(ctx context.Context, repo string) (int, error) {
	g, err := f.tagsFor(repo)
	if err != nil {
		return 0, err
	}
	n := 0
	for _, tg := range g {
		n += len(tg.Tags)
	}
	return n, nil
}

func (f *fakeClient) Manifest(_ context.Context, repo, ref string) (imgregistry.Manifest, error) {
	d, ok := f.manifests[repo+":"+ref]
	if !ok {
		return imgregistry.Manifest{}, fmt.Errorf("manifest %s:%s: %w", repo, ref, imgregistry.ErrNotFound)
	}
	return imgregistry.Manifest{Digest: d}, nil
}

func (f *fakeClient) Delete(_ context.Context, repo, digest string) error {
	f.deletes = append(f.deletes, repo+"@"+digest)
	if err, ok := f.deleteErr[repo+"@"+digest]; ok {
		return err
	}
	return nil
}

// --- helpers ---------------------------------------------------------------

const (
	digD = "sha256:dddd000000000000000000000000000000000000000000000000000000000004"
	digE = "sha256:eeee000000000000000000000000000000000000000000000000000000000005"
	digF = "sha256:ffff000000000000000000000000000000000000000000000000000000000006"
)

// liveFleet is the minimum inputs that make BuildInUseSet succeed: one host,
// one running container pinned to digA by digest. Nothing in the handler tests
// depends on the in-use half beyond "it is Valid and contains digA".
func liveFleet() (*fakeInv, *fakeSpecs, fakeHosts) {
	inv := &fakeInv{hosts: map[string]invEntry{
		"engine-1": {obs: []instance.Observed{obsLive("engine", "valvo", digA)}, fresh: freshOK()},
	}}
	specs := &fakeSpecs{}
	return inv, specs, hostsOf("engine-1")
}

func newHandler(t *testing.T, reg imgregistry.Client) *Handler {
	t.Helper()
	inv, specs, hosts := liveFleet()
	return &Handler{
		Registry:  reg,
		Hosts:     hosts,
		Inventory: inv,
		Specs:     specs,
		Config:    baseCfg(),
	}
}

func runJob(t *testing.T, h *Handler, p Payload) (*store.Memory, store.Job, error) {
	t.Helper()
	mem := store.NewMemory()
	args, err := json.Marshal(p)
	if err != nil {
		t.Fatal(err)
	}
	j, err := mem.Enqueue(context.Background(), JobKind, args, "")
	if err != nil {
		t.Fatal(err)
	}
	jc := jobs.NewJobContext(mem, j.ID)
	runErr := h.Run(context.Background(), j, jc)
	got, err := mem.GetJob(context.Background(), j.ID)
	if err != nil {
		t.Fatal(err)
	}
	return mem, got, runErr
}

func stepText(j store.Job) string {
	var b strings.Builder
	for _, s := range j.Steps {
		b.WriteString(s.Step)
		b.WriteString(": ")
		b.WriteString(s.Detail)
		b.WriteString("\n")
	}
	return b.String()
}

func wantStepContaining(t *testing.T, j store.Job, want string) {
	t.Helper()
	if !strings.Contains(stepText(j), want) {
		t.Fatalf("no job step containing %q; steps were:\n%s", want, stepText(j))
	}
}

func wantNoDeletes(t *testing.T, f *fakeClient) {
	t.Helper()
	if len(f.deletes) != 0 {
		t.Fatalf("expected ZERO deletes, got %d: %v", len(f.deletes), f.deletes)
	}
}

func testPolicy() Policy {
	p := DefaultPolicy()
	return p
}

// orphanGroup is a bare-hex tag group — ClassShaOrphan, i.e. deletable.
func orphanGroup(digest, tag string) imgregistry.TagGroup {
	return imgregistry.TagGroup{Digest: digest, Tags: []string{tag}, Created: fixedNow.Add(-time.Hour)}
}

// --- tests -----------------------------------------------------------------

// Positive control. Without this, every "zero deletes" assertion below could
// pass against a handler that never deletes anything at all.
func TestRun_DeletesClassifiedGarbage(t *testing.T) {
	reg := &fakeClient{
		catalogs: [][]string{{"engine"}},
		tags: map[string][]imgregistry.TagGroup{
			"engine": {orphanGroup(digD, "abc1234")},
		},
	}
	h := newHandler(t, reg)
	_, job, err := runJob(t, h, Payload{Policy: testPolicy()})
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	if len(reg.deletes) != 1 || reg.deletes[0] != "engine@"+digD {
		t.Fatalf("want one delete of engine@%s, got %v", digD, reg.deletes)
	}
	wantStepContaining(t, job, "delete")
}

func TestRun_DryRunIssuesNoDeletes(t *testing.T) {
	reg := &fakeClient{
		catalogs: [][]string{{"engine"}},
		tags: map[string][]imgregistry.TagGroup{
			"engine": {orphanGroup(digD, "abc1234"), orphanGroup(digE, "beef123")},
		},
	}
	h := newHandler(t, reg)
	_, job, err := runJob(t, h, Payload{Policy: testPolicy(), DryRun: true})
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	wantNoDeletes(t, reg)
	// A dry run that reports nothing is useless: the full classification must
	// still land as job steps.
	wantStepContaining(t, job, "dry-run")
	wantStepContaining(t, job, digD)
}

// The deliberate divergence from registry-gc.sh. The script aborts the WHOLE
// run when any repo exceeds the tripwire, which is what silently disabled GC
// for all 28 repos for a week (2026-08-02). Here the offending repo is skipped
// and recorded; every other repo still proceeds.
func TestRun_TripwireSkipsOnlyTheOffendingRepo(t *testing.T) {
	pol := testPolicy()
	pol.MaxDeletesPerRepo = 2
	reg := &fakeClient{
		catalogs: [][]string{{"engine", "otp"}},
		tags: map[string][]imgregistry.TagGroup{
			"engine": {
				orphanGroup(digD, "abc1234"),
				orphanGroup(digE, "beef123"),
				orphanGroup(digF, "cafe456"),
			},
			"otp": {orphanGroup(digC, "dead789")},
		},
	}
	h := newHandler(t, reg)
	_, job, err := runJob(t, h, Payload{Policy: pol})
	if err != nil {
		t.Fatalf("tripwire must not fail the run: %v", err)
	}
	if len(reg.deletes) != 1 || reg.deletes[0] != "otp@"+digC {
		t.Fatalf("otp must still be pruned while engine is skipped; deletes: %v", reg.deletes)
	}
	wantStepContaining(t, job, "tripwire")
	wantStepContaining(t, job, "engine")
	if strings.Contains(stepText(job), "delete:engine") {
		t.Fatalf("engine must be skipped entirely; steps:\n%s", stepText(job))
	}
}

// The tripwire must be reported through the metrics seam too — the 2026-08-02
// incident went unnoticed for a week precisely because nothing surfaced it.
func TestRun_TripwireIsRecordedAsAMetric(t *testing.T) {
	pol := testPolicy()
	pol.MaxDeletesPerRepo = 1
	reg := &fakeClient{
		catalogs: [][]string{{"engine"}},
		tags: map[string][]imgregistry.TagGroup{
			"engine": {orphanGroup(digD, "abc1234"), orphanGroup(digE, "beef123")},
		},
	}
	m := &fakeMetrics{}
	h := newHandler(t, reg)
	h.Metrics = m
	if _, _, err := runJob(t, h, Payload{Policy: pol}); err != nil {
		t.Fatal(err)
	}
	if len(m.skipped) != 1 || m.skipped[0] != "engine" {
		t.Fatalf("want engine recorded as tripwire-skipped, got %v", m.skipped)
	}
}

// ErrUnsafeToPrune from the in-use pass must abort with ZERO deletes. It is the
// single guard standing between this job and the #64 incident.
func TestRun_UnsafeToPruneAbortsWithZeroDeletes(t *testing.T) {
	reg := &fakeClient{
		catalogs: [][]string{{"engine"}},
		tags: map[string][]imgregistry.TagGroup{
			"engine": {orphanGroup(digD, "abc1234")},
		},
	}
	h := newHandler(t, reg)
	// Host unreachable -> BuildInUseSet aborts. A stale snapshot under-reports
	// what is in use, so nothing may be deleted from it.
	h.Inventory = &fakeInv{hosts: map[string]invEntry{
		"engine-1": {obs: nil, fresh: instance.Freshness{Reachable: false, HasData: true, FetchedAt: fixedNow}},
	}}
	_, _, err := runJob(t, h, Payload{Policy: testPolicy()})
	if !errors.Is(err, ErrUnsafeToPrune) {
		t.Fatalf("want ErrUnsafeToPrune, got %v", err)
	}
	wantNoDeletes(t, reg)
}

// BuildInUseSet has one abort path that returns a POPULATED diagnostic struct
// (NonDigestObserved) whose Digests is nil. A handler that checked only err !=
// nil would already be safe there — but a handler that checked only Valid, or
// neither, would delete the entire registry. Valid is the documented gate, so
// assert the handler honours it independently of err.
func TestRun_InvalidSetAbortsWithZeroDeletes(t *testing.T) {
	reg := &fakeClient{
		catalogs: [][]string{{"engine"}},
		tags: map[string][]imgregistry.TagGroup{
			"engine": {orphanGroup(digD, "abc1234")},
		},
	}
	h := newHandler(t, reg)
	// The only way to produce Valid==false with a nil error is to stub the
	// builder — which is exactly the shape of the bug being guarded against.
	h.buildSet = func(context.Context) (InUseSet, error) {
		return InUseSet{Valid: false, Digests: nil}, nil
	}
	_, _, err := runJob(t, h, Payload{Policy: testPolicy()})
	if err == nil {
		t.Fatal("an invalid in-use set must fail the run")
	}
	wantNoDeletes(t, reg)
}

// A run must fetch the catalog exactly once and drive BOTH the protection pass
// and the classification pass from that one slice. CachingClient.Catalog is a
// pass-through, so two calls in one run can disagree: the in-use set is built
// from a 28-repo listing, CI pushes a repo mid-run, classification enumerates
// 29, and the new repo's digests were never protected.
func TestRun_CatalogFetchedOnceAndDrivesBothPasses(t *testing.T) {
	reg := &fakeClient{
		// Second and later calls see a repo that did not exist during the
		// protection pass.
		catalogs: [][]string{{"engine"}, {"engine", "pushed-mid-run"}},
		tags: map[string][]imgregistry.TagGroup{
			"engine":         {orphanGroup(digD, "abc1234")},
			"pushed-mid-run": {orphanGroup(digE, "beef123")},
		},
	}
	h := newHandler(t, reg)
	if _, _, err := runJob(t, h, Payload{Policy: testPolicy()}); err != nil {
		t.Fatal(err)
	}
	if reg.catalogCalls != 1 {
		t.Fatalf("catalog must be fetched exactly once per run, got %d calls", reg.catalogCalls)
	}
	for _, r := range reg.tagCalls {
		if r == "pushed-mid-run" {
			t.Fatal("a repo absent from the protection-time catalog must not be classified")
		}
	}
	for _, d := range reg.deletes {
		if strings.HasPrefix(d, "pushed-mid-run@") {
			t.Fatalf("deleted from a repo the protection pass never saw: %v", reg.deletes)
		}
	}
}

// Classify's doc comment states its precondition: once per TagGroup, never once
// per tag. A caller that flattens tags makes ClassShaIsLatest permanently
// unreachable — silently, because the tag stays kept either way — and turns
// "latest"'s own hex alias into a ClassShaOrphan delete candidate.
func TestRun_ClassifiesOncePerTagGroup(t *testing.T) {
	reg := &fakeClient{
		catalogs: [][]string{{"engine"}},
		tags: map[string][]imgregistry.TagGroup{
			"engine": {{Digest: digD, Tags: []string{"latest", "abc1234"}, Created: fixedNow}},
		},
	}
	h := newHandler(t, reg)
	_, job, err := runJob(t, h, Payload{Policy: testPolicy()})
	if err != nil {
		t.Fatal(err)
	}
	wantNoDeletes(t, reg)
	wantStepContaining(t, job, string(ClassShaIsLatest))
}

// The backstop: the in-use and protected sets are re-checked immediately before
// every DELETE, even though classification already excluded them. Classify
// returns ClassInUse for an in-use digest, so the only way to reach this code
// path is to hand the delete pass a plan that classification could not have
// produced — which is the point: the backstop exists for the case where the
// classification pass is wrong.
func TestDeletePass_BackstopRefusesInUseDigest(t *testing.T) {
	reg := &fakeClient{catalogs: [][]string{{"engine"}}}
	h := &Handler{Registry: reg, Config: baseCfg()}
	inUse := InUseSet{Valid: true, Digests: map[string]struct{}{digD: {}}}
	plan := []repoPlan{{repo: "engine", candidates: []candidate{{digest: digD, class: ClassShaOrphan}}}}
	mem := store.NewMemory()
	j, _ := mem.Enqueue(context.Background(), JobKind, nil, "")
	jc := jobs.NewJobContext(mem, j.ID)

	n, err := h.deletePass(context.Background(), jc, plan, inUse, inUse, map[string]struct{}{},
		map[string]struct{}{"engine": {}}, Payload{Policy: testPolicy()})
	if err != nil {
		t.Fatalf("the backstop must skip, not fail: %v", err)
	}
	if n != 0 {
		t.Fatalf("want 0 deletions, got %d", n)
	}
	wantNoDeletes(t, reg)
	got, _ := mem.GetJob(context.Background(), j.ID)
	wantStepContaining(t, got, "backstop")
}

func TestDeletePass_BackstopRefusesProtectedDigest(t *testing.T) {
	reg := &fakeClient{catalogs: [][]string{{"engine"}}}
	h := &Handler{Registry: reg, Config: baseCfg()}
	inUse := InUseSet{Valid: true, Digests: map[string]struct{}{digA: {}}}
	plan := []repoPlan{{repo: "engine", candidates: []candidate{{digest: digD, class: ClassShaOrphan}}}}
	mem := store.NewMemory()
	j, _ := mem.Enqueue(context.Background(), JobKind, nil, "")
	jc := jobs.NewJobContext(mem, j.ID)

	n, err := h.deletePass(context.Background(), jc, plan, inUse, inUse,
		map[string]struct{}{digD: {}}, map[string]struct{}{"engine": {}}, Payload{Policy: testPolicy()})
	if err != nil || n != 0 {
		t.Fatalf("want 0 deletions and no error, got n=%d err=%v", n, err)
	}
	wantNoDeletes(t, reg)
}

// A repo absent from the protection-time catalog listing is refused even if it
// somehow reached the plan — the same predicate that made it invisible to
// BuildInUseSet's protection pass.
func TestDeletePass_RefusesRepoOutsideProtectionCatalog(t *testing.T) {
	reg := &fakeClient{catalogs: [][]string{{"engine"}}}
	h := &Handler{Registry: reg, Config: baseCfg()}
	inUse := InUseSet{Valid: true, Digests: map[string]struct{}{digA: {}}}
	plan := []repoPlan{{repo: "ghost", candidates: []candidate{{digest: digD, class: ClassShaOrphan}}}}
	mem := store.NewMemory()
	j, _ := mem.Enqueue(context.Background(), JobKind, nil, "")
	jc := jobs.NewJobContext(mem, j.ID)

	n, err := h.deletePass(context.Background(), jc, plan, inUse, inUse, map[string]struct{}{},
		map[string]struct{}{"engine": {}}, Payload{Policy: testPolicy()})
	if err != nil || n != 0 {
		t.Fatalf("want 0 deletions and no error, got n=%d err=%v", n, err)
	}
	wantNoDeletes(t, reg)
}

func TestDeletePass_DedupsRepoDigest(t *testing.T) {
	reg := &fakeClient{catalogs: [][]string{{"engine"}}}
	h := &Handler{Registry: reg, Config: baseCfg()}
	inUse := InUseSet{Valid: true, Digests: map[string]struct{}{digA: {}}}
	plan := []repoPlan{{repo: "engine", candidates: []candidate{
		{digest: digD, class: ClassShaOrphan},
		{digest: digD, class: ClassFeatStale},
	}}}
	mem := store.NewMemory()
	j, _ := mem.Enqueue(context.Background(), JobKind, nil, "")
	jc := jobs.NewJobContext(mem, j.ID)

	n, err := h.deletePass(context.Background(), jc, plan, inUse, inUse, map[string]struct{}{},
		map[string]struct{}{"engine": {}}, Payload{Policy: testPolicy()})
	if err != nil {
		t.Fatal(err)
	}
	if n != 1 || len(reg.deletes) != 1 {
		t.Fatalf("a repo+digest must be deleted once, got n=%d deletes=%v", n, reg.deletes)
	}
}

// NonDigestObserved and ForeignSkipped are how an operator learns the
// protection pass had blind spots. NonDigestObserved is fatal inside
// BuildInUseSet, so it must be recorded on the ABORT path — where it would
// otherwise be swallowed with the error.
func TestRun_RecordsNonDigestObservedOnAbort(t *testing.T) {
	reg := &fakeClient{catalogs: [][]string{{"engine"}}}
	h := newHandler(t, reg)
	h.Inventory = &fakeInv{hosts: map[string]invEntry{
		// A bare 64-hex image ID, not a manifest digest: podman gave us an
		// image ID and the real manifest may be unprotected.
		"engine-1": {obs: []instance.Observed{obs("engine", "valvo",
			"aaaa000000000000000000000000000000000000000000000000000000000001")}, fresh: freshOK()},
	}}
	_, job, err := runJob(t, h, Payload{Policy: testPolicy()})
	if !errors.Is(err, ErrUnsafeToPrune) {
		t.Fatalf("want ErrUnsafeToPrune, got %v", err)
	}
	wantNoDeletes(t, reg)
	wantStepContaining(t, job, "non-digest")
}

func TestRun_RecordsForeignSkipped(t *testing.T) {
	reg := &fakeClient{
		catalogs: [][]string{{"engine"}},
		tags:     map[string][]imgregistry.TagGroup{"engine": {}},
	}
	h := newHandler(t, reg)
	h.Specs = &fakeSpecs{
		keys: map[string][]store.SpecKey{"engine-1": {{Template: "engine", Slug: "valvo"}}},
		specs: map[string]store.Spec{
			"engine-1/engine/valvo": {Parameters: map[string]any{
				"pg_image": "docker.io/library/postgres:16",
			}},
		},
	}
	_, job, err := runJob(t, h, Payload{Policy: testPolicy()})
	if err != nil {
		t.Fatalf("a foreign ref is advisory, not fatal: %v", err)
	}
	wantStepContaining(t, job, "foreign")
	wantStepContaining(t, job, "postgres")
}

// Fail closed: a repo whose tag listing cannot be read may hold a digest shared
// with another repo's protected tag, so its absence from the protected set is
// not evidence. Abort before deleting anything, anywhere.
func TestRun_TagListingErrorAbortsBeforeAnyDelete(t *testing.T) {
	reg := &fakeClient{
		catalogs: [][]string{{"broken", "engine"}},
		tags: map[string][]imgregistry.TagGroup{
			"engine": {orphanGroup(digD, "abc1234")},
		},
		tagsErr: map[string]error{"broken": imgregistry.ErrUnreachable},
	}
	h := newHandler(t, reg)
	_, _, err := runJob(t, h, Payload{Policy: testPolicy()})
	if err == nil {
		t.Fatal("an unreadable repo must abort the run")
	}
	wantNoDeletes(t, reg)
}

// Ordering: every repo is classified before any delete is issued. A per-repo
// delete-as-you-go reclaimed blobs still needed by a not-yet-classified repo
// (#64). Asserted by construction: the last Tags call must precede the first
// Delete call.
func TestRun_ClassifiesEveryRepoBeforeDeleting(t *testing.T) {
	reg := &fakeClient{catalogs: [][]string{{"engine", "otp"}}}
	reg.tags = map[string][]imgregistry.TagGroup{
		"engine": {orphanGroup(digD, "abc1234")},
		"otp":    {orphanGroup(digE, "beef123")},
	}
	var order []string
	obs := &orderingClient{inner: reg, order: &order}
	h := newHandler(t, obs)
	if _, _, err := runJob(t, h, Payload{Policy: testPolicy()}); err != nil {
		t.Fatal(err)
	}
	firstDelete, lastTags := -1, -1
	for i, ev := range order {
		if strings.HasPrefix(ev, "tags:") {
			lastTags = i
		}
		if strings.HasPrefix(ev, "delete:") && firstDelete < 0 {
			firstDelete = i
		}
	}
	if firstDelete < 0 || lastTags < 0 {
		t.Fatalf("expected both tag listings and deletes, got %v", order)
	}
	if lastTags > firstDelete {
		t.Fatalf("classification of every repo must complete before the first delete: %v", order)
	}
}

// Registry unreachable mid-delete must fail the job rather than be reported as
// a clean run — "unreachable" is never "already gone".
func TestRun_DeleteFailureFailsTheJob(t *testing.T) {
	reg := &fakeClient{
		catalogs:  [][]string{{"engine"}},
		tags:      map[string][]imgregistry.TagGroup{"engine": {orphanGroup(digD, "abc1234")}},
		deleteErr: map[string]error{"engine@" + digD: imgregistry.ErrUnreachable},
	}
	h := newHandler(t, reg)
	_, job, err := runJob(t, h, Payload{Policy: testPolicy()})
	if err == nil {
		t.Fatal("a failed delete must fail the job")
	}
	wantStepContaining(t, job, "FAILED")
}

// A zero Policy would leave "latest" out of ProtectedExact. Nothing becomes
// deletable by it today (an unrecognised tag is kept), but a policy that
// arrived empty is a wiring bug, and this job fails closed on those.
func TestRun_EmptyPolicyIsRejected(t *testing.T) {
	reg := &fakeClient{
		catalogs: [][]string{{"engine"}},
		tags:     map[string][]imgregistry.TagGroup{"engine": {orphanGroup(digD, "abc1234")}},
	}
	h := newHandler(t, reg)
	_, _, err := runJob(t, h, Payload{})
	if err == nil {
		t.Fatal("an empty policy must fail the run")
	}
	wantNoDeletes(t, reg)
}

// --- ordering observer -----------------------------------------------------

type orderingClient struct {
	inner *fakeClient
	order *[]string
}

var _ imgregistry.Client = (*orderingClient)(nil)

func (o *orderingClient) Catalog(ctx context.Context) ([]string, error) {
	*o.order = append(*o.order, "catalog")
	return o.inner.Catalog(ctx)
}

func (o *orderingClient) Tags(ctx context.Context, repo string) ([]imgregistry.TagGroup, error) {
	*o.order = append(*o.order, "tags:"+repo)
	return o.inner.Tags(ctx, repo)
}

func (o *orderingClient) ResolveTags(ctx context.Context, repo string) (imgregistry.TagListing, error) {
	*o.order = append(*o.order, "tags:"+repo)
	return o.inner.ResolveTags(ctx, repo)
}

func (o *orderingClient) TagCount(ctx context.Context, repo string) (int, error) {
	return o.inner.TagCount(ctx, repo)
}

func (o *orderingClient) Manifest(ctx context.Context, repo, ref string) (imgregistry.Manifest, error) {
	return o.inner.Manifest(ctx, repo, ref)
}

func (o *orderingClient) Delete(ctx context.Context, repo, digest string) error {
	*o.order = append(*o.order, "delete:"+repo+"@"+digest)
	return o.inner.Delete(ctx, repo, digest)
}

// --- metrics double --------------------------------------------------------

type fakeMetrics struct {
	results []string
	deleted map[string]int
	skipped []string
}

func (m *fakeMetrics) RunDone(result string) { m.results = append(m.results, result) }

func (m *fakeMetrics) ManifestsDeleted(repo string, n int) {
	if m.deleted == nil {
		m.deleted = map[string]int{}
	}
	m.deleted[repo] += n
}

func (m *fakeMetrics) RepoSkipped(repo string, candidates int) {
	m.skipped = append(m.skipped, repo)
}

// ---------------------------------------------------------------------------
// Review round 2 — the handler must not trust what imgregistry.Tags returns.
// ---------------------------------------------------------------------------

// C-1. HTTPClient.Tags resolves every tag concurrently and SILENTLY DROPS any
// whose manifest fetch failed (client.go: `if r.err != nil { continue }`),
// returning err == nil over the survivors. A blip on "latest"'s manifest fetch
// removes it from its group, and its bare-hex alias — which would have been
// sha-is-latest and kept — becomes its own group and classifies sha-orphan.
// A dropped protected tag also stops seeding the cross-repo protected set, so
// the loss is not confined to its own repo.
func TestRun_IncompleteTagListingAbortsBeforeAnyDelete(t *testing.T) {
	reg := &fakeClient{
		catalogs: [][]string{{"engine", "otp"}},
		tags: map[string][]imgregistry.TagGroup{
			// "latest" was dropped by a 500 on its manifest fetch; only its
			// bare-hex alias survived, now looking like an orphan.
			"engine": {orphanGroup(digD, "abc1234")},
			"otp":    {orphanGroup(digE, "beef123")},
		},
		dropped: map[string][]imgregistry.DroppedTag{"engine": {{Tag: "latest", Err: imgregistry.ErrUnreachable}}},
	}
	h := newHandler(t, reg)
	_, job, err := runJob(t, h, Payload{Policy: testPolicy()})
	if !errors.Is(err, ErrUnsafeToPrune) {
		t.Fatalf("an incomplete tag listing must abort the run, got %v", err)
	}
	// Aborts the WHOLE run, not just the affected repo: the dropped tag may be
	// what protects a digest in some other repo.
	wantNoDeletes(t, reg)
	wantStepContaining(t, job, "latest")
}

func TestRun_CompleteTagListingProceeds(t *testing.T) {
	reg := &fakeClient{
		catalogs:  [][]string{{"engine"}},
		tags:      map[string][]imgregistry.TagGroup{"engine": {orphanGroup(digD, "abc1234")}},
		manifests: map[string]string{},
	}
	h := newHandler(t, reg)
	if _, _, err := runJob(t, h, Payload{Policy: testPolicy()}); err != nil {
		t.Fatal(err)
	}
	if len(reg.deletes) != 1 {
		t.Fatalf("a complete listing must still prune, got %v", reg.deletes)
	}
}

// C-2. imgregistry leaves TagGroup.Created zero both when there is genuinely no
// config blob (a multi-arch index) and when the blob fetch FAILED. policy.go
// treats zero Created on a feat-* tag as stale — a deliberate port of
// registry-gc.sh, justified by the index case. The handler cannot tell the two
// apart, so it refuses to act on either: a feat-* tag created seconds ago whose
// blob GET returned 500 would otherwise be deleted.
func TestRun_FeatTagWithUnknownAgeIsNotDeleted(t *testing.T) {
	reg := &fakeClient{
		catalogs: [][]string{{"engine"}},
		tags: map[string][]imgregistry.TagGroup{
			"engine": {{Digest: digD, Tags: []string{"feat-brand-new"}}}, // Created zero
		},
	}
	h := newHandler(t, reg)
	_, job, err := runJob(t, h, Payload{Policy: testPolicy()})
	if err != nil {
		t.Fatal(err)
	}
	wantNoDeletes(t, reg)
	wantStepContaining(t, job, "age-unknown")
}

// The control: an aged feat-* tag with a KNOWN creation time is still deleted.
// Without this, the C-2 fix could have been "never delete feat-* at all".
func TestRun_AgedFeatTagIsStillDeleted(t *testing.T) {
	reg := &fakeClient{
		catalogs: [][]string{{"engine"}},
		tags: map[string][]imgregistry.TagGroup{
			"engine": {{Digest: digD, Tags: []string{"feat-old"}, Created: fixedNow.AddDate(0, 0, -60)}},
		},
	}
	h := newHandler(t, reg)
	if _, _, err := runJob(t, h, Payload{Policy: testPolicy()}); err != nil {
		t.Fatal(err)
	}
	if len(reg.deletes) != 1 {
		t.Fatalf("an aged feat-* tag must still be deleted, got %v", reg.deletes)
	}
}

// I-1. The backstop re-read the SAME in-memory set built minutes earlier, so it
// defended only against a bug in classification, not against the fleet changing
// during the run (Tags on "engine" alone measures ~21s cold, times 28 repos).
// A deploy landing inside that window pushes an image whose CI tag is bare hex:
// absent from the snapshot, its spec not yet in the store — sha-orphan, deleted,
// and the instance can never be rebuilt. That is #64 exactly.
func TestRun_RebuildsInUseSetBeforeDeleting(t *testing.T) {
	reg := &fakeClient{
		catalogs: [][]string{{"engine"}},
		tags:     map[string][]imgregistry.TagGroup{"engine": {orphanGroup(digD, "abc1234")}},
	}
	h := newHandler(t, reg)
	calls := 0
	h.buildSet = func(context.Context) (InUseSet, error) {
		calls++
		if calls == 1 {
			return InUseSet{Valid: true, Digests: map[string]struct{}{digA: {}}}, nil
		}
		// A deploy landed while the catalog was being classified.
		return InUseSet{Valid: true, Digests: map[string]struct{}{digA: {}, digD: {}}}, nil
	}
	_, job, err := runJob(t, h, Payload{Policy: testPolicy()})
	if err != nil {
		t.Fatal(err)
	}
	if calls != 2 {
		t.Fatalf("the in-use set must be rebuilt before deleting, got %d builds", calls)
	}
	wantNoDeletes(t, reg)
	wantStepContaining(t, job, "backstop")
}

// The converse, at the delete pass itself: a candidate must be absent from
// BOTH snapshots. Neither one alone is authoritative — the first predates the
// classification pass, the second postdates it, and an instance can appear or
// disappear on either side of that window.
func TestDeletePass_ProtectedByEitherSnapshot(t *testing.T) {
	for _, tc := range []struct {
		name       string
		old, fresh InUseSet
	}{
		{"only the pre-classification snapshot knows it",
			InUseSet{Valid: true, Digests: map[string]struct{}{digD: {}}},
			InUseSet{Valid: true, Digests: map[string]struct{}{digA: {}}}},
		{"only the pre-delete snapshot knows it",
			InUseSet{Valid: true, Digests: map[string]struct{}{digA: {}}},
			InUseSet{Valid: true, Digests: map[string]struct{}{digD: {}}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			reg := &fakeClient{catalogs: [][]string{{"engine"}}}
			h := &Handler{Registry: reg, Config: baseCfg()}
			plan := []repoPlan{{repo: "engine", candidates: []candidate{{digest: digD, class: ClassShaOrphan}}}}
			mem := store.NewMemory()
			j, _ := mem.Enqueue(context.Background(), JobKind, nil, "")
			jc := jobs.NewJobContext(mem, j.ID)
			n, err := h.deletePass(context.Background(), jc, plan, tc.old, tc.fresh,
				map[string]struct{}{}, map[string]struct{}{"engine": {}}, Payload{Policy: testPolicy()})
			if err != nil || n != 0 {
				t.Fatalf("want 0 deletions and no error, got n=%d err=%v", n, err)
			}
			wantNoDeletes(t, reg)
		})
	}
}

func TestRun_SecondInUseBuildFailureAbortsWithZeroDeletes(t *testing.T) {
	reg := &fakeClient{
		catalogs: [][]string{{"engine"}},
		tags:     map[string][]imgregistry.TagGroup{"engine": {orphanGroup(digD, "abc1234")}},
	}
	h := newHandler(t, reg)
	calls := 0
	h.buildSet = func(context.Context) (InUseSet, error) {
		calls++
		if calls == 1 {
			return InUseSet{Valid: true, Digests: map[string]struct{}{digA: {}}}, nil
		}
		return InUseSet{}, fmt.Errorf("%w: host went unreachable mid-run", ErrUnsafeToPrune)
	}
	_, _, err := runJob(t, h, Payload{Policy: testPolicy()})
	if !errors.Is(err, ErrUnsafeToPrune) {
		t.Fatalf("want ErrUnsafeToPrune, got %v", err)
	}
	wantNoDeletes(t, reg)
}

// I-2. A nil catalog set must refuse EVERYTHING (a nil map lookup returns
// ok==false, which is fail-closed). The `if inCatalog != nil` wrapper inverted
// that: nil disabled the refusal and every repo passed.
func TestDeletePass_NilCatalogRefusesEverything(t *testing.T) {
	reg := &fakeClient{catalogs: [][]string{{"engine"}}}
	h := &Handler{Registry: reg, Config: baseCfg()}
	inUse := InUseSet{Valid: true, Digests: map[string]struct{}{digA: {}}}
	plan := []repoPlan{{repo: "engine", candidates: []candidate{{digest: digD, class: ClassShaOrphan}}}}
	mem := store.NewMemory()
	j, _ := mem.Enqueue(context.Background(), JobKind, nil, "")
	jc := jobs.NewJobContext(mem, j.ID)

	n, err := h.deletePass(context.Background(), jc, plan, inUse, inUse, map[string]struct{}{}, nil, Payload{Policy: testPolicy()})
	if err != nil || n != 0 {
		t.Fatalf("a nil catalog set must refuse everything, got n=%d err=%v", n, err)
	}
	wantNoDeletes(t, reg)
}

// Minor. A malformed payload must still record a run outcome — #220's alerting
// is built on that metric, and a job that fails without one is invisible.
func TestRun_MalformedPayloadRecordsRunOutcome(t *testing.T) {
	reg := &fakeClient{catalogs: [][]string{{"engine"}}}
	h := newHandler(t, reg)
	m := &fakeMetrics{}
	h.Metrics = m
	mem := store.NewMemory()
	j, _ := mem.Enqueue(context.Background(), JobKind, json.RawMessage(`{"policy":`), "")
	if err := h.Run(context.Background(), j, jobs.NewJobContext(mem, j.ID)); err == nil {
		t.Fatal("a malformed payload must fail the job")
	}
	if len(m.results) != 1 {
		t.Fatalf("want exactly one run outcome recorded, got %v", m.results)
	}
}

// Minor. The "registry is down, stop hammering it" break keyed on firstErr, so
// an earlier unrelated failure pinned firstErr and a LATER outage no longer
// stopped the per-repo loop.
func TestDeletePass_LaterUnreachableStopsTheLoop(t *testing.T) {
	reg := &fakeClient{
		catalogs: [][]string{{"a", "b", "c"}},
		deleteErr: map[string]error{
			"a@" + digD: errors.New("some other failure"),
			"b@" + digE: imgregistry.ErrUnreachable,
		},
	}
	h := &Handler{Registry: reg, Config: baseCfg()}
	inUse := InUseSet{Valid: true, Digests: map[string]struct{}{digA: {}}}
	plan := []repoPlan{
		{repo: "a", candidates: []candidate{{digest: digD, class: ClassShaOrphan}}},
		{repo: "b", candidates: []candidate{{digest: digE, class: ClassShaOrphan}}},
		{repo: "c", candidates: []candidate{{digest: digF, class: ClassShaOrphan}}},
	}
	mem := store.NewMemory()
	j, _ := mem.Enqueue(context.Background(), JobKind, nil, "")
	jc := jobs.NewJobContext(mem, j.ID)
	inCat := map[string]struct{}{"a": {}, "b": {}, "c": {}}

	if _, err := h.deletePass(context.Background(), jc, plan, inUse, inUse,
		map[string]struct{}{}, inCat, Payload{Policy: testPolicy()}); err == nil {
		t.Fatal("want an error")
	}
	if len(reg.deletes) != 2 {
		t.Fatalf("an unreachable registry must stop the loop before repo c; attempted %v", reg.deletes)
	}
}

// m12. The err and !Valid abort paths must be distinguishable in the job record.
// They were not: every error path also returns Valid==false, so deleting the err
// check left the suite green.
func TestRun_AbortStepNamesTheUnderlyingReason(t *testing.T) {
	reg := &fakeClient{catalogs: [][]string{{"engine"}}}
	h := newHandler(t, reg)
	h.Inventory = &fakeInv{hosts: map[string]invEntry{
		"engine-1": {obs: nil, fresh: instance.Freshness{Reachable: false, HasData: true, FetchedAt: fixedNow}},
	}}
	_, job, err := runJob(t, h, Payload{Policy: testPolicy()})
	if !errors.Is(err, ErrUnsafeToPrune) {
		t.Fatalf("want ErrUnsafeToPrune, got %v", err)
	}
	// The specific cause, not the generic "not valid" the Valid gate records.
	wantStepContaining(t, job, "unreachable")
	if strings.Contains(stepText(job), "in-use set is not valid") {
		t.Fatalf("the error path must record its own reason, not the Valid gate's:\n%s", stepText(job))
	}
}

// The Policy inside a Payload survives the job-args round trip. This rests on
// regexp.Regexp's stdlib MarshalText/UnmarshalText (Go 1.21+) — nothing in this
// package declares it, so a future Policy field that is not JSON-safe would
// regress silently, exactly where it cannot be seen: a nil CalVer reclassifies
// every calendar-versioned release tag as unclassified.
func TestPayloadRoundTripsThroughJobArgs(t *testing.T) {
	in := Payload{Policy: DefaultPolicy(), DryRun: true, SkipBlobGC: true}
	raw, err := json.Marshal(in)
	if err != nil {
		t.Fatal(err)
	}
	var out Payload
	if err := json.Unmarshal(raw, &out); err != nil {
		t.Fatal(err)
	}
	if out.Policy.CalVer == nil || !out.Policy.CalVer.MatchString("2026.08.07") {
		t.Fatalf("CalVer did not survive the round trip: %#v", out.Policy.CalVer)
	}
	extra, ok := out.Policy.ExtraPerRepo["otp"]
	if !ok || extra == nil || !extra.MatchString("runtime-base") {
		t.Fatalf("ExtraPerRepo did not survive the round trip: %#v", out.Policy.ExtraPerRepo)
	}
	if len(out.Policy.ProtectedExact) != len(in.Policy.ProtectedExact) ||
		out.Policy.RetentionDays != in.Policy.RetentionDays ||
		out.Policy.MaxDeletesPerRepo != in.Policy.MaxDeletesPerRepo ||
		out.Policy.DeleteUnrecognised != in.Policy.DeleteUnrecognised ||
		out.DryRun != in.DryRun || out.SkipBlobGC != in.SkipBlobGC {
		t.Fatalf("payload did not round trip: %#v -> %#v", in, out)
	}
	// And the classification it drives is byte-identical either way.
	tg := imgregistry.TagGroup{Digest: digD, Tags: []string{"2026.08.07"}, Created: fixedNow}
	empty := InUseSet{Valid: true, Digests: map[string]struct{}{}}
	if got, want := Classify("engine", tg, empty, nil, out.Policy, fixedNow),
		Classify("engine", tg, empty, nil, in.Policy, fixedNow); got != want {
		t.Fatalf("classification diverged across the round trip: %s vs %s", got, want)
	}
}

// ---------------------------------------------------------------------------
// Review round 3.
// ---------------------------------------------------------------------------

// Important B. The cross-repo protected-digest seeding is load-bearing and was
// untested: digD is reachable via "latest" in otp and only via a bare-hex tag
// in engine. Without the seeding, engine's group classifies sha-orphan and the
// digest — which "latest" still points at — is deleted out from under it.
// This is the property that justifies C-1's abort-the-whole-run choice.
func TestRun_DigestProtectedByNameInAnotherRepoIsKept(t *testing.T) {
	reg := &fakeClient{
		catalogs: [][]string{{"engine", "otp"}},
		tags: map[string][]imgregistry.TagGroup{
			"engine": {orphanGroup(digD, "abc1234")},
			"otp":    {{Digest: digD, Tags: []string{"latest"}, Created: fixedNow}},
		},
	}
	h := newHandler(t, reg)
	_, job, err := runJob(t, h, Payload{Policy: testPolicy()})
	if err != nil {
		t.Fatal(err)
	}
	wantNoDeletes(t, reg)
	wantStepContaining(t, job, string(ClassSharesProtected))
}

// Minor 9. A dry run deletes nothing, so re-deriving the in-use set costs a
// full fleet walk plus registry manifest resolutions for no safety at all.
func TestRun_DryRunDoesNotRebuildInUseSet(t *testing.T) {
	reg := &fakeClient{
		catalogs: [][]string{{"engine"}},
		tags:     map[string][]imgregistry.TagGroup{"engine": {orphanGroup(digD, "abc1234")}},
	}
	h := newHandler(t, reg)
	calls := 0
	h.buildSet = func(context.Context) (InUseSet, error) {
		calls++
		return InUseSet{Valid: true, Digests: map[string]struct{}{digA: {}}}, nil
	}
	if _, _, err := runJob(t, h, Payload{Policy: testPolicy(), DryRun: true}); err != nil {
		t.Fatal(err)
	}
	if calls != 1 {
		t.Fatalf("a dry run must not re-derive the in-use set, got %d builds", calls)
	}
	wantNoDeletes(t, reg)
}

// Minor 8. The two Valid gates were mutually masking: removing either alone
// left the suite green, because every path that reaches one also reaches the
// other. These two pin them independently — each drives ONE build to an
// invalid set and leaves the other valid.
func TestRun_FirstValidGateIsPinnedIndependently(t *testing.T) {
	reg := &fakeClient{
		catalogs: [][]string{{"engine"}},
		tags:     map[string][]imgregistry.TagGroup{"engine": {orphanGroup(digD, "abc1234")}},
	}
	h := newHandler(t, reg)
	calls := 0
	h.buildSet = func(context.Context) (InUseSet, error) {
		calls++
		if calls == 1 {
			return InUseSet{Valid: false, Digests: nil}, nil // only the FIRST is bad
		}
		return InUseSet{Valid: true, Digests: map[string]struct{}{digA: {}}}, nil
	}
	if _, _, err := runJob(t, h, Payload{Policy: testPolicy()}); err == nil {
		t.Fatal("an invalid first in-use set must fail the run on its own")
	}
	wantNoDeletes(t, reg)
}

func TestRun_SecondValidGateIsPinnedIndependently(t *testing.T) {
	reg := &fakeClient{
		catalogs: [][]string{{"engine"}},
		tags:     map[string][]imgregistry.TagGroup{"engine": {orphanGroup(digD, "abc1234")}},
	}
	h := newHandler(t, reg)
	calls := 0
	h.buildSet = func(context.Context) (InUseSet, error) {
		calls++
		if calls == 1 {
			return InUseSet{Valid: true, Digests: map[string]struct{}{digA: {}}}, nil
		}
		return InUseSet{Valid: false, Digests: nil}, nil // only the SECOND is bad
	}
	if _, _, err := runJob(t, h, Payload{Policy: testPolicy()}); err == nil {
		t.Fatal("an invalid re-derived in-use set must fail the run on its own")
	}
	wantNoDeletes(t, reg)
}

// mutableRegistry is a minimal inner Client whose tag list can change between
// calls, and which counts TagCount calls. It exists for the CachingClient
// hazard below.
type mutableRegistry struct {
	mu            sync.Mutex
	groups        []imgregistry.TagGroup
	tagCountCalls int
}

var _ imgregistry.Client = (*mutableRegistry)(nil)

func (m *mutableRegistry) Catalog(context.Context) ([]string, error) { return []string{"engine"}, nil }

func (m *mutableRegistry) ResolveTags(_ context.Context, _ string) (imgregistry.TagListing, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return imgregistry.TagListing{Groups: append([]imgregistry.TagGroup(nil), m.groups...)}, nil
}

func (m *mutableRegistry) Tags(ctx context.Context, repo string) ([]imgregistry.TagGroup, error) {
	l, err := m.ResolveTags(ctx, repo)
	return l.Groups, err
}

func (m *mutableRegistry) TagCount(_ context.Context, _ string) (int, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.tagCountCalls++
	n := 0
	for _, g := range m.groups {
		n += len(g.Tags)
	}
	return n, nil
}

func (m *mutableRegistry) Manifest(context.Context, string, string) (imgregistry.Manifest, error) {
	return imgregistry.Manifest{}, imgregistry.ErrNotFound
}

func (m *mutableRegistry) Delete(context.Context, string, string) error { return nil }

func (m *mutableRegistry) push(g imgregistry.TagGroup) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.groups = append(m.groups, g)
}

// The production wiring hazard, end to end against a REAL CachingClient.
//
// The drop signal is about the CLIENT degrading, never about the world
// changing. A tag pushed or deleted between two observations is normal and must
// not abort — the count-comparison this replaced could not tell the two apart,
// and a registry with ordinary tag churn would have stalled pruning
// indefinitely behind the scheduler's 1h failure backoff. This is the test that
// holds that property for a push (see TestRun_ConcurrentlyDeletedTagDoesNotAbort
// for a deletion).
//
// server.go builds exactly one registry client and it is a CachingClient:
// Tags is cached for 5 minutes (and the registry UI warms that cache on every
// page view) while TagCount is a live pass-through. Any completeness decision
// made by comparing those two therefore reads a push inside the window as a
// shortfall — indistinguishable from a silent drop — and aborts every run
// forever.
//
// The property that makes this impossible is structural, not documentary: the
// completeness evidence (TagListing.Dropped) is produced by the SAME resolution
// pass as the groups and travels with them through the cache, so there is no
// second observation to disagree with. This test pins both halves: the run
// succeeds, and TagCount is never consulted at all.
func TestRun_CachingClientPushMidWindowDoesNotAbort(t *testing.T) {
	inner := &mutableRegistry{groups: []imgregistry.TagGroup{orphanGroup(digD, "abc1234")}}
	cc := imgregistry.NewCachingClient(inner, 5*time.Minute)
	if _, err := cc.Tags(context.Background(), "engine"); err != nil { // the UI warms it
		t.Fatal(err)
	}
	inner.push(orphanGroup(digE, "beef123")) // CI pushes inside the window

	h := newHandler(t, cc)
	if _, _, err := runJob(t, h, Payload{Policy: testPolicy()}); err != nil {
		t.Fatalf("a push inside the cache window must not abort the run: %v", err)
	}
	if inner.tagCountCalls != 0 {
		t.Fatalf("completeness must come from the listing itself, not a second count call (%d made)", inner.tagCountCalls)
	}
}

// ---------------------------------------------------------------------------
// Review round 4.
// ---------------------------------------------------------------------------

// Important 2. A tag deleted between tags/list and its manifest fetch resolves
// ErrNotFound and lands in Dropped — and aborting on it costs a whole
// fleet-wide run plus an hour of scheduler backoff, for an event that is
// routine on a registry with active CI. It is safe by construction: the
// registry was asked with an Accept set that cannot be unsatisfied (imgregistry
// HEADs as a last resort) and said the tag is gone, so a tag that no longer
// exists protects nothing and its absence IS the correct view. Every other drop
// cause still aborts — see TestRun_IncompleteTagListingAbortsBeforeAnyDelete.
func TestRun_ConcurrentlyDeletedTagDoesNotAbort(t *testing.T) {
	reg := &fakeClient{
		catalogs: [][]string{{"engine"}},
		tags:     map[string][]imgregistry.TagGroup{"engine": {orphanGroup(digD, "abc1234")}},
		dropped: map[string][]imgregistry.DroppedTag{
			"engine": {{Tag: "feat-gone", Vanished: true, Err: fmt.Errorf("%w: engine/feat-gone is no longer in the repo's tag list", imgregistry.ErrNotFound)}},
		},
	}
	h := newHandler(t, reg)
	_, job, err := runJob(t, h, Payload{Policy: testPolicy()})
	if err != nil {
		t.Fatalf("a concurrently DELETED tag must not abort the run: %v", err)
	}
	if len(reg.deletes) != 1 {
		t.Fatalf("the run must still prune, got %v", reg.deletes)
	}
	// Recorded, not silent: an operator reading the job needs to see which
	// tags the run never got to look at.
	wantStepContaining(t, job, "feat-gone")
}

// The structural guard on the benign-drop rule. A drop's ERROR is not the
// evidence — imgregistry's manifest endpoints are content-negotiated and
// answer 404 MANIFEST_UNKNOWN for a manifest whose media type they cannot
// serve, so an ErrNotFound-wrapping drop can perfectly well describe a tag
// that exists. Only Vanished (set from a re-read of the tag list) may excuse a
// drop. This kills the mutation `d.Vanished` -> `errors.Is(d.Err,
// imgregistry.ErrNotFound)`, which is exactly the reading a reviewer would
// wave through and which proceeds to delete the image the dropped tag points
// at.
func TestRun_NotFoundDropThatDidNotVanishStillAborts(t *testing.T) {
	reg := &fakeClient{
		catalogs: [][]string{{"engine"}},
		tags:     map[string][]imgregistry.TagGroup{"engine": {orphanGroup(digD, "abc1234")}},
		dropped: map[string][]imgregistry.DroppedTag{
			// Not-found-shaped, but the tag is still in the registry's tag
			// list, so imgregistry left Vanished false.
			"engine": {{Tag: "latest", Err: fmt.Errorf("manifest engine/latest: %w", imgregistry.ErrNotFound)}},
		},
	}
	h := newHandler(t, reg)
	_, job, err := runJob(t, h, Payload{Policy: testPolicy()})
	if !errors.Is(err, ErrUnsafeToPrune) {
		t.Fatalf("a drop that did not vanish must abort whatever its error says, got %v", err)
	}
	wantNoDeletes(t, reg)
	if !strings.Contains(stepText(job), "ABORTED") {
		t.Fatalf("the abort must be recorded; steps were:\n%s", stepText(job))
	}
}

// The converse, and the reason the split is by error KIND rather than by
// "were there any resolvable tags": one benign drop alongside one unreadable
// manifest must still abort. A handler that proceeded because SOME drop was
// benign would delete from a listing with a real hole in it.
func TestRun_BenignDropAlongsideAnUnreadableOneStillAborts(t *testing.T) {
	reg := &fakeClient{
		catalogs: [][]string{{"engine"}},
		tags:     map[string][]imgregistry.TagGroup{"engine": {orphanGroup(digD, "abc1234")}},
		dropped: map[string][]imgregistry.DroppedTag{
			"engine": {
				{Tag: "feat-gone", Vanished: true, Err: fmt.Errorf("%w: engine/feat-gone is no longer in the repo's tag list", imgregistry.ErrNotFound)},
				{Tag: "latest", Err: fmt.Errorf("manifest engine/latest: %w", imgregistry.ErrUnreachable)},
			},
		},
	}
	h := newHandler(t, reg)
	_, job, err := runJob(t, h, Payload{Policy: testPolicy()})
	if !errors.Is(err, ErrUnsafeToPrune) {
		t.Fatalf("an unreadable tag must abort even alongside a benign drop, got %v", err)
	}
	wantNoDeletes(t, reg)
	// The abort must name the tag that caused it, not the benign one.
	if !strings.Contains(stepText(job), "ABORTED") || !strings.Contains(stepText(job), "latest") {
		t.Fatalf("the abort must name the unreadable tag; steps were:\n%s", stepText(job))
	}
}

// Minor 1. recordSetDiagnostics runs BEFORE the error check on the recheck
// build as well as the first one, and only the first ordering was pinned.
// NonDigestObserved is populated exclusively on an abort path, and the recheck
// abort is precisely where an operator needs to know which running containers
// went unprotected — moving the call below the error check would lose that
// silently while the suite stayed green.
func TestRun_RecordsRecheckDiagnosticsOnAbort(t *testing.T) {
	reg := &fakeClient{
		catalogs: [][]string{{"engine"}},
		tags:     map[string][]imgregistry.TagGroup{"engine": {orphanGroup(digD, "abc1234")}},
	}
	h := newHandler(t, reg)
	calls := 0
	h.buildSet = func(context.Context) (InUseSet, error) {
		calls++
		if calls == 1 {
			return InUseSet{Valid: true, Digests: map[string]struct{}{digA: {}}}, nil
		}
		// The second build fails AND carries diagnostics — exactly what
		// BuildInUseSet's abort path returns: a populated struct plus an error.
		return InUseSet{
				NonDigestObserved: []string{"engine-1 engine/valvo image id " + strings.Repeat("a", 64)},
			},
			fmt.Errorf("%w: host engine-1 unreachable", ErrUnsafeToPrune)
	}
	_, job, err := runJob(t, h, Payload{Policy: testPolicy()})
	if err == nil {
		t.Fatal("a failed recheck build must fail the run")
	}
	wantNoDeletes(t, reg)
	wantStepContaining(t, job, "non-digest")
}

package registryprune

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
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
	if err, ok := f.tagsErr[repo]; ok {
		return nil, err
	}
	return append([]imgregistry.TagGroup(nil), f.tags[repo]...), nil
}

func (f *fakeClient) TagCount(ctx context.Context, repo string) (int, error) {
	g, err := f.Tags(ctx, repo)
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

	n, err := h.deletePass(context.Background(), jc, plan, inUse, map[string]struct{}{},
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

	n, err := h.deletePass(context.Background(), jc, plan, inUse,
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

	n, err := h.deletePass(context.Background(), jc, plan, inUse, map[string]struct{}{},
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

	n, err := h.deletePass(context.Background(), jc, plan, inUse, map[string]struct{}{},
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

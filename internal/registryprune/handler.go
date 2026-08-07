package registryprune

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"sort"
	"strings"
	"time"

	"github.com/iotready/podman-api/internal/imgregistry"
	"github.com/iotready/podman-api/internal/jobs"
	"github.com/iotready/podman-api/internal/store"
)

// JobKind is this handler's key in the job registry.
const JobKind = "registry-prune"

// maxListedInStep bounds how many digests a single job step spells out. A repo
// can have hundreds of candidates; the step is for an operator to read, and the
// authoritative record of what happened is the delete count plus the per-repo
// steps around it.
const maxListedInStep = 20

// Payload is the job-args shape the scheduler enqueues and the handler reads.
// It carries a snapshot of the resolved policy, so a config reload mid-flight
// cannot change a running job's behaviour.
type Payload struct {
	Policy Policy `json:"policy"`
	// DryRun performs the full classification and records every job step, and
	// issues ZERO deletes.
	DryRun bool `json:"dry_run"`
	// SkipBlobGC is the "--no-gc" equivalent: manifest deletion only, leaving
	// the blobs recoverable. Stage B (blob GC) is a separate component; this
	// field is the payload half of its gate.
	SkipBlobGC bool `json:"skip_blob_gc"`
}

// Metrics records prune outcomes. nil-safe via Handler.metric().
type Metrics interface {
	// RunDone records a terminal run outcome ("succeeded", "failed",
	// "dry-run", "aborted").
	RunDone(result string)
	// ManifestsDeleted records manifests actually removed from repo.
	ManifestsDeleted(repo string, n int)
	// RepoSkipped records a repo skipped by the per-repo tripwire, with the
	// number of delete candidates that tripped it. This is the counter that
	// would have surfaced the 2026-08-02 incident within a tick instead of a
	// week.
	RepoSkipped(repo string, candidates int)
	// BytesReclaimed records the bytes Stage B's blob GC freed on disk. It is
	// called ONLY when the measurement is real (BlobGC.Result.Measured): an
	// unmeasured run must record nothing rather than zero, because "we could
	// not size the registry" and "the GC freed nothing" are different facts
	// and only one of them is a reason to look at the GC.
	BytesReclaimed(bytes int64)
}

// candidate is one digest classified deletable, within one repo.
type candidate struct {
	digest string
	tags   []string
	class  Class
}

// repoPlan is one repo's delete candidates. The plan for EVERY repo is built
// before any delete is issued — see Run.
type repoPlan struct {
	repo       string
	candidates []candidate
}

// Handler implements jobs.Handler for the "registry-prune" kind.
type Handler struct {
	// Registry is the full client, including Delete. Note that the in-use
	// pass is handed only the read-only ManifestResolver slice of it, so no
	// bug in the protection computation can reach a delete.
	Registry  imgregistry.Client
	Hosts     HostEnumerator
	Inventory InventorySource
	Specs     SpecSource
	Config    Config
	Metrics   Metrics // optional

	// BlobGC is Stage B (blob reclamation). nil disables it entirely, which is
	// Stage A only: manifests unlinked, blobs left recoverable. See blobgc.go.
	BlobGC *BlobGC

	// buildSet overrides the in-use computation. Test seam only; nil in
	// production, where BuildInUseSet is used.
	buildSet func(context.Context) (InUseSet, error)
}

var _ jobs.Handler = (*Handler)(nil)

func (h *Handler) metric() Metrics {
	if h.Metrics == nil {
		return noopMetrics{}
	}
	return h.Metrics
}

func (h *Handler) now() time.Time {
	if h.Config.Now != nil {
		return h.Config.Now()
	}
	return time.Now()
}

// fixedCatalog freezes one catalog listing for the whole run.
//
// BuildInUseSet takes a ManifestResolver and calls Catalog on it; the
// classification pass enumerates repos too. Letting each fetch its own listing
// is a real TOCTOU: CachingClient.Catalog is a pass-through, so two calls in
// one run can disagree — the protection pass sees 28 repos, CI pushes one, and
// classification enumerates 29 whose digests were never protected. Freezing the
// slice makes "invisible to the protection pass" and "not classified" the same
// predicate by construction rather than by timing.
type fixedCatalog struct {
	inner imgregistry.Client
	repos []string
}

func (f fixedCatalog) Catalog(context.Context) ([]string, error) { return f.repos, nil }

func (f fixedCatalog) Manifest(ctx context.Context, repo, ref string) (imgregistry.Manifest, error) {
	return f.inner.Manifest(ctx, repo, ref)
}

// validatePolicy fails closed on a policy that arrived empty. Nothing becomes
// deletable by an empty ProtectedExact today (an unrecognised tag is kept), but
// a policy with no protected names and no tripwire is a wiring bug, and this
// job does not run on inputs it cannot explain.
func validatePolicy(p Policy) error {
	if len(p.ProtectedExact) == 0 && p.CalVer == nil {
		return errors.New("policy protects no tag names; refusing to run (a zero Policy is a wiring bug, not a permissive configuration)")
	}
	if p.MaxDeletesPerRepo <= 0 {
		return errors.New("policy has no per-repo tripwire (MaxDeletesPerRepo must be positive)")
	}
	return nil
}

// Run executes one prune. The order is non-negotiable:
//
//  1. fetch the catalog ONCE,
//  2. build the in-use set from it,
//  3. classify EVERY repo,
//  4. apply the per-repo tripwire,
//  5. only then delete.
//
// Step 3 preceding step 5 is the #64 fix: a per-repo delete-as-you-go reclaims
// blobs still needed by a repo that has not been classified yet.
func (h *Handler) Run(ctx context.Context, job store.Job, jc *jobs.JobContext) error {
	var p Payload
	if err := json.Unmarshal(job.Args, &p); err != nil {
		// Record an outcome even here: #220's alerting is built on this
		// metric, and a job that fails without one is invisible.
		h.metric().RunDone("aborted")
		jc.Step("payload", "ABORTED: "+err.Error())
		return fmt.Errorf("decode registry-prune args: %w", err)
	}
	if err := validatePolicy(p.Policy); err != nil {
		h.metric().RunDone("aborted")
		jc.Step("policy", "ABORTED: "+err.Error())
		return fmt.Errorf("registry-prune policy: %w", err)
	}
	// FIRST step, deliberately: "is this run going to delete things" is the one
	// question an operator opens the job page to answer, and it belongs at the
	// top rather than inferred from the absence of "delete:*" steps twenty rows
	// down.
	jc.Step("mode", h.describeMode(p))

	// (1) One catalog listing for the whole run.
	repos, err := h.Registry.Catalog(ctx)
	if err != nil {
		h.metric().RunDone("aborted")
		jc.Step("catalog", "ABORTED: "+err.Error())
		return fmt.Errorf("registry catalog: %w", err)
	}
	repos = append([]string(nil), repos...)
	sort.Strings(repos)
	inCatalog := make(map[string]struct{}, len(repos))
	for _, r := range repos {
		inCatalog[r] = struct{}{}
	}
	jc.Step("catalog", fmt.Sprintf("%d repositories listed once for this run", len(repos)))

	// (2) The in-use set, from that same listing.
	inUse, err := h.inUseSet(ctx, repos)
	recordSetDiagnostics(jc, inUse)
	if err != nil {
		h.metric().RunDone("aborted")
		jc.Step("in-use", "ABORTED: "+err.Error())
		return err
	}
	// Not redundant with err != nil. BuildInUseSet has an abort path that
	// returns a POPULATED diagnostic struct whose Digests is nil; Valid is the
	// documented gate, and a set that is not Valid answers Has() false for
	// every digest in the registry — i.e. a caller that skipped this check
	// would delete all of it.
	if !inUse.Valid {
		h.metric().RunDone("aborted")
		jc.Step("in-use", "ABORTED: in-use set is not valid")
		return fmt.Errorf("%w: in-use set is not valid", ErrUnsafeToPrune)
	}
	jc.Step("in-use", fmt.Sprintf("%d digest(s) protected fleet-wide", inUse.Len()))

	// (3) Classify every repo before anything is deleted.
	plan, protected, err := h.classifyAll(ctx, jc, repos, inUse, p.Policy)
	if err != nil {
		h.metric().RunDone("aborted")
		return err
	}

	// (4) Tripwire, per repo. Deliberately diverges from registry-gc.sh, which
	// aborts the WHOLE run: on 2026-08-02 one repo's 274 candidates against a
	// threshold of 100 silently disabled GC for all 28 repos for a week. Here
	// the anomalous repo is skipped and recorded; every other repo proceeds.
	kept := make([]repoPlan, 0, len(plan))
	for _, rp := range plan {
		if len(rp.candidates) > p.Policy.MaxDeletesPerRepo {
			detail := fmt.Sprintf("SKIPPED: %d delete candidate(s) exceeds max_deletes_per_repo=%d; every other repository still proceeds",
				len(rp.candidates), p.Policy.MaxDeletesPerRepo)
			jc.Step("tripwire:"+rp.repo, detail)
			log.Printf("registryprune: tripwire skipped repo %s: %s", rp.repo, detail)
			h.metric().RepoSkipped(rp.repo, len(rp.candidates))
			continue
		}
		kept = append(kept, rp)
	}

	// (5) Re-derive the in-use set immediately before deleting. The first
	// snapshot is minutes old by now — Tags on the fleet's "engine" repo alone
	// measures ~21s cold, times 28 repos, on top of BuildInUseSet itself. A
	// deploy landing inside that window pushes an image whose CI tag is a
	// 7-12 char bare hex string: absent from the first snapshot, its spec not
	// yet written when the specs were read, so it classifies sha-orphan. The
	// instance would run and never be rebuildable — #64 exactly.
	//
	// The catalog is deliberately NOT re-listed: it is the fleet's state we
	// need to be fresh, not the repo set. Re-listing would reopen the TOCTOU
	// that freezing it closed (a repo pushed mid-run being classified against
	// a protection pass that never saw it).
	//
	// Skipped on a dry run: it deletes nothing, so a second full fleet walk
	// plus registry manifest resolutions would buy no safety at all.
	fresh := inUse
	if p.DryRun {
		jc.Step("in-use:recheck", "skipped: dry run deletes nothing")
		return h.finish(ctx, jc, kept, inUse, fresh, protected, inCatalog, p)
	}
	fresh, err = h.inUseSet(ctx, repos)
	recordSetDiagnostics(jc, fresh)
	if err != nil {
		h.metric().RunDone("aborted")
		jc.Step("in-use:recheck", "ABORTED: "+err.Error())
		return err
	}
	if !fresh.Valid {
		h.metric().RunDone("aborted")
		jc.Step("in-use:recheck", "ABORTED: re-derived in-use set is not valid")
		return fmt.Errorf("%w: re-derived in-use set is not valid", ErrUnsafeToPrune)
	}
	jc.Step("in-use:recheck", fmt.Sprintf("%d digest(s) protected fleet-wide at delete time", fresh.Len()))

	return h.finish(ctx, jc, kept, inUse, fresh, protected, inCatalog, p)
}

// finish is steps (6) and (7): delete, then Stage B. Split out only so the
// dry-run path can reach it without re-deriving the in-use set.
func (h *Handler) finish(ctx context.Context, jc *jobs.JobContext, kept []repoPlan, inUse, fresh InUseSet,
	protected map[string]struct{}, inCatalog map[string]struct{}, p Payload) error {
	// (6) Delete.
	deleted, delErr := h.deletePass(ctx, jc, kept, inUse, fresh, protected, inCatalog, p)
	if delErr != nil {
		h.metric().RunDone("failed")
		return delErr
	}
	if p.DryRun {
		jc.Step("dry-run", fmt.Sprintf("%d manifest(s) would be deleted (nothing removed)", deleted))
		h.metric().RunDone("dry-run")
		return nil
	}
	jc.Step("summary", fmt.Sprintf("%d manifest(s) deleted", deleted))

	// (7) Stage B: reclaim the blobs those deletions only unlinked. It gates
	// itself on DryRun/SkipBlobGC too, so the "no pod is ever played on a dry
	// run" property does not depend on this call site.
	if h.BlobGC != nil {
		// The Result is the only data path the bytes-reclaimed metric has.
		// Wrapping this call in an error-only helper that discards it leaves
		// that metric permanently at zero while everything still looks
		// correct — an earlier BlobGC.Run did exactly that and was deleted.
		res, err := h.BlobGC.Reclaim(ctx, jc, p)
		// Recorded before the error check, deliberately: the deferred restart
		// in Reclaim fills the after-size in even on a failing run, and bytes
		// that really were reclaimed are worth counting whatever the outcome.
		if res.Measured {
			h.metric().BytesReclaimed(res.Reclaimed)
		}
		if err != nil {
			h.metric().RunDone("failed")
			return err
		}
	}
	h.metric().RunDone("succeeded")
	return nil
}

// describeMode spells out what this run will and will not do.
func (h *Handler) describeMode(p Payload) string {
	if p.DryRun {
		return "DRY RUN — classifies and reports; deletes NOTHING and plays no GC pod"
	}
	if p.SkipBlobGC || h.BlobGC == nil {
		return "DELETING (stage A only) — manifests are unlinked; blob GC is off, so no disk is reclaimed " +
			"and the blobs stay recoverable until a later GC"
	}
	return "DELETING (stage A + B) — manifests are unlinked, then the registry is STOPPED for blob garbage collection and restarted"
}

// inUseSet builds the fleet-wide protected digest set from the frozen catalog.
func (h *Handler) inUseSet(ctx context.Context, repos []string) (InUseSet, error) {
	if h.buildSet != nil {
		return h.buildSet(ctx)
	}
	return BuildInUseSet(ctx, h.Hosts, h.Inventory, h.Specs,
		fixedCatalog{inner: h.Registry, repos: repos}, h.Config)
}

// recordSetDiagnostics surfaces the protection pass's blind spots as job steps.
// Both fields are recorded on the abort path too — NonDigestObserved is only
// ever populated there, and it is precisely the "which containers did we fail
// to protect" answer an operator needs.
func recordSetDiagnostics(jc *jobs.JobContext, s InUseSet) {
	if len(s.NonDigestObserved) > 0 {
		jc.Step("in-use:non-digest", fmt.Sprintf(
			"%d running container(s) reported an image ID rather than a manifest digest: %s",
			len(s.NonDigestObserved), strings.Join(s.NonDigestObserved, "; ")))
	}
	if len(s.ForeignSkipped) > 0 {
		jc.Step("in-use:foreign-skipped", fmt.Sprintf(
			"%d image reference(s) skipped as belonging to another registry: %s",
			len(s.ForeignSkipped), strings.Join(s.ForeignSkipped, ", ")))
	}
}

// classifyAll lists and classifies every repo in the catalog, returning the
// per-repo delete plan and the set of digests protected by name anywhere.
//
// Two passes over the listings, not one: protection is keyed by digest across
// ALL repos (blobs are shared), so a digest protected by a tag in repo B must
// be known before repo A's groups are classified. Doing it in one pass makes
// protection depend on catalog order.
//
// A repo whose tags cannot be listed ABORTS the run. Its groups may carry a
// digest that protects some other repo's, so its absence from the protected set
// is not evidence — it is a hole.
func (h *Handler) classifyAll(ctx context.Context, jc *jobs.JobContext, repos []string, inUse InUseSet, pol Policy) ([]repoPlan, map[string]struct{}, error) {
	listings := make(map[string][]imgregistry.TagGroup, len(repos))
	protected := map[string]struct{}{}
	for _, repo := range repos {
		if err := ctx.Err(); err != nil {
			return nil, nil, err
		}
		// ONE call, to ONE client, returning the groups and the evidence of
		// what could not be resolved together. There is deliberately no
		// second observation to compare against: any two calls — even to the
		// same client — can disagree because the world changed, and no
		// comparison of counts can separate "the client degraded" from "a tag
		// was pushed or deleted". Dropped comes from the same resolution pass
		// as Groups, so that distinction is carried, not inferred.
		listing, err := h.Registry.ResolveTags(ctx, repo)
		if err != nil {
			jc.Step("classify:"+repo, "ABORTED: "+err.Error())
			return nil, nil, fmt.Errorf("list tags for %s (aborting before any delete): %w", repo, err)
		}
		// A drop marked Vanished is the ONE benign kind: after resolution
		// failed, imgregistry re-read /v2/<repo>/tags/list and the tag was no
		// longer in it. A tag that no longer exists protects nothing, so its
		// absence from this listing is the correct view, not a hole in one.
		// Aborting on it would cost a whole fleet-wide run plus an hour of
		// scheduler backoff every time CI deletes a tag mid-run.
		//
		// The test is the Vanished FLAG, never errors.Is(d.Err, ErrNotFound).
		// A manifest 404 is content-negotiated and cannot mean "absent" (see
		// imgregistry.DroppedTag), so reading the decision out of the error
		// would make it hostage to how some future edit wraps a cause.
		// Every other drop still aborts.
		if len(listing.Dropped) > 0 {
			var hard []imgregistry.DroppedTag
			var vanished []string
			for _, d := range listing.Dropped {
				if d.Vanished {
					vanished = append(vanished, d.Tag)
					continue
				}
				hard = append(hard, d)
			}
			if len(vanished) > 0 {
				jc.Step("vanished:"+repo, fmt.Sprintf(
					"%d tag(s) were deleted from the registry between listing and resolution and are correctly absent from this run's view: %s",
					len(vanished), listSample(vanished)))
			}
			if len(hard) > 0 {
				err := fmt.Errorf("%w: %s", ErrUnsafeToPrune, describeDrops(repo, hard))
				jc.Step("classify:"+repo, "ABORTED: "+err.Error())
				return nil, nil, err
			}
		}
		groups := listing.Groups
		listings[repo] = groups
		for _, tg := range groups {
			if nameProtected(repo, tg, pol) {
				protected[tg.Digest] = struct{}{}
			}
		}
	}

	counts := map[Class]int{}
	var ageUnknown []string
	plan := make([]repoPlan, 0, len(repos))
	for _, repo := range repos {
		rp := repoPlan{repo: repo}
		for _, tg := range listings[repo] {
			// EXACTLY ONCE per TagGroup — Classify's documented
			// precondition. A caller that flattens tg.Tags and calls per tag
			// makes ClassShaIsLatest permanently unreachable and turns
			// "latest"'s own hex alias into a delete candidate.
			c := Classify(repo, tg, inUse, protected, pol, h.now())
			counts[c]++
			// C-2. imgregistry leaves Created zero for BOTH "this is a
			// multi-arch index, there is no config blob" and "the config blob
			// fetch failed" (client.go: the blobCreated error is swallowed).
			// policy.go maps zero Created on a feat-* tag to ClassFeatStale —
			// deletable — a deliberate port of registry-gc.sh justified only
			// by the index case. Since the two are indistinguishable here, the
			// handler refuses to act on either: a feat-* tag created seconds
			// ago whose blob GET returned 500 would otherwise be deleted, and
			// the blast radius is per-DIGEST, so one failure can condemn every
			// feat-* tag sharing it. Keeping a genuine multi-arch index
			// forever is the recoverable direction, and it is recorded rather
			// than silent.
			if c == ClassFeatStale && tg.Created.IsZero() {
				ageUnknown = append(ageUnknown, fmt.Sprintf("%s@%s %v", repo, tg.Digest, tg.Tags))
				continue
			}
			if Deletable(c, pol) {
				rp.candidates = append(rp.candidates, candidate{digest: tg.Digest, tags: tg.Tags, class: c})
			}
		}
		plan = append(plan, rp)
	}
	jc.Step("classify", summariseClasses(counts))
	if len(ageUnknown) > 0 {
		jc.Step("classify:age-unknown", fmt.Sprintf(
			"%d digest(s) kept: a feat-* tag whose creation time is unknown (no config blob, or the blob fetch failed — imgregistry cannot distinguish them): %s",
			len(ageUnknown), listSample(ageUnknown)))
	}
	return plan, protected, nil
}

// describeDrops explains an aborted run to an operator. It is only ever handed
// the drops NOT marked Vanished — a tag the registry's own tag list no longer
// contains is benign and recorded separately (see classifyAll).
//
// A dropped tag is the client failing to resolve a manifest and carrying on
// (client.go: the `if r.err != nil` arm of ResolveTags' grouping pass), which
// is right for browsing a repo and catastrophic for deleting from one. A
// dropped "latest" leaves its bare-hex alias alone in its group, reclassifying
// it from sha-is-latest (kept) to sha-orphan (DELETED); a dropped protected tag
// stops seeding the cross-repo protected-digest set, so shares-protected
// silently loses that digest in EVERY repo — see
// TestRun_DigestProtectedByNameInAnotherRepoIsKept for that property under
// test.
//
// Hence the whole run aborts rather than the repo being skipped tripwire-style:
// the tripwire's skip-and-proceed is right when the anomaly's blast radius is
// its own repo, and this one's is not.
func describeDrops(repo string, dropped []imgregistry.DroppedTag) string {
	names := make([]string, 0, len(dropped))
	for _, d := range dropped {
		names = append(names, fmt.Sprintf("%s (%v)", d.Tag, d.Err))
	}
	return fmt.Sprintf("registry listing for %s is incomplete: %d tag(s) could not be resolved and are absent from it: %s; refusing to classify from a partial view",
		repo, len(dropped), listSample(names))
}

// nameProtected reports whether any tag in tg is protected by NAME under pol.
//
// It is applied across the WHOLE catalog and the resulting digest set is shared
// by every repo — intended, not an oversight. Digests are shared between repos
// (blobs are), so a digest reachable through a protected tag anywhere must be
// protected everywhere; that is the same conservative keying InUseSet uses.
// Deliberately excludes the in-use and shares-protected rules: this feeds the
// cross-repo protected-digest set that Classify consumes, and seeding it from
// itself would be circular.
func nameProtected(repo string, tg imgregistry.TagGroup, p Policy) bool {
	if matchesAnyExact(tg.Tags, p.ProtectedExact) {
		return true
	}
	if matchesAnyRegex(tg.Tags, p.CalVer) {
		return true
	}
	if extra, ok := p.ExtraPerRepo[repo]; ok && matchesAnyRegex(tg.Tags, extra) {
		return true
	}
	return false
}

func summariseClasses(counts map[Class]int) string {
	keys := make([]string, 0, len(counts))
	for c := range counts {
		keys = append(keys, string(c))
	}
	sort.Strings(keys)
	parts := make([]string, 0, len(keys))
	for _, k := range keys {
		parts = append(parts, fmt.Sprintf("%s=%d", k, counts[Class(k)]))
	}
	if len(parts) == 0 {
		return "no tags to classify"
	}
	return strings.Join(parts, " ")
}

// deletePass issues the deletes for an already-classified, already-tripwired
// plan. It re-checks every guard immediately before each DELETE even though
// classification excluded them: the backstop exists for the case where the
// classification pass is wrong, so it must not trust it.
//
// Returns the number of manifests deleted (or, on a dry run, that would have
// been deleted).
func (h *Handler) deletePass(ctx context.Context, jc *jobs.JobContext, plan []repoPlan, inUse, fresh InUseSet,
	protected map[string]struct{}, inCatalog map[string]struct{}, p Payload) (int, error) {
	total := 0
	seen := map[string]struct{}{}
	var firstErr error
	// registryDown is tracked separately from firstErr: keying the "stop
	// hammering it" break on firstErr meant an earlier unrelated failure
	// pinned it, and a LATER outage no longer stopped the loop.
	registryDown := false

	for _, rp := range plan {
		if registryDown {
			break // registry is down; further deletes can only pile up failures
		}
		var wouldDelete []string
		n := 0
		for _, c := range rp.candidates {
			if err := ctx.Err(); err != nil {
				return total, err
			}
			// Dedup per repo+digest: two tag groups can never share a digest
			// within one repo (Tags groups by digest), but a plan assembled
			// differently could, and a second DELETE of the same digest is a
			// spurious 404.
			key := rp.repo + "@" + c.digest
			if _, dup := seen[key]; dup {
				continue
			}
			seen[key] = struct{}{}

			// --- backstop ---------------------------------------------------
			// A repo outside the protection-time catalog was never covered by
			// the in-use pass, so nothing there is known to be safe. By
			// construction the plan is built from that same listing; this is
			// the assertion that keeps it true.
			//
			// Deliberately NOT guarded by `if inCatalog != nil`: a nil map
			// lookup returns ok==false, so a nil set refuses everything, which
			// is the fail-closed direction. The guard inverted that — nil
			// disabled the refusal and every repo passed.
			if _, ok := inCatalog[rp.repo]; !ok {
				jc.Step("backstop:"+rp.repo, "REFUSED: repository is not in this run's catalog listing")
				continue
			}
			// Both snapshots, because neither alone is authoritative: inUse
			// predates the classification pass and fresh postdates it, and an
			// instance can appear or disappear on either side of that window.
			if inUse.Has(c.digest) || fresh.Has(c.digest) {
				jc.Step("backstop:"+rp.repo, fmt.Sprintf("REFUSED %s: in use (classified %s)", c.digest, c.class))
				continue
			}
			if _, ok := protected[c.digest]; ok {
				jc.Step("backstop:"+rp.repo, fmt.Sprintf("REFUSED %s: protected digest (classified %s)", c.digest, c.class))
				continue
			}
			// ----------------------------------------------------------------

			if p.DryRun {
				wouldDelete = append(wouldDelete, fmt.Sprintf("%s (%s) %v", c.digest, c.class, c.tags))
				n++
				continue
			}
			if err := h.Registry.Delete(ctx, rp.repo, c.digest); err != nil {
				if errors.Is(err, imgregistry.ErrNotFound) {
					// The registry answered and said it is already gone. That
					// is the one error that is not a failure — and it is
					// distinguishable from ErrUnreachable only because
					// imgregistry never collapses the two.
					jc.Step("delete:"+rp.repo, fmt.Sprintf("%s already absent", c.digest))
					continue
				}
				jc.Step("delete:"+rp.repo, fmt.Sprintf("FAILED %s: %v", c.digest, err))
				if firstErr == nil {
					firstErr = fmt.Errorf("delete %s@%s: %w", rp.repo, c.digest, err)
				}
				if errors.Is(err, imgregistry.ErrUnreachable) {
					registryDown = true
					break
				}
				continue
			}
			n++
		}
		total += n
		switch {
		case p.DryRun && n > 0:
			jc.Step("dry-run:"+rp.repo, fmt.Sprintf("%d manifest(s) would be deleted: %s", n, listSample(wouldDelete)))
		case !p.DryRun && n > 0:
			jc.Step("delete:"+rp.repo, fmt.Sprintf("%d manifest(s) deleted", n))
			h.metric().ManifestsDeleted(rp.repo, n)
		}
	}
	return total, firstErr
}

func listSample(items []string) string {
	if len(items) <= maxListedInStep {
		return strings.Join(items, ", ")
	}
	return strings.Join(items[:maxListedInStep], ", ") + fmt.Sprintf(", … and %d more", len(items)-maxListedInStep)
}

type noopMetrics struct{}

func (noopMetrics) RunDone(string)               {}
func (noopMetrics) ManifestsDeleted(string, int) {}
func (noopMetrics) RepoSkipped(string, int)      {}
func (noopMetrics) BytesReclaimed(int64)         {}

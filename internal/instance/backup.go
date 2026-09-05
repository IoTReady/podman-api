package instance

import (
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"slices"
	"sort"
	"strings"

	"github.com/iotready/podman-api/internal/podman"
	"github.com/iotready/podman-api/internal/render"
	"github.com/iotready/podman-api/internal/store"
)

// BackupRequest is the backup job's args. BackupID is generated at enqueue
// time (store.NewBackupID) so POST can return it before the job runs.
type BackupRequest struct {
	BackupID string `json:"backup_id"`
	Host     string `json:"host"`
	Template string `json:"template"`
	Slug     string `json:"slug"`
	// Volumes is the declared (short) volume names to capture. Empty means
	// every declared volume not marked `none`. Persisted in the job args so a
	// queued job re-claimed after a daemon restart (before it starts running)
	// carries the scope it was enqueued with — ReconcileBackup itself never
	// re-runs the export; it only fails the row and restarts the instance.
	Volumes []string `json:"volumes,omitempty"`
}

// volMeta bundles a volume's backup marker and exclude patterns — the two
// pieces of template metadata both checkBackupable's vetoed-check and
// Backup's export loop need per volume. Shared by both so a renamed/recovered
// volume's metadata (sourced from store.Spec.AppliedVolumeMeta rather than
// the CURRENT template, which no longer declares it under its old name) is
// represented identically to a currently-declared volume's.
type volMeta struct {
	marker  string
	exclude []string
}

// backupBlobKey is the blob layout: <host>/<template>/<slug>/<backup-id>/<volume>.tar
func backupBlobKey(host, tmpl, slug, id, volume string) string {
	return host + "/" + tmpl + "/" + slug + "/" + id + "/" + volume + ".tar"
}

// manifestBlobKey is the sidecar manifest's blob layout, alongside the tar
// under the same backup prefix so DeleteAll(backupBlobPrefix(...)) sweeps
// both together (#293).
func manifestBlobKey(host, tmpl, slug, id, volume string) string {
	return host + "/" + tmpl + "/" + slug + "/" + id + "/" + volume + ".manifest.json.gz"
}

// backupBlobPrefix addresses every blob of one backup (for DeleteAll).
func backupBlobPrefix(host, tmpl, slug, id string) string {
	return host + "/" + tmpl + "/" + slug + "/" + id
}

// CheckBackupable runs the cheap synchronous validation the POST handler
// needs: known host, known template, stored spec present, blob store wired,
// and — when volumes is non-empty — that every named volume is declared by the
// template and not vetoed by a `none` marker.
//
// Scope validation is synchronous and upfront so a typo or a misconfigured
// scheduler fails the request outright rather than stopping a pod and
// producing a green, empty backup.
//
// For an EXPLICIT scope it also confirms each named volume actually EXISTS on
// the host, which makes this check touch the host rather than being purely
// declarative. That cost buys the difference between a loud permanent error and
// a permanent OUTAGE loop: the names a scheduler passes come from template
// meta, so a template edit marking a volume on a fleet that has not been
// re-applied hands Backup a name that is declared but not materialised. Caught
// only after the export (the defensive set comparison in Backup), every
// scheduler tick stops the pod, exports, fails and restarts, forever, recording
// nothing. Caught here it is a synchronous 400 with the instance still serving.
// A host error is wrapped as a host error, never reported as an invalid scope.
//
// An UNSCOPED request IS existence-checked here too, for two independent
// reasons (#256, on top of #257's store.Spec.AppliedVolumes):
//
//  1. Materialised-but-vetoed: at least one volume EXISTS on the host under a
//     CURRENTLY declared name, and every one of those that exists is skipped
//     because it is `none`-vetoed. Stopping the pod, deliberately skipping
//     real data and capturing nothing is refused regardless of anything else.
//  2. Applied-vs-host loss: when the spec's applied volume set is known (a
//     spec written by an Apply since #257), every volume that set names is
//     checked for existence under the name it was APPLIED with — not the name
//     the template declares now. A volume missing under both names is refused
//     ONLY when it is the LAST applied volume standing — total loss, the
//     unambiguous case (a host rebuild, `podman volume prune`, a half-landed
//     evacuation with nothing left of what was applied). When at least one
//     OTHER applied volume is still confirmed present, a missing one is NOT
//     refused: `AppliedVolumes` records what the template DECLARED at apply
//     time, not what podman actually created, so a volume some deployments
//     never mount (an optional sidecar cache, a lazily-created path) looks
//     identical here to one that genuinely went missing, and there is no
//     signal in this codebase today to tell them apart (round-2 review
//     finding 1). The backup proceeds with whatever the host does hold. A
//     volume missing under the CURRENT name but present under its applied
//     name is a template-meta rename the instance has not been re-applied
//     for — not loss either way.
//
// A template edited down to declaring ZERO volumes does not, by itself, skip
// this: if the spec's applied set still names volumes that materialise on the
// host, an unscoped backup would silently capture nothing (the export loop
// only ever walks currently-declared names) while real data sits untouched —
// refused rather than recorded as an empty `complete` row (round-2 review
// finding 2). Only a template with NO declared volumes AND an applied set that
// is unknown or known-empty short-circuits without touching the host.
//
// For a spec written before #257 (AppliedVolumes == nil, unknown rather than
// empty), check 2 does not run: an instance with NO materialised volumes under
// a currently-declared name is accepted unconditionally, exactly as before —
// whether that means "brand new" or "renamed" or "lost" is not decidable
// without the applied set, and four review rounds of heuristics guessing at it
// each misclassified a real state (see #257 for what each one broke). That
// fallback clears itself the next time the instance is applied.
func (s *Service) CheckBackupable(ctx context.Context, host, tmpl, slug string, volumes []string) error {
	_, _, err := s.checkBackupable(ctx, host, tmpl, slug, volumes, false)
	return err
}

// CheckBackupScopeDeclared is CheckBackupable's DECLARATIVE half: everything
// answerable from the store alone — backups configured, host and template
// known, spec present, every named volume declared by the template and not
// `backup: none`-vetoed, and the template not vetoed end to end. It never
// contacts a host.
//
// It exists so a caller can reject a bad scope before paying for host
// resolution, and specifically so backupctl can run it ahead of its dedupe
// (which is store-local too) and resolve volumes only for a tick that will
// actually enqueue. A misconfigured scope must never be masked by dedupe —
// that was review-1 finding 6 — but only this half is needed to prevent it,
// and doing the whole check first put a VolumeInspect per declared volume on
// every tick of every instance, including ones already queued (review-6
// finding 5).
//
// Passing it is NOT sufficient to admit a backup: the existence checks in
// CheckBackupable can still refuse the run. Call this to fail early, then
// CheckBackupable before enqueuing.
func (s *Service) CheckBackupScopeDeclared(ctx context.Context, host, tmpl, slug string, volumes []string) error {
	_, _, err := s.checkBackupable(ctx, host, tmpl, slug, volumes, true)
	return err
}

// checkBackupable is CheckBackupable's implementation, additionally returning
// the instance's volumes as resolved on the host AND the per-volume
// marker/exclude metadata that applies to each of them, keyed by FULL podman
// volume name. Backup calls this under its own lock and reuses both for the
// export loop, so one backup costs one InstanceVolumes round trip there rather
// than two (review-5 finding 7), and — since #265 review finding 1 — one SPEC
// read rather than two.
//
// The metadata map is authoritative and must be the only source Backup consults:
// it merges the CURRENT template's declared markers with the applied-time
// markers of any volume recovered under an old (renamed) applied name. Backup
// used to rebuild the second half itself from a SECOND, independent GetSpec,
// silently skipping the merge on any error and — because Backup and
// Apply/Delete serialize under different locks — potentially observing a
// DIFFERENT spec than this function did. Either way a `backup: none` veto
// carried across a rename evaporated and the volume was exported. There is now
// exactly one read, and its result is returned rather than re-derived.
//
// The volume list is nil when the check short-circuited before touching the
// host, which happens only when the template declares no volumes at all — in
// which case InstanceVolumes would return an empty list anyway, since it
// iterates exactly the declared names.
func (s *Service) checkBackupable(ctx context.Context, host, tmpl, slug string, volumes []string, declaredOnly bool) ([]podman.Volume, map[string]volMeta, error) {
	if s.blobs == nil {
		return nil, nil, ErrBackupsDisabled
	}
	t, err := s.lookup(ctx, host, tmpl)
	if err != nil {
		return nil, nil, err
	}
	sp, err := s.store.GetSpec(ctx, host, tmpl, slug)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return nil, nil, ErrInstanceNotFound
		}
		return nil, nil, err
	}
	declared := make(map[string]string, len(t.Meta.Volumes))
	meta := make(map[string]volMeta, len(t.Meta.Volumes))
	for _, v := range t.Meta.Volumes {
		declared[v.Name] = v.Backup
		meta[volumeName(tmpl, slug, v.Name)] = volMeta{marker: v.Backup, exclude: v.Exclude}
	}
	if len(volumes) == 0 {
		// An empty scope means "every declared volume not marked `none`". If the
		// template declares volumes and every one of them is vetoed, that set is
		// empty by an explicit operator decision and the backup would capture
		// nothing: a green row, a real stop/restart outage, and no data. Reject
		// it here, synchronously.
		//
		// A template declaring NO volumes at all, AND an applied set that is
		// either unknown or known-empty, is a different case and is ACCEPTED: a
		// stateless template (the bundled `basic-web`) has nothing to back up,
		// that is a valid no-op, and rejecting it would regress every caller
		// that backs one up today. This is the only path that avoids touching
		// the host at all — every other combination below needs at least one
		// host round trip to tell a real no-op from a template edited down to
		// zero volumes while the instance still holds real applied data
		// (review-#256/#257-round-2 finding 2).
		//
		// A declared volume that does not yet exist on the host is neither case —
		// this check is over what the template declares, not over what currently
		// exists (a brand-new instance must be able to take its first, empty
		// backup).
		if len(declared) == 0 && len(sp.AppliedVolumes) == 0 {
			return nil, meta, nil
		}
		// #265 review finding 4: this refusal used to be stated over the
		// CURRENTLY declared markers alone, and fired BEFORE the rename-recovery
		// logic below ever ran. A volume renamed by a template edit that also
		// marked its NEW name `backup: none` therefore refused the whole run,
		// even though the real, unvetoed data was still recoverable on the host
		// under its OLD applied name — precisely the case rename recovery
		// exists for. anyExportableVolume considers the applied set's own
		// markers too, so recovery is no longer pre-empted; when the recovered
		// volume IS vetoed at its apply, the refusal still stands.
		//
		// It stays here, ahead of the host round trip, rather than moving after
		// recovery: it is answerable from the store alone, and
		// CheckBackupScopeDeclared (declaredOnly) — which must never contact a
		// host — depends on it to reject a fully vetoed template early. Anything
		// it now lets through is decided by the materialised-and-vetoed check at
		// the end of this branch, which sees the recovered volumes.
		if len(declared) > 0 && !anyExportableVolume(declared, sp) {
			return nil, nil, fmt.Errorf("%w: template %s declares volumes but every one of them is marked `backup: none`", ErrInvalidBackupScope, tmpl)
		}
		if declaredOnly {
			return nil, meta, nil
		}
		// At least one declared volume is exportable in principle (or none are
		// declared at all but the applied set is non-trivial and needs
		// resolving below). Whether an unscoped run can actually capture
		// anything depends on what is MATERIALISED on the host, so resolve the
		// whole picture — host listing, applied-set classification, rename
		// recovery and its metadata — in ONE step, and let every guard below
		// read that one value.
		//
		// It used to be inline: five locals (vols/present/materialized/lost/
		// recovered plus meta) built and mutated in sequence, with each
		// subsequent guard depending on exactly how far that sequence had
		// progressed. Three of the last four review rounds found an ordering
		// bug in it. Bundling the result makes the guards a linear pipeline
		// over an immutable classification rather than a chain of mutations
		// (#265 review round-2 cleanup) — this is a pure refactor, no
		// behaviour changes with it.
		c, err := s.classifyVolumes(ctx, host, tmpl, slug, declared, meta, sp)
		if err != nil {
			return nil, nil, err
		}
		// Total loss: the ENTIRE applied set is gone under both its applied and
		// current names — nothing survives to prove the instance ever ran with
		// data on this host, so this cannot be misread as "declared but never
		// materialised" the way a partial miss can (see below). Refuse rather
		// than record a green row with nothing in it.
		//
		// BUT only when there is nothing else exportable either: `c.vols`
		// already carries every CURRENTLY declared volume InstanceVolumes
		// found present on the host, whether or not it was ever part of the
		// applied set (e.g. a template edited to add a new volume the instance
		// has not been re-applied for yet, but which already materialised). That
		// set was resolved before this check ever ran, so discarding it on a
		// hard refusal here would throw away real, already-fetched, exportable
		// data that has nothing to do with the applied set's loss (#256 review
		// round-4, addendum finding 1). Only refuse when NOTHING survives at
		// all — the applied set AND every other currently-declared volume.
		//
		// AND only with CORROBORATING EVIDENCE that the applied set ever
		// materialised in the first place (#265 review finding 2). "Nothing
		// applied survives" is not on its own distinguishable from "nothing
		// applied was ever created": `AppliedVolumes` records what the template
		// DECLARED at apply time, not what podman actually created — the exact
		// ambiguity the partial-loss branch below deliberately refuses to refuse
		// on. With a single-member applied set there is no surviving sibling to
		// make the miss "partial", so an instance applied with one optional or
		// lazily-created volume that has not materialised yet took the total-loss
		// refusal and was permanently blocked from ever taking its first backup.
		// The two cases must be treated alike, so the refusal is narrowed to the
		// case where the ambiguity is resolved: a prior COMPLETE backup of this
		// instance captured one of the now-missing volumes, which proves it
		// existed and is therefore genuinely lost rather than never created.
		if c.totalLoss() {
			// The message names only the volumes the evidence actually covers,
			// not the whole `lost` list (#265 review round-2, minor finding):
			// a lost volume with no earlier backup behind it may simply never
			// have been created, and claiming a backup captured it misleads
			// the operator about which volume to go looking for. The refusal
			// still stands for the run as a whole.
			corroborated, cerr := s.appliedLossCorroborated(ctx, host, tmpl, slug, c.lost)
			if cerr != nil {
				return nil, nil, cerr
			}
			if len(corroborated) > 0 {
				return nil, nil, fmt.Errorf("%w: volume(s) %s of %s/%s were applied and captured by an earlier backup but no longer exist on %s (a host rebuild, `podman volume prune`, or an evacuation that did not complete) — this backup would silently miss them",
					ErrInvalidBackupScope, strings.Join(corroborated, " "), tmpl, slug, host)
			}
		}
		// A template edited down to zero declared volumes while the applied set
		// still names volumes that DO materialise on the host: an unscoped
		// backup can never capture them (the export loop only ever walks
		// currently-declared names), so silently returning nil, nil here would
		// stop the pod for nothing and, worse, let the caller record a green
		// `complete` row with an empty volume list while real data sits
		// untouched on the host (review-round-2 finding 2 — "lying green").
		// Refuse instead; an operator who genuinely wants to stop backing this
		// template's volumes up can pass an explicit empty-of-real-intent scope
		// or delete/re-apply the instance to clear AppliedVolumes.
		if len(declared) == 0 {
			if c.materialized > 0 {
				return nil, nil, fmt.Errorf("%w: template %s no longer declares any volumes, but %s/%s was applied with volume(s) that still exist on %s — an unscoped backup would capture nothing while real data remains on the host",
					ErrInvalidBackupScope, tmpl, tmpl, slug, host)
			}
			return nil, c.meta, nil
		}

		// The ONE guard that survives from before #257, stated over direct
		// observation only: a volume EXISTS on the host under the CURRENTLY
		// declared name, and we would deliberately skip it because it is
		// `none`-vetoed, and there is nothing else to capture. Stopping the pod,
		// walking past real data on purpose and recording a `complete` row with
		// zero volumes is a lie dressed as a backup — retention counts that
		// green row and ages out the last one that actually held data. Every
		// term here is something the runner directly observed: the volume was
		// inspected on the host, and the veto is read from the template meta.
		// `c.meta` already merges the current template's declared markers with
		// the applied-time markers of every recovered/renamed volume (see
		// classifyVolumes), so this reads the SAME marker the export loop in
		// Backup will.
		var vetoed []string
		for _, v := range c.vols {
			if IsBackupMarkerNone(c.meta[v.Name].marker) {
				vetoed = append(vetoed, v.Name)
				continue
			}
			return c.vols, c.meta, nil // something exportable exists
		}
		if len(vetoed) > 0 {
			// Reaching here means every MATERIALISED volume is vetoed while some
			// DECLARED one is exportable but absent — the app has not created it
			// yet, or it was lost. Which of those is the unanswerable question
			// above, so the message asserts neither: it names what is present
			// and vetoed, and what is declared, exportable and absent, and lets
			// the operator recognise their own case.
			//
			// The rejection itself does not depend on telling them apart. Either
			// way this run would stop the pod, walk past real data on purpose,
			// and record a `complete` row with nothing in it — worth refusing on
			// the outage alone, before the lying green row is even considered.
			sort.Strings(vetoed)
			absent := make([]string, 0, len(declared))
			for short, marker := range declared {
				if !IsBackupMarkerNone(marker) && !c.present[volumeName(tmpl, slug, short)] {
					absent = append(absent, short)
				}
			}
			sort.Strings(absent)
			detail := ""
			if len(absent) > 0 {
				detail = fmt.Sprintf("; the exportable volume(s) %s are declared but do not exist on %s — if the instance has not created them yet, this resolves itself once it does",
					strings.Join(absent, " "), host)
			}
			return nil, nil, fmt.Errorf("%w: every volume of %s/%s present on %s is marked `backup: none` (%s), so this backup would stop the pod and capture nothing%s",
				ErrInvalidBackupScope, tmpl, slug, host, strings.Join(vetoed, " "), detail)
		}
		// Nothing currently materialises under a declared name that is not
		// vetoed, and (above) either nothing was applied, everything applied
		// still materialises (directly or under a rename), or SOME applied
		// volume is confirmed lost while at least one OTHER applied volume is
		// not — a shape this function deliberately does not refuse.
		//
		// That last case is a genuine, acknowledged gap, not an oversight: a
		// template can legitimately declare a volume that some deployments
		// never mount (an optional sidecar cache, a lazily-created path a
		// container only touches under a feature flag), and nothing this core
		// observes distinguishes "this specific applied volume was never
		// materialised" from "it existed once and is now gone" — `AppliedVolumes`
		// records what the template DECLARED at apply time, not what podman
		// actually created (review-round-2 finding 1). Refusing the whole
		// backup on that ambiguity would permanently 400 every future unscoped
		// backup of an instance with any such volume, for volumes that were
		// never expected to exist — a regression against the pre-#256 fallback,
		// which accepted whatever the host held. Total loss carries the same
		// ambiguity unless an earlier backup corroborates that the volumes
		// existed (see the total-loss branch above): the corroborated case is
		// refused, the ambiguous one is not, wherever it appears.
		return c.vols, c.meta, nil
	}
	for _, name := range volumes {
		marker, ok := declared[name]
		if !ok {
			return nil, nil, fmt.Errorf("%w: %q is not declared by template %s", ErrInvalidBackupScope, name, tmpl)
		}
		if IsBackupMarkerNone(marker) {
			return nil, nil, fmt.Errorf("%w: %q is marked `backup: none`", ErrInvalidBackupScope, name)
		}
	}
	if declaredOnly {
		return nil, meta, nil
	}
	// Existence, resolved through the same volumeName() the pod manifest and
	// Backup itself use, so the two cannot drift. InstanceVolumes returns only
	// the declared volumes that actually exist (it skips ErrNotFound and fails
	// loud on anything else), which is exactly the set wanted here.
	vols, err := s.InstanceVolumes(ctx, host, tmpl, slug)
	if err != nil {
		return nil, nil, fmt.Errorf("list volumes on %s: %w", host, err)
	}
	present := make(map[string]bool, len(vols))
	for _, v := range vols {
		present[v.Name] = true
	}
	var missing []string
	for _, name := range volumes {
		if !present[volumeName(tmpl, slug, name)] {
			missing = append(missing, name)
		}
	}
	if len(missing) > 0 {
		return nil, nil, fmt.Errorf("%w: requested volumes %v are declared by template %s but do not exist on %s (the instance predates the declaration — re-apply it first)", ErrInvalidBackupScope, missing, tmpl, host)
	}
	return vols, meta, nil
}

// volumeClassification is the resolved answer to "what does this instance
// actually hold on the host, and what may be exported from it" for an UNSCOPED
// backup — everything checkBackupable's guards need, computed once, in one
// place, and read-only thereafter (#265 review round-2 cleanup).
//
// It replaces the five sequentially-mutated locals those guards used to share
// (vols/present/materialized/lost/recovered, plus a meta map folded into
// mid-flight). Each guard's meaning depended on how far that mutation sequence
// had run by the time it was reached, which is how three of the last four
// review rounds each found a fresh ordering bug in this one function.
type volumeClassification struct {
	// vols is the export set: every CURRENTLY declared volume that exists on
	// the host, plus every applied volume recovered under its OLD (pre-rename)
	// name. Backup's export loop walks exactly this list.
	vols []podman.Volume
	// present is the full names in vols, for existence lookups.
	present map[string]bool
	// meta is the authoritative marker/exclude map keyed by FULL volume name:
	// the current template's declared metadata merged with the applied-time
	// metadata of every recovered volume.
	meta map[string]volMeta
	// materialized counts applied volumes confirmed to exist — under their
	// applied name or, for a rename, their old one.
	materialized int
	// lost names the applied (short) volumes confirmed absent under BOTH their
	// applied name and, when still declared, their current one.
	lost []string
	// recovered is the subset of vols found only under an old applied name.
	recovered []podman.Volume
}

// totalLoss reports the one unambiguous shape: the ENTIRE applied set is gone
// AND nothing else survives to export either. See checkBackupable's use of it
// for why each term is needed — in particular why a surviving,
// never-applied-but-declared volume disqualifies it.
func (c *volumeClassification) totalLoss() bool {
	return c.materialized == 0 && len(c.lost) > 0 && len(c.vols) == 0
}

// classifyVolumes resolves an instance's volumes on the host into a
// volumeClassification: it lists what currently materialises under the
// CURRENTLY declared names, classifies the spec's applied set against that,
// folds any rename-recovered volume into the export set, and merges that
// volume's applied-time metadata into the marker map.
//
// declared is the current template's short name -> backup marker; meta is the
// marker/exclude map already built from it, keyed by full name — it is
// extended here, not replaced, and returned inside the classification.
//
// #256/#257: when the spec's applied volume set is KNOWN (a spec written by an
// Apply after #257 shipped), it settles new/renamed/lost directly instead of
// guessing from what currently materialises under the CURRENTLY declared
// names. For each volume the instance was actually APPLIED with, existence is
// checked under the name it was applied under — which is what lets a template
// rename (declared differently now, but still present under its old name) read
// as "still here" rather than "gone".
//
// sp.AppliedVolumes == nil means the spec predates #257 (or has not been
// re-applied since) — unknown, not empty. Those rows classify as zero
// materialized, nothing lost, nothing recovered, and the caller's guards fall
// through to the pre-#257 behaviour, unchanged, until their next Apply
// populates it.
func (s *Service) classifyVolumes(ctx context.Context, host, tmpl, slug string, declared map[string]string, meta map[string]volMeta, sp store.Spec) (*volumeClassification, error) {
	// Safe to call even when declared is empty: InstanceVolumes then iterates
	// nothing and returns nil. Same call, same cost, as the explicit-scope
	// branch of checkBackupable.
	vols, err := s.InstanceVolumes(ctx, host, tmpl, slug)
	if err != nil {
		return nil, fmt.Errorf("list volumes on %s: %w", host, err)
	}
	c := &volumeClassification{
		vols:    vols,
		present: make(map[string]bool, len(vols)),
		meta:    meta,
	}
	for _, v := range vols {
		c.present[v.Name] = true
	}
	if sp.AppliedVolumes == nil {
		return c, nil
	}

	currentlyDeclared := make(map[string]bool, len(declared))
	for short := range declared {
		currentlyDeclared[volumeName(tmpl, slug, short)] = true
	}
	materialized, lost, recovered, recoveredMeta, err := s.appliedVolumeLoss(ctx, host, tmpl, slug, sp.AppliedVolumes, sp.AppliedVolumeMeta, c.present, currentlyDeclared)
	if err != nil {
		return nil, err
	}
	c.materialized, c.lost, c.recovered = materialized, lost, recovered

	// A rename appliedVolumeLoss confirmed present under its OLD applied name
	// never shows up in `vols` on its own: `vols` came from InstanceVolumes,
	// which walks only CURRENTLY declared names, so a renamed volume (declared
	// differently now) is invisible to it. Fold the resolved volume(s) in here,
	// under their applied name, so the export loop in Backup() — which walks
	// exactly this list — actually captures them instead of silently completing
	// without them (round-3 review, critical finding 1).
	for _, v := range recovered {
		c.vols = append(c.vols, v)
		c.present[v.Name] = true
	}
	// A recovered/renamed volume's full name is NOT a key in the declared
	// `meta` — the CURRENT template no longer declares it under that name, that
	// is exactly what makes it "recovered" rather than "materialized" (see
	// appliedVolumeLoss). Without this, both checkBackupable's vetoed-check and
	// Backup's export loop read its marker back as the zero value ("", i.e. not
	// vetoed) and its exclude patterns as none, regardless of what it was
	// actually marked — silently dropping a `backup: none` veto across a rename
	// (#256 review round-4, blocking finding). recoveredMeta is sourced from
	// store.Spec.AppliedVolumeMeta — captured at the apply that is still in
	// effect for this volume — not guessed from the current template.
	for name, m := range recoveredMeta {
		if _, ok := c.meta[name]; !ok {
			c.meta[name] = m
		}
	}
	return c, nil
}

// anyExportableVolume reports whether an unscoped backup of this instance could
// export anything at all, judged from the store alone — no host contact.
//
// It is true when any CURRENTLY declared volume is not `backup: none`, and also
// when the spec's applied set names a volume the template no longer declares
// whose APPLIED marker is not `none`: such a volume may still exist on the host
// under its old applied name and is exactly what checkBackupable's rename
// recovery goes looking for (#265 review finding 4). An applied name with no
// recorded marker (a spec whose last Apply predates AppliedVolumeMeta) counts
// as possibly exportable — the unknown must not manufacture a veto.
//
// Only the refusal is decided here. Whether such a volume actually exists is
// left to the host round trip that follows.
func anyExportableVolume(declared map[string]string, sp store.Spec) bool {
	for _, marker := range declared {
		if !IsBackupMarkerNone(marker) {
			return true
		}
	}
	for _, short := range sp.AppliedVolumes {
		if _, stillDeclared := declared[short]; stillDeclared {
			continue
		}
		if m, ok := sp.AppliedVolumeMeta[short]; ok && IsBackupMarkerNone(m.Backup) {
			continue
		}
		return true
	}
	return false
}

// appliedLossCorroborated returns WHICH of the given lost (short) applied
// volume names are known to have EXISTED at some point, by looking each up in
// an earlier COMPLETE backup of this instance. The result is sorted, and empty
// when nothing corroborates any of them.
//
// This is the only evidence of prior existence this core holds.
// store.Spec.AppliedVolumes records what the template DECLARED at apply time,
// not what podman actually created, so "applied but absent" alone cannot tell
// genuine loss from a volume that was never materialised — which is why the
// total-loss refusal needs this corroboration and the partial-loss path does
// not refuse at all (#265 review finding 2).
//
// It reports the corroborated NAMES rather than a single bool for the whole
// list (#265 review round-2, minor finding): the evidence is per volume, and
// the caller's refusal message names what it was given. Collapsed to a bool,
// a two-volume loss with evidence for one told the operator that BOTH were
// "captured by an earlier backup" — misattributing proof of existence to a
// volume that may never have been created at all. The refusal itself is
// unchanged: any corroborated volume still refuses the whole run.
//
// A store failure is returned, not swallowed: it is the same class of error as
// the GetSpec at the top of checkBackupable, and reporting it as "no evidence"
// would silently downgrade a refusal.
func (s *Service) appliedLossCorroborated(ctx context.Context, host, tmpl, slug string, lost []string) ([]string, error) {
	backups, err := s.store.ListBackups(ctx, host, tmpl, slug, store.MaxJobLimit)
	if err != nil {
		return nil, fmt.Errorf("list backups of %s/%s on %s: %w", tmpl, slug, host, err)
	}
	short := make(map[string]string, len(lost))
	for _, name := range lost {
		short[volumeName(tmpl, slug, name)] = name
	}
	seen := map[string]bool{}
	var corroborated []string
	for _, b := range backups {
		if b.State != store.BackupComplete {
			continue
		}
		for _, v := range b.Volumes {
			name, ok := short[v.Name]
			if !ok || seen[name] {
				continue
			}
			seen[name] = true
			corroborated = append(corroborated, name)
		}
	}
	sort.Strings(corroborated)
	return corroborated, nil
}

// appliedVolumeLoss classifies a spec's applied volume set against the host:
// for each applied (short) name it decides whether the volume is confirmed
// present — "materialized", counted — or confirmed absent under BOTH its
// applied name and, when still declared, its current name — "lost", named in
// the returned slice.
//
// present is the set of full volume names InstanceVolumes already resolved as
// existing under a CURRENTLY declared name; currentlyDeclared is the full set
// of full names the template declares NOW (present or not). Together they let
// this function skip a second host round trip for any applied name that maps
// to a currently-declared name: InstanceVolumes already inspected every one of
// those (it iterates exactly t.Meta.Volumes), so a currently-declared name
// missing from `present` is confirmed absent without asking the host again
// (review-round-2 minor finding: this used to cost up to 2N round trips for N
// applied volumes; now only names the template no longer declares — a rename
// or a drop — need their own VolumeInspect).
//
// recovered carries the resolved podman.Volume for every applied name found
// ONLY under its old applied name (i.e. not already covered by `present`) —
// the caller folds these into its export set, since InstanceVolumes never
// looked for a name the template no longer declares (round-3 review, critical
// finding 1: without this, a confirmed-surviving rename was never actually
// backed up, only accepted).
//
// appliedMeta is sp.AppliedVolumeMeta — the marker/exclude each applied
// (short) name carried at the apply that produced it. recoveredMeta returns
// that metadata keyed by each recovered volume's OLD full name, since neither
// caller can look it up any other way once the volume is folded into `vols`
// under that name: the CURRENT template's declared map has no entry for a
// name it no longer declares. appliedMeta may be nil (a spec whose last Apply
// predates this field) — recoveredMeta is then simply empty, and both callers
// fall back to their pre-existing zero-value behaviour, unchanged (#256
// review round-4, blocking finding).
func (s *Service) appliedVolumeLoss(ctx context.Context, host, tmpl, slug string, applied []string, appliedMeta map[string]store.AppliedVolumeMarker, present, currentlyDeclared map[string]bool) (materialized int, lost []string, recovered []podman.Volume, recoveredMeta map[string]volMeta, err error) {
	recoveredMeta = map[string]volMeta{}
	for _, short := range applied {
		full := volumeName(tmpl, slug, short)
		if present[full] {
			materialized++
			continue
		}
		if currentlyDeclared[full] {
			// Already inspected (and found absent) by the InstanceVolumes call
			// that produced `present` — no need to ask again.
			lost = append(lost, short)
			continue
		}
		// The template no longer declares this short name under itself — either
		// a rename or a drop — so InstanceVolumes never looked for it. It needs
		// its own inspect, under the name it was APPLIED with, to tell "moved"
		// from "gone".
		v, ierr := s.client.VolumeInspect(ctx, host, full)
		if ierr != nil {
			if errors.Is(ierr, podman.ErrNotFound) {
				lost = append(lost, short)
				continue
			}
			return 0, nil, nil, nil, fmt.Errorf("inspect volume %q: %w", full, ierr)
		}
		materialized++
		recovered = append(recovered, v)
		if m, ok := appliedMeta[short]; ok {
			recoveredMeta[full] = volMeta{marker: m.Backup, exclude: m.Exclude}
		}
	}
	return materialized, lost, recovered, recoveredMeta, nil
}

// Backup snapshots every volume of an instance into the blob store: stop,
// export each volume (teed into the blob write and the manifest build in one
// pass), record metadata, restart. The instance is restarted even on failure;
// it is only restarted at all if it was running to begin with. step is a
// best-effort progress callback (may be nil).
func (s *Service) Backup(ctx context.Context, req BackupRequest, step func(step, detail string)) error {
	if step == nil {
		step = func(string, string) {}
	}
	// Same lock as migrate: backup/restore/migrate of one instance serialize.
	lk := s.migrateLock(req.Template, req.Slug)
	lk.Lock()
	defer lk.Unlock()

	// The recheck under the lock is deliberate — the handler's own pre-lock
	// check may be arbitrarily stale by now — but its resolved volume list is
	// reused for the export loop below rather than being recomputed from
	// scratch, so a backup costs one InstanceVolumes round trip here, not two
	// (review-5 finding 7). `volMetas` is the merged marker/exclude map that
	// same call built, keyed by full volume name: the current template's
	// declared metadata plus the applied-time metadata of any volume recovered
	// under an old (renamed) applied name. Backup used to rebuild the latter
	// half from a SECOND, independent GetSpec that could fail or observe a
	// different spec, silently dropping a `backup: none` veto across a rename
	// (#265 review finding 1). There is exactly one spec read now, and the
	// export loop below consumes its result.
	vols, volMetas, err := s.checkBackupable(ctx, req.Host, req.Template, req.Slug, req.Volumes, false)
	if err != nil {
		return err
	}

	// Image hint + prior run-state. Get also confirms the pod exists.
	obs, err := s.Get(ctx, req.Host, req.Template, req.Slug)
	if err != nil {
		return err
	}
	// wasRunning gates the post-backup restart. Degraded (some containers up,
	// some crashed) counts as running: the instance was serving before the
	// backup, so leaving it fully stopped afterwards would be a downgrade.
	wasRunning := obs.Pod.Status == "Running" || obs.Pod.Status == "Degraded"
	image := ""
	if len(obs.Containers) > 0 {
		image = obs.Containers[0].Image
	}
	step("load", req.Host+"/"+req.Template+"/"+req.Slug)

	if err := s.store.CreateBackup(ctx, store.Backup{
		ID: req.BackupID, Host: req.Host, Template: req.Template, Slug: req.Slug,
		State: store.BackupCreating, Image: image,
	}); err != nil {
		return fmt.Errorf("record backup: %w", err)
	}

	// Cleanup helpers run on a detached context: the failure may BE a ctx
	// cancellation, and the row must still be marked failed / the instance
	// restarted (same pattern as migrate's rollback).
	fail := func(cause error) error {
		dctx := context.WithoutCancel(ctx)
		if _, ferr := s.store.FailBackup(dctx, req.BackupID); ferr != nil {
			step("mark-failed-failed", ferr.Error())
		}
		if derr := s.blobs.DeleteAll(dctx, backupBlobPrefix(req.Host, req.Template, req.Slug, req.BackupID)); derr != nil {
			step("cleanup-blobs-failed", derr.Error())
		}
		return cause
	}
	restart := func() {
		if !wasRunning {
			return
		}
		if _, rerr := s.Start(context.WithoutCancel(ctx), req.Host, req.Template, req.Slug); rerr != nil {
			step("restart-failed", rerr.Error())
		} else {
			step("restart", req.Host)
		}
	}

	if err := s.runPreBackup(ctx, req, step); err != nil {
		return fail(err) // instance not yet stopped; nothing to restart
	}

	if err := s.Stop(ctx, req.Host, req.Template, req.Slug); err != nil {
		return fail(fmt.Errorf("stop instance: %w", err))
	}
	step("stop", req.Host)

	// `vols` and `volMetas` were both resolved by the under-lock
	// checkBackupable above; neither the host nor the store is consulted again
	// for them here.

	// Scope in declared short names, resolved through the same volumeName() the
	// pod manifest uses. Empty scope means "everything not vetoed".
	scope := make(map[string]bool, len(req.Volumes))
	for _, short := range req.Volumes {
		scope[volumeName(req.Template, req.Slug, short)] = true
	}

	var bvols []store.BackupVolume
	exported := map[string]bool{}
	for _, v := range vols {
		m := volMetas[v.Name]
		if IsBackupMarkerNone(m.marker) {
			// State the absence rather than leaving it to be inferred from a
			// backup that silently lacks a volume.
			step("skip-volume", v.Name+" (backup: none)")
			continue
		}
		if len(scope) > 0 && !scope[v.Name] {
			// A DIFFERENT step name from the veto above, deliberately. Both
			// produce a backup missing a volume, and after the fact the job
			// trail is the only place an operator can tell "you asked for db
			// and it was vetoed" from "db was never in this request's scope" —
			// so `skip-volume` keeps meaning exactly the veto.
			step("skip-volume-scope", v.Name+" (not in the requested scope)")
			continue
		}
		// Emitted BEFORE the export so a multi-minute volume shows the phase in
		// progress rather than nothing until it returns (#135).
		step("export-volume", v.Name)
		bv, err := s.backupVolume(ctx, req, v.Name, m.exclude)
		if err != nil {
			restart()
			return fail(fmt.Errorf("backup volume %q: %w", v.Name, err))
		}
		bvols = append(bvols, bv)
		exported[v.Name] = true
		step("export-volume-done", fmt.Sprintf("%s (%d bytes)", v.Name, bv.SizeBytes))
	}

	// Every volume an EXPLICIT scope named must have been captured — this is a
	// SET comparison, not an emptiness check. A partial hit is the dangerous
	// shape: {"db","sites"} with `db` gone (host rebuild, an evacuated volume)
	// captures `sites` alone and would otherwise record a green `complete` row
	// that a later restore tears the pod down for and brings `db` back
	// unrestored — while the operator believes it was captured. An emptiness
	// check catches only the all-missing case, so it misses exactly that.
	//
	// This is now a DEFENSIVE BACKSTOP, not the primary guard: CheckBackupable
	// resolves the same existence question up front, before anything is
	// stopped, so in the ordinary case nothing reaches here. What is left is the
	// race — a volume removed between that check and this export — where
	// failing after the fact is the only option available.
	if len(req.Volumes) > 0 {
		var missing []string
		for _, short := range req.Volumes {
			if !exported[volumeName(req.Template, req.Slug, short)] {
				missing = append(missing, short)
			}
		}
		if len(missing) > 0 {
			restart()
			return fail(fmt.Errorf("%w: requested volumes %v were not captured (they disappeared from %s during the backup)", ErrInvalidBackupScope, missing, req.Host))
		}
	}
	// There is deliberately NO post-export emptiness check on the UNSCOPED path.
	// The one guard that case needs — "volumes exist on the host and every one
	// of them is `none`-vetoed" — is decided by checkBackupable above, from the
	// same volume listing this loop just consumed, BEFORE the pod is stopped. A
	// second copy here could only differ from it by re-observing state mid-run,
	// which is the inference this design stopped making.

	ok, err := s.store.CompleteBackup(ctx, req.BackupID, bvols)
	if err != nil {
		restart()
		return fail(fmt.Errorf("complete backup: %w", err))
	}
	if !ok {
		// Row left creating-state while we held the lock — only a concurrent
		// reconciler marking it failed can do that, which cannot happen while
		// the job itself is live. Defensive.
		restart()
		return fail(fmt.Errorf("backup %s no longer in creating state", req.BackupID))
	}
	restart()
	step("complete", req.BackupID)
	return nil
}

// backupVolume exports one volume, teeing the tar into the blob store and
// the manifest builder in a single pass. The blob is committed only after a
// clean EOF + manifest build.
//
// Integrity assumption: Go's archive/tar returns a clean io.EOF (not
// io.ErrUnexpectedEOF) when a stream is truncated on a 512-byte entry
// boundary, so buildManifest would fingerprint a well-formed-but-short tar
// without error. The integrity of the committed blob therefore rests on the
// transport surfacing short reads as errors — which Go's net/http body does
// for all three framing modes (Content-Length, chunked, connection-close) when
// a connection drops mid-transfer. A truncated tar emitted by podman itself
// would not be detected: no expected-size oracle exists on this path.
//
// When patterns is non-empty the copy is not byte-for-byte: the tar is decoded,
// filtered and re-encoded in the same pass that builds the manifest (#248).
// With no patterns the original TeeReader copy runs unchanged, so every volume
// that has not opted in is bit-identical to before.
func (s *Service) backupVolume(ctx context.Context, req BackupRequest, name string, patterns []string) (store.BackupVolume, error) {
	rc, err := s.client.VolumeExport(ctx, req.Host, name)
	if err != nil {
		return store.BackupVolume{}, fmt.Errorf("export: %w", err)
	}
	defer rc.Close()

	w, err := s.blobs.Put(ctx, backupBlobKey(req.Host, req.Template, req.Slug, req.BackupID, name))
	if err != nil {
		return store.BackupVolume{}, fmt.Errorf("open blob: %w", err)
	}
	cw := &countingWriter{w: w}

	var (
		m     Manifest
		stats dropStats
	)
	if len(patterns) == 0 {
		m, err = buildManifest(io.TeeReader(rc, cw))
	} else {
		m, stats, err = filterTar(cw, rc, patterns)
	}
	if err != nil {
		_ = w.Abort()
		return store.BackupVolume{}, fmt.Errorf("read tar: %w", err)
	}
	if err := w.Commit(); err != nil {
		return store.BackupVolume{}, fmt.Errorf("commit blob: %w", err)
	}
	raw, err := json.Marshal(m)
	if err != nil {
		return store.BackupVolume{}, fmt.Errorf("marshal manifest: %w", err)
	}
	// #293: the manifest is written as its own gzip-compressed blob, not
	// inline in the row — see the BackupVolume doc comment for the two-shape
	// read contract this establishes.
	sum := sha256.Sum256(raw)
	if err := s.writeManifestBlob(ctx, req.Host, req.Template, req.Slug, req.BackupID, name, raw); err != nil {
		return store.BackupVolume{}, fmt.Errorf("write manifest blob: %w", err)
	}
	bv := store.BackupVolume{Name: name, SizeBytes: cw.n, ManifestSHA256: hex.EncodeToString(sum[:])}
	if len(patterns) > 0 {
		bv.Excluded = &store.ExcludedPaths{
			Patterns: stats.Patterns, Entries: stats.Entries, Bytes: stats.Bytes,
		}
	}
	return bv, nil
}

// writeManifestBlob gzip-compresses raw (the manifest's uncompressed JSON)
// and commits it to the blob store at manifestBlobKey(...). Mirrors the
// Put/Commit/Abort contract backupVolume uses for the tar itself.
func (s *Service) writeManifestBlob(ctx context.Context, host, tmpl, slug, id, volume string, raw []byte) error {
	w, err := s.blobs.Put(ctx, manifestBlobKey(host, tmpl, slug, id, volume))
	if err != nil {
		return fmt.Errorf("open blob: %w", err)
	}
	gz := gzip.NewWriter(w)
	if _, err := gz.Write(raw); err != nil {
		_ = w.Abort()
		return fmt.Errorf("gzip: %w", err)
	}
	if err := gz.Close(); err != nil {
		_ = w.Abort()
		return fmt.Errorf("gzip close: %w", err)
	}
	if err := w.Commit(); err != nil {
		return fmt.Errorf("commit: %w", err)
	}
	return nil
}

// loadManifest returns the manifest for one backed-up volume, transparently
// handling both storage shapes (#293): an OLD row with the manifest inline
// (bv.Manifest non-empty) is unmarshaled directly; a NEW row (ManifestSHA256
// set, bv.Manifest empty) fetches and gunzips the sidecar blob, then verifies
// its sha256 against the row before trusting its content — the row is the
// thing restore does NOT have to trust the blob store alone for.
//
// A row with neither (should not happen — backupVolume always sets one or the
// other) is reported as a corrupt/missing manifest rather than silently
// treated as an empty one, since an empty manifest would make restore
// verification pass vacuously.
func (s *Service) loadManifest(ctx context.Context, b store.Backup, bv store.BackupVolume) (Manifest, error) {
	if len(bv.Manifest) > 0 {
		var m Manifest
		if err := json.Unmarshal(bv.Manifest, &m); err != nil {
			return nil, fmt.Errorf("stored manifest corrupt: %w", err)
		}
		return m, nil
	}
	if bv.ManifestSHA256 == "" {
		return nil, fmt.Errorf("no manifest recorded for volume %q (neither inline nor blob reference)", bv.Name)
	}
	rc, err := s.blobs.Get(ctx, manifestBlobKey(b.Host, b.Template, b.Slug, b.ID, bv.Name))
	if err != nil {
		return nil, fmt.Errorf("open manifest blob: %w", err)
	}
	defer rc.Close()
	gz, err := gzip.NewReader(rc)
	if err != nil {
		return nil, fmt.Errorf("gzip reader: %w", err)
	}
	raw, err := io.ReadAll(gz)
	if err != nil {
		return nil, fmt.Errorf("read manifest blob: %w", err)
	}
	if err := gz.Close(); err != nil {
		return nil, fmt.Errorf("gzip close: %w", err)
	}
	sum := sha256.Sum256(raw)
	if got := hex.EncodeToString(sum[:]); got != bv.ManifestSHA256 {
		return nil, fmt.Errorf("manifest blob for volume %q does not match recorded sha256 (want %s, got %s)", bv.Name, bv.ManifestSHA256, got)
	}
	var m Manifest
	if err := json.Unmarshal(raw, &m); err != nil {
		return nil, fmt.Errorf("manifest blob corrupt: %w", err)
	}
	return m, nil
}

// countingWriter counts bytes through to w.
type countingWriter struct {
	w io.Writer
	n int64
}

func (c *countingWriter) Write(p []byte) (int, error) {
	n, err := c.w.Write(p)
	c.n += int64(n)
	return n, err
}

// RestoreRequest is the restore job's args.
type RestoreRequest struct {
	BackupID string `json:"backup_id"`
}

// CheckRestorable runs the synchronous validation the POST handler needs and
// returns the backup row: row exists and is complete, host known and not
// draining, instance (spec) still present, and EVERY blob of the backup
// readable. The drain check is upfront so a draining host can't fail the job
// after teardown; the blob preflight is here for the same reason and a stronger
// one. It used to run at the top of restorePostTeardown, i.e. after Restore had
// already deleted the pod — so a missing blob left every volume untouched (the
// point of checking the whole set at once) while the instance was DOWN, Apply
// unreached, and stayed down until a human noticed a failed job. It needs
// nothing from the teardown, so running it here turns that total outage into a
// rejected request with the instance still serving.
func (s *Service) CheckRestorable(ctx context.Context, backupID string) (store.Backup, error) {
	if s.blobs == nil {
		return store.Backup{}, ErrBackupsDisabled
	}
	b, err := s.store.GetBackup(ctx, backupID)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return store.Backup{}, fmt.Errorf("%w: %s", ErrBackupNotFound, backupID)
		}
		return store.Backup{}, err
	}
	if b.State != store.BackupComplete {
		return store.Backup{}, fmt.Errorf("%w: state %s", ErrBackupNotRestorable, b.State)
	}
	hostCfg, ok := s.host(b.Host)
	if !ok {
		return store.Backup{}, ErrUnknownHost
	}
	if hostCfg.Drain {
		return store.Backup{}, ErrHostDraining
	}
	if _, err := s.store.GetSpec(ctx, b.Host, b.Template, b.Slug); err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return store.Backup{}, ErrInstanceNotFound
		}
		return store.Backup{}, err
	}
	if err := s.preflightBlobs(ctx, b); err != nil {
		return store.Backup{}, err
	}
	return b, nil
}

// Restore replaces an instance's volumes in place from a backup: stop, tear
// down containers + volumes, recreate volumes from blobs, verify each against
// the stored manifest, re-apply the CURRENT spec, wait healthy. There is no
// rollback: a failure after teardown leaves the instance DOWN with volumes
// partially restored, but the spec row is preserved so the restore can be
// retried. The job error names the failed step.
// step is a best-effort progress callback (may be nil).
func (s *Service) Restore(ctx context.Context, req RestoreRequest, step func(step, detail string)) error {
	if step == nil {
		step = func(string, string) {}
	}
	b, err := s.CheckRestorable(ctx, req.BackupID)
	if err != nil {
		return err
	}

	lk := s.migrateLock(b.Template, b.Slug)
	lk.Lock()
	defer lk.Unlock()

	// Re-check under the lock (a concurrent delete may have raced us). This
	// re-runs the blob preflight too, so the set is known complete as late as
	// possible before the teardown — the narrowest window this design allows.
	b, err = s.CheckRestorable(ctx, req.BackupID)
	if err != nil {
		return err
	}
	spec, err := s.store.GetSpec(ctx, b.Host, b.Template, b.Slug)
	if err != nil {
		return err
	}
	step("load", b.Host+"/"+b.Template+"/"+b.Slug)
	// The preflight itself ran inside CheckRestorable above (twice: once before
	// the lock, once under it). Report it here so the job trail keeps naming the
	// phase, in the same load → preflight → teardown order it always had.
	step("preflight-blobs", fmt.Sprintf("%d volume(s)", len(b.Volumes)))

	// Teardown: pod + volumes (a referenced volume can't be removed). Keep
	// per-instance secrets — Apply below re-pushes them from the spec anyway,
	// and host-scoped secrets must survive. Delete also reconciles away the
	// spec row; Apply re-persists it. Tolerate an already-gone pod.
	// PruneVolumes is deliberately false: a backup may cover fewer volumes than
	// the instance declares (a scoped backup, or one whose `none`-marked volumes
	// were vetoed), and a pruning teardown would delete those and never put them
	// back. Each volume the backup DOES carry is removed by restoreVolume
	// immediately before it is recreated, so an import never merges into stale
	// content. Removing the pod first is what makes those volumes unreferenced.
	if err := s.Delete(ctx, b.Host, b.Template, b.Slug, DeleteOptions{PruneVolumes: false}); err != nil && !errors.Is(err, ErrInstanceNotFound) {
		return fmt.Errorf("teardown: %w", err)
	}
	step("teardown", b.Host)

	if err := s.restorePostTeardown(ctx, b, spec, step); err != nil {
		// Re-persist the desired-state row on a detached context: the teardown
		// above deleted it, Apply (which re-persists it) was not reached or
		// failed, and the failure may BE a ctx cancellation. Without this, a
		// failed restore strands the instance spec-less and unretryable
		// (CheckRestorable requires the spec) — losing desired state, which the
		// no-rollback design does NOT permit. Volumes stay as the failure left
		// them; the instance stays down; the job error names the failed step.
		if perr := s.store.PutSpec(context.WithoutCancel(ctx), spec); perr != nil {
			step("respec-failed", perr.Error())
		} else {
			step("respec", b.Host+"/"+b.Template+"/"+b.Slug)
		}
		return err
	}
	return nil
}

// restorePostTeardown runs the post-teardown steps of a restore: recreate
// volumes from blobs, re-apply the spec, wait healthy. Any error here is
// handled by the caller, which re-persists the spec row before returning.
// The whole-set blob preflight this used to open with now runs in
// CheckRestorable, ahead of the pod teardown (see there).
func (s *Service) restorePostTeardown(ctx context.Context, b store.Backup, spec store.Spec, step func(step, detail string)) error {
	for _, bv := range b.Volumes {
		if err := s.restoreVolume(ctx, b, bv); err != nil {
			return fmt.Errorf("restore volume %q: %w", bv.Name, err)
		}
		step("restore-volume", bv.Name)
	}

	if err := s.Apply(ctx, b.Host, ApplyRequest{
		Template: b.Template, Slug: b.Slug,
		Parameters: spec.Parameters, Secrets: spec.Secrets, Domains: spec.Domains,
		// A restore re-applies the instance as it was, extra networks included (#270).
		Networks: slices.Clone(spec.AppliedNetworks),
	}, ApplyOptions{Replace: false}); err != nil {
		return fmt.Errorf("apply: %w", err)
	}
	step("apply", b.Host)

	if err := s.waitRunning(ctx, b.Host, b.Template, b.Slug); err != nil {
		return fmt.Errorf("verify: %w", err)
	}
	step("verify", b.Host)
	return nil
}

// preflightBlobs confirms every blob of a backup is readable, without keeping
// any of them open. It is the instance-wide half of "a restore may never
// delete a volume it has no copy of": restoreVolume enforces it per volume,
// this enforces it across the set before the first removal happens — and, run
// from CheckRestorable, before the pod is torn down at all. A 3-volume restore
// whose third blob is missing would otherwise roll volumes 1 and 2 back to
// backup-epoch content, then fail, leaving the instance down and mixed-epoch
// with those two overwrites unrecoverable.
//
// BlobStore has no Stat, so existence is checked by opening and immediately
// closing each reader. They are deliberately not held open across the restore —
// a many-volume instance would otherwise pin one file handle (or one HTTP body,
// on the S3 backend) per volume for the whole run.
func (s *Service) preflightBlobs(ctx context.Context, b store.Backup) error {
	for _, bv := range b.Volumes {
		rc, err := s.blobs.Get(ctx, backupBlobKey(b.Host, b.Template, b.Slug, b.ID, bv.Name))
		if err != nil {
			if errors.Is(err, fs.ErrNotExist) {
				return fmt.Errorf("%w: blob for volume %q missing", ErrBackupNotRestorable, bv.Name)
			}
			return fmt.Errorf("open blob for volume %q: %w", bv.Name, err)
		}
		_ = rc.Close()
	}
	return nil
}

// restoreVolume recreates one volume from its blob and verifies the imported
// content against the manifest recorded at backup time. Unlike migrate, restore
// always verifies regardless of the verifyVolumes flag — it is the only safety
// mechanism available when restoring from a blob (no live source to compare against).
func (s *Service) restoreVolume(ctx context.Context, b store.Backup, bv store.BackupVolume) error {
	// Open the blob FIRST, before anything destructive. A missing or unreadable
	// blob (blob dir wiped, a partial DeleteAll, the blob store re-pointed) must
	// leave the existing volume exactly as it was: the instance is already
	// stopped and torn down by this point, so a remove/create ahead of this
	// check would destroy the live content and then discover there is no copy to
	// put back. "A restore may never delete a volume it has no copy of" is the
	// invariant, and this ordering is what enforces it.
	rc, err := s.blobs.Get(ctx, backupBlobKey(b.Host, b.Template, b.Slug, b.ID, bv.Name))
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return fmt.Errorf("%w: blob for volume %q missing", ErrBackupNotRestorable, bv.Name)
		}
		return fmt.Errorf("open blob: %w", err)
	}
	defer rc.Close()

	// Remove before create: VolumeCreate is idempotent, so without this an
	// import would merge into whatever the old volume still held. ErrNotFound is
	// ordinary — a DR rebuild restores onto a host with no such volume yet.
	if err := s.client.VolumeRemove(ctx, b.Host, bv.Name, true); err != nil && !errors.Is(err, podman.ErrNotFound) {
		return fmt.Errorf("remove before restore: %w", err)
	}
	if err := s.client.VolumeCreate(ctx, b.Host, bv.Name); err != nil {
		return fmt.Errorf("create: %w", err)
	}
	if err := s.client.VolumeImport(ctx, b.Host, bv.Name, rc); err != nil {
		return fmt.Errorf("import: %w", err)
	}

	want, err := s.loadManifest(ctx, b, bv)
	if err != nil {
		return fmt.Errorf("load manifest: %w", err)
	}
	// Strip excluded paths from the stored manifest so old backups (captured
	// before the exclusion filter existed) compare equally with the re-exported
	// volume. (#142 review)
	for k := range want {
		if excludePath(k) {
			delete(want, k)
		}
	}
	got, err := s.volumeManifest(ctx, b.Host, bv.Name)
	if err != nil {
		return fmt.Errorf("re-export for verify: %w", err)
	}
	if diff, ok := want.firstDiff(got); !ok {
		return fmt.Errorf("%w: volume %q differs at %q", ErrVolumeIntegrity, bv.Name, diff)
	}
	return nil
}

// ListBackups returns an instance's backups, newest first.
func (s *Service) ListBackups(ctx context.Context, host, tmpl, slug string, limit int) ([]store.Backup, error) {
	if _, ok := s.host(host); !ok {
		return nil, ErrUnknownHost
	}
	return s.store.ListBackups(ctx, host, tmpl, slug, limit)
}

// GetBackup returns one backup row, mapping absence to ErrBackupNotFound.
func (s *Service) GetBackup(ctx context.Context, id string) (store.Backup, error) {
	b, err := s.store.GetBackup(ctx, id)
	if errors.Is(err, store.ErrNotFound) {
		return store.Backup{}, fmt.Errorf("%w: %s", ErrBackupNotFound, id)
	}
	return b, err
}

// DeleteBackup removes a backup's blobs, then its row — in that order, so a
// crash between the two leaves a harmless blob-less row rather than orphaned
// blobs. Callers must check BackupDeletable first.
func (s *Service) DeleteBackup(ctx context.Context, id string) error {
	if s.blobs == nil {
		return ErrBackupsDisabled
	}
	b, err := s.GetBackup(ctx, id)
	if err != nil {
		return err
	}
	if err := s.blobs.DeleteAll(ctx, backupBlobPrefix(b.Host, b.Template, b.Slug, b.ID)); err != nil {
		return fmt.Errorf("delete blobs: %w", err)
	}
	if err := s.store.DeleteBackup(ctx, id); err != nil && !errors.Is(err, store.ErrNotFound) {
		return err
	}
	return nil
}

// ReconcileBackup drives a backup interrupted by a daemon restart to a
// terminal state: mark the row failed (CAS — a row that already completed
// means the job finished its work and only the terminal write was lost),
// delete any partial blobs, and restart the instance. Returns
// (ok=true) when the backup actually completed, (ok=false, message) when it
// was failed. resolved=false only when the host is unreachable and the
// restart attempt was inconclusive.
//
// Unlike Backup, which only restarts if the instance was running before the
// snapshot began, ReconcileBackup always attempts to restart: post-crash the
// prior run-state is unknowable, so reconcile errs on the side of
// availability. A deliberately-stopped instance interrupted mid-backup may
// therefore come back running.
func (s *Service) ReconcileBackup(ctx context.Context, req BackupRequest, step func(step, detail string)) (resolved, ok bool, message string, err error) {
	if step == nil {
		step = func(string, string) {}
	}
	lk := s.migrateLock(req.Template, req.Slug)
	lk.Lock()
	defer lk.Unlock()

	b, gerr := s.store.GetBackup(ctx, req.BackupID)
	if gerr != nil {
		if errors.Is(gerr, store.ErrNotFound) {
			// Row never created — the job died before CreateBackup. Nothing on
			// disk, nothing to clean.
			return true, false, "interrupted before the backup row was created", nil
		}
		return false, false, "", gerr
	}
	if b.State == store.BackupComplete {
		// Work finished; only the job's terminal write was lost.
		return true, true, "", nil
	}

	// Mutations run on a detached context so a sweep/shutdown cancellation
	// cannot strand a half-finished compensation, mirroring Backup's own
	// fail/restart helpers.
	dctx := context.WithoutCancel(ctx)

	if _, ferr := s.store.FailBackup(dctx, req.BackupID); ferr != nil {
		return false, false, "", ferr
	}
	step("reconcile-mark-failed", req.BackupID)
	if derr := s.blobs.DeleteAll(dctx, backupBlobPrefix(req.Host, req.Template, req.Slug, req.BackupID)); derr != nil {
		step("reconcile-cleanup-blobs-failed", derr.Error())
	}

	// Guard before Start: if the host left the config, Start returns
	// ErrUnknownHost (via lookup), which is not ErrInstanceNotFound, so
	// without this check the else-branch below would return a non-nil err and
	// the runner would retry every sweep forever — the same infinite-retry-loop
	// ReconcileMigrate's host guards prevent.
	// FailBackup and DeleteAll above are store-local / API-server-local and
	// succeed regardless of host reachability, so they run unconditionally.
	if _, ok := s.host(req.Host); !ok {
		return true, false, "host " + req.Host + " is no longer configured; manual cleanup may be required", nil
	}

	// Restart best-effort: Start of a running pod is harmless; an unreachable
	// host leaves the job reconciling for the next sweep.
	if _, serr := s.Start(dctx, req.Host, req.Template, req.Slug); serr != nil {
		if errors.Is(serr, ErrInstanceNotFound) || errors.Is(serr, podman.ErrNotFound) {
			step("reconcile-restart-skipped", "instance gone")
		} else {
			return false, false, "", fmt.Errorf("restart instance: %w", serr)
		}
	} else {
		step("reconcile-restart", req.Host)
	}
	return true, false, "backup interrupted by daemon restart; instance restarted", nil
}

// runPreBackup runs the template's pre_backup command inside the named container
// before any stop/export. A transport error or non-zero exit aborts the backup
// so a failed dump never yields a stale/partial snapshot. No-op when the
// template declares no pre_backup. The "pre-backup" step is emitted only when a
// command actually runs, so no-op templates don't report a phantom step.
func (s *Service) runPreBackup(ctx context.Context, req BackupRequest, step func(step, detail string)) error {
	t, err := s.store.GetTemplate(ctx, req.Template)
	if err != nil {
		return fmt.Errorf("pre-backup: get template: %w", err)
	}
	if t.Meta.PreBackup == nil || t.Meta.PreBackup.Command == "" {
		return nil
	}
	spec, err := s.store.GetSpec(ctx, req.Host, req.Template, req.Slug)
	if err != nil {
		return fmt.Errorf("pre-backup: get spec: %w", err)
	}
	cmdStr, err := render.RenderBody(t.Meta.PreBackup.Command, spec.Parameters)
	if err != nil {
		return fmt.Errorf("pre-backup: render command: %w", err)
	}
	container := podName(req.Template, req.Slug) + "-" + t.Meta.PreBackup.Container
	// Emit before the exec so a long-running command (e.g. a DB dump) shows the
	// phase in progress rather than nothing until it returns.
	step("pre-backup", t.Meta.PreBackup.Container)
	res, err := s.client.ContainerExec(ctx, req.Host, container, []string{"/bin/sh", "-lc", cmdStr})
	if err != nil {
		return fmt.Errorf("pre-backup: exec in %s: %w", container, err)
	}
	if res.ExitCode != 0 {
		return fmt.Errorf("pre-backup: command exited %d in %s: %s", res.ExitCode, container, res.Output)
	}
	return nil
}

// BackupDeletable checks whether a backup is safe to delete: returns nil if
// neither a backup nor a restore job is active for it, ErrBackupBusy if one
// is. When js is nil (jobs disabled) the backup is always considered deletable.
// Callers must invoke this before Service.DeleteBackup.
func BackupDeletable(ctx context.Context, js store.JobStore, backupID string) error {
	if js == nil {
		return nil
	}
	for _, check := range []func(context.Context, store.JobStore, string) (bool, error){
		RestoreInFlight,
		BackupInFlight,
	} {
		busy, err := check(ctx, js, backupID)
		if err != nil {
			return err
		}
		if busy {
			return ErrBackupBusy
		}
	}
	return nil
}

// jobTargetsBackup scans active jobs of the given kind for one whose args
// carry backupID. Returns true if found. It is the shared inner loop for
// RestoreInFlight and BackupInFlight.
//
// The scan covers at most store.MaxJobLimit (1000) active jobs per state, so a
// deployment exceeding that limit could theoretically slip the busy gate —
// accepted at current scale.
func jobTargetsBackup(ctx context.Context, js store.JobStore, kind, backupID string, unmarshal func([]byte) (string, error)) (bool, error) {
	for _, st := range []store.JobState{store.JobQueued, store.JobRunning, store.JobReconciling} {
		jobsList, err := js.ListJobs(ctx, store.JobFilter{State: st, Kind: kind, Limit: store.MaxJobLimit})
		if err != nil {
			return false, err
		}
		for _, j := range jobsList {
			id, err := unmarshal(j.Args)
			if err != nil {
				continue
			}
			if id == backupID {
				return true, nil
			}
		}
	}
	return false, nil
}

// RestoreInFlight reports whether any active (queued/running/reconciling)
// restore job targets backupID. Shared by the API and UI delete handlers to
// refuse deleting a backup mid-restore (ErrBackupBusy).
func RestoreInFlight(ctx context.Context, js store.JobStore, backupID string) (bool, error) {
	return jobTargetsBackup(ctx, js, "restore", backupID, func(raw []byte) (string, error) {
		var req RestoreRequest
		if err := json.Unmarshal(raw, &req); err != nil {
			return "", err
		}
		return req.BackupID, nil
	})
}

// BackupInFlight reports whether any active (queued/running/reconciling)
// backup job targets backupID. Used by the delete handler to refuse deleting a
// backup while it is still being written (ErrBackupBusy). Note: the gate is
// intentionally job-based, not row-state-based — a crashed daemon can leave a
// creating row with no live job, and that row must stay deletable.
func BackupInFlight(ctx context.Context, js store.JobStore, backupID string) (bool, error) {
	return jobTargetsBackup(ctx, js, "backup", backupID, func(raw []byte) (string, error) {
		var req BackupRequest
		if err := json.Unmarshal(raw, &req); err != nil {
			return "", err
		}
		return req.BackupID, nil
	})
}

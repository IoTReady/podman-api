package instance

import (
	"context"
	"errors"
	"fmt"
	"log"
	"maps"
	"slices"
	"sort"
	"strings"

	"github.com/iotready/podman-api/internal/podman"
	"github.com/iotready/podman-api/internal/render"
	"github.com/iotready/podman-api/internal/store"
)

// errTemplateSkipped is a sentinel returned by reconcileOneSpec when the
// instance's template was deleted from the catalog since the instance was
// deployed. The caller logs a human-readable message instead of the error
// text, distinguishing this from other transient (unreachable) failures.
var errTemplateSkipped = errors.New("template not found — instance skipped")

// errVolumeRenamePending is a sentinel returned by reconcileOneSpec when the
// template's declared volumes have drifted from spec.AppliedVolumes in a way
// that would fork real data: an applied (short) volume name the template no
// longer declares still has a materialized volume on the host under its
// applied name. Boot converge replays the CURRENT template body, whose
// persistentVolumeClaim.claimName is authored against the CURRENT declared
// name — so blindly calling PlayKube here would bind a brand-new, empty
// volume under the new name while the real data sits untouched under the old
// one, with spec.AppliedVolumes still (correctly) naming the old volume as
// applied. Boot converge only ever REPLAYS what was already applied; a rename
// is a template edit that has not actually been applied to this instance yet,
// so it is not this path's job to decide how to reconcile it — that is what
// an explicit re-Apply (which does have rename-vs-loss context, #256) is for.
// The caller logs a human-readable message instead of the error text.
//
// It is returned only when a rename is CORROBORATED — the template also
// declares a volume this instance was never applied with that does not yet
// exist on the host, i.e. somewhere the old data could have been renamed to.
// Without that, the same signature is just an ordinary dropped-and-unpruned
// volume, which is replayed with a warning rather than blocked (#265 review
// finding 3): a routine template cleanup must not leave pods down across every
// reboot.
var errVolumeRenamePending = errors.New("template volume renamed since last apply — instance skipped, re-apply required")

// renameTargets returns the (short) names of volumes the CURRENT template
// declares that this instance was never applied with AND that do not yet exist
// on the host — the only shape a renamed applied volume could have been renamed
// TO, and the corroborating evidence Step 3.5 requires before refusing to
// replay (#265 review finding 3).
//
// A newly declared volume that ALREADY exists is not a target: replaying binds
// the pod to real, existing data, forking nothing. A transient VolumeInspect
// failure is logged and treated as "not a target", matching how Step 3.5's own
// loop treats a transient error — an inconclusive host call must not be the
// thing that keeps a pod down across reboots.
func (s *Service) renameTargets(ctx context.Context, hostID, tmpl, slug string, declared []render.Volume, applied []string) []string {
	appliedSet := make(map[string]bool, len(applied))
	for _, short := range applied {
		appliedSet[short] = true
	}
	var targets []string
	for _, v := range declared {
		if appliedSet[v.Name] {
			continue
		}
		full := volumeName(tmpl, slug, v.Name)
		if _, ierr := s.client.VolumeInspect(ctx, hostID, full); errors.Is(ierr, podman.ErrNotFound) {
			targets = append(targets, v.Name)
		} else if ierr != nil {
			log.Printf("boot converge %s/%s/%s: inspect volume %q: %v (transient — not treated as a rename target)",
				hostID, tmpl, slug, full, ierr)
		}
	}
	sort.Strings(targets)
	return targets
}

// pendingRenames narrows a set of applied volume names that are "no longer
// declared but still on the host" down to the ones a rename target can
// actually account for — the only ones replaying could fork (#265 review
// round-2, blocking finding).
//
// `renamed` and `targets` used to be two flat, uncorrelated lists: the guard
// refused for the WHOLE `renamed` list as soon as ANY target existed anywhere
// in the template. An instance applied with ["logs","cache"] whose template
// drops `logs` outright (old volume left unpruned — routine and harmless) and
// separately renames `cache` -> `cache2` therefore stayed down on every boot
// sweep for `logs` too, which no target names and which cannot fork anything.
//
// The pairing rule, in the order it errs:
//
//  1. A renamed name is BLOCKED when some target plausibly IS its new name
//     (plausibleRename below). That is the direct evidence of a fork.
//  2. A rename is 1:1 — each target can absorb at most one applied volume — so
//     any target left over once every plausibly-matched rename has claimed one
//     is UNACCOUNTED FOR. A rename may change a name beyond all recognition
//     (`logs` -> `journal`), so an unaccounted target could be the destination
//     of any renamed name the heuristic did not match, and every one of them is
//     blocked as well. This is the deliberate safe side: the guard exists to
//     prevent data-forking, so an ambiguous target refuses rather than replays.
//  3. Only with NO targets at all — or none left over, with nothing plausibly
//     matched — does a renamed name fall through as an ordinary drop.
//
// Two lists come back. `blocked` is the heuristic's best answer: the applied
// names it believes were renamed. `ambiguous` is every other name
// in `renamed` — undeclared and still present, but matched to no target. The
// split exists because the pairing is name-similarity, NOT a true bipartite
// match, so `blocked` can name the wrong volume (#265 review round-5): applied
// ["cache","data"], `cache` dropped and `data` renamed to `cache2`, pairs
// `cache` with `cache2` on containment alone and never names `data`. That case
// is indistinguishable by name from the round-4 one this pairing exists to fix
// (`logs` dropped, `cache` -> `cache2`), so no heuristic can get both right.
// Surfacing `ambiguous` keeps the diagnostic honest without giving up the
// precision the round-4 fix bought: the refusal is whole-set either way and the
// remedy — re-apply — is the same whichever of the two it was, so the caller
// logs the caveat rather than naming these in the error.
//
// Both results are sorted, so the refusal message reads deterministically.
func pendingRenames(renamed, targets []string) (blocked, ambiguous []string) {
	if len(targets) == 0 {
		return nil, nil
	}
	var matched, unmatched []string
	for _, r := range renamed {
		if slices.ContainsFunc(targets, func(t string) bool { return plausibleRename(r, t) }) {
			matched = append(matched, r)
		} else {
			unmatched = append(unmatched, r)
		}
	}
	if len(targets) > len(matched) {
		// At least one target is unclaimed and could be any of them, so every
		// name is blocked outright and nothing is left merely ambiguous.
		blocked = slices.Clone(renamed)
		sort.Strings(blocked)
		return blocked, nil
	}
	sort.Strings(matched)
	sort.Strings(unmatched)
	return matched, unmatched
}

// plausibleRename reports whether the newly declared (short) volume name `to`
// looks like what the applied (short) name `from` was renamed TO, judged from
// the names alone — the only signal available, since nothing records a rename
// as such.
//
// A rename in practice keeps a recognisable stem: `cache` -> `cache2`,
// `data` -> `pgdata`, `wal` -> `pgwal`. Containment either way covers those,
// and a shared prefix or suffix of three or more characters covers the
// prefix/suffix swaps it misses (`pgdata` -> `pgdata_v2` is containment;
// `db-data` -> `db-store` is a shared prefix). Anything shorter than three
// characters matches far too much to mean anything.
//
// A false NEGATIVE here is caught by pendingRenames' leftover-target rule
// above, which blocks the unmatched names anyway; a false positive only ever
// makes the guard refuse, which is the safe direction. Neither can fork data.
//
// What a false POSITIVE does cost is attribution: it can let a coincidentally
// similar name claim the target that a genuinely renamed volume needed, so the
// refusal names the wrong volume. That is why pendingRenames reports the names
// it did not match as `ambiguous` rather than dropping them silently.
func plausibleRename(from, to string) bool {
	a, b := strings.ToLower(from), strings.ToLower(to)
	if a == "" || b == "" {
		return false
	}
	if strings.Contains(a, b) || strings.Contains(b, a) {
		return true
	}
	return commonAffix(a, b) >= 3
}

// commonAffix returns the longer of a and b's common prefix and common suffix
// lengths, in bytes (volume names are restricted to ASCII by podman).
func commonAffix(a, b string) int {
	pre := 0
	for pre < len(a) && pre < len(b) && a[pre] == b[pre] {
		pre++
	}
	suf := 0
	for suf < len(a) && suf < len(b) && a[len(a)-1-suf] == b[len(b)-1-suf] {
		suf++
	}
	return max(pre, suf)
}

// ReconcileSpecsOnHost checks every stored instance spec on host against real
// pod state and re-converges any that are missing (not running), so managed
// pods survive a reboot. Errors are logged per-instance and never propagated
// to the HTTP layer — the method always returns nil (it tolerates any
// failure by logging and continuing so a partial host outage does not block
// the rest).
//
// It has two callers: a one-shot pass in server.go, 2s after daemon startup
// (covers podman-api's own restart), and — since #231 —
// internal/inventory.Poller.checkBoot, which calls this once per host each
// time that host's kernel uptime resets, covering a managed host rebooting
// while podman-api keeps running. server.go only wires the poller's Boot
// field to this service (Boot: svc); it does not call this method itself for
// that path. Each call here is still a one-shot sweep of the host's specs;
// nothing inside this method itself loops or schedules anything.
//
// Concurrency: per-instance operations are serialized under the existing
// per-instance lock so this cannot race a concurrent Apply/Delete/Upgrade.
// No per-host lock is taken because boot converge re-creates only instances
// whose store row already exists and whose domains are already claimed —
// it creates no new cross-instance domain claims.
//
// Limitations (by design):
//   - No image pull: images are expected to be cached from the original deploy.
//   - Template-missing instances are skipped with a warning (not reaped).
//   - Secrets-undecryptable instances are skipped (wrong key file — operator
//     must restart with the correct -spec-key-file).
//   - Each call logs its own per-instance skips independently; a caller that
//     invokes this repeatedly (the poller) must not do so more often than
//     "once per detected reboot", or a permanent per-instance skip becomes a
//     permanent log flood. The poller's reboot detector already guarantees
//     that: see internal/inventory.Poller.checkBoot.
func (s *Service) ReconcileSpecsOnHost(ctx context.Context, hostID string) {
	keys, err := s.store.ListSpecKeys(ctx, hostID)
	if err != nil {
		log.Printf("boot converge %s: list specs: %v", hostID, err)
		return
	}
	if len(keys) == 0 {
		log.Printf("boot converge %s: no stored specs", hostID)
		return
	}

	reconverged := false
	for _, k := range keys {
		reconciled, err := s.reconcileOneSpec(ctx, hostID, k.Template, k.Slug)
		if errors.Is(err, errTemplateSkipped) {
			log.Printf("boot converge %s/%s/%s: template %q not found — skipping (spec not reaped)",
				hostID, k.Template, k.Slug, k.Template)
		} else if errors.Is(err, errVolumeRenamePending) {
			log.Printf("boot converge %s/%s/%s: %v — pod left down, re-apply to reconcile the rename",
				hostID, k.Template, k.Slug, err)
		} else if err != nil {
			log.Printf("boot converge %s/%s/%s: %v", hostID, k.Template, k.Slug, err)
		} else if reconciled {
			log.Printf("boot converge %s/%s/%s: re-converged (pod was missing)", hostID, k.Template, k.Slug)
			reconverged = true
		} else {
			log.Printf("boot converge %s/%s/%s: already running", hostID, k.Template, k.Slug)
		}
	}

	// Any pod re-created by boot converge (via PlayKube, outside the normal
	// Apply/Delete/lifecycle funnels that already invalidate) changes this
	// host's instance list. Drop the cache once, after the loop, rather than
	// per-instance, so a host with many already-running specs doesn't force
	// an extra sweep for a no-op reconcile.
	if reconverged {
		s.invalidateInstances(hostID)
	}

	// Reconcile ingress once per host rather than once per instance, avoiding
	// N Caddyfile generations and reloads when N instances are reconverged.
	// The reconcile is idempotent — it reads all specs from the store for this
	// host and derives routes from scratch.
	if reconverged && s.ingressEnabled() {
		if err := s.ingress.Reconcile(ctx, hostID); err != nil {
			log.Printf("boot converge %s: ingress reconcile failed: %v", hostID, err)
		}
	}
}

// reconcileOneSpec checks whether the pod for (host, tmpl, slug) exists; if it
// does and is running, returns false (already converged). If it does not, or if
// it exists but is not running, re-creates it from the stored spec and returns
// true. Sentinels errTemplateSkipped and errAlreadyConverged are never
// propagated — the caller logs a human-readable message for each.
//
// An ingress.Reconcile is NOT called here because the caller (ReconcileSpecsOnHost)
// reconciles ingress once per host after the loop, avoiding N reloads when N
// instances on the same host are reconverged.
func (s *Service) reconcileOneSpec(ctx context.Context, hostID, tmpl, slug string) (reconciled bool, err error) {
	// Serialize per-instance so a concurrent Apply/Delete/Upgrade of the same
	// instance does not race with boot converge.
	lock := s.instanceLock(hostID, tmpl, slug)
	lock.Lock()
	defer lock.Unlock()

	// Step 1: check if the pod already exists and is running.
	pod, pErr := s.client.PodInspect(ctx, hostID, podName(tmpl, slug))
	if pErr == nil && pod.Status == "Running" {
		return false, nil // already converged
	}
	if pErr != nil && !errors.Is(pErr, podman.ErrNotFound) {
		return false, fmt.Errorf("inspect pod: %w", pErr) // host unreachable, etc.
	}
	// podMissing is true when ErrNotFound (pod absent), false when the pod
	// exists but isn't running (Exited/Stopped/Paused). Needed below to decide
	// replace=true vs replace=false for PlayKube.
	podMissing := errors.Is(pErr, podman.ErrNotFound)

	// Step 2: load the stored spec.
	spec, err := s.store.GetSpec(ctx, hostID, tmpl, slug)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			// Spec was deleted between ListSpecKeys and GetSpec — concurrent
			// Delete. Nothing to reconcile.
			return false, nil
		}
		if errors.Is(err, store.ErrSpecCorrupt) {
			return false, fmt.Errorf("spec corrupt (malformed): %w", err)
		}
		if errors.Is(err, store.ErrSecretsUndecryptable) || errors.Is(err, store.ErrSecretsNeedKey) {
			return false, fmt.Errorf("spec secrets unreadable (wrong/missing -spec-key-file): %w", err)
		}
		return false, fmt.Errorf("get spec: %w", err)
	}

	// Step 3: load the template.
	tmplObj, err := s.store.GetTemplate(ctx, tmpl)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			// Template was deleted since the instance was deployed. We cannot
			// re-render the spec. Skip and warn — return the sentinel so the
			// caller logs a clear message rather than "already running".
			return false, errTemplateSkipped
		}
		return false, fmt.Errorf("get template %q: %w", tmpl, err)
	}

	// Step 3.5: refuse to silently fork a renamed applied volume's data. If an
	// applied (short) name is no longer among the template's CURRENT declared
	// volumes AND its OLD applied-name volume still exists on the host, the
	// template was renamed since this instance was last actually applied —
	// replaying the current body below would bind a fresh, empty volume under
	// the new claim name while the real data sits orphaned under the old one.
	// A volume that is simply gone (never checked here) is not this guard's
	// concern; only a rename that would fork surviving data is.
	//
	// Every applied name is evaluated before deciding anything (#256 review
	// round-4, addendum finding 2): returning on the FIRST confirmed rename
	// used to skip inspecting the rest of the set, so the refusal message
	// named only one volume even when several had renamed. A TRANSIENT
	// VolumeInspect error (anything but ErrNotFound) does not abort the whole
	// reconcile either — it is logged and treated as "can't determine", not as
	// a rename, matching this repo's general preference for treating a
	// transient host error as retry-worthy rather than a hard block. Before
	// this fix, ANY instance with a renamed volume depended on a live,
	// error-free host round trip just to come back up on a routine restart.
	if len(spec.AppliedVolumes) > 0 {
		declaredShort := make(map[string]bool, len(tmplObj.Meta.Volumes))
		for _, v := range tmplObj.Meta.Volumes {
			declaredShort[v.Name] = true
		}
		var renamed []string
		for _, short := range spec.AppliedVolumes {
			if declaredShort[short] {
				continue
			}
			full := volumeName(tmpl, slug, short)
			if _, ierr := s.client.VolumeInspect(ctx, hostID, full); ierr == nil {
				renamed = append(renamed, short)
			} else if !errors.Is(ierr, podman.ErrNotFound) {
				log.Printf("boot converge %s/%s/%s: inspect volume %q: %v (transient — not blocking reconcile)",
					hostID, tmpl, slug, full, ierr)
			}
		}
		if len(renamed) > 0 {
			sort.Strings(renamed)
			// "Applied name no longer declared, but still on the host" is the
			// signature of a rename AND of a volume that was simply DROPPED
			// from the template while the old podman volume was never pruned —
			// a routine, unremarkable template edit (#265 review finding 3).
			// Blocking on that ambiguity left the pod down on every boot
			// converge sweep until someone manually re-applied, turning a
			// template cleanup into a fleet-wide outage where the pre-#256
			// behaviour simply replayed PlayKube.
			//
			// A rename has one corroborating signal a drop does not: the NEW
			// name. Only refuse when the template declares a volume this
			// instance was never applied with AND that volume does not yet
			// exist on the host — i.e. there is somewhere for the old data to
			// have been renamed TO, and replaying now would create it empty
			// while the real data sits orphaned. With no such target, no data
			// can fork, so warn and let the pod come back up.
			//
			// The two lists are CORRELATED rather than gated on "any target
			// exists anywhere" (#265 review round-2, blocking finding): one
			// genuine rename used to block every unrelated dropped-and-unpruned
			// volume that happened to share the applied set, leaving the pod
			// down for a name no target could ever account for. pendingRenames
			// pairs them, erring toward refusal wherever the pairing is
			// ambiguous — see its doc comment for the rule.
			targets := s.renameTargets(ctx, hostID, tmpl, slug, tmplObj.Meta.Volumes, spec.AppliedVolumes)
			if blocked, ambiguous := pendingRenames(renamed, targets); len(blocked) > 0 {
				if len(ambiguous) > 0 {
					// The pairing is a name heuristic, so a coincidental match
					// can claim the target a genuinely renamed volume needed and
					// leave the refusal naming the wrong one (#265 review
					// round-5). The error stays precise — naming an unmatched
					// name there is exactly what round-4 fixed — but the
					// ambiguity is worth a log line, since the operator reads it
					// to decide what to re-apply.
					log.Printf("boot converge %s/%s/%s: applied volume(s) %s are also no longer declared but still present, matched to no rename target; the pairing is a name heuristic, so if the rename was actually one of those, the refusal below names the wrong volume — re-applying covers either way",
						hostID, tmpl, slug, strings.Join(ambiguous, ", "))
				}
				return false, fmt.Errorf("%w: applied volume(s) %s no longer declared by template %q but still exist on %s (newly declared, not-yet-created volume(s) %s look like the rename target)",
					errVolumeRenamePending, strings.Join(blocked, ", "), tmpl, hostID, strings.Join(targets, ", "))
			}
			log.Printf("boot converge %s/%s/%s: applied volume(s) %s are no longer declared by template %q but still exist on %s — no newly declared volume looks like a rename target, so this is treated as a dropped (unpruned) volume and the pod is replayed; prune them once you are sure",
				hostID, tmpl, slug, strings.Join(renamed, ", "), tmpl, hostID)
		}
	}

	// Step 4: apply defaults and validate.
	params := render.ApplyDefaults(tmplObj.Meta, spec.Parameters)
	// Use AllowMissingSecrets: a stored spec may lack a per-instance secret
	// that was added to the template after this instance was deployed. The pod
	// was running before the template change; we do not want boot converge to
	// fail because of it.
	if err := render.ValidateAllowMissingSecrets(tmplObj.Meta, params, spec.Secrets); err != nil {
		return false, fmt.Errorf("validate: %w", err)
	}

	// Step 5: re-render the YAML from stored parameters.
	yaml, err := render.RenderAndValidate(tmplObj.Body, params)
	if err != nil {
		return false, fmt.Errorf("render: %w", err)
	}

	if s.sidecar != nil {
		// Reconcile never carries a RestoreIntent: a point-in-time restore is a
		// one-shot operation supplied on an explicit Apply, never replayed here.
		inj, err := s.sidecar.InjectSidecars(ctx, yaml, toExtMeta(tmplObj.Meta), params, slug, nil)
		if err != nil {
			return false, fmt.Errorf("sidecar inject: %w", err)
		}
		yaml = inj.YAML
		for _, sec := range inj.Secrets {
			spec.InjectorSecrets = append(spec.InjectorSecrets, store.InjectorSecret{Name: sec.Name, Key: sec.Key, Value: sec.Value})
		}
	}

	// Step 6: re-create per-instance secrets on the host. Template-declared
	// secrets use the namespaced name as the K8s data key (backward compat
	// with every bundled template's secretKeyRef.key field).
	for k, v := range spec.Secrets {
		name := instanceSecretName(tmpl, slug, k)
		if _, iErr := s.client.SecretInspect(ctx, hostID, name); iErr == nil {
			_ = s.client.SecretRemove(ctx, hostID, name)
		}
		if err := s.client.SecretCreate(ctx, hostID, name, wrapAsKubeSecret(name, name, []byte(v))); err != nil {
			return false, fmt.Errorf("create secret %q: %w", name, err)
		}
	}
	// Re-create injector-declared secrets. The K8s data key is the declared Key,
	// so the injected sidecar's secretKeyRef.key resolves correctly.
	for _, sec := range spec.InjectorSecrets {
		name := instanceSecretName(tmpl, slug, sec.Name)
		if _, iErr := s.client.SecretInspect(ctx, hostID, name); iErr == nil {
			_ = s.client.SecretRemove(ctx, hostID, name)
		}
		if err := s.client.SecretCreate(ctx, hostID, name, wrapAsKubeSecret(name, sec.Key, []byte(sec.Value))); err != nil {
			return false, fmt.Errorf("create injector secret %q: %w", name, err)
		}
	}

	// Step 7: ensure the ingress network (when declared) and the template's own
	// shared networks, so a pod re-converged after a reboot comes back with the
	// same connectivity the apply path gave it.
	networks, err := s.ensureNetworks(ctx, hostID, tmplObj.Meta)
	if err != nil {
		return false, err
	}
	// Converge does not REFUSE a duplicate DNS claim the way apply does — an
	// instance that stays down after a reboot is worse than one whose alias is
	// contended, and by this point the conflicting state already exists on disk.
	// It must not be silent either: this is exactly the arbitrary-resolution
	// failure aliases exist to prevent, so name both instances in the log (#269).
	s.warnOnNetworkNameConflict(ctx, hostID, tmpl, slug, tmplObj.Meta)

	// Step 8: play kube. replace=true when the pod exists (non-Running) so
	// podman replaces the stale pod; replace=false when the pod is absent.
	if err := s.client.PlayKube(ctx, hostID, yaml, !podMissing, networks...); err != nil {
		return false, fmt.Errorf("play kube: %w", err)
	}

	// Step 9: persist the spec (upsert — updates timestamp, data unchanged).
	// Use spec.Parameters (raw stored params) rather than params (which has
	// ApplyDefaults merged in). If the template's defaults changed since the
	// original deploy, persisting params would silently overwrite the stored
	// values and create drift between what the pod is running and the spec
	// row. spec.Parameters preserves the original deploy-time values.
	// Defensive clones: spec.Parameters and spec.Secrets came from GetSpec
	// and may share backing arrays with the store depending on the implementation.
	sp := store.Spec{
		Host:            hostID,
		Template:        tmpl,
		Slug:            slug,
		Parameters:      maps.Clone(spec.Parameters),
		Secrets:         maps.Clone(spec.Secrets),
		InjectorSecrets: slices.Clone(spec.InjectorSecrets),
		Domains:         slices.Clone(spec.Domains),
		// AppliedVolumes is preserved as-is, not re-derived from tmplObj.Meta.Volumes:
		// boot converge re-creates the pod from what was already applied, it does
		// not itself constitute a new Apply of (possibly changed) template
		// declarations. Re-deriving here would silently update the recorded
		// applied set to match a template edit the operator has not actually
		// re-applied, defeating #257's rename detection.
		AppliedVolumes: slices.Clone(spec.AppliedVolumes),
		// AppliedVolumeMeta is preserved for the same reason: it must keep
		// naming what THIS spec was last actually applied with, not whatever
		// the current template now declares.
		AppliedVolumeMeta: maps.Clone(spec.AppliedVolumeMeta),
	}
	if err := s.store.PutSpec(ctx, sp); err != nil {
		// Spec persist failed but the pod is already running. Log the error
		// but do not fail the reconcile — the pod is the primary concern.
		log.Printf("boot converge %s/%s/%s: pod re-created but spec persist failed: %v",
			hostID, tmpl, slug, err)
	}

	return true, nil
}

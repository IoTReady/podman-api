package instance

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"

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

// backupBlobKey is the blob layout: <host>/<template>/<slug>/<backup-id>/<volume>.tar
func backupBlobKey(host, tmpl, slug, id, volume string) string {
	return host + "/" + tmpl + "/" + slug + "/" + id + "/" + volume + ".tar"
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
// An UNSCOPED request is deliberately NOT existence-checked here: "every
// declared volume not vetoed" legitimately resolves to nothing on a brand-new
// instance whose volumes podman has not created yet, and that first, empty
// backup must succeed. Backup carries that half of the rule, where it can tell
// "nothing exists yet" from "everything that exists was skipped".
func (s *Service) CheckBackupable(ctx context.Context, host, tmpl, slug string, volumes []string) error {
	if s.blobs == nil {
		return ErrBackupsDisabled
	}
	t, err := s.lookup(ctx, host, tmpl)
	if err != nil {
		return err
	}
	if _, err := s.store.GetSpec(ctx, host, tmpl, slug); err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return ErrInstanceNotFound
		}
		return err
	}
	declared := make(map[string]string, len(t.Meta.Volumes))
	for _, v := range t.Meta.Volumes {
		declared[v.Name] = v.Backup
	}
	if len(volumes) == 0 {
		// An empty scope means "every declared volume not marked `none`". If the
		// template declares volumes and every one of them is vetoed, that set is
		// empty by an explicit operator decision and the backup would capture
		// nothing: a green row, a real stop/restart outage, and no data. Reject
		// it here, synchronously.
		//
		// A template declaring NO volumes at all is a different case and is
		// ACCEPTED: a stateless template (the bundled `basic-web`) has nothing to
		// back up, that is a valid no-op, and rejecting it would regress every
		// caller that backs one up today.
		//
		// A declared volume that does not yet exist on the host is neither case —
		// this check is over what the template declares, not over what currently
		// exists (a brand-new instance must be able to take its first, empty
		// backup).
		if len(declared) == 0 {
			return nil
		}
		for _, marker := range declared {
			if !IsBackupMarkerNone(marker) {
				return nil
			}
		}
		return fmt.Errorf("%w: template %s declares volumes but every one of them is marked `backup: none`", ErrInvalidBackupScope, tmpl)
	}
	for _, name := range volumes {
		marker, ok := declared[name]
		if !ok {
			return fmt.Errorf("%w: %q is not declared by template %s", ErrInvalidBackupScope, name, tmpl)
		}
		if IsBackupMarkerNone(marker) {
			return fmt.Errorf("%w: %q is marked `backup: none`", ErrInvalidBackupScope, name)
		}
	}
	// Existence, resolved through the same volumeName() the pod manifest and
	// Backup itself use, so the two cannot drift. InstanceVolumes returns only
	// the declared volumes that actually exist (it skips ErrNotFound and fails
	// loud on anything else), which is exactly the set wanted here.
	vols, err := s.InstanceVolumes(ctx, host, tmpl, slug)
	if err != nil {
		return fmt.Errorf("list volumes on %s: %w", host, err)
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
		return fmt.Errorf("%w: requested volumes %v are declared by template %s but do not exist on %s (the instance predates the declaration — re-apply it first)", ErrInvalidBackupScope, missing, tmpl, host)
	}
	return nil
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

	if err := s.CheckBackupable(ctx, req.Host, req.Template, req.Slug, req.Volumes); err != nil {
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

	// Template metadata is needed before the volume loop: it carries both the
	// `none` veto and the declared exclude patterns. Declared exclude patterns
	// are keyed by the template's short volume name; InstanceVolumes returns
	// podman's full <template>-<slug>-<vol> names. Resolve through the same
	// volumeName() the pod manifest uses, so the two cannot drift.
	tpl, err := s.lookup(ctx, req.Host, req.Template)
	if err != nil {
		restart()
		return fail(fmt.Errorf("lookup template: %w", err))
	}
	type volMeta struct {
		marker  string
		exclude []string
	}
	declared := map[string]volMeta{}
	for _, v := range tpl.Meta.Volumes {
		declared[volumeName(req.Template, req.Slug, v.Name)] = volMeta{marker: v.Backup, exclude: v.Exclude}
	}

	vols, err := s.InstanceVolumes(ctx, req.Host, req.Template, req.Slug)
	if err != nil {
		restart()
		return fail(fmt.Errorf("list volumes: %w", err))
	}

	// Scope in declared short names, resolved through the same volumeName() the
	// pod manifest uses. Empty scope means "everything not vetoed".
	scope := make(map[string]bool, len(req.Volumes))
	for _, short := range req.Volumes {
		scope[volumeName(req.Template, req.Slug, short)] = true
	}

	var bvols []store.BackupVolume
	exported := map[string]bool{}
	for _, v := range vols {
		m := declared[v.Name]
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
	} else if len(bvols) == 0 && len(vols) > 0 {
		// An UNSCOPED run that exported nothing WHILE THE INSTANCE HAS VOLUMES.
		// The carve-out here is "no volume exists yet" — a brand-new instance
		// whose first, empty backup is legitimate — and NOT "no scope was
		// given". Without that distinction, a template declaring a vetoed `data`
		// plus a `cache` existing instances have not materialised stops the pod
		// for real, skips `data`, finds no `cache`, and records a green
		// `complete` row with an empty blob set: a real outage, a backup of
		// nothing counted by retention, and a restore that restores nothing.
		restart()
		return fail(fmt.Errorf("%w: every volume of %s/%s was skipped, so this backup would capture nothing (all %d existing volume(s) are marked `backup: none`)", ErrInvalidBackupScope, req.Template, req.Slug, len(vols)))
	}

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
	bv := store.BackupVolume{Name: name, SizeBytes: cw.n, Manifest: raw}
	if len(patterns) > 0 {
		bv.Excluded = &store.ExcludedPaths{
			Patterns: stats.Patterns, Entries: stats.Entries, Bytes: stats.Bytes,
		}
	}
	return bv, nil
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

	var want Manifest
	if err := json.Unmarshal(bv.Manifest, &want); err != nil {
		return fmt.Errorf("stored manifest corrupt: %w", err)
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

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
// needs: known host, known template, stored spec present, blob store wired.
func (s *Service) CheckBackupable(ctx context.Context, host, tmpl, slug string) error {
	if s.blobs == nil {
		return ErrBackupsDisabled
	}
	if _, err := s.lookup(ctx, host, tmpl); err != nil {
		return err
	}
	if _, err := s.store.GetSpec(ctx, host, tmpl, slug); err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return ErrInstanceNotFound
		}
		return err
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

	if err := s.CheckBackupable(ctx, req.Host, req.Template, req.Slug); err != nil {
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

	vols, err := s.InstanceVolumes(ctx, req.Host, req.Template, req.Slug)
	if err != nil {
		restart()
		return fail(fmt.Errorf("list volumes: %w", err))
	}
	var bvols []store.BackupVolume
	for _, v := range vols {
		bv, err := s.backupVolume(ctx, req, v.Name)
		if err != nil {
			restart()
			return fail(fmt.Errorf("backup volume %q: %w", v.Name, err))
		}
		bvols = append(bvols, bv)
		step("export-volume", v.Name)
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
func (s *Service) backupVolume(ctx context.Context, req BackupRequest, name string) (store.BackupVolume, error) {
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
	m, err := buildManifest(io.TeeReader(rc, cw))
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
	return store.BackupVolume{Name: name, SizeBytes: cw.n, Manifest: raw}, nil
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
// draining, instance (spec) still present. The drain check is upfront so a
// draining host can't fail the job after teardown.
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

	// Re-check under the lock (a concurrent delete may have raced us).
	b, err = s.CheckRestorable(ctx, req.BackupID)
	if err != nil {
		return err
	}
	spec, err := s.store.GetSpec(ctx, b.Host, b.Template, b.Slug)
	if err != nil {
		return err
	}
	step("load", b.Host+"/"+b.Template+"/"+b.Slug)

	// Teardown: pod + volumes (a referenced volume can't be removed). Keep
	// per-instance secrets — Apply below re-pushes them from the spec anyway,
	// and host-scoped secrets must survive. Delete also reconciles away the
	// spec row; Apply re-persists it. Tolerate an already-gone pod.
	if err := s.Delete(ctx, b.Host, b.Template, b.Slug, DeleteOptions{PruneVolumes: true}); err != nil && !errors.Is(err, ErrInstanceNotFound) {
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

// restoreVolume recreates one volume from its blob and verifies the imported
// content against the manifest recorded at backup time. Unlike migrate, restore
// always verifies regardless of the verifyVolumes flag — it is the only safety
// mechanism available when restoring from a blob (no live source to compare against).
func (s *Service) restoreVolume(ctx context.Context, b store.Backup, bv store.BackupVolume) error {
	if err := s.client.VolumeCreate(ctx, b.Host, bv.Name); err != nil {
		return fmt.Errorf("create: %w", err)
	}
	rc, err := s.blobs.Get(ctx, backupBlobKey(b.Host, b.Template, b.Slug, b.ID, bv.Name))
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return fmt.Errorf("%w: blob for volume %q missing", ErrBackupNotRestorable, bv.Name)
		}
		return fmt.Errorf("open blob: %w", err)
	}
	defer rc.Close()
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

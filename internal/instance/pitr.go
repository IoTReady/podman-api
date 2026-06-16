package instance

import (
	"context"
	"errors"
	"fmt"

	"github.com/iotready/podman-api/extension"
	"github.com/iotready/podman-api/internal/store"
)

// CheckInstanceExists validates the precondition for operating on a live instance
// — host known, template present, instance spec stored — and returns
// ErrUnknownHost / ErrUnknownTemplate / ErrInstanceNotFound otherwise.
//
// Unlike CheckBackupable it does NOT require the tarball blob store: a
// point-in-time restore replays the Litestream S3 replica via the injected
// initContainer, which is independent of the -backup-dir/blob store. Gating PITR
// on the blob store would 501 a Litestream-only deployment that has no tarball
// backups configured.
func (s *Service) CheckInstanceExists(ctx context.Context, host, tmpl, slug string) error {
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

// PITRRestoreRequest is the pitr-restore job's args: which instance to restore
// and to what point in time. Timestamp is the opaque selector handed to the
// injector (the Litestream injector interprets it as RFC3339); Volumes optionally
// narrows the restore to specific volumes (empty = all backup-marked volumes).
type PITRRestoreRequest struct {
	Host      string   `json:"host"`
	Template  string   `json:"template"`
	Slug      string   `json:"slug"`
	Timestamp string   `json:"timestamp"`
	Volumes   []string `json:"volumes,omitempty"`
}

// PITRRestore performs a one-shot point-in-time restore: it recreates the
// instance's pod with a RestoreIntent handed to the SidecarInjector, then waits
// for the pod to come back up. The intent travels via ApplyOptions and is NOT
// persisted into the stored spec, so the reconcile path never replays the
// rollback — it fires exactly once.
//
// Unlike volume (tarball) Restore, PITR keeps the volume in place: the injected
// initContainer overwrites the database inside it. The per-instance migrate lock
// serializes it against migrate and other restores.
func (s *Service) PITRRestore(ctx context.Context, req PITRRestoreRequest, step func(step, detail string)) error {
	if step == nil {
		step = func(string, string) {}
	}

	lk := s.migrateLock(req.Template, req.Slug)
	lk.Lock()
	defer lk.Unlock()

	spec, err := s.store.GetSpec(ctx, req.Host, req.Template, req.Slug)
	if err != nil {
		return err
	}
	step("load", req.Host+"/"+req.Template+"/"+req.Slug)

	intent := &extension.RestoreIntent{Timestamp: req.Timestamp, Volumes: req.Volumes}
	if err := s.Apply(ctx, req.Host, ApplyRequest{
		Template:   req.Template,
		Slug:       req.Slug,
		Parameters: spec.Parameters,
		Secrets:    spec.Secrets,
		Domains:    spec.Domains,
	}, ApplyOptions{Replace: true, RestoreIntent: intent}); err != nil {
		return fmt.Errorf("apply: %w", err)
	}
	step("apply", req.Host)

	if err := s.waitRunning(ctx, req.Host, req.Template, req.Slug); err != nil {
		return fmt.Errorf("verify: %w", err)
	}
	step("verify", req.Host)
	return nil
}

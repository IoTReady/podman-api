package instance

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"maps"
	"time"

	"github.com/iotready/podman-api/internal/podman"
	"github.com/iotready/podman-api/internal/store"
)

var (
	ErrNewSlugSameAsOld = errors.New("new slug is the same as the old slug")
)

// RenameRequest is the POST /rename body.
type RenameRequest struct {
	NewSlug string   `json:"new_slug"`
	Domains []string `json:"domains,omitempty"`
	// KeepOldStandby, when true, leaves the old pod stopped with volumes and
	// secrets intact so it can be re-started as a standby.
	KeepOldStandby bool `json:"keep_old_standby,omitempty"`
}

// CheckRenameable runs the cheap synchronous validation the handler needs:
// instance exists, new slug differs, template known, new slug not taken.
func (s *Service) CheckRenameable(ctx context.Context, host, tmpl, slug string, req RenameRequest) error {
	if req.NewSlug == "" {
		return errors.New("new_slug is required")
	}
	if req.NewSlug == slug {
		return ErrNewSlugSameAsOld
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
	if _, err := s.client.PodInspect(ctx, host, podName(tmpl, req.NewSlug)); err == nil {
		return fmt.Errorf("%w: instance %s/%s already exists on %s", ErrInstanceExists, tmpl, req.NewSlug, host)
	} else if !errors.Is(err, podman.ErrNotFound) {
		return fmt.Errorf("inspect new slug pod: %w", err)
	}
	if _, err := s.store.GetSpec(ctx, host, tmpl, req.NewSlug); err == nil {
		return fmt.Errorf("%w: instance %s/%s already has a stored spec", ErrInstanceExists, tmpl, req.NewSlug)
	} else if !errors.Is(err, store.ErrNotFound) {
		return fmt.Errorf("check new slug spec: %w", err)
	}
	return nil
}

// Rename renames an instance to a new slug on the same host: stop, copy
// volumes and secrets, deploy under the new slug, verify health, then reap
// or keep the old instance. step is a best-effort progress callback (may be nil).
func (s *Service) Rename(ctx context.Context, host, tmpl, slug string, req RenameRequest, step func(step, detail string)) error {
	if step == nil {
		step = func(string, string) {}
	}

	lk := s.migrateLock(tmpl, slug)
	lk.Lock()
	defer lk.Unlock()

	if err := s.CheckRenameable(ctx, host, tmpl, slug, req); err != nil {
		return err
	}

	spec, err := s.store.GetSpec(ctx, host, tmpl, slug)
	if err != nil {
		return err
	}
	step("load", host+"/"+tmpl+"/"+slug)

	domains := spec.Domains
	if req.Domains != nil {
		domains = req.Domains
	}

	if err := s.Stop(ctx, host, tmpl, slug); err != nil {
		return fmt.Errorf("stop instance: %w", err)
	}
	step("stop", host)

	vols, err := s.InstanceVolumes(ctx, host, tmpl, slug)
	if err != nil {
		return fmt.Errorf("list volumes: %w", err)
	}
	for _, v := range vols {
		shortName := v.Name[len(tmpl+"-"+slug+"-"):]
		newName := tmpl + "-" + req.NewSlug + "-" + shortName

		if err := s.client.VolumeCreate(ctx, host, newName); err != nil {
			return fmt.Errorf("create volume %q: %w", newName, err)
		}
		step("create-volume", newName)

		srcManifest, err := s.copyVolumeAs(ctx, host, host, v.Name, newName)
		if err != nil {
			return fmt.Errorf("copy volume %q -> %q: %w", v.Name, newName, err)
		}
		step("copy-volume", newName)

		if s.verifyVolumes {
			dst, err := s.volumeManifest(ctx, host, newName)
			if err != nil {
				return fmt.Errorf("verify volume %q: re-export: %w", newName, err)
			}
			if diff, ok := srcManifest.firstDiff(dst); !ok {
				return fmt.Errorf("%w: volume %q differs at %q", ErrVolumeIntegrity, newName, diff)
			}
			step("verify-volume", newName)
		}
	}
	step("volumes", host)

	for k, v := range spec.Secrets {
		newName := instanceSecretName(tmpl, req.NewSlug, k)
		_ = s.client.SecretRemove(ctx, host, newName)
		if err := s.client.SecretCreate(ctx, host, newName, wrapAsKubeSecret(newName, newName, []byte(v))); err != nil {
			return fmt.Errorf("create secret %q: %w", newName, err)
		}
		step("copy-secret", k)
	}
	for _, sec := range spec.InjectorSecrets {
		newName := instanceSecretName(tmpl, req.NewSlug, sec.Name)
		_ = s.client.SecretRemove(ctx, host, newName)
		if err := s.client.SecretCreate(ctx, host, newName, wrapAsKubeSecret(newName, sec.Key, []byte(sec.Value))); err != nil {
			return fmt.Errorf("create injector secret %q: %w", newName, err)
		}
	}
	step("secrets", host)

	params := maps.Clone(spec.Parameters)
	params["slug"] = req.NewSlug

	step("apply", host)
	if err := s.Apply(ctx, host, ApplyRequest{
		Template:   tmpl,
		Slug:       req.NewSlug,
		Parameters: params,
		Secrets:    spec.Secrets,
		Domains:    domains,
	}, ApplyOptions{Replace: false}); err != nil {
		rbctx := context.WithoutCancel(ctx)
		if _, rerr := s.Start(rbctx, host, tmpl, slug); rerr != nil {
			step("rollback-restart-old-failed", rerr.Error())
		}
		if rerr := s.Delete(rbctx, host, tmpl, req.NewSlug, DeleteOptions{PruneVolumes: true, PruneSecrets: true}); rerr != nil {
			step("rollback-reap-new-failed", rerr.Error())
		}
		return fmt.Errorf("apply new slug: %w", err)
	}
	step("apply-done", req.NewSlug)

	if err := s.waitRunning(ctx, host, tmpl, req.NewSlug); err != nil {
		rbctx := context.WithoutCancel(ctx)
		if _, rerr := s.Start(rbctx, host, tmpl, slug); rerr != nil {
			step("rollback-restart-old-failed", rerr.Error())
		}
		if rerr := s.Delete(rbctx, host, tmpl, req.NewSlug, DeleteOptions{PruneVolumes: true, PruneSecrets: true}); rerr != nil {
			step("rollback-reap-new-failed", rerr.Error())
		}
		return fmt.Errorf("verify new slug: %w", err)
	}
	step("verify", req.NewSlug)

	if err := s.store.DeleteSpec(ctx, host, tmpl, slug); err != nil && !errors.Is(err, store.ErrNotFound) {
		return fmt.Errorf("delete old spec: %w", err)
	}
	newSpec := spec
	newSpec.Slug = req.NewSlug
	newSpec.Domains = domains
	newSpec.Parameters = params
	newSpec.Updated = time.Now()
	if err := s.store.PutSpec(ctx, newSpec); err != nil {
		return fmt.Errorf("persist new spec: %w", err)
	}
	step("spec", host+"/"+tmpl+"/"+req.NewSlug)

	if s.ingressEnabled() {
		if err := s.ingress.Reconcile(ctx, host); err != nil {
			return fmt.Errorf("ingress reconcile: %w", err)
		}
		step("ingress", host)
	}

	if req.KeepOldStandby {
		log.Printf("rename: keeping old instance %s/%s as standby on %s", tmpl, slug, host)
	} else {
		if err := s.Delete(context.WithoutCancel(ctx), host, tmpl, slug, DeleteOptions{PruneVolumes: true, PruneSecrets: true}); err != nil {
			return fmt.Errorf("reap old instance: %w", err)
		}
		step("reap-old", host)
	}

	return nil
}

// copyVolumeAs streams a named volume's contents from one host to another,
// with possibly different source and destination volume names. The destination
// volume must already exist.
func (s *Service) copyVolumeAs(ctx context.Context, fromHost, toHost, fromName, toName string) (Manifest, error) {
	rc, err := s.client.VolumeExport(ctx, fromHost, fromName)
	if err != nil {
		return nil, fmt.Errorf("export volume %q from %s: %w", fromName, fromHost, err)
	}
	defer rc.Close()

	pr, pw := io.Pipe()
	copyDone := make(chan error, 1)
	var srcManifest Manifest

	go func() {
		tr := io.TeeReader(rc, pw)
		m, err := buildManifest(tr)
		if err == nil {
			srcManifest = m
		}
		pw.CloseWithError(err)
		copyDone <- err
	}()

	importErr := s.client.VolumeImport(ctx, toHost, toName, pr)
	pr.CloseWithError(importErr)

	copyErr := <-copyDone
	if importErr != nil {
		return nil, fmt.Errorf("import volume %q to %s: %w", toName, toHost, importErr)
	}
	if copyErr != nil {
		return nil, fmt.Errorf("copy volume %q -> %q: build manifest: %w", fromName, toName, copyErr)
	}
	return srcManifest, nil
}

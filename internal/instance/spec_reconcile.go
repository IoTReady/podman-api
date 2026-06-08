package instance

import (
	"context"
	"errors"
	"fmt"
	"log"
	"maps"
	"slices"

	"github.com/iotready/podman-api/internal/podman"
	"github.com/iotready/podman-api/internal/render"
	"github.com/iotready/podman-api/internal/store"
)

// ReconcileSpecsOnHost checks every stored instance spec on host against real
// pod state and re-converges any that are missing (not running). It is called
// once at daemon startup as a one-shot boot converge, so managed pods survive
// a host reboot. Errors are logged per-instance and never propagated to the
// HTTP layer — the method always returns nil (it tolerates any failure by
// logging and continuing so a partial host outage does not block the rest).
//
// Concurrency: per-instance operations are serialized under the existing
// per-instance lock so this cannot race a concurrent Apply/Delete/Upgrade.
// No per-host lock is taken because boot converge re-creates only instances
// whose store row already exists and whose domains are already claimed —
// it creates no new cross-instance domain claims.
//
// Limitations (by design):
//   - No image pull: images are expected to be cached from the original deploy.
//   - One-shot: called once on startup; no periodic drift-correction loop.
//   - Template-missing instances are skipped with a warning (not reaped).
//   - Secrets-undecryptable instances are skipped (wrong key file — operator
//     must restart with the correct -spec-key-file).
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

	for _, k := range keys {
		reconciled, err := s.reconcileOneSpec(ctx, hostID, k.Template, k.Slug)
		if err != nil {
			log.Printf("boot converge %s/%s/%s: %v", hostID, k.Template, k.Slug, err)
		} else if reconciled {
			log.Printf("boot converge %s/%s/%s: re-converged (pod was missing)", hostID, k.Template, k.Slug)
		} else {
			log.Printf("boot converge %s/%s/%s: already running", hostID, k.Template, k.Slug)
		}
	}
}

// reconcileOneSpec checks whether the pod for (host, tmpl, slug) exists; if it
// does, returns (false, nil). If it does not, re-creates it from the stored
// spec and returns (true, nil). Errors are returned so the caller can log them
// — the caller decides whether to retry or skip.
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
	// PodInspect returned ErrNotFound or the pod exists but isn't running.
	// Either way we need to re-converge.

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
			// re-render the spec. Skip and warn.
			log.Printf("boot converge %s/%s/%s: template %q not found — skipping (spec not reaped)",
				hostID, tmpl, slug, tmpl)
			return false, nil
		}
		return false, fmt.Errorf("get template %q: %w", tmpl, err)
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
	yaml, err := render.RenderBody(tmplObj.Body, params)
	if err != nil {
		return false, fmt.Errorf("render: %w", err)
	}

	// Step 6: re-create per-instance secrets on the host.
	for k, v := range spec.Secrets {
		name := instanceSecretName(tmpl, slug, k)
		// Remove before create (idempotent rotation — same as applyLocked).
		if _, iErr := s.client.SecretInspect(ctx, hostID, name); iErr == nil {
			_ = s.client.SecretRemove(ctx, hostID, name)
		}
		if err := s.client.SecretCreate(ctx, hostID, name, wrapAsKubeSecret(name, []byte(v))); err != nil {
			return false, fmt.Errorf("create secret %q: %w", name, err)
		}
	}

	// Step 7: ensure ingress network if the template declares ingress.
	var networks []string
	if s.ingressEnabled() && tmplObj.Meta.Ingress != nil {
		if err := s.client.NetworkEnsure(ctx, hostID, s.ingressNet); err != nil {
			return false, fmt.Errorf("ensure ingress network: %w", err)
		}
		networks = []string{s.ingressNet}
	}

	// Step 8: play kube (replace=false is correct — the pod is absent).
	if err := s.client.PlayKube(ctx, hostID, yaml, false, networks...); err != nil {
		return false, fmt.Errorf("play kube: %w", err)
	}

	// Step 9: persist the spec (upsert — updates timestamp, data unchanged).
	sp := store.Spec{
		Host:       hostID,
		Template:   tmpl,
		Slug:       slug,
		Parameters: maps.Clone(params),
		Secrets:    maps.Clone(spec.Secrets),
		Domains:    slices.Clone(spec.Domains),
	}
	if err := s.store.PutSpec(ctx, sp); err != nil {
		// Spec persist failed but the pod is already running. Log the error
		// but do not fail the reconcile — the pod is the primary concern.
		log.Printf("boot converge %s/%s/%s: pod re-created but spec persist failed: %v",
			hostID, tmpl, slug, err)
	}

	// Step 10: ingress reconcile (idempotent).
	if s.ingressEnabled() {
		if err := s.ingress.Reconcile(ctx, hostID); err != nil {
			log.Printf("boot converge %s/%s/%s: pod re-created but ingress reconcile failed: %v",
				hostID, tmpl, slug, err)
		}
	}

	return true, nil
}

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

// errTemplateSkipped is a sentinel returned by reconcileOneSpec when the
// instance's template was deleted from the catalog since the instance was
// deployed. The caller logs a human-readable message instead of the error
// text, distinguishing this from other transient (unreachable) failures.
var errTemplateSkipped = errors.New("template not found — instance skipped")

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

	needsIngress := false
	for _, k := range keys {
		reconciled, err := s.reconcileOneSpec(ctx, hostID, k.Template, k.Slug)
		if errors.Is(err, errTemplateSkipped) {
			log.Printf("boot converge %s/%s/%s: template %q not found — skipping (spec not reaped)",
				hostID, k.Template, k.Slug, k.Template)
		} else if err != nil {
			log.Printf("boot converge %s/%s/%s: %v", hostID, k.Template, k.Slug, err)
		} else if reconciled {
			log.Printf("boot converge %s/%s/%s: re-converged (pod was missing)", hostID, k.Template, k.Slug)
			needsIngress = true
		} else {
			log.Printf("boot converge %s/%s/%s: already running", hostID, k.Template, k.Slug)
		}
	}

	// Reconcile ingress once per host rather than once per instance, avoiding
	// N Caddyfile generations and reloads when N instances are reconverged.
	// The reconcile is idempotent — it reads all specs from the store for this
	// host and derives routes from scratch.
	if needsIngress && s.ingressEnabled() {
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
		inj, err := s.sidecar.InjectSidecars(ctx, yaml, toExtMeta(tmplObj.Meta), params, slug)
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

	// Step 7: ensure ingress network if the template declares ingress.
	var networks []string
	if s.ingressEnabled() && tmplObj.Meta.Ingress != nil {
		if err := s.client.NetworkEnsure(ctx, hostID, s.ingressNet); err != nil {
			return false, fmt.Errorf("ensure ingress network: %w", err)
		}
		networks = []string{s.ingressNet}
	}

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
	}
	if err := s.store.PutSpec(ctx, sp); err != nil {
		// Spec persist failed but the pod is already running. Log the error
		// but do not fail the reconcile — the pod is the primary concern.
		log.Printf("boot converge %s/%s/%s: pod re-created but spec persist failed: %v",
			hostID, tmpl, slug, err)
	}

	return true, nil
}

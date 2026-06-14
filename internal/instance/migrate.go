package instance

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
	"time"

	"gopkg.in/yaml.v3"

	"github.com/iotready/podman-api/internal/podman"
	"github.com/iotready/podman-api/internal/render"
	"github.com/iotready/podman-api/internal/store"
)

// verify-poll knobs; vars (not consts) so same-package tests can shorten them.
var (
	verifyTimeout  = 180 * time.Second
	verifyInterval = 2 * time.Second
)

// SetVerifyTimeout overrides the maximum time waitRunning waits for the
// destination to become ready before the migrate fails (and rolls back).
// No-op for d <= 0. Called once at startup from the -migrate-verify-timeout flag.
func SetVerifyTimeout(d time.Duration) {
	if d > 0 {
		verifyTimeout = d
	}
}

// PairedInstanceRef identifies an instance (on FromHost) that shares volumes
// with the instance being migrated. Stopping it before the volume copy prevents
// concurrent writes that would cause a verification mismatch.
type PairedInstanceRef struct {
	Template string `json:"template"`
	Slug     string `json:"slug"`
}

// MigrateRequest is the POST /migrate body and the migrate job's args.
type MigrateRequest struct {
	FromHost   string              `json:"from_host"`
	ToHost     string              `json:"to_host"`
	Template   string              `json:"template"`
	Slug       string              `json:"slug"`
	Parameters map[string]any      `json:"parameters"`
	AlsoStop   []PairedInstanceRef `json:"also_stop,omitempty"`
}

// migrateLock serialises migrates of the same instance without colliding with
// the per-host instance locks taken by Apply/Delete/Start/Stop (which would
// self-deadlock, sync.Mutex being non-reentrant). The sentinel "host" cannot
// collide with any real host id.
func (s *Service) migrateLock(tmpl, slug string) *sync.Mutex {
	return s.instanceLock("\x00migrate", tmpl, slug)
}

// CheckMigratable runs the cheap synchronous validation the POST handler needs:
// distinct known hosts, known template, and an existing stored spec. No mutation.
func (s *Service) CheckMigratable(ctx context.Context, req MigrateRequest) error {
	if req.FromHost == req.ToHost {
		return ErrSameHost
	}
	if _, ok := s.host(req.FromHost); !ok {
		return ErrUnknownHost
	}
	if _, ok := s.host(req.ToHost); !ok {
		return ErrUnknownHost
	}
	if _, err := s.lookup(ctx, req.ToHost, req.Template); err != nil {
		return err
	}
	if _, err := s.store.GetSpec(ctx, req.FromHost, req.Template, req.Slug); err != nil {
		return err
	}
	return nil
}

// requiredHostPorts parses the rendered template YAML and returns the host
// ports its Pod(s) bind.
func (s *Service) requiredHostPorts(rendered string) ([]int, error) {
	var ports []int
	dec := yaml.NewDecoder(strings.NewReader(rendered))
	for {
		var d struct {
			Kind string `yaml:"kind"`
			Spec struct {
				Containers []struct {
					Ports []struct {
						HostPort int `yaml:"hostPort"`
					} `yaml:"ports"`
				} `yaml:"containers"`
			} `yaml:"spec"`
		}
		err := dec.Decode(&d)
		if err == io.EOF {
			break
		}
		if err != nil {
			continue // skip a malformed document
		}
		if d.Kind != "Pod" {
			continue
		}
		for _, c := range d.Spec.Containers {
			for _, p := range c.Ports {
				if p.HostPort > 0 {
					ports = append(ports, p.HostPort)
				}
			}
		}
	}
	return ports, nil
}

// hostSecretProvisionable reports whether per-host secret `name` — already known
// absent on the destination — can be auto-provisioned from the source host's
// persisted value. A non-nil error is an infra/store failure the caller should
// treat as inconclusive. Returns (false, nil) when the store holds no value
// (i.e. genuinely missing, not an error).
func (s *Service) hostSecretProvisionable(ctx context.Context, fromHost, name string) (bool, error) {
	switch _, err := s.store.GetHostSecret(ctx, fromHost, name); {
	case err == nil:
		return true, nil
	case errors.Is(err, store.ErrNotFound):
		return false, nil
	default:
		return false, err
	}
}

// preflightIssues runs every destination preflight check and returns (issues,
// provisionable): all problems found in check order, plus the per-host secrets
// that are absent on the destination but can be auto-provisioned from the source
// host's persisted value. A nil/empty issues slice means the destination would
// accept the instance (after provisioning any returned secrets). Each issue is a
// sentinel-wrapped blocking condition or an infrastructure error that made a
// check inconclusive. preflightDest (the executor's fail-fast gate) and
// PlanEvacuation (the collect-all preview) both build on this, so they never disagree.
//
// When pullImages is true the method also attempts to pull every container image
// referenced in the template on the destination host. This is a mutation (the
// pull is idempotent for already-pulled images). The preview path
// (PlanEvacuation) passes false to preserve its read-only contract.
func (s *Service) preflightIssues(ctx context.Context, req MigrateRequest, tmpl store.Template, eff map[string]any, pullImages bool) ([]error, []string) {
	var issues []error
	var provisionable []string
	hostCfg, _ := s.host(req.ToHost)
	if hostCfg.Drain {
		issues = append(issues, ErrHostDraining)
	}
	if _, err := s.client.PodInspect(ctx, req.ToHost, podName(req.Template, req.Slug)); err == nil {
		issues = append(issues, ErrInstanceExists)
	} else if !errors.Is(err, podman.ErrNotFound) {
		return append(issues, fmt.Errorf("inspect dest pod: %w", err)), provisionable
	}
	for _, name := range tmpl.Meta.Secrets.PerHostReferenced {
		_, err := s.client.SecretInspect(ctx, req.ToHost, name)
		if err == nil {
			continue // present on dest
		}
		if !errors.Is(err, podman.ErrNotFound) {
			// Infra error (host unreachable): report and stop — avoids piling up
			// timeout-length RPCs on the executor's failure path.
			return append(issues, fmt.Errorf("inspect host secret %q: %w", name, err)), provisionable
		}
		// Absent on dest: provisionable from the source host's persisted value?
		ok, perr := s.hostSecretProvisionable(ctx, req.FromHost, name)
		if perr != nil {
			// A store-lookup failure is a fast local error, not a timeout-prone
			// RPC: record it as this secret's issue and keep scanning so the
			// collect-all preview still reports the remaining checks. The executor
			// (preflightDest) takes the first issue, so it still aborts.
			issues = append(issues, fmt.Errorf("lookup persisted host secret %q: %w", name, perr))
			continue
		}
		if ok {
			provisionable = append(provisionable, name)
			continue
		}
		issues = append(issues, fmt.Errorf("%w: %s", ErrHostSecretMissing, name))
	}
	// Render the template body once and reuse it for the port check and, when
	// pullImages is true, the image-pull preflight.
	rendered, rerr := render.RenderBody(tmpl.Body, eff)
	if rerr != nil {
		return append(issues, fmt.Errorf("render: %w", rerr)), provisionable
	}
	want, err := s.requiredHostPorts(rendered)
	if err != nil {
		return append(issues, err), provisionable
	}
	if len(want) > 0 {
		used, err := s.PortsInUse(ctx, req.ToHost)
		if err != nil {
			return append(issues, fmt.Errorf("ports in use: %w", err)), provisionable
		}
		busy := map[int]bool{}
		for _, p := range used {
			busy[p.HostPort] = true
		}
		for _, p := range want {
			if busy[p] {
				issues = append(issues, fmt.Errorf("%w: %d", ErrPortConflict, p))
			}
		}
	}

	if pullImages {
		for _, img := range containerImages(rendered) {
			if err := s.client.ImagePull(ctx, req.ToHost, img); err != nil {
				issues = append(issues, fmt.Errorf("%w: %s: %v", ErrImagePull, img, err))
			}
		}
	}
	return issues, provisionable
}

// preflightDest runs all fail-fast destination checks (no mutation), returning
// the first blocking condition or infrastructure error encountered, in check
// order. It is the executor's guard; PlanEvacuation uses preflightIssues to
// collect every problem instead.
func (s *Service) preflightDest(ctx context.Context, req MigrateRequest, tmpl store.Template, eff map[string]any) error {
	if errs, _ := s.preflightIssues(ctx, req, tmpl, eff, true); len(errs) > 0 {
		return errs[0]
	}
	return nil
}

// Migrate moves an instance from one host to another: stop source, copy
// volumes, apply the spec on the destination, verify it is healthy, then reap
// the source. Failures before the destination is verified roll back. step is a
// best-effort progress callback (may be nil).
func (s *Service) Migrate(ctx context.Context, req MigrateRequest, step func(step, detail string)) error {
	if step == nil {
		step = func(string, string) {}
	}
	lk := s.migrateLock(req.Template, req.Slug)
	lk.Lock()
	defer lk.Unlock()

	if req.FromHost == req.ToHost {
		return ErrSameHost
	}
	if _, ok := s.host(req.FromHost); !ok {
		return ErrUnknownHost
	}

	tmpl, err := s.lookup(ctx, req.ToHost, req.Template)
	if err != nil {
		return err
	}
	spec, err := s.store.GetSpec(ctx, req.FromHost, req.Template, req.Slug)
	if err != nil {
		return err
	}
	step("load", req.FromHost+"/"+req.Template+"/"+req.Slug)

	eff := mergeParams(spec.Parameters, req.Parameters)
	eff["slug"] = req.Slug // canonical slug always wins; pod name must match podName()
	if err := s.preflightDest(ctx, req, tmpl, eff); err != nil {
		return err
	}
	step("preflight", req.ToHost)

	// From here the source is mutated; failures before verify roll back.
	if err := s.Stop(ctx, req.FromHost, req.Template, req.Slug); err != nil {
		return fmt.Errorf("stop source: %w", err)
	}
	step("stop-source", req.FromHost)

	stoppedPaired, err := s.migratePostStop(ctx, req, eff, tmpl, spec.Secrets, spec.Domains, step)
	if err != nil {
		step("rollback", err.Error())
		// Compensate on a detached context: migratePostStop may have failed
		// *because* ctx was cancelled/timed out (the verify poll returns
		// ctx.Err()), and the source must still be restarted and the partial
		// destination reaped regardless.
		rbctx := context.WithoutCancel(ctx)
		if _, rerr := s.Start(rbctx, req.FromHost, req.Template, req.Slug); rerr != nil {
			step("rollback-restore-failed", rerr.Error())
		}
		if rerr := s.Delete(rbctx, req.ToHost, req.Template, req.Slug, DeleteOptions{PruneVolumes: true, PruneSecrets: true}); rerr != nil {
			step("rollback-reap-failed", rerr.Error())
		}
		// Restart paired instances that were stopped before the copy
		// (best-effort — these are separate instances sharing volumes).
		for _, ps := range stoppedPaired {
			if _, rerr := s.Start(rbctx, req.FromHost, ps.Template, ps.Slug); rerr != nil {
				step("rollback-start-paired-failed", rerr.Error())
			}
		}
		return err
	}

	// Verified healthy: dest is now truth. Commit by reaping the source on a
	// detached context so a late ctx cancellation can't strand a half-committed
	// state (dest live, source not removed).
	if err := s.Delete(context.WithoutCancel(ctx), req.FromHost, req.Template, req.Slug, DeleteOptions{PruneVolumes: true, PruneSecrets: true}); err != nil {
		return fmt.Errorf("commit (delete source): %w", err)
	}
	step("commit", req.FromHost)

	// Restart paired instances on the (still-live) source host after the source
	// is reaped. The paired instances are separate instances that share volumes
	// and are not deleted by the commit.
	for _, ps := range stoppedPaired {
		if _, serr := s.Start(context.WithoutCancel(ctx), req.FromHost, ps.Template, ps.Slug); serr != nil {
			step("start-paired-failed", serr.Error())
		} else {
			step("start-paired", ps.Template+"/"+ps.Slug)
		}
	}
	return nil
}

// migratePostStop runs the source-side preparation and destination-mutating
// steps: stop paired instances, copy volumes, apply the spec, verify health.
// Any error here is rolled back by the caller. It returns the list of paired
// instances that were successfully stopped, so the caller can restart them
// on rollback or after commit. domains are the source spec's public hostnames,
// threaded through so an ingress instance keeps its route on the destination
// (Apply re-validates host-wide uniqueness and serializes the claim per host —
// #82 — so passing them here is sufficient).
func (s *Service) migratePostStop(ctx context.Context, req MigrateRequest, eff map[string]any, tmpl store.Template, secrets map[string]string, domains []string, step func(step, detail string)) (stoppedPaired []PairedInstanceRef, err error) {
	// Provision any persisted per-host secrets the destination is missing, from
	// the source host's stored value. Idempotent: only creates what is absent.
	// Provisioned secrets are intentionally left in place on rollback — they are
	// shared, host-scoped, and additive, and other instances on the destination
	// may rely on them (Delete's PruneSecrets only reaps per-instance secrets).
	for _, name := range tmpl.Meta.Secrets.PerHostReferenced {
		if _, err := s.client.SecretInspect(ctx, req.ToHost, name); err == nil {
			continue // already present on dest
		} else if !errors.Is(err, podman.ErrNotFound) {
			return nil, fmt.Errorf("inspect dest host secret %q: %w", name, err)
		}
		val, err := s.store.GetHostSecret(ctx, req.FromHost, name)
		if errors.Is(err, store.ErrNotFound) {
			// Defensive: preflight (same store lookup, before Stop) already gated
			// this, so it is unreachable in the executor path. Apply's pre-check is
			// the backstop if it ever is reached.
			continue
		}
		if err != nil {
			return nil, fmt.Errorf("load host secret %q: %w", name, err)
		}
		if err := s.client.SecretCreate(ctx, req.ToHost, name, wrapAsKubeSecret(name, val)); err != nil {
			// A concurrent move may have created it between inspect and create;
			// tolerate that, fail on anything else.
			if _, ie := s.client.SecretInspect(ctx, req.ToHost, name); ie == nil {
				continue
			}
			return nil, fmt.Errorf("provision host secret %q: %w", name, err)
		}
		step("provision-secret", name)
		// Record the dest as a valid future provisioning source so multi-hop
		// evacuations chain (h1->h2->h3). Best-effort: the secret is already on the
		// dest and the move is sound, so a failed record must not roll back a
		// healthy migration — a later hop would simply re-provision.
		if rerr := s.store.PutHostSecret(ctx, req.ToHost, name, val); rerr != nil {
			step("record-host-secret-failed", name)
		}
	}

	// Stop paired instances that share volumes with this instance before the
	// copy, so they cannot modify the volume during export/verify. This is the
	// first mutation after secret provisioning; a failure here is rolled back
	// by the caller (which restarts the main source instance).
	for _, ps := range req.AlsoStop {
		name := ps.Template + "/" + ps.Slug
		if err := s.Stop(ctx, req.FromHost, ps.Template, ps.Slug); errors.Is(err, ErrInstanceNotFound) {
			step("stop-paired-skipped", name)
		} else if err != nil {
			return nil, fmt.Errorf("stop paired instance %s: %w", name, err)
		} else {
			stoppedPaired = append(stoppedPaired, ps)
			step("stop-paired", name)
		}
	}

	vols, err := s.InstanceVolumes(ctx, req.FromHost, req.Template, req.Slug)
	if err != nil {
		return stoppedPaired, fmt.Errorf("list source volumes: %w", err)
	}
	for _, v := range vols {
		if err := s.client.VolumeCreate(ctx, req.ToHost, v.Name); err != nil {
			return stoppedPaired, fmt.Errorf("create dest volume %q: %w", v.Name, err)
		}
		step("copy-volume", v.Name)
		volStart := time.Now()
		srcManifest, err := s.CopyVolume(ctx, req.FromHost, req.ToHost, v.Name)
		if err != nil {
			return stoppedPaired, fmt.Errorf("copy volume %q: %w", v.Name, err)
		}
		elapsed := time.Since(volStart)
		if v.SizeBytes > 0 {
			step("copy-volume-done", fmt.Sprintf("%s (%d bytes, %s)", v.Name, v.SizeBytes, elapsed))
		} else {
			step("copy-volume-done", fmt.Sprintf("%s (%s)", v.Name, elapsed))
		}
		if s.verifyVolumes {
			dst, err := s.volumeManifest(ctx, req.ToHost, v.Name)
			if err != nil {
				return stoppedPaired, fmt.Errorf("verify volume %q: re-export dest: %w", v.Name, err)
			}
			if diff, ok := srcManifest.firstDiff(dst); !ok {
				return stoppedPaired, fmt.Errorf("%w: volume %q differs at %q", ErrVolumeIntegrity, v.Name, diff)
			}
			step("verify-volume", v.Name)
		}
	}

	step("apply-dest", req.ToHost)
	applyStart := time.Now()
	if err := s.Apply(ctx, req.ToHost, ApplyRequest{
		Template: req.Template, Slug: req.Slug, Parameters: eff, Secrets: secrets, Domains: domains,
	}, ApplyOptions{Replace: false}); err != nil {
		return stoppedPaired, fmt.Errorf("apply on dest: %w", err)
	}
	step("apply-dest-done", fmt.Sprintf("%s (%s)", req.ToHost, time.Since(applyStart)))

	if err := s.waitRunning(ctx, req.ToHost, req.Template, req.Slug); err != nil {
		return stoppedPaired, fmt.Errorf("verify dest: %w", err)
	}
	step("verify", req.ToHost)
	return stoppedPaired, nil
}

// waitRunning polls the dest pod until Running, bounded by verifyTimeout and the
// caller's context.
func (s *Service) waitRunning(ctx context.Context, host, tmpl, slug string) error {
	if err := s.waitReady(ctx, host, tmpl, slug, verifyTimeout, verifyStableCount); err != nil {
		if errors.Is(err, errReadyTimeout) {
			return fmt.Errorf("pod %s not running within %s", podName(tmpl, slug), verifyTimeout)
		}
		return err
	}
	return nil
}

// podReady reports whether the pod is up and serving: the pod is Running, every
// container is Running, and every container that declares a healthcheck reports
// "healthy". Containers with no declared healthcheck (Health == "") are gated on
// liveness alone, so an instance without healthchecks behaves exactly as before.
// "starting" (still inside the healthcheck start_period) counts as not ready.
// Status comparisons use EqualFold because podman's API returns pod state
// capitalised ("Running") but container state lowercased ("running") — #149.
func podReady(p podman.Pod) bool {
	if !strings.EqualFold(p.Status, "running") {
		return false
	}
	for _, c := range p.Containers {
		if !strings.EqualFold(c.Status, "running") {
			return false
		}
		if c.Health != "" && !strings.EqualFold(c.Health, "healthy") {
			return false
		}
	}
	return true
}

// mergeParams returns a new map: base overlaid by override (override wins).
func mergeParams(base, override map[string]any) map[string]any {
	out := make(map[string]any, len(base)+len(override))
	for k, v := range base {
		out[k] = v
	}
	for k, v := range override {
		out[k] = v
	}
	return out
}

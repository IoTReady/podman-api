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

	"github.com/iotready/podman-api/internal/config"
	"github.com/iotready/podman-api/internal/podman"
	"github.com/iotready/podman-api/internal/render"
)

// verify-poll knobs; vars (not consts) so same-package tests can shorten them.
var (
	verifyTimeout  = 60 * time.Second
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

// MigrateRequest is the POST /migrate body and the migrate job's args.
type MigrateRequest struct {
	FromHost   string         `json:"from_host"`
	ToHost     string         `json:"to_host"`
	Template   string         `json:"template"`
	Slug       string         `json:"slug"`
	Parameters map[string]any `json:"parameters"`
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
	if _, err := s.lookup(req.ToHost, req.Template); err != nil {
		return err
	}
	if s.store == nil {
		return ErrStoreDisabled
	}
	if _, err := s.store.GetSpec(ctx, req.FromHost, req.Template, req.Slug); err != nil {
		return err
	}
	return nil
}

// requiredHostPorts renders the template with eff params and returns the host
// ports its Pod(s) bind.
func (s *Service) requiredHostPorts(tmpl config.Template, params map[string]any) ([]int, error) {
	rendered, err := render.Render(rawTemplate(tmpl), params)
	if err != nil {
		return nil, fmt.Errorf("render: %w", err)
	}
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

// preflightIssues runs every destination preflight check and returns all
// problems found, in check order. A nil/empty result means the destination
// would currently accept the instance. Each error is either a sentinel-wrapped
// blocking condition (ErrHostDraining / ErrInstanceExists / ErrHostSecretMissing
// / ErrPortConflict) or an infrastructure error that made a check inconclusive.
// preflightDest (fail-fast executor path) and PlanEvacuation (collect-all preview
// path) both build on it, so the preview and the executor never disagree.
func (s *Service) preflightIssues(ctx context.Context, req MigrateRequest, tmpl config.Template, eff map[string]any) []error {
	var issues []error
	hostCfg, _ := s.host(req.ToHost)
	if hostCfg.Drain {
		issues = append(issues, ErrHostDraining)
	}
	// An infra error here usually means the host is unreachable, which would
	// fail every subsequent check too — report it once and stop, mirroring the
	// executor's original fail-fast behaviour.
	if _, err := s.client.PodInspect(ctx, req.ToHost, podName(req.Template, req.Slug)); err == nil {
		issues = append(issues, ErrInstanceExists)
	} else if !errors.Is(err, podman.ErrNotFound) {
		return append(issues, fmt.Errorf("inspect dest pod: %w", err))
	}
	for _, name := range tmpl.Meta.Secrets.PerHostReferenced {
		if _, err := s.client.SecretInspect(ctx, req.ToHost, name); err != nil {
			if errors.Is(err, podman.ErrNotFound) {
				issues = append(issues, fmt.Errorf("%w: %s", ErrHostSecretMissing, name))
			} else {
				issues = append(issues, fmt.Errorf("inspect host secret %q: %w", name, err))
			}
		}
	}
	want, err := s.requiredHostPorts(tmpl, eff)
	if err != nil {
		return append(issues, err)
	}
	if len(want) > 0 {
		used, err := s.PortsInUse(ctx, req.ToHost)
		if err != nil {
			return append(issues, fmt.Errorf("ports in use: %w", err))
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
	return issues
}

// preflightDest runs all fail-fast destination checks (no mutation), returning
// the first blocking condition or infrastructure error encountered, in check
// order. It is the executor's guard; PlanEvacuation uses preflightIssues to
// collect every problem instead.
func (s *Service) preflightDest(ctx context.Context, req MigrateRequest, tmpl config.Template, eff map[string]any) error {
	if errs := s.preflightIssues(ctx, req, tmpl, eff); len(errs) > 0 {
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

	tmpl, err := s.lookup(req.ToHost, req.Template)
	if err != nil {
		return err
	}
	if s.store == nil {
		return ErrStoreDisabled
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

	if err := s.migratePostStop(ctx, req, eff, spec.Secrets, step); err != nil {
		step("rollback", err.Error())
		// Compensate on a detached context: migratePostStop may have failed
		// *because* ctx was cancelled/timed out (the verify poll returns
		// ctx.Err()), and the source must still be restarted and the partial
		// destination reaped regardless.
		rbctx := context.WithoutCancel(ctx)
		if rerr := s.Start(rbctx, req.FromHost, req.Template, req.Slug); rerr != nil {
			step("rollback-restore-failed", rerr.Error())
		}
		if rerr := s.Delete(rbctx, req.ToHost, req.Template, req.Slug, DeleteOptions{PruneVolumes: true, PruneSecrets: true}); rerr != nil {
			step("rollback-reap-failed", rerr.Error())
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
	return nil
}

// migratePostStop runs the destination-mutating steps: copy volumes, apply the
// spec, verify health. Any error here is rolled back by the caller.
func (s *Service) migratePostStop(ctx context.Context, req MigrateRequest, eff map[string]any, secrets map[string]string, step func(step, detail string)) error {
	vols, err := s.InstanceVolumes(ctx, req.FromHost, req.Template, req.Slug)
	if err != nil {
		return fmt.Errorf("list source volumes: %w", err)
	}
	for _, v := range vols {
		if err := s.client.VolumeCreate(ctx, req.ToHost, v.Name); err != nil {
			return fmt.Errorf("create dest volume %q: %w", v.Name, err)
		}
		if err := s.CopyVolume(ctx, req.FromHost, req.ToHost, v.Name); err != nil {
			return fmt.Errorf("copy volume %q: %w", v.Name, err)
		}
		step("copy-volume", v.Name)
		if s.verifyVolumes {
			src, err := s.volumeManifest(ctx, req.FromHost, v.Name)
			if err != nil {
				return fmt.Errorf("verify volume %q: re-export source: %w", v.Name, err)
			}
			dst, err := s.volumeManifest(ctx, req.ToHost, v.Name)
			if err != nil {
				return fmt.Errorf("verify volume %q: re-export dest: %w", v.Name, err)
			}
			if diff, ok := src.firstDiff(dst); !ok {
				return fmt.Errorf("%w: volume %q differs at %q", ErrVolumeIntegrity, v.Name, diff)
			}
			step("verify-volume", v.Name)
		}
	}

	if err := s.Apply(ctx, req.ToHost, ApplyRequest{
		Template: req.Template, Slug: req.Slug, Parameters: eff, Secrets: secrets,
	}, ApplyOptions{Replace: false}); err != nil {
		return fmt.Errorf("apply on dest: %w", err)
	}
	step("apply-dest", req.ToHost)

	if err := s.waitRunning(ctx, req.ToHost, req.Template, req.Slug); err != nil {
		return fmt.Errorf("verify dest: %w", err)
	}
	step("verify", req.ToHost)
	return nil
}

// waitRunning polls the dest pod until Running, bounded by verifyTimeout and the
// caller's context.
func (s *Service) waitRunning(ctx context.Context, host, tmpl, slug string) error {
	deadline := time.Now().Add(verifyTimeout)
	ticker := time.NewTicker(verifyInterval)
	defer ticker.Stop()
	for {
		p, err := s.client.PodInspect(ctx, host, podName(tmpl, slug))
		if err == nil && podReady(p) {
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("pod %s not running within %s", podName(tmpl, slug), verifyTimeout)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

// podReady reports whether the pod is up and serving: the pod is Running, every
// container is Running, and every container that declares a healthcheck reports
// "healthy". Containers with no declared healthcheck (Health == "") are gated on
// liveness alone, so an instance without healthchecks behaves exactly as before.
// "starting" (still inside the healthcheck start_period) counts as not ready.
func podReady(p podman.Pod) bool {
	if p.Status != "Running" {
		return false
	}
	for _, c := range p.Containers {
		if c.Status != "Running" {
			return false
		}
		if c.Health != "" && c.Health != "healthy" {
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

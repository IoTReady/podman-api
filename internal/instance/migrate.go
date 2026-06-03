package instance

import (
	"context"
	"errors"
	"fmt"
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
	for _, doc := range strings.Split(rendered, "\n---\n") {
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
		if yaml.Unmarshal([]byte(doc), &d) != nil || d.Kind != "Pod" {
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

// preflightDest runs all fail-fast destination checks (no mutation).
func (s *Service) preflightDest(ctx context.Context, req MigrateRequest, tmpl config.Template, eff map[string]any) error {
	hostCfg, _ := s.host(req.ToHost)
	if hostCfg.Drain {
		return ErrHostDraining
	}
	if _, err := s.client.PodInspect(ctx, req.ToHost, podName(req.Template, req.Slug)); err == nil {
		return ErrInstanceExists
	} else if !errors.Is(err, podman.ErrNotFound) {
		return fmt.Errorf("inspect dest pod: %w", err)
	}
	for _, name := range tmpl.Meta.Secrets.PerHostReferenced {
		if _, err := s.client.SecretInspect(ctx, req.ToHost, name); err != nil {
			if errors.Is(err, podman.ErrNotFound) {
				return fmt.Errorf("%w: %s", ErrHostSecretMissing, name)
			}
			return fmt.Errorf("inspect host secret %q: %w", name, err)
		}
	}
	want, err := s.requiredHostPorts(tmpl, eff)
	if err != nil {
		return err
	}
	if len(want) > 0 {
		used, err := s.PortsInUse(ctx, req.ToHost)
		if err != nil {
			return fmt.Errorf("ports in use: %w", err)
		}
		busy := map[int]bool{}
		for _, p := range used {
			busy[p.HostPort] = true
		}
		for _, p := range want {
			if busy[p] {
				return fmt.Errorf("%w: %d", ErrPortConflict, p)
			}
		}
	}
	return nil
}

// Migrate moves an instance from one host to another. step is a best-effort
// progress callback (may be nil). NOTE: mutation steps are added in a later task;
// for now Migrate stops after preflight.
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
	if err := s.preflightDest(ctx, req, tmpl, eff); err != nil {
		return err
	}
	step("preflight", req.ToHost)
	return nil // mutation steps added in a later task
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

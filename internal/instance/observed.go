package instance

import (
	"strings"
	"time"

	"github.com/iotready/podman-api/internal/podman"
	"github.com/iotready/podman-api/internal/render"
)

// Observed is the JSON shape returned for an instance.
type Observed struct {
	Template   string              `json:"template"`
	Slug       string              `json:"slug"`
	Ready      bool                `json:"ready"`
	Pod        ObservedPod         `json:"pod"`
	Containers []ObservedContainer `json:"containers"`
	Volumes    []ObservedVolume    `json:"volumes,omitempty"`
	EnvSummary map[string]string   `json:"env_summary,omitempty"`
	// Parameters is the instance's stored render parameters, as last applied
	// (#200). Only the single-instance Get path populates it; list sweeps leave
	// it nil so a host listing stays cheap and small. Absent (not null) when the
	// instance has no stored spec, or when that spec is unreadable. Never
	// carries secret material — see PublicParameters.
	Parameters map[string]any `json:"parameters,omitempty"`
	Warnings   []string       `json:"warnings,omitempty"`
}

type ObservedPod struct {
	ID      string    `json:"id,omitempty"`
	Status  string    `json:"status"`
	Created time.Time `json:"created,omitempty"`
}

type ObservedContainer struct {
	Name         string                `json:"name"`
	Image        string                `json:"image"`
	ImageTag     string                `json:"image_tag,omitempty"`
	Status       string                `json:"status"`
	Health       string                `json:"health,omitempty"`
	StartedAt    time.Time             `json:"started_at,omitempty"`
	RestartCount int                   `json:"restart_count"`
	Ports        []ObservedPortMapping `json:"ports,omitempty"`
}

type ObservedPortMapping struct {
	HostIP        string `json:"host_ip,omitempty"`
	HostPort      int    `json:"host_port"`
	ContainerPort int    `json:"container_port"`
	Protocol      string `json:"protocol,omitempty"`
}

type ObservedVolume struct {
	Name      string `json:"name"`
	SizeBytes int64  `json:"size_bytes,omitempty"`
}

// Normalize builds Observed from a Pod + the volumes the API thinks the
// instance owns. env_summary is redacted in two passes, because neither alone
// is sufficient:
//
//   - by NAME: names in secretEnvs (derived from the template's secretKeyRef
//     blocks) plus a defensive substring check on SECRET. This sees only what
//     the stored template declares.
//   - by VALUE: values in secretVals (the instance's known secret values, from
//     the stored spec). A SidecarInjector adds containers and env after the
//     template body was stored, so its secretKeyRef env is invisible to the
//     name pass — this pass catches it however the value arrived (#198). An
//     empty string never matches here, even if secretVals contains "" — a
//     legitimately-empty env var must never be blanked, and callers building
//     secretVals are expected to exclude "" too (defense in depth).
//
// Either match omits the key from env_summary entirely; there is no redaction
// marker. Both sets may be nil.
func Normalize(p podman.Pod, template, slug string, vols []podman.Volume, secretEnvs, secretVals map[string]bool) Observed {
	out := Observed{
		Template: template,
		Slug:     slug,
		Pod:      ObservedPod{ID: p.ID, Status: p.Status, Created: p.Created},
	}
	ready := true
	for _, c := range p.Containers {
		oc := ObservedContainer{
			Name: c.Name, Image: c.Image, ImageTag: c.ImageTag,
			Status: c.Status, Health: c.Health,
			StartedAt: c.StartedAt, RestartCount: c.RestartCount,
		}
		for _, port := range c.Ports {
			oc.Ports = append(oc.Ports, ObservedPortMapping{
				HostIP: port.HostIP, HostPort: port.HostPort,
				ContainerPort: port.ContainerPort, Protocol: port.Protocol,
			})
		}
		out.Containers = append(out.Containers, oc)
		if c.Health != "" && c.Health != "healthy" {
			ready = false
		}
	}
	out.Ready = ready
	for _, v := range vols {
		out.Volumes = append(out.Volumes, ObservedVolume{Name: v.Name, SizeBytes: v.SizeBytes})
	}

	// EnvSummary takes the union of non-secret env vars across containers.
	out.EnvSummary = map[string]string{}
	for _, c := range p.Containers {
		for k, v := range c.Env {
			if secretEnvs[k] || (v != "" && secretVals[v]) || strings.Contains(strings.ToUpper(k), "SECRET") {
				continue
			}
			out.EnvSummary[k] = v
		}
	}
	if len(out.EnvSummary) == 0 {
		out.EnvSummary = nil
	}
	return out
}

// PublicParameters returns an instance's stored render parameters with anything
// that could carry secret material removed, for Observed.Parameters (#200).
//
// Secrets are stored in Spec.Secrets (encrypted at rest) and reach the pod via
// secretKeyRef, so parameters are non-secret by construction. A template
// declaring a parameter `secret: true` is now rejected at validation time
// (render.ValidateParamDefs, #205), but a template stored before that check
// existed may still carry one. So the same two-pass redaction env_summary uses
// applies here:
//
//   - by NAME: parameters whose ParamDef sets Secret are dropped — a backstop
//     for such pre-existing templates.
//   - by VALUE: string parameters whose value equals one of this instance's
//     known secret values are dropped, catching a secret that was passed as a
//     parameter without being declared one. An empty string never matches.
//
// A dropped parameter is omitted entirely; there is no redaction marker.
// Returns nil (so the field is absent, not null) when nothing survives.
func PublicParameters(m render.Meta, params map[string]any, secretVals map[string]bool) map[string]any {
	if len(params) == 0 {
		return nil
	}
	secretNames := make(map[string]bool, len(m.Parameters))
	for _, p := range m.Parameters {
		if p.Secret {
			secretNames[p.Name] = true
		}
	}
	out := make(map[string]any, len(params))
	for k, v := range params {
		if secretNames[k] {
			continue
		}
		if s, ok := v.(string); ok && s != "" && secretVals[s] {
			continue
		}
		out[k] = v
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

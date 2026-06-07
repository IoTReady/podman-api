package instance

import (
	"strings"
	"time"

	"github.com/iotready/podman-api/internal/podman"
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
	Warnings   []string            `json:"warnings,omitempty"`
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
// instance owns. Env vars whose names appear in secretEnvs (the set derived
// from the template's secretKeyRef blocks) are dropped from env_summary so
// secret material never returns to the CMS. A defensive substring check on
// SECRET also catches anything not anchored to a known template.
func Normalize(p podman.Pod, template, slug string, vols []podman.Volume, secretEnvs map[string]bool) Observed {
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
			if secretEnvs[k] || strings.Contains(strings.ToUpper(k), "SECRET") {
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

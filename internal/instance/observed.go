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
	Pod        ObservedPod         `json:"pod"`
	Containers []ObservedContainer `json:"containers"`
	Volumes    []ObservedVolume    `json:"volumes,omitempty"`
	EnvSummary map[string]string   `json:"env_summary,omitempty"`
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

// secretishKeys are env var names whose VALUES are never returned to the CMS.
var secretishKeys = map[string]bool{
	"AUTH_SECRET":                  true,
	"LITESTREAM_ACCESS_KEY_ID":     true,
	"LITESTREAM_SECRET_ACCESS_KEY": true,
	"AWS_ACCESS_KEY_ID":            true,
	"AWS_SECRET_ACCESS_KEY":        true,
}

// Normalize builds Observed from a Pod + the volumes the API thinks the
// instance owns. It redacts known-secret env keys from env_summary.
func Normalize(p podman.Pod, template, slug string, vols []podman.Volume) Observed {
	out := Observed{
		Template: template,
		Slug:     slug,
		Pod:      ObservedPod{ID: p.ID, Status: p.Status, Created: p.Created},
	}
	for _, c := range p.Containers {
		oc := ObservedContainer{
			Name: c.Name, Image: c.Image, ImageTag: c.ImageTag,
			Status: c.Status, StartedAt: c.StartedAt, RestartCount: c.RestartCount,
		}
		for _, port := range c.Ports {
			oc.Ports = append(oc.Ports, ObservedPortMapping{
				HostIP: port.HostIP, HostPort: port.HostPort,
				ContainerPort: port.ContainerPort, Protocol: port.Protocol,
			})
		}
		out.Containers = append(out.Containers, oc)
	}
	for _, v := range vols {
		out.Volumes = append(out.Volumes, ObservedVolume{Name: v.Name, SizeBytes: v.SizeBytes})
	}

	// EnvSummary takes the union of non-secret env vars across containers.
	out.EnvSummary = map[string]string{}
	for _, c := range p.Containers {
		for k, v := range c.Env {
			if secretishKeys[k] || strings.Contains(strings.ToUpper(k), "SECRET") {
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

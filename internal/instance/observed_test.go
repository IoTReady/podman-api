package instance

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/iotready/podman-api/internal/podman"
)

func TestNormalize(t *testing.T) {
	created := time.Date(2026, 5, 9, 10, 0, 0, 0, time.UTC)
	p := podman.Pod{
		Name:    "lite-engine-iotready",
		Status:  "Running",
		Created: created,
		Labels: map[string]string{
			"podman-api/template": "lite-engine",
			"podman-api/slug":     "iotready",
		},
		Containers: []podman.Container{
			{
				Name:      "app",
				Image:     "localhost/lite-engine@sha256:abc",
				ImageTag:  "localhost/lite-engine:latest",
				Status:    "Running",
				StartedAt: created,
				Ports: []podman.PortMapping{
					{HostIP: "127.0.0.1", HostPort: 31001, ContainerPort: 30000, Protocol: "tcp"},
				},
				Env: map[string]string{"BASE_URL": "https://x.example", "AUTH_SECRET": "leak-me-not"},
			},
			{
				Name: "litestream", Image: "docker.io/litestream/litestream:latest",
				Status: "Running", StartedAt: created,
			},
		},
	}

	obs := Normalize(p, "lite-engine", "iotready", []podman.Volume{
		{Name: "lite-engine-iotready-data", SizeBytes: 100},
	})

	assert.Equal(t, "lite-engine", obs.Template)
	assert.Equal(t, "iotready", obs.Slug)
	assert.Equal(t, "Running", obs.Pod.Status)
	require.Len(t, obs.Containers, 2)
	assert.Equal(t, "app", obs.Containers[0].Name)
	assert.Equal(t, "localhost/lite-engine@sha256:abc", obs.Containers[0].Image)
	assert.Equal(t, "localhost/lite-engine:latest", obs.Containers[0].ImageTag)
	require.Len(t, obs.Containers[0].Ports, 1)
	assert.Equal(t, 31001, obs.Containers[0].Ports[0].HostPort)
	require.Len(t, obs.Volumes, 1)
	assert.Equal(t, "lite-engine-iotready-data", obs.Volumes[0].Name)

	// EnvSummary must NOT contain anything that looks like a secret.
	assert.Equal(t, "https://x.example", obs.EnvSummary["BASE_URL"])
	_, hasSecret := obs.EnvSummary["AUTH_SECRET"]
	assert.False(t, hasSecret, "AUTH_SECRET must be redacted from env_summary")
}

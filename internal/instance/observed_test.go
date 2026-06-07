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
		Name:    "postgres-demo",
		Status:  "Running",
		Created: created,
		Labels: map[string]string{
			"podman-api/template": "postgres",
			"podman-api/slug":     "demo",
		},
		Containers: []podman.Container{
			{
				Name:      "db",
				Image:     "docker.io/library/postgres@sha256:abc",
				ImageTag:  "docker.io/library/postgres:16",
				Status:    "Running",
				StartedAt: created,
				Ports: []podman.PortMapping{
					{HostIP: "127.0.0.1", HostPort: 31001, ContainerPort: 5432, Protocol: "tcp"},
				},
				Env: map[string]string{"POSTGRES_DB": "app", "POSTGRES_PASSWORD": "leak-me-not"},
			},
		},
	}

	obs := Normalize(p, "postgres", "demo", []podman.Volume{
		{Name: "postgres-demo-data", SizeBytes: 100},
	}, map[string]bool{"POSTGRES_PASSWORD": true})

	assert.Equal(t, "postgres", obs.Template)
	assert.Equal(t, "demo", obs.Slug)
	assert.Equal(t, "Running", obs.Pod.Status)
	require.Len(t, obs.Containers, 1)
	assert.Equal(t, "db", obs.Containers[0].Name)
	assert.Equal(t, "docker.io/library/postgres@sha256:abc", obs.Containers[0].Image)
	assert.Equal(t, "docker.io/library/postgres:16", obs.Containers[0].ImageTag)
	require.Len(t, obs.Containers[0].Ports, 1)
	assert.Equal(t, 31001, obs.Containers[0].Ports[0].HostPort)
	require.Len(t, obs.Volumes, 1)
	assert.Equal(t, "postgres-demo-data", obs.Volumes[0].Name)

	// EnvSummary must NOT contain anything that looks like a secret.
	assert.Equal(t, "app", obs.EnvSummary["POSTGRES_DB"])
	_, hasSecret := obs.EnvSummary["POSTGRES_PASSWORD"]
	assert.False(t, hasSecret, "POSTGRES_PASSWORD must be redacted from env_summary")
}

func TestNormalize_HealthPropagation(t *testing.T) {
	p := podman.Pod{
		Status: "Running",
		Containers: []podman.Container{
			{Name: "app", Image: "nginx", Status: "Running", Health: "healthy"},
			{Name: "sidecar", Image: "alpine", Status: "Running", Health: ""},
		},
	}
	obs := Normalize(p, "web", "s1", nil, nil)

	require.Len(t, obs.Containers, 2)
	assert.Equal(t, "healthy", obs.Containers[0].Health)
	assert.Equal(t, "", obs.Containers[1].Health)
}

func TestNormalize_ReadyAggregation(t *testing.T) {
	tests := []struct {
		name       string
		containers []podman.Container
		wantReady  bool
	}{
		{
			"all healthy",
			[]podman.Container{
				{Status: "Running", Health: "healthy"},
				{Status: "Running", Health: "healthy"},
			},
			true,
		},
		{
			"one still starting",
			[]podman.Container{
				{Status: "Running", Health: "healthy"},
				{Status: "Running", Health: "starting"},
			},
			false,
		},
		{
			"one unhealthy",
			[]podman.Container{{Status: "Running", Health: "unhealthy"}},
			false,
		},
		{
			"no healthchecks declared — ready when Running",
			[]podman.Container{
				{Status: "Running"},
				{Status: "Running"},
			},
			true,
		},
		{
			"mixed declared and undeclared — only declared gates Ready",
			[]podman.Container{
				{Status: "Running", Health: "healthy"},
				{Status: "Running"},
			},
			true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := podman.Pod{Status: "Running", Containers: tt.containers}
			obs := Normalize(p, "web", "s1", nil, nil)
			assert.Equal(t, tt.wantReady, obs.Ready)
		})
	}
}

package instance

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/iotready/podman-api/internal/podman"
	"github.com/iotready/podman-api/internal/render"
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
	}, map[string]bool{"POSTGRES_PASSWORD": true}, nil)

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
	obs := Normalize(p, "web", "s1", nil, nil, nil)

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
			obs := Normalize(p, "web", "s1", nil, nil, nil)
			assert.Equal(t, tt.wantReady, obs.Ready)
		})
	}
}

// An env var whose VALUE matches a known instance secret is dropped even when
// its NAME is not in the template-derived set — this is the injector case from
// #198, where the secretKeyRef lives only in the post-injection manifest.
func TestNormalize_RedactsByValue(t *testing.T) {
	p := podman.Pod{
		Status: "Running",
		Containers: []podman.Container{
			{
				Name:   "vpn",
				Status: "Running",
				Env:    map[string]string{"VPN_PSK": "s3cr3t-psk", "VPN_ENDPOINT": "vpn.example.com"},
			},
		},
	}

	obs := Normalize(p, "erp", "acme", nil, nil, map[string]bool{"s3cr3t-psk": true})

	_, leaked := obs.EnvSummary["VPN_PSK"]
	assert.False(t, leaked, "an env var whose value is a known secret must be redacted")
	assert.Equal(t, "vpn.example.com", obs.EnvSummary["VPN_ENDPOINT"], "unrelated env vars survive")
}

// An empty string must never be treated as a secret value: it would blank every
// legitimately-empty env var in the summary.
func TestNormalize_EmptySecretValueDoesNotBlankEnv(t *testing.T) {
	p := podman.Pod{
		Status: "Running",
		Containers: []podman.Container{
			{Name: "app", Status: "Running", Env: map[string]string{"OPTIONAL_FLAG": ""}},
		},
	}

	obs := Normalize(p, "web", "s1", nil, nil, map[string]bool{"": true})

	v, present := obs.EnvSummary["OPTIONAL_FLAG"]
	assert.True(t, present, "an empty env var is not a secret")
	assert.Equal(t, "", v)
}

// PublicParameters is the redaction gate for Observed.Parameters (#200): a
// parameter a template declared `secret: true` is dropped by name, and any
// string parameter whose value happens to equal one of the instance's secrets
// is dropped by value.
func TestPublicParameters_RedactsSecretBearingEntries(t *testing.T) {
	m := render.Meta{Parameters: []render.ParamDef{
		{Name: "image", Type: "string"},
		{Name: "admin_token", Type: "string", Secret: true},
	}}
	params := map[string]any{
		"image":       "i:1",
		"admin_token": "declared-secret",
		"smuggled":    "sealed-value",
		"port":        5432,
		"empty":       "",
	}

	out := PublicParameters(m, params, map[string]bool{"sealed-value": true, "": true})

	assert.Equal(t, "i:1", out["image"])
	assert.Equal(t, 5432, out["port"])
	assert.Equal(t, "", out["empty"], "an empty value never matches a secret")
	assert.NotContains(t, out, "admin_token", "a secret-declared parameter is dropped by name")
	assert.NotContains(t, out, "smuggled", "a parameter carrying a known secret value is dropped")
}

// Nothing to report yields nil, so the JSON field is absent rather than null.
func TestPublicParameters_EmptyIsNil(t *testing.T) {
	assert.Nil(t, PublicParameters(render.Meta{}, nil, nil))
	m := render.Meta{Parameters: []render.ParamDef{{Name: "tok", Secret: true}}}
	assert.Nil(t, PublicParameters(m, map[string]any{"tok": "x"}, nil))
}

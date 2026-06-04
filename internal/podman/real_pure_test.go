package podman

import (
	"errors"
	"testing"
	"time"

	"github.com/containers/podman/v5/libpod/define"
	"github.com/containers/podman/v5/pkg/domain/entities"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestURIFor_UnknownHost(t *testing.T) {
	c, err := NewReal(nil)
	require.NoError(t, err)

	_, err = c.URIFor("nope")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unknown host")
}

func TestSplitPortKey(t *testing.T) {
	cases := []struct {
		key       string
		wantPort  int
		wantProto string
	}{
		{"5432/tcp", 5432, "tcp"},
		{"53/udp", 53, "udp"},
		{"8080", 8080, "tcp"}, // no protocol -> defaults to tcp
		{"", 0, "tcp"},        // empty -> zero port, tcp
		{"abc/tcp", 0, "tcp"}, // non-numeric port -> 0 (Atoi error swallowed)
		{"5432/", 5432, ""},   // trailing slash -> empty protocol preserved
		{"9/sctp", 9, "sctp"},
	}
	for _, c := range cases {
		t.Run(c.key, func(t *testing.T) {
			port, proto := splitPortKey(c.key)
			assert.Equal(t, c.wantPort, port)
			assert.Equal(t, c.wantProto, proto)
		})
	}
}

func TestIsNotFound(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want bool
	}{
		{"nil", nil, false},
		{"no such pod", errors.New("no such pod foo"), true},
		{"no such container", errors.New("no such container bar"), true},
		{"no such secret", errors.New("no such secret baz"), true},
		{"no such volume", errors.New("no such volume qux"), true},
		{"generic not found", errors.New("container quux not found"), true},
		{"mixed case", errors.New("No Such Pod FOO"), true}, // matched case-insensitively
		{"unrelated", errors.New("connection refused"), false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			assert.Equal(t, c.want, isNotFound(c.err))
		})
	}
}

func TestMapNotFound(t *testing.T) {
	// A not-found error is normalised to the sentinel ErrNotFound.
	require.ErrorIs(t, mapNotFound(errors.New("no such pod x")), ErrNotFound)

	// An unrelated error is passed through untouched.
	other := errors.New("connection refused")
	assert.Equal(t, other, mapNotFound(other))

	// nil stays nil.
	assert.NoError(t, mapNotFound(nil))
}

func TestEnrichContainer_Health(t *testing.T) {
	t.Run("healthcheck status copied", func(t *testing.T) {
		var c Container
		enrichContainer(&c, &define.InspectContainerData{
			State: &define.InspectContainerState{
				Health: &define.HealthCheckResults{Status: "healthy"},
			},
		})
		if c.Health != "healthy" {
			t.Fatalf("Health = %q, want %q", c.Health, "healthy")
		}
	})

	t.Run("no healthcheck leaves Health empty", func(t *testing.T) {
		var c Container
		enrichContainer(&c, &define.InspectContainerData{State: &define.InspectContainerState{}})
		if c.Health != "" {
			t.Fatalf("Health = %q, want empty", c.Health)
		}
	})
}

func TestPodFromInspect(t *testing.T) {
	created := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	rep := &entities.PodInspectReport{InspectPodData: &define.InspectPodData{
		ID:      "pod1",
		Name:    "postgres-demo",
		State:   "Running",
		Labels:  map[string]string{"podman-api/template": "postgres"},
		Created: created,
		Containers: []define.InspectPodContainerInfo{
			{ID: "c1", Name: "db", State: "running"},
			{ID: "c2", Name: "infra", State: "running"},
		},
	}}

	got := podFromInspect(rep)
	assert.Equal(t, "pod1", got.ID)
	assert.Equal(t, "postgres-demo", got.Name)
	assert.Equal(t, "Running", got.Status)
	assert.Equal(t, map[string]string{"podman-api/template": "postgres"}, got.Labels)
	assert.Equal(t, created, got.Created)
	require.Len(t, got.Containers, 2)
	assert.Equal(t, Container{ID: "c1", Name: "db", Status: "running"}, got.Containers[0])
	assert.Equal(t, "infra", got.Containers[1].Name)
}

func TestPodFromInspect_ZeroCreatedLeftUnset(t *testing.T) {
	rep := &entities.PodInspectReport{InspectPodData: &define.InspectPodData{
		ID: "pod1", Name: "p", State: "Exited",
	}}
	got := podFromInspect(rep)
	assert.True(t, got.Created.IsZero(), "zero Created must not be populated")
	assert.Empty(t, got.Containers)
}

func TestPodFromList(t *testing.T) {
	created := time.Date(2026, 6, 2, 12, 0, 0, 0, time.UTC)
	rep := &entities.ListPodsReport{
		Id:      "pod9",
		Name:    "redis-demo",
		Status:  "Degraded",
		Labels:  map[string]string{"env": "test"},
		Created: created,
		Containers: []*entities.ListPodContainer{
			{Id: "c1", Names: "cache", Status: "running"},
		},
	}

	got := podFromList(rep)
	assert.Equal(t, "pod9", got.ID)
	assert.Equal(t, "redis-demo", got.Name)
	assert.Equal(t, "Degraded", got.Status)
	assert.Equal(t, map[string]string{"env": "test"}, got.Labels)
	assert.Equal(t, created, got.Created)
	require.Len(t, got.Containers, 1)
	assert.Equal(t, Container{ID: "c1", Name: "cache", Status: "running"}, got.Containers[0])
}

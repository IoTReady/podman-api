package instance

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/iotready/podman-api/internal/config"
	"github.com/iotready/podman-api/internal/podman"
	"github.com/iotready/podman-api/internal/podman/fake"
)

func readySvc(t *testing.T, f *fake.Fake) *Service {
	t.Helper()
	hosts := []config.Host{{ID: "h1", Addr: "unix", Socket: "/x"}}
	svc := NewService(f, hosts)
	svc.SetStore(seedStore(t, webTemplate()))
	return svc
}

func TestWaitReady_NilWhenReady(t *testing.T) {
	defer setVerifyKnobs(50*time.Millisecond, 5*time.Millisecond)()
	f := fake.New()
	f.AddPod("h1", podman.Pod{Name: "web-ok", Status: "Running",
		Containers: []podman.Container{{Status: "Running", Health: "healthy"}}})
	require.NoError(t, readySvc(t, f).waitReady(context.Background(), "h1", "web", "ok", 50*time.Millisecond))
}

func TestWaitReady_TimeoutSentinel(t *testing.T) {
	defer setVerifyKnobs(50*time.Millisecond, 5*time.Millisecond)()
	f := fake.New()
	f.AddPod("h1", podman.Pod{Name: "web-bad", Status: "Running",
		Containers: []podman.Container{{Status: "Running", Health: "starting"}}})
	err := readySvc(t, f).waitReady(context.Background(), "h1", "web", "bad", 50*time.Millisecond)
	require.Error(t, err)
	assert.True(t, errors.Is(err, errReadyTimeout), "expected errReadyTimeout, got %v", err)
}

func TestWaitReady_ContextCancel(t *testing.T) {
	defer setVerifyKnobs(200*time.Millisecond, 5*time.Millisecond)()
	f := fake.New()
	f.AddPod("h1", podman.Pod{Name: "web-slow", Status: "Running",
		Containers: []podman.Container{{Status: "Running", Health: "starting"}}})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err := readySvc(t, f).waitReady(ctx, "h1", "web", "slow", 200*time.Millisecond)
	require.Error(t, err)
	assert.ErrorIs(t, err, context.Canceled)
}

func TestWaitReady_ZeroTimeout(t *testing.T) {
	// timeout=0 means disabled: must return nil immediately without polling
	f := fake.New() // no pods added — any poll would fail
	require.NoError(t, readySvc(t, f).waitReady(context.Background(), "h1", "web", "x", 0))
}

func TestWaitReady_NoHealthcheck(t *testing.T) {
	// Container with no declared healthcheck (Health=="") is ready when Running
	defer setVerifyKnobs(50*time.Millisecond, 5*time.Millisecond)()
	f := fake.New()
	f.AddPod("h1", podman.Pod{Name: "web-nohc", Status: "Running",
		Containers: []podman.Container{{Status: "Running"}}}) // Health==""
	require.NoError(t, readySvc(t, f).waitReady(context.Background(), "h1", "web", "nohc", 50*time.Millisecond))
}

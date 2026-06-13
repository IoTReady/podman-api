package instance

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/iotready/podman-api/internal/config"
	"github.com/iotready/podman-api/internal/podman"
	"github.com/iotready/podman-api/internal/podman/fake"
)

// errAfterNClient wraps a podman.Client and injects an error once the call
// counter passes N. Lets tests exercise error-freeze logic in waitReady.
type errAfterNClient struct {
	podman.Client
	after int64
	err   error
	calls atomic.Int64
}

func (c *errAfterNClient) PodInspect(ctx context.Context, host, name string) (podman.Pod, error) {
	if c.calls.Add(1) > c.after {
		return podman.Pod{}, c.err
	}
	return c.Client.PodInspect(ctx, host, name)
}

// errOnceClient wraps a podman.Client and injects a single error on exactly
// one preset call, then reverts to the underlying client. Lets tests verify
// that a single transient error mid-accumulation does not reset the stable
// counter (#145).
type errOnceClient struct {
	podman.Client
	at    int64
	err   error
	calls atomic.Int64
}

func (c *errOnceClient) PodInspect(ctx context.Context, host, name string) (podman.Pod, error) {
	if c.calls.Add(1) == c.at {
		return podman.Pod{}, c.err
	}
	return c.Client.PodInspect(ctx, host, name)
}

// altErrorClient wraps a podman.Client and injects an error on every other
// call (odd-numbered calls succeed, even-numbered calls fail). Lets tests
// verify that alternating transient errors don't reset the stable counter
// in waitReady (#145).
type altErrorClient struct {
	podman.Client
	err   error
	calls atomic.Int64
}

func (c *altErrorClient) PodInspect(ctx context.Context, host, name string) (podman.Pod, error) {
	if c.calls.Add(1)%2 == 0 {
		return podman.Pod{}, c.err
	}
	return c.Client.PodInspect(ctx, host, name)
}

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
	require.NoError(t, readySvc(t, f).waitReady(context.Background(), "h1", "web", "ok", 50*time.Millisecond, 1))
}

func TestWaitReady_TimeoutSentinel(t *testing.T) {
	defer setVerifyKnobs(50*time.Millisecond, 5*time.Millisecond)()
	f := fake.New()
	f.AddPod("h1", podman.Pod{Name: "web-bad", Status: "Running",
		Containers: []podman.Container{{Status: "Running", Health: "starting"}}})
	err := readySvc(t, f).waitReady(context.Background(), "h1", "web", "bad", 50*time.Millisecond, 1)
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
	err := readySvc(t, f).waitReady(ctx, "h1", "web", "slow", 200*time.Millisecond, 1)
	require.Error(t, err)
	assert.ErrorIs(t, err, context.Canceled)
}

func TestWaitReady_ZeroTimeout(t *testing.T) {
	// timeout=0 means disabled: must return nil immediately without polling
	f := fake.New() // no pods added — any poll would fail
	require.NoError(t, readySvc(t, f).waitReady(context.Background(), "h1", "web", "x", 0, 1))
}

func TestWaitReady_NoHealthcheck(t *testing.T) {
	// Container with no declared healthcheck (Health=="") is ready when Running
	defer setVerifyKnobs(50*time.Millisecond, 5*time.Millisecond)()
	f := fake.New()
	f.AddPod("h1", podman.Pod{Name: "web-nohc", Status: "Running",
		Containers: []podman.Container{{Status: "Running"}}}) // Health==""
	require.NoError(t, readySvc(t, f).waitReady(context.Background(), "h1", "web", "nohc", 50*time.Millisecond, 1))
}

func TestWaitReady_ErrorHandling(t *testing.T) {
	defer setVerifyKnobs(200*time.Millisecond, 5*time.Millisecond)()
	f := fake.New()
	f.AddPod("h1", podman.Pod{Name: "web-x", Status: "Running",
		Containers: []podman.Container{{Status: "Running"}}})

	t.Run("persistent error after 2 ready polls prevents reaching stable count", func(t *testing.T) {
		// stableCount=3 so call 1 → stable=1, call 2 → stable=2, then
		// error on call 3+ doesn't reset (stays at 2) but never reaches 3.
		wc := &errAfterNClient{Client: f, after: 2, err: errors.New("host unreachable")}
		svc := readySvc(t, f)
		svc.client = wc
		err := svc.waitReady(context.Background(), "h1", "web", "x", 200*time.Millisecond, 3)
		require.Error(t, err)
		assert.ErrorIs(t, err, errReadyTimeout)
	})

	t.Run("single transient error blip does not reset stable counter", func(t *testing.T) {
		// stableCount=3. Error on call 3 only — counter does NOT reset (#145),
		// so call 4 succeeds and reaches stable=3 immediately.
		wc := &errOnceClient{Client: f, at: 3, err: errors.New("transient blip")}
		svc := readySvc(t, f)
		svc.client = wc
		err := svc.waitReady(context.Background(), "h1", "web", "x", 200*time.Millisecond, 3)
		require.NoError(t, err)
	})

	t.Run("transient errors don't cause timeout when pod stays ready", func(t *testing.T) {
		// stableCount=3 with a pattern of success, error, success, error, success.
		// Under old code, alternating errors resets counter to 0 every time → timeout.
		// Under new code, errors don't reset; 3 successes → stable=3 → success.
		p := &altErrorClient{Client: f, err: errors.New("ssh blip")}
		svc := readySvc(t, f)
		svc.client = p
		require.NoError(t, svc.waitReady(context.Background(), "h1", "web", "x", 200*time.Millisecond, 3))
	})
}

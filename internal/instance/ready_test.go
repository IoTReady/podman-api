package instance

import (
	"context"
	"errors"
	"fmt"
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
	require.NoError(t, readySvc(t, f).waitReady(context.Background(), "h1", "web", "ok", readyOpts{timeout: 50 * time.Millisecond, stableCount: 1}))
}

func TestWaitReady_TimeoutSentinel(t *testing.T) {
	defer setVerifyKnobs(50*time.Millisecond, 5*time.Millisecond)()
	f := fake.New()
	f.AddPod("h1", podman.Pod{Name: "web-bad", Status: "Running",
		Containers: []podman.Container{{Status: "Running", Health: "starting"}}})
	err := readySvc(t, f).waitReady(context.Background(), "h1", "web", "bad", readyOpts{timeout: 50 * time.Millisecond, stableCount: 1})
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
	err := readySvc(t, f).waitReady(ctx, "h1", "web", "slow", readyOpts{timeout: 200 * time.Millisecond, stableCount: 1})
	require.Error(t, err)
	assert.ErrorIs(t, err, context.Canceled)
}

func TestWaitReady_ZeroTimeout(t *testing.T) {
	// timeout=0 means disabled: must return nil immediately without polling
	f := fake.New() // no pods added — any poll would fail
	require.NoError(t, readySvc(t, f).waitReady(context.Background(), "h1", "web", "x", readyOpts{timeout: 0, stableCount: 1}))
}

func TestWaitReady_NoHealthcheck(t *testing.T) {
	// Container with no declared healthcheck (Health=="") is ready when Running
	defer setVerifyKnobs(50*time.Millisecond, 5*time.Millisecond)()
	f := fake.New()
	f.AddPod("h1", podman.Pod{Name: "web-nohc", Status: "Running",
		Containers: []podman.Container{{Status: "Running"}}}) // Health==""
	require.NoError(t, readySvc(t, f).waitReady(context.Background(), "h1", "web", "nohc", readyOpts{timeout: 50 * time.Millisecond, stableCount: 1}))
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
		err := svc.waitReady(context.Background(), "h1", "web", "x", readyOpts{timeout: 200 * time.Millisecond, stableCount: 3})
		require.Error(t, err)
		assert.ErrorIs(t, err, errReadyTimeout)
	})

	t.Run("single transient error blip does not reset stable counter", func(t *testing.T) {
		// stableCount=3. Error on call 3 only — counter does NOT reset (#145),
		// so call 4 succeeds and reaches stable=3 immediately.
		wc := &errOnceClient{Client: f, at: 3, err: errors.New("transient blip")}
		svc := readySvc(t, f)
		svc.client = wc
		err := svc.waitReady(context.Background(), "h1", "web", "x", readyOpts{timeout: 200 * time.Millisecond, stableCount: 3})
		require.NoError(t, err)
	})

	t.Run("transient errors don't cause timeout when pod stays ready", func(t *testing.T) {
		// stableCount=3 with a pattern of success, error, success, error, success.
		// Under old code, alternating errors resets counter to 0 every time → timeout.
		// Under new code, errors don't reset; 3 successes → stable=3 → success.
		wc := &altErrorClient{Client: f, err: errors.New("ssh blip")}
		svc := readySvc(t, f)
		svc.client = wc
		require.NoError(t, svc.waitReady(context.Background(), "h1", "web", "x", readyOpts{timeout: 200 * time.Millisecond, stableCount: 3}))
	})
}

// --- #196: readiness budget vs declared healthcheck start period ---

// startingPod builds a pod whose single container declares a healthcheck and is
// still inside its start period.
func startingPod(name string, started time.Time, startPeriod, interval time.Duration) podman.Pod {
	return podman.Pod{Name: name, Status: "Running", Containers: []podman.Container{{
		Name: "app", Status: "Running", Health: "starting",
		HealthStartPeriod: startPeriod, HealthInterval: interval, StartedAt: started,
	}}}
}

func TestStartupDeadline(t *testing.T) {
	start := time.Now()

	t.Run("no healthcheck declared yields zero time", func(t *testing.T) {
		p := podman.Pod{Containers: []podman.Container{{Status: "Running", StartedAt: start}}}
		assert.True(t, startupDeadline(p).IsZero(),
			"a pod with no declared healthcheck must not extend any deadline")
	})

	t.Run("start period plus one interval from container start", func(t *testing.T) {
		p := startingPod("web-x", start, 300*time.Second, 30*time.Second)
		assert.WithinDuration(t, start.Add(330*time.Second), startupDeadline(p), time.Second)
	})

	t.Run("takes the latest across containers", func(t *testing.T) {
		p := podman.Pod{Containers: []podman.Container{
			{Status: "Running", Health: "healthy", HealthStartPeriod: 10 * time.Second, HealthInterval: time.Second, StartedAt: start},
			{Status: "Running", Health: "starting", HealthStartPeriod: 300 * time.Second, HealthInterval: 30 * time.Second, StartedAt: start},
		}}
		assert.WithinDuration(t, start.Add(330*time.Second), startupDeadline(p), time.Second)
	})

	t.Run("zero StartedAt measures from now rather than the epoch", func(t *testing.T) {
		p := startingPod("web-x", time.Time{}, 300*time.Second, 30*time.Second)
		got := startupDeadline(p)
		require.False(t, got.IsZero())
		assert.True(t, got.After(time.Now().Add(300*time.Second)),
			"an unknown start time must extend forward, never resolve to the zero epoch (which would be in the past and fail instantly)")
	})
}

// The regression under #196: podman cannot report "healthy" before one check
// interval has elapsed, so a budget shorter than the declared start period fails
// a container that is behaving exactly as its own spec permits. With
// grantStartPeriod the wait must honour that spec.
func TestWaitReady_GrantStartPeriod(t *testing.T) {
	t.Run("without the grant, a short budget fails a still-starting container", func(t *testing.T) {
		defer setVerifyKnobs(50*time.Millisecond, 5*time.Millisecond)()
		f := fake.New()
		f.AddPod("h1", startingPod("web-strict", time.Now(), time.Hour, time.Minute))
		err := readySvc(t, f).waitReady(context.Background(), "h1", "web", "strict",
			readyOpts{timeout: 50 * time.Millisecond, stableCount: 1})
		assert.ErrorIs(t, err, errReadyTimeout)
	})

	t.Run("with the grant, the deadline extends past the base budget", func(t *testing.T) {
		defer setVerifyKnobs(50*time.Millisecond, 5*time.Millisecond)()
		f := fake.New()
		// Start period far in the future: the wait must still be running well
		// after the 20ms base budget would have expired.
		f.AddPod("h1", startingPod("web-grace", time.Now(), time.Hour, time.Minute))
		ctx, cancel := context.WithTimeout(context.Background(), 120*time.Millisecond)
		defer cancel()
		err := readySvc(t, f).waitReady(ctx, "h1", "web", "grace",
			readyOpts{timeout: 20 * time.Millisecond, stableCount: 1, grantStartPeriod: true})
		// It ran until ctx expired instead of returning errReadyTimeout at 20ms.
		assert.ErrorIs(t, err, context.DeadlineExceeded)
		assert.NotErrorIs(t, err, errReadyTimeout)
	})

	t.Run("the grant does not rescue a container with no healthcheck", func(t *testing.T) {
		defer setVerifyKnobs(50*time.Millisecond, 5*time.Millisecond)()
		f := fake.New()
		f.AddPod("h1", podman.Pod{Name: "web-nohc2", Status: "Created",
			Containers: []podman.Container{{Status: "Created"}}})
		err := readySvc(t, f).waitReady(context.Background(), "h1", "web", "nohc2",
			readyOpts{timeout: 30 * time.Millisecond, stableCount: 1, grantStartPeriod: true})
		assert.ErrorIs(t, err, errReadyTimeout,
			"no declared start period means the configured budget still governs")
	})

	t.Run("an expired start period does not extend the deadline", func(t *testing.T) {
		defer setVerifyKnobs(50*time.Millisecond, 5*time.Millisecond)()
		f := fake.New()
		// Started long ago; its start period elapsed well before now.
		f.AddPod("h1", startingPod("web-old", time.Now().Add(-time.Hour), time.Second, time.Second))
		err := readySvc(t, f).waitReady(context.Background(), "h1", "web", "old",
			readyOpts{timeout: 30 * time.Millisecond, stableCount: 1, grantStartPeriod: true})
		assert.ErrorIs(t, err, errReadyTimeout,
			"a container past its grace must not get an unbounded extension")
	})
}

func TestWaitReady_StillStartingIsDistinctFromFailure(t *testing.T) {
	defer setVerifyKnobs(50*time.Millisecond, 5*time.Millisecond)()

	t.Run("starting wraps errStillStarting", func(t *testing.T) {
		f := fake.New()
		f.AddPod("h1", startingPod("web-s", time.Now().Add(-time.Hour), time.Second, time.Second))
		err := readySvc(t, f).waitReady(context.Background(), "h1", "web", "s",
			readyOpts{timeout: 20 * time.Millisecond, stableCount: 1})
		assert.ErrorIs(t, err, errReadyTimeout)
		assert.ErrorIs(t, err, errStillStarting)
	})

	t.Run("unhealthy does not wrap errStillStarting", func(t *testing.T) {
		f := fake.New()
		f.AddPod("h1", podman.Pod{Name: "web-u", Status: "Running",
			Containers: []podman.Container{{Status: "Running", Health: "unhealthy"}}})
		err := readySvc(t, f).waitReady(context.Background(), "h1", "web", "u",
			readyOpts{timeout: 20 * time.Millisecond, stableCount: 1})
		assert.ErrorIs(t, err, errReadyTimeout)
		assert.NotErrorIs(t, err, errStillStarting,
			"an unhealthy container has actually failed and must not be excused as initialising")
	})
}

func TestReadinessWarning(t *testing.T) {
	assert.Empty(t, readinessWarning(nil))
	assert.Contains(t, readinessWarning(fmt.Errorf("%w: %w", errReadyTimeout, errStillStarting)),
		"still inside its healthcheck start period")
	assert.Contains(t, readinessWarning(errReadyTimeout), "readiness timeout")
	assert.Empty(t, readinessWarning(errors.New("some unrelated error")),
		"an unrelated error is surfaced by the caller, not as a readiness warning")
}

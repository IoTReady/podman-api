package podman

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestWaitForCompletion_VanishedContainer_NeverSilentSuccess is the
// regression test for the fail-open bug: a container libpod listed via
// inspectPod but that has since vanished (isNotFound on inspectContainer)
// must never be reported as a clean (0, nil) success. It genuinely is not
// known to have exited zero — it might have crashed, been reaped by an
// external `podman rm`, or raced kube-play teardown.
func TestWaitForCompletion_VanishedContainer_NeverSilentSuccess(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	code, err := waitForCompletion(ctx, "gc-job",
		func(ctx context.Context, name string) ([]string, error) {
			return []string{"gone-container"}, nil
		},
		func(ctx context.Context, id string) (bool, int, error) {
			return false, 0, errors.New("no such container " + id)
		},
	)
	require.Error(t, err, "a vanished container must never be reported as a successful exit")
	assert.Equal(t, 0, code)
	assert.False(t, errors.Is(err, ErrWaitTimeout), "vanished-container is not a timeout")
}

// TestWaitForCompletion_CallerTimeoutIsAuthoritative proves the wait's
// deadline comes from ctx as the caller built it, and that exceeding it
// surfaces as ErrWaitTimeout specifically (not a raw context.DeadlineExceeded
// a caller can't branch on the same way).
func TestWaitForCompletion_CallerTimeoutIsAuthoritative(t *testing.T) {
	restore := waitPollInterval
	waitPollInterval = 5 * time.Millisecond
	defer func() { waitPollInterval = restore }()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()

	code, err := waitForCompletion(ctx, "gc-job",
		func(ctx context.Context, name string) ([]string, error) {
			return []string{"still-running"}, nil
		},
		func(ctx context.Context, id string) (bool, int, error) {
			return true, 0, nil // never exits
		},
	)
	require.Error(t, err)
	assert.True(t, errors.Is(err, ErrWaitTimeout), "want ErrWaitTimeout, got %v", err)
	assert.Equal(t, 0, code)
}

// TestWaitForCompletion_PollsUntilExited proves the poll loop keeps going
// (rather than misreading a transiently-running container as vanished or
// erroring out) and returns the first non-zero exit code once every
// container has actually exited.
func TestWaitForCompletion_PollsUntilExited(t *testing.T) {
	restore := waitPollInterval
	waitPollInterval = 5 * time.Millisecond
	defer func() { waitPollInterval = restore }()

	polls := 0
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	code, err := waitForCompletion(ctx, "gc-job",
		func(ctx context.Context, name string) ([]string, error) {
			return []string{"a", "b"}, nil
		},
		func(ctx context.Context, id string) (bool, int, error) {
			if id == "a" {
				return false, 3, nil
			}
			// "b" exits only after the second poll.
			polls++
			if polls < 2 {
				return true, 0, nil
			}
			return false, 5, nil
		},
	)
	require.NoError(t, err)
	assert.Equal(t, 3, code, "first non-zero in container order")
}

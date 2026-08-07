package fake

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/iotready/podman-api/internal/podman"
)

func TestFake_WaitForPodCompletion_AllExitedZero(t *testing.T) {
	f := New()
	f.AddPod("h1", podman.Pod{
		Name: "gc-job",
		Containers: []podman.Container{
			{Name: "gc", Exited: true, ExitCode: 0},
		},
	})

	code, err := f.WaitForPodCompletion(context.Background(), "h1", "gc-job", time.Minute)
	require.NoError(t, err)
	assert.Equal(t, 0, code)
}

func TestFake_WaitForPodCompletion_FirstNonZero(t *testing.T) {
	f := New()
	f.AddPod("h1", podman.Pod{
		Name: "gc-job",
		Containers: []podman.Container{
			{Name: "sidecar", Exited: true, ExitCode: 0},
			{Name: "gc", Exited: true, ExitCode: 7},
			{Name: "other", Exited: true, ExitCode: 2},
		},
	})

	code, err := f.WaitForPodCompletion(context.Background(), "h1", "gc-job", time.Minute)
	require.NoError(t, err)
	assert.Equal(t, 7, code, "first non-zero exit code among containers")
}

func TestFake_WaitForPodCompletion_TimesOutWhileStillRunning(t *testing.T) {
	f := New()
	f.AddPod("h1", podman.Pod{
		Name: "gc-job",
		Containers: []podman.Container{
			{Name: "gc", Exited: false},
		},
	})

	code, err := f.WaitForPodCompletion(context.Background(), "h1", "gc-job", time.Millisecond)
	require.Error(t, err)
	assert.True(t, errors.Is(err, podman.ErrWaitTimeout), "want ErrWaitTimeout, got %v", err)
	assert.Equal(t, 0, code, "a timeout must not be reported as a zero exit code")
}

func TestFake_WaitForPodCompletion_PodNotFound(t *testing.T) {
	f := New()
	_, err := f.WaitForPodCompletion(context.Background(), "h1", "nope", time.Minute)
	require.Error(t, err)
	assert.True(t, errors.Is(err, podman.ErrNotFound))
}

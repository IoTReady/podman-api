package fake

import (
	"context"
	"errors"
	"testing"

	"github.com/iotready/podman-api/internal/podman"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestFakeHostInfo_ReturnsSetValue(t *testing.T) {
	f := New()
	cpu := 42.0
	f.HostInfoVal = podman.HostInfo{CPUs: 8, MemTotal: 100, MemFree: 25, MemUsedPct: 75, CPUPct: &cpu}

	got, err := f.HostInfo(context.Background(), "h1")
	require.NoError(t, err)
	assert.Equal(t, 8, got.CPUs)
	assert.Equal(t, 75.0, got.MemUsedPct)
	require.NotNil(t, got.CPUPct)
	assert.Equal(t, 42.0, *got.CPUPct)
}

func TestFakeHostInfo_Error(t *testing.T) {
	f := New()
	f.HostInfoErr = errors.New("boom")
	_, err := f.HostInfo(context.Background(), "h1")
	assert.Error(t, err)
}

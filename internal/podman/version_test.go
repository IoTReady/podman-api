package podman

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/iotready/podman-api/internal/config"
)

func TestCheckVersion(t *testing.T) {
	cases := []struct {
		name    string
		version string
		wantErr bool
	}{
		{"below floor", "5.4.2", true},
		{"at floor", "5.6.0", false},
		{"above floor", "5.8.2", false},
		{"dev suffix above floor", "5.8.2-dev", false},
		{"v prefix tolerated", "v5.7.0", false},
		{"pre-release of floor is below floor", "5.6.0-rc1", true},
		{"pre-release of floor is below floor (dev)", "5.6.0-dev", true},
		{"empty", "", true},
		{"garbage", "abc", true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			err := checkVersion("h1", c.version)
			if c.wantErr {
				require.Error(t, err)
				assert.True(t, errors.Is(err, ErrHostVersionUnsupported),
					"must wrap sentinel, got: %v", err)
				assert.Contains(t, err.Error(), `host "h1"`)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

// stubReal returns a Real with one host "h1" whose connection is pre-cached
// (no dial) and whose version probe is stubbed.
func stubReal(t *testing.T, probe func(context.Context) (string, error)) *Real {
	t.Helper()
	r, err := NewReal([]config.Host{{ID: "h1", Addr: "unix", Socket: "/x"}})
	require.NoError(t, err)
	// Pre-seed before any concurrent use; no lock needed.
	r.ctx["h1"] = context.Background()
	r.versionProbe = probe
	return r
}

func TestOpCtxFor_RefusesOldVersion(t *testing.T) {
	r := stubReal(t, func(context.Context) (string, error) { return "5.4.2", nil })
	_, err := r.opCtxFor(context.Background(), "h1")
	require.Error(t, err)
	assert.True(t, errors.Is(err, ErrHostVersionUnsupported))
}

func TestOpCtxFor_VerifiesOncePerHost(t *testing.T) {
	calls := 0
	r := stubReal(t, func(context.Context) (string, error) { calls++; return "5.8.2", nil })
	for i := 0; i < 3; i++ {
		_, err := r.opCtxFor(context.Background(), "h1")
		require.NoError(t, err)
	}
	assert.Equal(t, 1, calls, "version probed once, then cached")
}

func TestOpCtxFor_InPlaceUpgradeRecovers(t *testing.T) {
	calls := 0
	r := stubReal(t, func(context.Context) (string, error) {
		calls++
		if calls == 1 {
			return "5.4.2", nil // old at first
		}
		return "5.8.2", nil // host upgraded in place
	})
	_, err := r.opCtxFor(context.Background(), "h1")
	require.Error(t, err)
	_, err = r.opCtxFor(context.Background(), "h1")
	require.NoError(t, err, "no daemon restart needed after host upgrade")
	assert.Equal(t, 2, calls, "failed check is not cached; probe re-ran")
}

func TestOpCtxFor_ProbeErrorIsNotVerified(t *testing.T) {
	calls := 0
	r := stubReal(t, func(context.Context) (string, error) {
		calls++
		if calls == 1 {
			return "", errors.New("transient")
		}
		return "5.8.2", nil
	})
	_, err := r.opCtxFor(context.Background(), "h1")
	require.Error(t, err)
	assert.False(t, errors.Is(err, ErrHostVersionUnsupported),
		"transient probe failure is not a version verdict")
	_, err = r.opCtxFor(context.Background(), "h1")
	require.NoError(t, err)
}

func TestVersion_BypassesEnforcement(t *testing.T) {
	r := stubReal(t, func(context.Context) (string, error) { return "5.4.2", nil })
	v, err := r.Version(context.Background(), "h1")
	require.NoError(t, err, "diagnostics stay readable on unsupported hosts")
	assert.Equal(t, "5.4.2", v)
}

func TestPreflight_FatalOnReachableOldHost(t *testing.T) {
	r := stubReal(t, func(context.Context) (string, error) { return "5.4.2", nil })
	err := r.Preflight(context.Background())
	require.Error(t, err, "reachable host below floor must refuse boot")
	assert.True(t, errors.Is(err, ErrHostVersionUnsupported))
}

func TestPreflight_MarksVerified_NoReprobe(t *testing.T) {
	calls := 0
	r := stubReal(t, func(context.Context) (string, error) { calls++; return "5.8.2", nil })
	require.NoError(t, r.Preflight(context.Background()))
	_, err := r.opCtxFor(context.Background(), "h1")
	require.NoError(t, err)
	assert.Equal(t, 1, calls, "preflight pass is cached; first op does not re-probe")
}

func TestPreflight_ToleratesUnreachable(t *testing.T) {
	// No pre-seeded ctx: ctxFor dials the (nonexistent) unix socket and fails.
	r, err := NewReal([]config.Host{{ID: "h1", Addr: "unix", Socket: "/nonexistent/podman.sock"}})
	require.NoError(t, err)
	require.NoError(t, r.Preflight(context.Background()),
		"unreachable host is a warning, not a boot failure")
	r.mu.Lock()
	defer r.mu.Unlock()
	assert.False(t, r.verified["h1"], "unreachable host stays unverified")
}

func TestPreflight_TimeoutDefers(t *testing.T) {
	// Mutates a package-level var; safe only while this package's tests stay serial (no t.Parallel).
	old := preflightTimeout
	preflightTimeout = 50 * time.Millisecond
	defer func() { preflightTimeout = old }()

	r := stubReal(t, func(context.Context) (string, error) {
		time.Sleep(500 * time.Millisecond)
		return "5.4.2", nil
	})
	require.NoError(t, r.Preflight(context.Background()),
		"slow host is treated as unreachable at boot")
	r.mu.Lock()
	defer r.mu.Unlock()
	assert.False(t, r.verified["h1"])
}

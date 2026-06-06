package podman

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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

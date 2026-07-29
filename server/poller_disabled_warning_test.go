package server

import (
	"strings"
	"testing"
	"time"
)

// -container-stats and -volume-usage-interval are silently no-ops without the
// inventory poller: both collectors and StartVolumeUsage live inside the
// poller-enabled block. An operator who explicitly asked for them and got
// neither metrics nor an explanation has nothing to grep for. (#209 review)
func TestPollerDisabledMetricsWarning(t *testing.T) {
	const hour = time.Hour

	tests := []struct {
		name     string
		set      map[string]bool
		stats    bool
		interval time.Duration
		want     []string // substrings that must appear; empty means want ""
	}{
		{
			// The default deployment that has deliberately turned the poller off.
			// Neither flag was typed, so there is nothing to explain.
			name: "neither flag set is silent", set: map[string]bool{},
			stats: true, interval: hour,
		},
		{
			// The reviewer's exact scenario.
			name: "volume usage explicitly set", set: map[string]bool{"volume-usage-interval": true},
			stats: true, interval: 15 * time.Minute,
			want: []string{"-volume-usage-interval", "inventory poller is disabled", "-inventory-refresh-interval=0"},
		},
		{
			name: "container stats explicitly enabled", set: map[string]bool{"container-stats": true},
			stats: true, interval: hour,
			want: []string{"-container-stats", "inventory poller is disabled"},
		},
		{
			name:  "both explicitly asked for",
			set:   map[string]bool{"container-stats": true, "volume-usage-interval": true},
			stats: true, interval: 15 * time.Minute,
			want: []string{"-container-stats and -volume-usage-interval"},
		},
		{
			// Explicit disables. The operator wants no metrics and is getting
			// exactly that — warning here would be noise.
			name:  "explicitly disabled flags are silent",
			set:   map[string]bool{"container-stats": true, "volume-usage-interval": true},
			stats: false, interval: 0,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := pollerDisabledMetricsWarning(tc.set, tc.stats, tc.interval)
			if len(tc.want) == 0 {
				if got != "" {
					t.Fatalf("want no warning, got %q", got)
				}
				return
			}
			for _, sub := range tc.want {
				if !strings.Contains(got, sub) {
					t.Errorf("warning %q missing %q", got, sub)
				}
			}
		})
	}
}

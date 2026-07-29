package server

import (
	"strings"
	"testing"
	"time"
)

// The stats sample gets its own budget (#212), so a host's worst-case per-tick
// cost is -inventory-refresh-timeout + -container-stats-timeout. tick blocks the
// ticker, so once that sum reaches -inventory-refresh-interval a slow host
// stretches the whole fleet's poll cadence. Nothing structurally prevents an
// operator tuning into that state — this warning is what makes the invariant
// visible, and it must stay a warning: a deliberately long timeout should not
// refuse to start.
func TestStatsBudgetWarning(t *testing.T) {
	tests := []struct {
		name                       string
		interval, timeout, statsTO time.Duration
		statsEnabled               bool
		want                       []string // substrings that must appear; empty means want ""
	}{
		{
			// The shipped defaults: 20s + 5s < 30s.
			name: "defaults are silent", interval: 30 * time.Second,
			timeout: 20 * time.Second, statsTO: 5 * time.Second, statsEnabled: true,
		},
		{
			// The exact boundary is already too tight: at equality one host's
			// worst case consumes a whole interval.
			name: "sum equal to the interval warns", interval: 30 * time.Second,
			timeout: 25 * time.Second, statsTO: 5 * time.Second, statsEnabled: true,
			want: []string{
				"-inventory-refresh-timeout", "-container-stats-timeout",
				"-inventory-refresh-interval", "25s", "5s", "30s",
			},
		},
		{
			// An operator who raised the refresh timeout without looking at the
			// interval — the tuning the old shared budget made impossible.
			name: "sum over the interval warns", interval: 30 * time.Second,
			timeout: 45 * time.Second, statsTO: 5 * time.Second, statsEnabled: true,
			want: []string{"-inventory-refresh-timeout", "poll cadence"},
		},
		{
			// No sampler, no second budget to spend: warning here would be noise.
			name: "sampler disabled is silent", interval: 30 * time.Second,
			timeout: 45 * time.Second, statsTO: 5 * time.Second, statsEnabled: false,
		},
		{
			// The poller is off entirely; pollerDisabledMetricsWarning owns that
			// case and nothing ticks, so there is no cadence to stretch.
			name: "poller disabled is silent", interval: 0,
			timeout: 45 * time.Second, statsTO: 5 * time.Second, statsEnabled: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := statsBudgetWarning(tc.interval, tc.timeout, tc.statsTO, tc.statsEnabled)
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

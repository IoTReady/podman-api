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
			// A bare -container-stats-timeout=0 does NOT mean zero budget: the
			// poller spends its 5s default. A raw-value check computes 28+0=28,
			// stays silent, and the poller then spends 33s against a 30s
			// interval — the hole this whole function exists to close. The
			// message must name the effective 5s, not the flag's literal 0.
			name: "zero stats timeout is normalised to the default", interval: 30 * time.Second,
			timeout: 28 * time.Second, statsTO: 0, statsEnabled: true,
			want: []string{"-container-stats-timeout (5s)", "33s", "30s"},
		},
		{
			// flag.Duration parses -1s happily; same normalisation applies.
			name: "negative stats timeout is normalised to the default", interval: 30 * time.Second,
			timeout: 28 * time.Second, statsTO: -1 * time.Second, statsEnabled: true,
			want: []string{"-container-stats-timeout (5s)", "33s"},
		},
		{
			// The normalisation must not manufacture a warning where the
			// effective sum genuinely fits: 10s + 5s < 30s.
			name: "zero stats timeout still silent when the default fits", interval: 30 * time.Second,
			timeout: 10 * time.Second, statsTO: 0, statsEnabled: true,
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

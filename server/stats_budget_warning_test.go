package server

import (
	"strings"
	"testing"
	"time"
)

// The stats sample gets its own budget (#212) and the boot-reboot probe gets
// its own budget too (#231), so a host's worst-case per-tick cost is
// -inventory-refresh-timeout + boot-probe-timeout + -container-stats-timeout
// (the last only when the sampler is enabled). tick blocks the ticker, so once
// that sum reaches -inventory-refresh-interval a slow host stretches the whole
// fleet's poll cadence. Nothing structurally prevents an operator tuning into
// that state — this warning is what makes the invariant visible, and it must
// stay a warning: a deliberately long timeout should not refuse to start.
func TestStatsBudgetWarning(t *testing.T) {
	tests := []struct {
		name                               string
		interval, timeout, statsTO, bootTO time.Duration
		statsEnabled                       bool
		want                               []string // substrings that must appear; empty means want ""
	}{
		{
			// #231 review finding #2: the shipped defaults (20s refresh + 5s
			// stats) used to look fine against a 30s interval (25s<30s), but
			// Boot: svc is wired unconditionally and spends a further 5s
			// default boot-probe budget every tick — the true per-host cost is
			// 30s, AT the interval, which must now warn.
			name:     "shipped defaults are actually at budget once the boot probe is counted",
			interval: 30 * time.Second, timeout: 20 * time.Second, statsTO: 5 * time.Second,
			bootTO: 0, statsEnabled: true,
			want: []string{
				"-inventory-refresh-timeout", "boot-reboot-probe", "-container-stats-timeout",
				"-inventory-refresh-interval", "20s", "30s",
			},
		},
		{
			// Genuinely small budgets across all three terms fit comfortably.
			name:     "small budgets across all three terms are silent",
			interval: 30 * time.Second, timeout: 10 * time.Second, statsTO: 5 * time.Second,
			bootTO: 5 * time.Second, statsEnabled: true,
		},
		{
			// An operator who raised the refresh timeout without looking at the
			// interval — the tuning the old shared budget made impossible.
			name: "sum over the interval warns", interval: 30 * time.Second,
			timeout: 45 * time.Second, statsTO: 5 * time.Second, bootTO: 5 * time.Second, statsEnabled: true,
			want: []string{"-inventory-refresh-timeout", "poll cadence"},
		},
		{
			// A bare -container-stats-timeout=0 does NOT mean zero budget: the
			// poller spends its 5s default. A raw-value check computes 28+0=28,
			// stays silent, and the poller then spends 28+5(boot)+5(stats)=38s
			// against a 30s interval — the hole this whole function exists to
			// close. The message must name the effective 5s, not the flag's
			// literal 0.
			name: "zero stats timeout is normalised to the default", interval: 30 * time.Second,
			timeout: 28 * time.Second, statsTO: 0, bootTO: 5 * time.Second, statsEnabled: true,
			want: []string{"-container-stats-timeout (5s)", "38s", "30s"},
		},
		{
			// flag.Duration parses -1s happily; same normalisation applies.
			name: "negative stats timeout is normalised to the default", interval: 30 * time.Second,
			timeout: 28 * time.Second, statsTO: -1 * time.Second, bootTO: 5 * time.Second, statsEnabled: true,
			want: []string{"-container-stats-timeout (5s)", "38s"},
		},
		{
			// The boot-probe term is normalised the same way, and independently
			// of the stats term: a zero/negative BootTimeout means the 5s
			// default is what's actually spent, not zero.
			name: "zero boot timeout is normalised to the default", interval: 30 * time.Second,
			timeout: 20 * time.Second, statsTO: 5 * time.Second, bootTO: 0, statsEnabled: true,
			want: []string{"boot-reboot-probe timeout (5s)", "30s"},
		},
		{
			// The normalisation must not manufacture a warning where the
			// effective sum genuinely fits: 10s + 5s(boot) + 5s(stats) < 30s.
			name: "zero timeouts still silent when the defaults fit", interval: 30 * time.Second,
			timeout: 10 * time.Second, statsTO: 0, bootTO: 0, statsEnabled: true,
		},
		{
			// No sampler, no second budget to spend: warning here would be noise
			// UNLESS the refresh + boot-probe terms alone already reach the
			// interval — proven by the next case.
			name:     "sampler disabled and remaining budget fits is silent",
			interval: 30 * time.Second, timeout: 20 * time.Second, statsTO: 5 * time.Second,
			bootTO: 5 * time.Second, statsEnabled: false,
		},
		{
			// #231 review finding #2, isolated: the boot probe alone (no
			// sampler at all) can push a host over budget, since it is never
			// gated by a flag the way the stats sampler is.
			name:     "boot probe alone can push the budget over with stats disabled",
			interval: 30 * time.Second, timeout: 26 * time.Second, statsTO: 5 * time.Second,
			bootTO: 5 * time.Second, statsEnabled: false,
			want: []string{"boot-reboot-probe", "31s", "30s"},
		},
		{
			// The poller is off entirely; pollerDisabledMetricsWarning owns that
			// case and nothing ticks, so there is no cadence to stretch.
			name: "poller disabled is silent", interval: 0,
			timeout: 45 * time.Second, statsTO: 5 * time.Second, bootTO: 5 * time.Second, statsEnabled: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := statsBudgetWarning(tc.interval, tc.timeout, tc.statsTO, tc.bootTO, tc.statsEnabled)
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

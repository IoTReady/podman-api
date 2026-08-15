package server

import (
	"strings"
	"testing"
	"time"
)

// The stats sample gets its own budget (#212), the boot-reboot probe gets its
// own budget too (#231), and so does the loadavg sample (#258). Until #260
// tick ran the three sub-samplers one after another, so a host's worst-case
// per-tick cost was -inventory-refresh-timeout + boot-probe-timeout +
// loadavg-sample-timeout + -container-stats-timeout (the last only when the
// sampler is enabled). #260 fans the three out concurrently instead — they
// touch separate state maps and only stats' skip-and-drop path depends on the
// refresh's own outcome, never on the other two — so the true worst case is
// now -inventory-refresh-timeout + max(boot-probe, loadavg-sample,
// stats-timeout-if-enabled). tick blocks the ticker, so once that total
// reaches -inventory-refresh-interval a slow host stretches the whole fleet's
// poll cadence. Nothing structurally prevents an operator tuning into that
// state — this warning is what makes the invariant visible, and it must stay
// a warning: a deliberately long timeout should not refuse to start.
func TestStatsBudgetWarning(t *testing.T) {
	tests := []struct {
		name                                       string
		interval, timeout, statsTO, bootTO, loadTO time.Duration
		statsEnabled                               bool
		want                                       []string // substrings that must appear; empty means want ""
	}{
		{
			// The whole point of #260: at shipped defaults (20s refresh, 5s
			// each for stats/boot/loadavg) the old sum was 20+5+5+5=35s,
			// over a 30s interval. Fanned out concurrently the true cost is
			// 20 + max(5,5,5) = 25s, comfortably under. This case is the
			// concrete numeric claim the issue makes.
			name:     "shipped defaults are under budget once fanned out concurrently",
			interval: 30 * time.Second, timeout: 20 * time.Second, statsTO: 5 * time.Second,
			bootTO: 5 * time.Second, loadTO: 5 * time.Second, statsEnabled: true,
		},
		{
			// Genuinely small budgets across all terms fit comfortably.
			name:     "small budgets across all terms are silent",
			interval: 40 * time.Second, timeout: 10 * time.Second, statsTO: 5 * time.Second,
			bootTO: 5 * time.Second, loadTO: 5 * time.Second, statsEnabled: true,
		},
		{
			// An operator who raised the refresh timeout without looking at
			// the interval — no choice of max(...) shape fixes a refresh
			// timeout that alone is close to the interval.
			name: "large refresh timeout alone warns", interval: 30 * time.Second,
			timeout: 27 * time.Second, statsTO: 5 * time.Second, bootTO: 5 * time.Second,
			loadTO: 5 * time.Second, statsEnabled: true,
			want: []string{"-inventory-refresh-timeout", "poll cadence"},
		},
		{
			// A bare -container-stats-timeout=0 does NOT mean zero budget:
			// the poller spends its 5s default. Boot/loadavg are given
			// smaller explicit budgets so the (normalised) stats term is
			// unambiguously the max. A raw-value check would compute
			// 25+0=25 and stay silent; the poller actually spends
			// 25+5(stats)=30s against a 30s interval.
			name:     "zero stats timeout is normalised to the default and can be the max term",
			interval: 30 * time.Second,
			timeout:  25 * time.Second, statsTO: 0, bootTO: 1 * time.Second, loadTO: 1 * time.Second,
			statsEnabled: true,
			want:         []string{"-container-stats-timeout (5s)", "30s"},
		},
		{
			// flag.Duration parses -1s happily; same normalisation applies.
			name: "negative stats timeout is normalised to the default", interval: 30 * time.Second,
			timeout: 25 * time.Second, statsTO: -1 * time.Second, bootTO: 1 * time.Second,
			loadTO: 1 * time.Second, statsEnabled: true,
			want: []string{"-container-stats-timeout (5s)", "30s"},
		},
		{
			// The boot-probe term is normalised the same way, and
			// independently of the stats term: a zero/negative BootTimeout
			// means the 5s default is what's actually spent, not zero, and
			// it can be the max term.
			name:     "zero boot timeout is normalised to the default and can be the max term",
			interval: 30 * time.Second,
			timeout:  25 * time.Second, statsTO: 1 * time.Second, bootTO: 0, loadTO: 1 * time.Second,
			statsEnabled: true,
			want:         []string{"boot-reboot-probe timeout (5s)", "30s"},
		},
		{
			// Same normalisation for the loadavg term.
			name:     "zero loadavg timeout is normalised to the default and can be the max term",
			interval: 30 * time.Second,
			timeout:  25 * time.Second, statsTO: 1 * time.Second, bootTO: 1 * time.Second, loadTO: 0,
			statsEnabled: true,
			want:         []string{"loadavg-sample timeout (5s)", "30s"},
		},
		{
			// The normalisation must not manufacture a warning where the
			// effective total genuinely fits: 10 + max(5,5,5) < 30.
			name: "zero timeouts still silent when the defaults fit", interval: 30 * time.Second,
			timeout: 10 * time.Second, statsTO: 0, bootTO: 0, loadTO: 0, statsEnabled: true,
		},
		{
			// No sampler, no stats term in the max: warning here would be
			// noise, since refresh + max(boot, loadavg) alone fits.
			name:     "sampler disabled and remaining budget fits is silent",
			interval: 30 * time.Second, timeout: 20 * time.Second, statsTO: 5 * time.Second,
			bootTO: 5 * time.Second, loadTO: 5 * time.Second, statsEnabled: false,
		},
		{
			// The boot probe alone (no sampler at all) can still push a host
			// over budget, since it is never gated by a flag the way the
			// stats sampler is.
			name:     "boot probe alone can push the budget over with stats disabled",
			interval: 30 * time.Second, timeout: 20 * time.Second, statsTO: 5 * time.Second,
			bootTO: 15 * time.Second, loadTO: 5 * time.Second, statsEnabled: false,
			want: []string{"boot-reboot-probe timeout (15s)", "35s"},
		},
		{
			// The loadavg sampler is not gated by a flag either (#258), so
			// like the boot probe it can push a host over budget on its own.
			name:     "loadavg sample alone can push the budget over with stats disabled",
			interval: 30 * time.Second, timeout: 20 * time.Second, statsTO: 5 * time.Second,
			bootTO: 5 * time.Second, loadTO: 15 * time.Second, statsEnabled: false,
			want: []string{"loadavg-sample timeout (15s)", "35s"},
		},
		{
			// A large -container-stats-timeout must NOT be counted toward
			// the max when the sampler is disabled: no stats call is ever
			// made, so that budget is never actually spent.
			name:     "a large stats timeout is excluded from the max when the sampler is disabled",
			interval: 30 * time.Second, timeout: 20 * time.Second, statsTO: 100 * time.Second,
			bootTO: 5 * time.Second, loadTO: 5 * time.Second, statsEnabled: false,
		},
		{
			// The poller is off entirely; pollerDisabledMetricsWarning owns
			// that case and nothing ticks, so there is no cadence to stretch.
			name: "poller disabled is silent", interval: 0,
			timeout: 45 * time.Second, statsTO: 5 * time.Second, bootTO: 5 * time.Second,
			loadTO: 5 * time.Second, statsEnabled: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := statsBudgetWarning(tc.interval, tc.timeout, tc.statsTO, tc.bootTO, tc.loadTO, tc.statsEnabled)
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

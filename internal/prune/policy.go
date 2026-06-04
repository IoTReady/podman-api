// Package prune implements scheduled host-health cleanup: a "prune" job kind run
// by the jobs runner, fed by a per-host scheduler that fires on a configurable
// interval or disk high-water threshold. It never reaps in-use resources.
package prune

import (
	"fmt"
	"time"

	"github.com/iotready/podman-api/internal/config"
)

// Scope tokens. Dangling is the only default; the rest are opt-in.
const (
	ScopeDangling   = "dangling"    // dangling image layers
	ScopeAllImages  = "all-images"  // also unused tagged images
	ScopeContainers = "containers"  // exited containers
	ScopeBuildCache = "build-cache" // dangling build cache (libpod prunes it via the images-prune endpoint, which also reaps dangling image layers)
	ScopeVolumes    = "volumes"     // unused (unattached) volumes, protect-filtered
)

func validScope(s string) bool {
	switch s {
	case ScopeDangling, ScopeAllImages, ScopeContainers, ScopeBuildCache, ScopeVolumes:
		return true
	}
	return false
}

// Defaults are the global flag-derived policy defaults a per-host config merges over.
type Defaults struct {
	Enabled       bool
	Interval      time.Duration
	DiskThreshold int // percent 0..100; 0 disables the threshold trigger
	Scope         []string
	DryRun        bool
}

// Policy is a fully-resolved, validated per-host prune policy. Defaults and
// Policy are intentionally the same shape: Defaults carries unvalidated
// flag-derived values, Policy is the post-Resolve, post-validate form.
type Policy struct {
	Enabled       bool
	Interval      time.Duration // zero disables the interval trigger
	DiskThreshold int           // percent 0..100; 0 disables the threshold trigger
	Scope         []string
	DryRun        bool
}

// Resolve merges a raw per-host config (nil = inherit everything) over defaults
// and validates the result. Unknown scope tokens, unparseable intervals, and
// out-of-range thresholds are errors.
func Resolve(hc *config.PruneConfig, def Defaults) (Policy, error) {
	p := Policy{
		Enabled:       def.Enabled,
		Interval:      def.Interval,
		DiskThreshold: def.DiskThreshold,
		Scope:         append([]string(nil), def.Scope...),
		DryRun:        def.DryRun,
	}
	if hc != nil {
		if hc.Enabled != nil {
			p.Enabled = *hc.Enabled
		}
		if hc.Interval != nil {
			d, err := time.ParseDuration(*hc.Interval)
			if err != nil {
				return Policy{}, fmt.Errorf("prune interval %q: %w", *hc.Interval, err)
			}
			p.Interval = d
		}
		if hc.DiskThreshold != nil {
			p.DiskThreshold = *hc.DiskThreshold
		}
		if hc.Scope != nil {
			p.Scope = append([]string(nil), *hc.Scope...)
		}
		if hc.DryRun != nil {
			p.DryRun = *hc.DryRun
		}
	}
	if p.DiskThreshold < 0 || p.DiskThreshold > 100 {
		return Policy{}, fmt.Errorf("prune disk_threshold_pct %d out of range 0..100", p.DiskThreshold)
	}
	seen := make(map[string]struct{}, len(p.Scope))
	for _, s := range p.Scope {
		if !validScope(s) {
			return Policy{}, fmt.Errorf("unknown prune scope %q", s)
		}
		if _, dup := seen[s]; dup {
			return Policy{}, fmt.Errorf("duplicate prune scope %q", s)
		}
		seen[s] = struct{}{}
	}
	return p, nil
}

// HasScope reports whether the policy enables scope s.
func (p Policy) HasScope(s string) bool {
	for _, x := range p.Scope {
		if x == s {
			return true
		}
	}
	return false
}

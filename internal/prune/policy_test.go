package prune

import (
	"testing"
	"time"

	"github.com/iotready/podman-api/internal/config"
)

func ptr[T any](v T) *T { return &v }

func TestResolveUsesDefaultsWhenHostHasNoPolicy(t *testing.T) {
	def := Defaults{Enabled: false, Interval: 24 * time.Hour, DiskThreshold: 85, Scope: []string{ScopeDangling}, DryRun: false}
	got, err := Resolve(nil, def)
	if err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	if got.Enabled || got.Interval != 24*time.Hour || got.DiskThreshold != 85 ||
		len(got.Scope) != 1 || got.Scope[0] != ScopeDangling || got.DryRun {
		t.Fatalf("unexpected resolved policy: %+v", got)
	}
}

func TestResolveOverridesPerField(t *testing.T) {
	def := Defaults{Enabled: false, Interval: 24 * time.Hour, DiskThreshold: 85, Scope: []string{ScopeDangling}}
	hc := &config.PruneConfig{
		Enabled:       ptr(true),
		Interval:      ptr("6h"),
		DiskThreshold: ptr(60),
		Scope:         ptr([]string{ScopeDangling, ScopeVolumes}),
		DryRun:        ptr(true),
	}
	got, err := Resolve(hc, def)
	if err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	if !got.Enabled || got.Interval != 6*time.Hour || got.DiskThreshold != 60 ||
		len(got.Scope) != 2 || !got.DryRun {
		t.Fatalf("unexpected resolved policy: %+v", got)
	}
}

func TestResolveRejectsUnknownScope(t *testing.T) {
	def := Defaults{Scope: []string{ScopeDangling}}
	_, err := Resolve(&config.PruneConfig{Scope: ptr([]string{"bogus"})}, def)
	if err == nil {
		t.Fatal("expected unknown-scope error")
	}
}

func TestResolveRejectsBadInterval(t *testing.T) {
	_, err := Resolve(&config.PruneConfig{Interval: ptr("nope")}, Defaults{})
	if err == nil {
		t.Fatal("expected bad-duration error")
	}
}

func TestResolveRejectsThresholdOutOfRange(t *testing.T) {
	if _, err := Resolve(&config.PruneConfig{DiskThreshold: ptr(150)}, Defaults{}); err == nil {
		t.Fatal("expected threshold range error (high)")
	}
	if _, err := Resolve(&config.PruneConfig{DiskThreshold: ptr(-1)}, Defaults{}); err == nil {
		t.Fatal("expected threshold range error (negative)")
	}
}

func TestResolveRejectsDuplicateScope(t *testing.T) {
	_, err := Resolve(&config.PruneConfig{Scope: ptr([]string{ScopeDangling, ScopeDangling})}, Defaults{})
	if err == nil {
		t.Fatal("expected duplicate-scope error")
	}
}

func TestResolveRejectsEmptyScopeWhenEnabled(t *testing.T) {
	// An enabled policy with no scopes would enqueue no-op prune jobs forever.
	_, err := Resolve(&config.PruneConfig{Enabled: ptr(true), Scope: ptr([]string{})}, Defaults{})
	if err == nil {
		t.Fatal("expected empty-scope error for an enabled policy")
	}
	// A disabled policy with an empty scope is fine (it never runs).
	if _, err := Resolve(&config.PruneConfig{Enabled: ptr(false), Scope: ptr([]string{})}, Defaults{}); err != nil {
		t.Fatalf("disabled empty-scope policy should resolve: %v", err)
	}
}

func TestResolveScopeIsNotAliasOfDefaults(t *testing.T) {
	def := Defaults{Scope: []string{ScopeDangling}}
	got, err := Resolve(nil, def)
	if err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	got.Scope[0] = ScopeVolumes
	if def.Scope[0] != ScopeDangling {
		t.Fatal("resolved scope aliases Defaults.Scope")
	}
}

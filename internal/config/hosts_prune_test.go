package config

import (
	"os"
	"path/filepath"
	"testing"
)

func writeHost(t *testing.T, dir, name, body string) {
	t.Helper()
	if err := os.WriteFile(filepath.Join(dir, name), []byte(body), 0o644); err != nil {
		t.Fatal(err)
	}
}

func TestLoadHostsParsesPruneBlock(t *testing.T) {
	dir := t.TempDir()
	writeHost(t, dir, "h1.yaml", `id: h1
addr: unix
socket: /run/podman/podman.sock
prune:
  enabled: true
  interval: 12h
  disk_threshold_pct: 70
  scope: [dangling, volumes]
  dry_run: true
`)
	hosts, err := LoadHosts(dir)
	if err != nil {
		t.Fatalf("LoadHosts: %v", err)
	}
	if len(hosts) != 1 || hosts[0].Prune == nil {
		t.Fatalf("prune not parsed: %+v", hosts)
	}
	p := hosts[0].Prune
	if p.Enabled == nil || !*p.Enabled || p.Interval == nil || *p.Interval != "12h" ||
		p.DiskThreshold == nil || *p.DiskThreshold != 70 || p.Scope == nil ||
		len(*p.Scope) != 2 || p.DryRun == nil || !*p.DryRun {
		t.Fatalf("unexpected prune config: %+v", p)
	}
}

func TestLoadHostsNoPruneBlockLeavesNil(t *testing.T) {
	dir := t.TempDir()
	writeHost(t, dir, "h1.yaml", "id: h1\naddr: unix\nsocket: /s\n")
	hosts, err := LoadHosts(dir)
	if err != nil {
		t.Fatalf("LoadHosts: %v", err)
	}
	if hosts[0].Prune != nil {
		t.Fatalf("expected nil prune, got %+v", hosts[0].Prune)
	}
}

package config

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"gopkg.in/yaml.v3"
)

// Host is the in-memory representation of a single hosts/*.yaml file.
type Host struct {
	ID     string            `yaml:"id"`
	Addr   string            `yaml:"addr"`              // "unix" or "user@host"
	Socket string            `yaml:"socket"`            // path on the host
	SSHKey string            `yaml:"ssh_key,omitempty"` // optional
	Labels map[string]string `yaml:"labels,omitempty"`
	// Drain, when true, makes the API refuse to *create* new instances on
	// this host. Replace-shaped writes against existing pods, lifecycle
	// ops, and reads are unaffected. Hot-reloadable via SIGHUP.
	Drain bool `yaml:"drain,omitempty"`
	// Prune is the optional per-host host-health cleanup policy. nil means "use
	// the global flag defaults". Pointer fields inside distinguish "unset"
	// (inherit default) from an explicit zero value.
	Prune *PruneConfig `yaml:"prune,omitempty"`
	// CaddyAdminAddr is the Caddy admin API address (host:port) on this host,
	// e.g. "100.64.1.2:2019". When set, the ingress controller targets this
	// address instead of the global -ingress-caddy-admin-addr default. Empty
	// means use the global default.
	CaddyAdminAddr string `yaml:"caddy_admin_addr,omitempty"`
}

// PruneConfig is the raw per-host prune policy as parsed from hosts/*.yaml.
// Every field is a pointer so an omitted field inherits the global default
// rather than overriding it with a zero value. Resolution lives in the prune
// package (config must not depend on prune).
type PruneConfig struct {
	Enabled       *bool     `yaml:"enabled,omitempty"`
	Interval      *string   `yaml:"interval,omitempty"` // Go duration, e.g. "12h"
	DiskThreshold *int      `yaml:"disk_threshold_pct,omitempty"`
	Scope         *[]string `yaml:"scope,omitempty"`
	DryRun        *bool     `yaml:"dry_run,omitempty"`
}

// LoadHosts reads every *.yaml in dir into a Host. Unknown fields are rejected.
// Duplicate IDs are an error.
func LoadHosts(dir string) ([]Host, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, fmt.Errorf("read hosts dir %q: %w", dir, err)
	}

	var hosts []Host
	seen := map[string]string{}
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".yaml") {
			continue
		}
		path := filepath.Join(dir, e.Name())
		raw, err := os.ReadFile(path)
		if err != nil {
			return nil, fmt.Errorf("read %s: %w", path, err)
		}
		var h Host
		dec := yaml.NewDecoder(strings.NewReader(string(raw)))
		dec.KnownFields(true)
		if err := dec.Decode(&h); err != nil {
			return nil, fmt.Errorf("parse %s: %w", path, err)
		}
		if h.ID == "" {
			return nil, fmt.Errorf("%s: id is required", path)
		}
		if prev, ok := seen[h.ID]; ok {
			return nil, fmt.Errorf("duplicate host id %q in %s and %s", h.ID, prev, path)
		}
		seen[h.ID] = path
		hosts = append(hosts, h)
	}
	return hosts, nil
}

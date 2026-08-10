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

// RenameHostFile finds the *.yaml file in dir whose parsed id equals oldID,
// replaces its exact "id: <oldID>" line with "id: <newID>", and atomically
// rewrites the file (temp file + os.Rename, same directory). It does NOT
// re-marshal the YAML — every other line, including comments and formatting,
// is preserved byte-for-byte. Returns the file's path and its ORIGINAL full
// content (before the rewrite), so a caller can restore it verbatim if a
// later step (e.g. a store migration) fails.
//
// Fails closed rather than guessing: if no file's parsed id matches oldID, or
// the id line isn't found in the exact "id: <oldID>" form (e.g. it carries a
// trailing comment), this returns an error and touches nothing.
func RenameHostFile(dir, oldID, newID string) (path string, original []byte, err error) {
	hosts, err := LoadHosts(dir)
	if err != nil {
		return "", nil, fmt.Errorf("rename host file: %w", err)
	}
	found := false
	for _, h := range hosts {
		if h.ID == oldID {
			found = true
			break
		}
	}
	if !found {
		return "", nil, fmt.Errorf("rename host file: no host with id %q in %s", oldID, dir)
	}

	entries, err := os.ReadDir(dir)
	if err != nil {
		return "", nil, fmt.Errorf("rename host file: read dir %q: %w", dir, err)
	}
	oldLine := "id: " + oldID
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".yaml") {
			continue
		}
		p := filepath.Join(dir, e.Name())
		raw, err := os.ReadFile(p)
		if err != nil {
			return "", nil, fmt.Errorf("rename host file: read %s: %w", p, err)
		}
		lines := strings.Split(string(raw), "\n")
		lineIdx := -1
		for i, l := range lines {
			if l == oldLine {
				lineIdx = i
				break
			}
		}
		if lineIdx == -1 {
			continue // this file isn't the one, or its id line isn't in the expected exact form
		}
		lines[lineIdx] = "id: " + newID
		newContent := strings.Join(lines, "\n")

		if err := WriteFileAtomic(p, []byte(newContent)); err != nil {
			return "", nil, fmt.Errorf("rename host file: %w", err)
		}
		return p, raw, nil
	}
	return "", nil, fmt.Errorf("rename host file: host %q resolved but no file had the exact line %q", oldID, oldLine)
}

// DefaultConfigFileMode is the mode WriteFileAtomic creates a file with when
// the target does not already exist.
const DefaultConfigFileMode os.FileMode = 0o644

// WriteFileAtomic replaces path's contents with data via a temp file in the
// same directory followed by os.Rename, so a crash mid-write can never leave a
// truncated config file — only the old content or the new. When path already
// exists its mode is preserved; otherwise DefaultConfigFileMode is used.
//
// Used for both directions of a host rename: RenameHostFile's forward write and
// the API handler's revert when the store migration that follows fails. The
// revert used a plain os.WriteFile with a hardcoded 0644 until the final review
// of #246 — non-atomic, and it flattened the file's mode on the way back.
func WriteFileAtomic(path string, data []byte) error {
	mode := DefaultConfigFileMode
	if fi, err := os.Stat(path); err == nil {
		mode = fi.Mode().Perm()
	} else if !os.IsNotExist(err) {
		return fmt.Errorf("stat %s: %w", path, err)
	}

	tmp := path + ".tmp-write"
	if err := os.WriteFile(tmp, data, mode); err != nil {
		return fmt.Errorf("write temp file: %w", err)
	}
	// os.WriteFile applies the mode only when it CREATES the file; a leftover
	// temp file from a crashed earlier write would keep its old mode.
	if err := os.Chmod(tmp, mode); err != nil {
		_ = os.Remove(tmp)
		return fmt.Errorf("chmod temp file: %w", err)
	}
	if err := os.Rename(tmp, path); err != nil {
		_ = os.Remove(tmp)
		return fmt.Errorf("replace %s: %w", path, err)
	}
	return nil
}

// HostsDir is a directory of hosts/*.yaml files. Its RenameHostFile method
// lets it satisfy internal/api's HostFileRenamer interface without api
// importing config's LoadHosts internals or config importing api.
type HostsDir string

func (d HostsDir) RenameHostFile(oldID, newID string) (path string, original []byte, err error) {
	return RenameHostFile(string(d), oldID, newID)
}

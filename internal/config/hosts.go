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

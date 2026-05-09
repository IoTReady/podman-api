package config

import (
	"fmt"
	"io/fs"
	"strings"

	"github.com/iotready/podman-api/internal/render"
)

// Template is a parsed template ready to render.
type Template struct {
	Meta   render.Meta
	Body   string // body after the template-meta block, fed to text/template
	Source string // filename, for diagnostics
}

// LoadTemplates reads every *.yaml under root in fsys and parses each into a Template.
// Use root="." with embed.FS for the bundled templates.
func LoadTemplates(fsys fs.FS, root string) ([]Template, error) {
	var out []Template

	err := fs.WalkDir(fsys, root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() || !strings.HasSuffix(path, ".yaml") {
			return nil
		}
		raw, err := fs.ReadFile(fsys, path)
		if err != nil {
			return fmt.Errorf("read %s: %w", path, err)
		}
		meta, body, err := render.ParseMeta(string(raw))
		if err != nil {
			return fmt.Errorf("%s: %w", path, err)
		}
		out = append(out, Template{Meta: meta, Body: body, Source: path})
		return nil
	})
	if err != nil {
		return nil, err
	}

	seen := map[string]string{}
	for _, t := range out {
		if prev, ok := seen[t.Meta.ID]; ok {
			return nil, fmt.Errorf("duplicate template id %q in %s and %s", t.Meta.ID, prev, t.Source)
		}
		seen[t.Meta.ID] = t.Source
	}
	return out, nil
}

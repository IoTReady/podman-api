package store

import (
	"io/fs"
	"strings"

	"github.com/iotready/podman-api/internal/render"
)

// ParseSeeds reads every *.yaml in fsys, parses each via render.ParseMeta, and
// returns them as Origin:"seed" templates. Used to seed an empty store at boot.
func ParseSeeds(fsys fs.FS) ([]Template, error) {
	var out []Template
	err := fs.WalkDir(fsys, ".", func(path string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() || !strings.HasSuffix(path, ".yaml") {
			return err
		}
		b, err := fs.ReadFile(fsys, path)
		if err != nil {
			return err
		}
		meta, body, err := render.ParseMeta(string(b))
		if err != nil {
			return err
		}
		out = append(out, Template{Meta: meta, Body: body, Origin: "seed"})
		return nil
	})
	return out, err
}

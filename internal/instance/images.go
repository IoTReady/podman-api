package instance

import (
	"strings"

	"gopkg.in/yaml.v3"
)

// containerImages returns the unique image refs for every container in every
// Pod document within the (already rendered) YAML body. Order is preserved
// in first-seen order so callers see deterministic pull progress.
func containerImages(body string) []string {
	seen := map[string]bool{}
	var out []string
	dec := yaml.NewDecoder(strings.NewReader(body))
	for {
		var doc map[string]any
		if err := dec.Decode(&doc); err != nil {
			break
		}
		if doc["kind"] != "Pod" {
			continue
		}
		spec, _ := doc["spec"].(map[string]any)
		if spec == nil {
			continue
		}
		for _, key := range []string{"containers", "initContainers"} {
			cs, _ := spec[key].([]any)
			for _, c := range cs {
				cm, _ := c.(map[string]any)
				img, _ := cm["image"].(string)
				if img == "" || seen[img] {
					continue
				}
				seen[img] = true
				out = append(out, img)
			}
		}
	}
	return out
}

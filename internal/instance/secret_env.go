package instance

import (
	"regexp"
	"strings"

	"gopkg.in/yaml.v3"
)

// goTemplateRe matches Go text/template actions like {{.slug}} or {{ .x }}.
// We strip these before YAML decoding because their leading "{{" looks like
// the start of a YAML flow mapping and breaks the parser.
var goTemplateRe = regexp.MustCompile(`{{[^}]*}}`)

// secretEnvNames returns the set of env var names that are sourced from a
// Kubernetes secretKeyRef in the given (unrendered) template body. Used to
// redact those values from Observed.EnvSummary so secret material never
// leaks back through the API.
//
// The structural keys (kind, spec, containers, env, name, valueFrom,
// secretKeyRef) are static across all templates, so we don't need to
// fully render — only neutralise Go-template placeholders enough to make
// the body parse as YAML.
func secretEnvNames(body string) map[string]bool {
	body = goTemplateRe.ReplaceAllString(body, "PLACEHOLDER")
	out := map[string]bool{}
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
		containers, _ := spec["containers"].([]any)
		for _, c := range containers {
			cm, _ := c.(map[string]any)
			envs, _ := cm["env"].([]any)
			for _, e := range envs {
				em, _ := e.(map[string]any)
				name, _ := em["name"].(string)
				vf, _ := em["valueFrom"].(map[string]any)
				if vf == nil || name == "" {
					continue
				}
				if _, ok := vf["secretKeyRef"]; ok {
					out[name] = true
				}
			}
		}
	}
	return out
}

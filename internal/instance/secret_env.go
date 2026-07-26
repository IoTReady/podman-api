package instance

import (
	"regexp"
	"strings"

	"github.com/iotready/podman-api/internal/store"
	"gopkg.in/yaml.v3"
)

// goTemplateRe matches Go text/template actions like {{.slug}} or {{ .x }}.
// We strip these before YAML decoding because their leading "{{" looks like
// the start of a YAML flow mapping and breaks the parser.
var goTemplateRe = regexp.MustCompile(`{{[^}]*}}`)

// secretEnvNames returns the set of env var names that are sourced from a
// Kubernetes secretKeyRef in the given (unrendered) template body, across both
// initContainers and containers. Used to redact those values from
// Observed.EnvSummary so secret material never leaks back through the API.
//
// The structural keys (kind, spec, containers, env, name, valueFrom,
// secretKeyRef) are static across all templates, so we don't need to
// fully render — only neutralise Go-template placeholders enough to make
// the body parse as YAML.
//
// NOTE: this sees only what the TEMPLATE declares. A SidecarInjector adds
// containers and env at render time, after this body was stored, so an
// injector-added secretKeyRef is invisible here — that gap is closed by the
// value-based pass in Normalize (#198), not by this function.
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
		scan := func(key string) {
			containers, _ := spec[key].([]any)
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
		scan("initContainers")
		scan("containers")
	}
	return out
}

// secretValues returns the set of plaintext secret values recorded for an
// instance: the template-declared per-instance secrets plus every value a
// SidecarInjector declared. Normalize drops any env_summary entry whose value
// is in this set, which catches injector-added secretKeyRef env that
// secretEnvNames cannot see (#198).
//
// Empty values are excluded deliberately: a secret stored as "" would otherwise
// blank every legitimately-empty env var in the summary.
func secretValues(sp store.Spec) map[string]bool {
	out := map[string]bool{}
	for _, v := range sp.Secrets {
		if v != "" {
			out[v] = true
		}
	}
	for _, s := range sp.InjectorSecrets {
		if s.Value != "" {
			out[s.Value] = true
		}
	}
	return out
}

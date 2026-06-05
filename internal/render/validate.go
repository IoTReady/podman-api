package render

import (
	"errors"
	"fmt"
	"sort"
	"strings"
)

// ErrInvalidParameters is the sentinel returned by Validate.
var ErrInvalidParameters = errors.New("invalid parameters")

// Validate checks that params and secrets satisfy the template's contract:
//   - All Required parameters are present.
//   - No params outside the declared parameter set.
//   - All PerInstance secrets are present.
//   - No secrets outside PerInstance (PerHostReferenced are not in this map).
//
// Returns a single error listing every problem found.
func Validate(m Meta, params map[string]any, secrets map[string]string) error {
	var problems []string

	allowed := map[string]bool{}
	for _, p := range m.Parameters {
		allowed[p.Name] = true
		if p.Required {
			if _, ok := params[p.Name]; !ok {
				problems = append(problems, fmt.Sprintf("missing required parameter %q", p.Name))
			}
		}
	}
	for k := range params {
		if !allowed[k] {
			problems = append(problems, fmt.Sprintf("unknown parameter %q", k))
		}
	}

	allowedSecrets := stringSet(m.Secrets.PerInstance)
	for _, k := range m.Secrets.PerInstance {
		if _, ok := secrets[k]; !ok {
			problems = append(problems, fmt.Sprintf("missing required secret %q", k))
		}
	}
	for k := range secrets {
		if !allowedSecrets[k] {
			problems = append(problems, fmt.Sprintf("unknown secret %q", k))
		}
	}

	if len(problems) == 0 {
		return nil
	}
	sort.Strings(problems)
	return fmt.Errorf("%w: %s", ErrInvalidParameters, strings.Join(problems, "; "))
}

// ApplyDefaults returns a copy of params with any omitted parameter filled from
// its ParamDef.Default. Caller-supplied values always win. Parameters without a
// default are left absent (Validate enforces required ones).
func ApplyDefaults(m Meta, params map[string]any) map[string]any {
	out := make(map[string]any, len(params)+len(m.Parameters))
	for k, v := range params {
		out[k] = v
	}
	for _, p := range m.Parameters {
		if _, ok := out[p.Name]; !ok && p.Default != nil {
			out[p.Name] = p.Default
		}
	}
	return out
}

func stringSet(lists ...[]string) map[string]bool {
	out := map[string]bool{}
	for _, l := range lists {
		for _, s := range l {
			out[s] = true
		}
	}
	return out
}

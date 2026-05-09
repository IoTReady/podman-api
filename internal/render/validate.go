package render

import (
	"errors"
	"fmt"
	"sort"
	"strings"
)

// Validate checks that params and secrets satisfy the template's contract:
//   - All Required parameters are present.
//   - No params outside Required ∪ Optional.
//   - All PerInstance secrets are present.
//   - No secrets outside PerInstance (PerHostReferenced are not in this map).
//
// Returns a single error listing every problem found.
func Validate(m Meta, params map[string]any, secrets map[string]string) error {
	var problems []string

	allowedParams := stringSet(m.Parameters.Required, m.Parameters.Optional)
	for _, k := range m.Parameters.Required {
		if _, ok := params[k]; !ok {
			problems = append(problems, fmt.Sprintf("missing required parameter %q", k))
		}
	}
	for k := range params {
		if !allowedParams[k] {
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
	return errors.New(strings.Join(problems, "; "))
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

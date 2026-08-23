package render

import (
	"errors"
	"fmt"
	"io"
	"strings"

	"gopkg.in/yaml.v3"
)

// ContainerNames reports the container names a template body declares, split
// into the ones that are LITERAL (identical for every instance) and the ones
// that are parameter-dependent.
//
// It exists because podman registers names the daemon never asked it to: kube
// play adds every container name in the played YAML as an alias on every
// network the pod joins (podman 5.8.2, pkg/domain/infra/abi/play.go — "Add the
// original container names from the kube yaml as aliases for it"). A literal
// container name is therefore a claim on a shared network's DNS namespace
// exactly like a declared alias, and must be policed like one (#272).
//
// The body is a Go template, so it is generally NOT parseable YAML on its own —
// `hostPort: {{.port}}` opens what YAML reads as a flow mapping. The names are
// instead extracted from two dry-run renders that differ in every parameter
// value: a name that comes out the same both times cannot depend on a
// parameter and is literal; one that differs is templated. Defaults are
// deliberately ignored when building the probe values — a defaulted parameter
// is still per-instance, so rendering both probes with the default would
// mislabel `{{.name}}` as literal.
//
// Both lists are in declaration order and de-duplicated. Errors from either
// render or from decoding the rendered YAML are returned; callers on the write
// path already report a body that will not render, so they generally treat a
// failure here as "nothing extractable" rather than a second error.
func ContainerNames(body string, m Meta) (literal, templated []string, err error) {
	a, err := containerNamesRendered(body, probeParams(m, "aaa", 1, false))
	if err != nil {
		return nil, nil, err
	}
	b, err := containerNamesRendered(body, probeParams(m, "bbb", 2, true))
	if err != nil {
		return nil, nil, err
	}
	if len(a) != len(b) {
		// A parameter that changes the SHAPE of the pod (a conditional container)
		// cannot be paired up positionally. Nothing here is provably literal.
		return nil, a, nil
	}
	seenLit := map[string]bool{}
	seenTmpl := map[string]bool{}
	for i := range a {
		if a[i] == b[i] {
			if !seenLit[a[i]] {
				seenLit[a[i]] = true
				literal = append(literal, a[i])
			}
			continue
		}
		if !seenTmpl[a[i]] {
			seenTmpl[a[i]] = true
			templated = append(templated, a[i])
		}
	}
	return literal, templated, nil
}

// probeParams gives every declared parameter a value of the right type,
// IGNORING any declared default so two probes really do differ everywhere.
func probeParams(m Meta, s string, i int, b bool) map[string]any {
	out := make(map[string]any, len(m.Parameters))
	for _, p := range m.Parameters {
		switch p.Type {
		case "int":
			out[p.Name] = i
		case "bool":
			out[p.Name] = b
		default: // string, select, or unspecified
			out[p.Name] = s
		}
	}
	return out
}

// containerNamesRendered renders body with params and returns every container
// name in the result, in document then declaration order.
//
// It walks every YAML document (a body may carry a ConfigMap alongside its Pod)
// and reads both the Pod shape (spec.containers) and the workload shape
// (spec.template.spec.containers, as in a Deployment) — kube play accepts both,
// and aliases the container names either way.
func containerNamesRendered(body string, params map[string]any) ([]string, error) {
	rendered, err := RenderBody(body, params)
	if err != nil {
		return nil, err
	}
	type ctr struct {
		Name string `yaml:"name"`
	}
	type spec struct {
		Containers []ctr `yaml:"containers"`
		Template   *struct {
			Spec struct {
				Containers []ctr `yaml:"containers"`
			} `yaml:"spec"`
		} `yaml:"template"`
	}
	var out []string
	dec := yaml.NewDecoder(strings.NewReader(rendered))
	for {
		var doc struct {
			Spec spec `yaml:"spec"`
		}
		if err := dec.Decode(&doc); err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return nil, fmt.Errorf("parse rendered pod: %w", err)
		}
		for _, c := range doc.Spec.Containers {
			if c.Name != "" {
				out = append(out, c.Name)
			}
		}
		if doc.Spec.Template != nil {
			for _, c := range doc.Spec.Template.Spec.Containers {
				if c.Name != "" {
					out = append(out, c.Name)
				}
			}
		}
	}
	return out, nil
}

// ValidateAliasesAgainstContainerNames rejects a template that declares an
// alias equal to one of its own literal container names.
//
// The two are the same kind of name: podman registers both as DNS aliases for
// the pod on the network. Declaring one that duplicates the other is at best
// redundant, and it makes the meta lie about where the name comes from — an
// operator reading `aliases: [db]` sees a name they can rename away, when the
// pod would answer to "db" regardless because a container is called that.
// Registration is the only place an author is standing, so it is where the
// duplicate is named (#272).
func ValidateAliasesAgainstContainerNames(m Meta, containers []string) error {
	if len(containers) == 0 {
		return nil
	}
	isContainer := make(map[string]bool, len(containers))
	for _, c := range containers {
		isContainer[c] = true
	}
	for _, n := range m.Networks {
		for _, a := range n.Aliases {
			if isContainer[a] {
				return fmt.Errorf("template-meta: networks: %q: alias %q is already a container name in this template — podman registers every container name as an alias on each joined network, so the declaration is redundant and the pod answers to %q with or without it", n.Name, a, a)
			}
		}
	}
	return nil
}

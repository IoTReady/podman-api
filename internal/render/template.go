package render

import (
	"bytes"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"text/template"
)

// ErrRenderInvalid is returned when the rendered template output is invalid.
var ErrRenderInvalid = errors.New("rendered template is invalid")

// templateFuncs are registered as Go template functions available in template
// bodies. Callers can use e.g. {{.config | indent 4}} to auto-indent every
// line of a multi-line value.
var templateFuncs = template.FuncMap{
	"indent": func(spaces int, v string) string {
		pad := strings.Repeat(" ", spaces)
		lines := strings.Split(v, "\n")
		for i, line := range lines {
			if line != "" {
				lines[i] = pad + line
			}
		}
		return strings.Join(lines, "\n")
	},
}

// RenderBody substitutes params into an already-separated template body using
// text/template (with missingkey=error) and returns the final YAML. Callers
// that hold the full source (meta + body) split it with ParseMeta first; a
// store.Template keeps Meta and Body apart and renders the body directly.
func RenderBody(body string, params map[string]any) (string, error) {
	tmpl, err := template.New("template").
		Funcs(templateFuncs).
		Option("missingkey=error").
		Parse(body)
	if err != nil {
		return "", fmt.Errorf("parse template: %w", err)
	}
	var buf bytes.Buffer
	if err := tmpl.Execute(&buf, params); err != nil {
		return "", fmt.Errorf("execute template: %w", err)
	}
	return buf.String(), nil
}

// RenderAndValidate renders the template body and validates the result.
// It calls RenderBody then checks for issues with multi-line string params
// whose continuation lines lack the indentation needed to stay valid in a
// YAML block-scalar context.
func RenderAndValidate(body string, params map[string]any) (string, error) {
	rendered, err := RenderBody(body, params)
	if err != nil {
		return "", err
	}
	if err := validateRendered(body, params); err != nil {
		return "", err
	}
	return rendered, nil
}

// leadingWhitespace returns the number of leading space characters in s.
func leadingWhitespace(s string) int {
	n := 0
	for _, r := range s {
		if r == ' ' {
			n++
		} else {
			break
		}
	}
	return n
}

// paramRefPattern is a compiled regex matching Go template parameter references
// like {{.name}} with any amount of inner whitespace (e.g. {{ .name }},
// {{.name }}, {{ .name}}).
var paramRefPattern = regexp.MustCompile(`\{\{-?\s*\.(\w+)\s*-?\}\}`)

// paramRefIndent returns the leading whitespace before the first occurrence of
// a template parameter reference to name in body, or -1 if not found. Handles
// standard delimiters with any inner spacing: {{.name}}, {{ .name }}, etc.
func paramRefIndent(body, name string) int {
	matches := paramRefPattern.FindAllStringSubmatchIndex(body, -1)
	if matches == nil {
		return -1
	}
	best := -1
	for _, m := range matches {
		if len(m) < 4 {
			continue
		}
		matchedName := body[m[2]:m[3]]
		if matchedName != name {
			continue
		}
		idx := m[0]
		indent := 0
		for i := idx - 1; i >= 0 && body[i] != '\n'; i-- {
			indent++
		}
		if best < 0 || indent < best {
			best = indent
		}
	}
	return best
}

// validateRendered checks the template body and params for common issues with
// multi-line string parameters. For each multi-line string param, it verifies
// that continuation lines are indented enough to stay inside a YAML block
// scalar — otherwise the config file rendered from that param would come out
// silently empty or truncated.
//
// The YAML block-scalar baseline is set by the first content line's total
// indentation in the rendered output: templateRefIndent + firstLineLeadingWS.
// Since Go's text/template only prefixes the first line with the template's
// leading whitespace, continuation lines must already have enough leading
// whitespace in the param value to reach the same baseline.
func validateRendered(body string, params map[string]any) error {
	for name, val := range params {
		s, ok := val.(string)
		if !ok || !strings.Contains(s, "\n") {
			continue
		}
		got := paramRefIndent(body, name)
		if got < 0 {
			continue
		}
		lines := strings.Split(s, "\n")
		if len(lines) < 2 {
			continue
		}
		firstLineWS := leadingWhitespace(lines[0])
		baseline := got + firstLineWS
		for i, line := range lines[1:] {
			if strings.TrimSpace(line) == "" {
				continue
			}
			if ws := leadingWhitespace(line); ws < baseline {
				return fmt.Errorf("%w: parameter %q line %d has indentation %d, need at least %d to produce valid YAML (indent the param value or use the | indent template function with the {{.name}} reference moved to column 0)",
					ErrRenderInvalid, name, i+2, ws, baseline)
			}
		}
	}
	return nil
}

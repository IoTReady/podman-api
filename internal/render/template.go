package render

import (
	"bytes"
	"fmt"
	"text/template"
)

// RenderBody substitutes params into an already-separated template body using
// text/template (with missingkey=error) and returns the final YAML. Callers
// that hold the full source (meta + body) split it with ParseMeta first; a
// store.Template keeps Meta and Body apart and renders the body directly.
func RenderBody(body string, params map[string]any) (string, error) {
	tmpl, err := template.New("template").
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

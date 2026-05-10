package render

import (
	"bytes"
	"fmt"
	"text/template"
)

// Render parses src with ParseMeta, substitutes params into the body using
// text/template (with missingkey=error), and returns the final YAML.
func Render(src string, params map[string]any) (string, error) {
	_, body, err := ParseMeta(src)
	if err != nil {
		return "", err
	}
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

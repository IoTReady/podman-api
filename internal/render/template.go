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
	return RenderBody(body, params)
}

// RenderBody substitutes params into an already-separated template body using
// text/template (with missingkey=error) and returns the final YAML. Unlike
// Render it skips ParseMeta: callers that already hold the body (e.g. a
// store.Template whose Meta and Body are stored apart) render it directly.
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

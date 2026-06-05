package render

import (
	"bufio"
	"errors"
	"fmt"
	"strings"

	"gopkg.in/yaml.v3"
)

// Meta describes a template's parameter and secret contract.
// It is parsed from the leading "# template-meta:" comment block.
type Meta struct {
	ID         string     `yaml:"id"`
	Display    Display    `yaml:"display,omitempty" json:"display,omitempty"`
	Parameters []ParamDef `yaml:"parameters"`
	Secrets    Secrets    `yaml:"secrets"`
	Volumes    []Volume   `yaml:"volumes"`
	Ingress    *Ingress   `yaml:"ingress"`
}

// Display holds human-readable presentation metadata for a template.
type Display struct {
	Name        string `yaml:"name,omitempty" json:"name,omitempty"`
	Description string `yaml:"description,omitempty" json:"description,omitempty"`
	Category    string `yaml:"category,omitempty" json:"category,omitempty"`
	Icon        string `yaml:"icon,omitempty" json:"icon,omitempty"`
}

// ParamDef describes a single template parameter, its type, and constraints.
type ParamDef struct {
	Name        string   `yaml:"name" json:"name"`
	Type        string   `yaml:"type" json:"type"`
	Required    bool     `yaml:"required,omitempty" json:"required,omitempty"`
	Label       string   `yaml:"label,omitempty" json:"label,omitempty"`
	Description string   `yaml:"description,omitempty" json:"description,omitempty"`
	Default     any      `yaml:"default,omitempty" json:"default,omitempty"`
	Placeholder string   `yaml:"placeholder,omitempty" json:"placeholder,omitempty"`
	Options     []string `yaml:"options,omitempty" json:"options,omitempty"`
	Secret      bool     `yaml:"secret,omitempty" json:"secret,omitempty"`
}

type Secrets struct {
	PerInstance       []string `yaml:"per_instance"`
	PerHostReferenced []string `yaml:"per_host_referenced"`
}

type Volume struct {
	Name   string `yaml:"name"`
	Backup string `yaml:"backup,omitempty"`
}

// Ingress declares which container+port in the rendered pod serves HTTP, so the
// ingress layer can route a domain to it. Absent on non-web templates.
type Ingress struct {
	Container string `yaml:"container"`
	Port      int    `yaml:"port"`
}

// validParamTypes is the set of recognised ParamDef.Type values.
var validParamTypes = map[string]bool{
	"string": true,
	"int":    true,
	"bool":   true,
	"select": true,
}

// RequiredParams returns the names of all required parameters.
func (m Meta) RequiredParams() []string {
	var out []string
	for _, p := range m.Parameters {
		if p.Required {
			out = append(out, p.Name)
		}
	}
	return out
}

// ParamNames returns the names of all parameters in declaration order.
func (m Meta) ParamNames() []string {
	out := make([]string, 0, len(m.Parameters))
	for _, p := range m.Parameters {
		out = append(out, p.Name)
	}
	return out
}

// Param looks up a ParamDef by name.
func (m Meta) Param(name string) (ParamDef, bool) {
	for _, p := range m.Parameters {
		if p.Name == name {
			return p, true
		}
	}
	return ParamDef{}, false
}

// ParseMeta extracts the template-meta block from the head of the file
// and returns the rest of the file as the renderable body.
//
// The block must look like:
//
//	# template-meta:
//	#   id: postgres
//	#   parameters: ...
//
// The parser stops at the first non-comment line. The body is everything
// from that point onward (with a leading "---" preserved if present).
func ParseMeta(src string) (Meta, string, error) {
	var (
		yamlLines []string
		bodyStart int
		started   bool
	)

	sc := bufio.NewScanner(strings.NewReader(src))
	sc.Buffer(make([]byte, 0, 64*1024), 1024*1024)

	lineNo := 0
	var sawBody bool
	for sc.Scan() {
		line := sc.Text()
		lineNo++

		if !started {
			trim := strings.TrimSpace(line)
			if trim == "" {
				continue
			}
			if !strings.HasPrefix(trim, "# template-meta:") {
				return Meta{}, "", errors.New("template-meta: block not found at top of file")
			}
			started = true
			yamlLines = append(yamlLines, "template-meta:")
			continue
		}

		if strings.HasPrefix(line, "#") {
			// Strip the leading "# " (or "#") and keep indentation after it.
			stripped := strings.TrimPrefix(line, "#")
			stripped = strings.TrimPrefix(stripped, " ")
			yamlLines = append(yamlLines, stripped)
			continue
		}

		// Non-comment line ends the block.
		bodyStart = lineNo
		sawBody = true
		break
	}
	if err := sc.Err(); err != nil {
		return Meta{}, "", fmt.Errorf("scan: %w", err)
	}

	if !started {
		return Meta{}, "", errors.New("template-meta: block not found at top of file")
	}

	// If the file ended inside the meta comment block (no non-comment line was
	// ever seen), bodyStart is still 0. Set it past the last line so that
	// bodyAfterLine returns "".
	if !sawBody {
		bodyStart = lineNo + 1
	}

	var wrapper struct {
		Meta Meta `yaml:"template-meta"`
	}
	if err := yaml.Unmarshal([]byte(strings.Join(yamlLines, "\n")), &wrapper); err != nil {
		return Meta{}, "", fmt.Errorf("parse template-meta: %w", err)
	}
	if wrapper.Meta.ID == "" {
		return Meta{}, "", errors.New("template-meta: id is required")
	}

	// Validate and normalise parameter types.
	for i, p := range wrapper.Meta.Parameters {
		if p.Type == "" {
			wrapper.Meta.Parameters[i].Type = "string"
		} else if !validParamTypes[p.Type] {
			return Meta{}, "", fmt.Errorf("template-meta: parameter %q has unknown type %q", p.Name, p.Type)
		}
	}

	if ing := wrapper.Meta.Ingress; ing != nil {
		if ing.Container == "" {
			return Meta{}, "", errors.New("template-meta: ingress.container is required")
		}
		if ing.Port <= 0 || ing.Port > 65535 {
			return Meta{}, "", fmt.Errorf("template-meta: ingress.port %d out of range", ing.Port)
		}
	}

	body := bodyAfterLine(src, bodyStart)
	return wrapper.Meta, body, nil
}

// bodyAfterLine returns the substring of src starting at line number n (1-indexed).
func bodyAfterLine(src string, n int) string {
	if n <= 0 {
		return src
	}
	cur := 0
	for i := 0; i < n-1; i++ {
		idx := strings.IndexByte(src[cur:], '\n')
		if idx == -1 {
			return ""
		}
		cur += idx + 1
	}
	return src[cur:]
}

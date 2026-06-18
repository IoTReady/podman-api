package render

import (
	"bufio"
	"errors"
	"fmt"
	"regexp"
	"strings"

	"gopkg.in/yaml.v3"
)

// NameRe is the DNS-label constraint for template ids and instance slugs.
var NameRe = regexp.MustCompile(`^[a-z0-9][a-z0-9-]{0,38}[a-z0-9]$`)

// ValidName reports whether s is a valid template id / name.
func ValidName(s string) bool { return NameRe.MatchString(s) }

// Meta describes a template's parameter and secret contract.
// It is parsed from the leading "# template-meta:" comment block.
type Meta struct {
	ID         string     `yaml:"id" json:"id"`
	Display    Display    `yaml:"display,omitempty" json:"display,omitempty"`
	Parameters []ParamDef `yaml:"parameters" json:"parameters,omitempty"`
	Secrets    Secrets    `yaml:"secrets" json:"secrets,omitempty"`
	Volumes    []Volume   `yaml:"volumes" json:"volumes,omitempty"`
	Ingress    *Ingress   `yaml:"ingress" json:"ingress,omitempty"`
	PreBackup  *PreBackup `yaml:"pre_backup,omitempty" json:"pre_backup,omitempty"`
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
	PerInstance       []string `yaml:"per_instance" json:"per_instance,omitempty"`
	PerHostReferenced []string `yaml:"per_host_referenced" json:"per_host_referenced,omitempty"`
}

type Volume struct {
	Name   string `yaml:"name" json:"name"`
	Backup string `yaml:"backup,omitempty" json:"backup,omitempty"`
}

// Ingress declares which container+port in the rendered pod serves HTTP, so the
// ingress layer can route a domain to it. Absent on non-web templates.
type Ingress struct {
	Container string `yaml:"container" json:"container"`
	Port      int    `yaml:"port" json:"port"`
}

// PreBackup is a command run inside a named container immediately before the
// backup job stops+exports the instance. A non-zero exit fails the backup, so a
// failed dump never ships a stale/partial snapshot.
//
// Command is rendered with the instance's parameters (text/template) and then
// run as `/bin/sh -lc "<rendered command>"` in Container. The target container
// must therefore provide /bin/sh and support a login profile — minimal or
// distroless images will fail the exec (and thus the backup). Because rendering
// happens before the shell, parameters are interpolated directly into the shell
// line.
type PreBackup struct {
	Container string `yaml:"container" json:"container"`
	Command   string `yaml:"command" json:"command"`
}

// ValidateIngress checks an ingress declaration: container non-empty and
// port in 1..65535. nil is valid (no ingress).
func ValidateIngress(ing *Ingress) error {
	if ing == nil {
		return nil
	}
	if ing.Container == "" {
		return errors.New("template-meta: ingress.container is required")
	}
	if ing.Port <= 0 || ing.Port > 65535 {
		return fmt.Errorf("template-meta: ingress.port %d out of range", ing.Port)
	}
	return nil
}

// validParamTypes is the set of recognised ParamDef.Type values.
var validParamTypes = map[string]bool{
	"string": true,
	"int":    true,
	"bool":   true,
	"select": true,
}

// NormalizeParams normalizes each parameter's Type (blank → "string") and
// returns an error for an unknown type (allowed: string|int|bool|select).
func NormalizeParams(m *Meta) error {
	for i, p := range m.Parameters {
		if p.Type == "" {
			m.Parameters[i].Type = "string"
		} else if !validParamTypes[p.Type] {
			return fmt.Errorf("template-meta: parameter %q has unknown type %q", p.Name, p.Type)
		}
	}
	return nil
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
	if err := NormalizeParams(&wrapper.Meta); err != nil {
		return Meta{}, "", err
	}

	if err := ValidateIngress(wrapper.Meta.Ingress); err != nil {
		return Meta{}, "", err
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

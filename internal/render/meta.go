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
	Parameters Parameters `yaml:"parameters"`
	Secrets    Secrets    `yaml:"secrets"`
	Volumes    []Volume   `yaml:"volumes"`
}

type Parameters struct {
	Required []string `yaml:"required"`
	Optional []string `yaml:"optional"`
}

type Secrets struct {
	PerInstance       []string `yaml:"per_instance"`
	PerHostReferenced []string `yaml:"per_host_referenced"`
}

type Volume struct {
	Name   string `yaml:"name"`
	Backup string `yaml:"backup,omitempty"`
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

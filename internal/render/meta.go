package render

import (
	"bufio"
	"errors"
	"fmt"
	"path"
	"regexp"
	"strings"

	"github.com/bmatcuk/doublestar/v4"
	"gopkg.in/yaml.v3"

	"github.com/iotready/podman-api/extension"
)

// NameRe is the DNS-label constraint for template ids and instance slugs.
//
// It is ALSO the shape check for a new host id on POST /hosts/{host}/rename
// (via the API layer's validName). A change made for template-id reasons
// therefore silently changes what host ids the rename route will accept — and
// a host id that no longer matches is one nobody can rename to, even though
// hosts/*.yaml itself imposes no such constraint. Check both callers.
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
	// Networks names shared podman networks the instance's pod joins, in
	// addition to (and independent of) the ingress network. Two instances that
	// name the same network resolve each other by pod DNS name with no host
	// port published at all. Names are literal, not parameter-rendered, so a
	// bad one fails at template registration rather than at deploy (#243).
	Networks  []string   `yaml:"networks,omitempty" json:"networks,omitempty"`
	PreBackup *PreBackup `yaml:"pre_backup,omitempty" json:"pre_backup,omitempty"`
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

	// Secret is rejected, not honored: a template declaring it fails validation
	// (ValidateParamDefs). Parameter values are stored in a plaintext column, so
	// the flag once promised an encryption-at-rest guarantee the storage layer
	// never delivered — it only ever redacted the value from GET responses.
	// Authors want secrets.per_instance, which is encrypted at rest and reaches
	// the pod via secretKeyRef. The field is kept solely so the flag is *seen*
	// and refused: meta is decoded with non-strict YAML, so deleting it would
	// silently ignore `secret: true` instead of failing on it. (#205)
	Secret bool `yaml:"secret,omitempty" json:"secret,omitempty"`
}

type Secrets struct {
	PerInstance       []string `yaml:"per_instance" json:"per_instance,omitempty"`
	PerHostReferenced []string `yaml:"per_host_referenced" json:"per_host_referenced,omitempty"`
}

// BackupMarkerNone is the one `backup:` marker literal the core interprets: a
// volume declaring it is never exported by a backup, on any path. Every other
// marker string stays opaque and belongs to the commercial marker grammar.
//
// It is an ALIAS of extension.BackupMarkerNone, which is the canonical
// definition: the literal is part of the public seam (a commercial consumer
// links against it rather than hardcoding "none"), and a second declaration
// here could drift from it. instance.BackupMarkerNone aliases this in turn.
const BackupMarkerNone = extension.BackupMarkerNone

// IsBackupMarkerNone reports whether a raw marker is the `none` veto, folding
// case and trimming whitespace so the comparison fails CLOSED on a near-miss
// registration never saw. Re-exported from extension so render's own consumers
// have one comparison site; see extension.IsBackupMarkerNone for why.
func IsBackupMarkerNone(marker string) bool { return extension.IsBackupMarkerNone(marker) }

type Volume struct {
	Name string `yaml:"name" json:"name"`
	// Backup is the volume's backup target/marker. The core interprets exactly
	// one literal, BackupMarkerNone ("none"); every other string is opaque.
	//
	// A near-miss — `None`, `NONE`, `"none "` — is handled on BOTH sides, and
	// they are not redundant. ValidateVolumes REJECTS such a value at
	// registration, so an author who wrote `None` is told rather than guessed
	// at. But registration only runs on the write path: a template row
	// persisted before that validator existed is never re-validated on read, so
	// every consumer ALSO compares through IsBackupMarkerNone, which folds case
	// and trims whitespace. For a marker whose entire job is "never capture
	// this", failing open on a typo is the wrong direction — the volume would
	// be exported into every blob against the operator's explicit veto.
	// The stored string is never rewritten; only the comparison is tolerant.
	Backup string `yaml:"backup,omitempty" json:"backup,omitempty"`
	// Exclude lists glob patterns (doublestar syntax, `**` spans separators)
	// matched against each tar entry's path.Clean'ed name relative to the
	// volume root. Matching entries are omitted from the volume's BACKUP tar
	// and from nothing else — rename/migrate/copy always export everything.
	//
	// A directory's own tar entry is NEVER dropped, no matter which pattern
	// matches it or how — only files and links are ever removed. VolumeImport
	// recreates a directory implicitly from the paths beneath it, so a tar
	// missing a directory entry its children still need would fail restore's
	// integrity verification (a re-export compared against the stored
	// manifest), and that failure lands only after the instance has already
	// been torn down for the restore. A pattern can therefore empty a
	// directory but never remove it: "dir/**" drops everything under "dir"
	// while "dir" itself still ships.
	Exclude []string `yaml:"exclude,omitempty" json:"exclude,omitempty"`
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

// ValidateNetworks checks a template's shared-network declarations: each name
// non-empty, a valid podman network name (same charset as a template id), and
// declared at most once. Rejecting at registration means a typo fails visibly
// at the point the template is written, instead of surfacing as a NetworkEnsure
// failure on every deploy of every instance of that template.
func ValidateNetworks(m Meta) error {
	seen := make(map[string]struct{}, len(m.Networks))
	for _, n := range m.Networks {
		if strings.TrimSpace(n) == "" {
			return errors.New("template-meta: networks entries must not be empty")
		}
		if !ValidName(n) {
			return fmt.Errorf("template-meta: networks: %q is not a valid network name", n)
		}
		if _, dup := seen[n]; dup {
			return fmt.Errorf("template-meta: networks: %q declared twice", n)
		}
		seen[n] = struct{}{}
	}
	return nil
}

// ValidateVolumes checks each volume's exclude patterns: non-empty, relative,
// no ".." segment (so a pattern cannot be read as host-absolute or escape the
// volume root), clean (see below), not a "/**"-suffixed pattern whose
// zero-segment rescue swallows everything (see below), and compilable.
// Rejecting at registration means a typo fails visibly instead of silently
// matching nothing at backup time.
//
// Patterns are matched against path.Clean'ed tar entry names (tarfilter.go),
// so a pattern that is not itself already clean — a leading "./", a trailing
// slash, an internal "//" — can never match anything: it differs from every
// cleaned name by construction. That failure mode is silent (a legitimate
// zero-match backup and a typo'd one both show `excluded.entries: 0` in a
// backup row nobody reads), which is exactly what this validator exists to
// catch, so such a pattern is rejected outright rather than accepted and
// left to quietly do nothing. path.Clean does not strip a leading "/" or
// resolve a leading "..", so those two checks above remain load-bearing on
// their own and are kept ahead of this one so their more specific messages
// win first.
//
// A "/**"-suffixed pattern whose stripped prefix ALSO ends in a wildcard
// segment ("*" or "**") and contains a "**" segment somewhere in it is
// rejected for a related reason: newDropper's zero-segment rescue
// (tarfilter.go) skips a match when the pattern's own stripped prefix
// matches the same entry. If that prefix's last segment is itself a
// wildcard, the prefix matches every entry the full pattern matches too —
// so the rescue fires unconditionally and the pattern silently drops
// nothing at all, files included, with the same invisible
// `excluded.entries: 0` signal. "**/b/**" is unaffected (its prefix "**/b"
// ends in the literal "b", not a wildcard) and stays valid; "a/**/**",
// "**/**", "**/*/**" and "a/**/*/**" are all rejected by this rule. A bare
// "**" (no "/**" suffix to strip) is untouched by this check and stays
// valid — it legitimately matches, and so drops, every entry.
//
// It validates a template being CREATED: a near-miss `none` marker is rejected
// outright. Use ValidateVolumesUpdate for an edit of a stored template, which
// must not reject a near-miss the stored row already carries.
func ValidateVolumes(m Meta) error { return validateVolumes(m, nil) }

// ValidateVolumesUpdate validates m as an UPDATE of the already-stored meta.
// It is ValidateVolumes with exactly one relaxation: a near-miss `none` marker
// that the STORED row already carries on the same volume is accepted, while one
// the update newly introduces (or changes to a different near-miss) is rejected
// as on create.
//
// Without this, a row registered before the near-miss rule existed — precisely
// the population that rule was written about — becomes permanently uneditable:
// every PUT fails, including one changing an unrelated field, and there is no
// in-product path to the row to fix the marker. Consumption fails CLOSED
// (IsBackupMarkerNone folds case, so `None` really does veto), so grandfathering
// the stored value costs no safety; it just declines to hold an unrelated edit
// hostage. The write-path warning still names the volume on every such edit.
func ValidateVolumesUpdate(m, stored Meta) error { return validateVolumes(m, &stored) }

func validateVolumes(m Meta, stored *Meta) error {
	storedMarker := map[string]string{}
	if stored != nil {
		for _, v := range stored.Volumes {
			storedMarker[v.Name] = v.Backup
		}
	}
	for _, v := range m.Volumes {
		// A near-miss `none` is caught at registration so an author who wrote
		// `None` is TOLD rather than guessed at. This is belt-and-braces, not the
		// safety mechanism: every consumer compares through IsBackupMarkerNone,
		// which folds case and trims, so a near-miss already vetoes — the
		// comparison fails closed. What the strict rule buys is that the stored
		// text says what it does, so an operator reading the meta and the code
		// reading the marker cannot reach different conclusions.
		if IsBackupMarkerNone(v.Backup) && v.Backup != BackupMarkerNone &&
			storedMarker[v.Name] != v.Backup {
			return fmt.Errorf("template-meta: volume %q: backup marker %q is not the veto literal — it must be exactly %q (lowercase, no surrounding whitespace); as written it is still read as the veto, but only because every consumer folds case, and the stored text says something the marker grammar does not define", v.Name, v.Backup, BackupMarkerNone)
		}
		for _, p := range v.Exclude {
			if strings.TrimSpace(p) == "" {
				return fmt.Errorf("template-meta: volume %q: exclude pattern must not be empty", v.Name)
			}
			if strings.HasPrefix(p, "/") {
				return fmt.Errorf("template-meta: volume %q: exclude pattern %q must be relative to the volume root", v.Name, p)
			}
			for _, seg := range strings.Split(p, "/") {
				if seg == ".." {
					return fmt.Errorf("template-meta: volume %q: exclude pattern %q must not contain a %q segment", v.Name, p, "..")
				}
			}
			if cleaned := path.Clean(p); cleaned != p {
				return fmt.Errorf("template-meta: volume %q: exclude pattern %q is not clean (did you mean %q?); it would never match a path.Clean'ed tar entry name", v.Name, p, cleaned)
			}
			if prefix, isGlobstar := strings.CutSuffix(p, "/**"); isGlobstar {
				prefixSegs := strings.Split(prefix, "/")
				last := prefixSegs[len(prefixSegs)-1]
				hasGlobstarSeg := false
				for _, seg := range prefixSegs {
					if seg == "**" {
						hasGlobstarSeg = true
						break
					}
				}
				if hasGlobstarSeg && (last == "*" || last == "**") {
					return fmt.Errorf("template-meta: volume %q: exclude pattern %q: the \"/**\"-stripped prefix %q matches everything the pattern matches, so the zero-segment rescue swallows the whole pattern and it never drops anything", v.Name, p, prefix)
				}
			}
			if !doublestar.ValidatePattern(p) {
				return fmt.Errorf("template-meta: volume %q: invalid pattern %q", v.Name, p)
			}
		}
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

// ValidateParamDefs checks the parameter declarations themselves (as opposed to
// Validate, which checks supplied values against them). It rejects a parameter
// marked `secret: true`: parameter values land in a plaintext column, so the
// flag cannot mean what it says. secrets.per_instance is the encrypted-at-rest
// path and already does this job. (#205)
func ValidateParamDefs(m Meta) error {
	for _, p := range m.Parameters {
		if p.Secret {
			return fmt.Errorf("template-meta: parameter %q sets secret: true, which is not supported "+
				"(parameters are stored in plaintext); declare it under secrets.per_instance instead", p.Name)
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

	if err := ValidateParamDefs(wrapper.Meta); err != nil {
		return Meta{}, "", err
	}

	if err := ValidateIngress(wrapper.Meta.Ingress); err != nil {
		return Meta{}, "", err
	}

	if err := ValidateNetworks(wrapper.Meta); err != nil {
		return Meta{}, "", err
	}

	if err := ValidateVolumes(wrapper.Meta); err != nil {
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

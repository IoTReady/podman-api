package imgregistry

import (
	"regexp"
	"strings"
)

// repoComponentRe matches one "/"-separated path component of a Docker
// Registry v2 repository name, per the distribution spec's repository-name
// grammar: lowercase alphanumerics, with single dots/underscores or dash
// runs allowed only between alphanumeric runs. This is deliberately a
// reasonably permissive approximation rather than a byte-for-byte
// transcription of the spec grammar — good enough to reject anything that
// would break URL routing, YAML rendering, or shell/path handling downstream.
var repoComponentRe = regexp.MustCompile(`^[a-z0-9]+(?:(?:[._]|__|[-]+)[a-z0-9]+)*$`)

// ValidRepoName reports whether repo is an acceptable Docker Registry v2
// repository name: one or more "/"-separated components, each matching
// repoComponentRe. Registry v2 repo names legally contain "/" (e.g.
// "iotready/engine"), unlike the DNS-label-style names (host/template/slug)
// validated elsewhere by render.ValidName — so this validator is intentionally
// separate, not a reuse of that one. Shared by internal/api and internal/ui so
// the two edges cannot drift into accepting different repo shapes.
func ValidRepoName(repo string) bool {
	if repo == "" {
		return false
	}
	for _, part := range strings.Split(repo, "/") {
		if !repoComponentRe.MatchString(part) {
			return false
		}
	}
	return true
}

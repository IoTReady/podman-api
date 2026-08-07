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

// tagRe matches a Docker Registry v2 tag per the distribution spec's tag
// grammar: starts with a word character, then up to 127 more word
// characters/dots/dashes.
var tagRe = regexp.MustCompile(`^[a-zA-Z0-9_][a-zA-Z0-9._-]{0,127}$`)

// digestRe matches a content digest ("algorithm:hex", e.g.
// "sha256:abcdef..."), per the distribution spec's digest grammar.
var digestRe = regexp.MustCompile(`^[a-z0-9]+(?:[+._-][a-z0-9]+)*:[a-fA-F0-9]{32,}$`)

// ValidRef reports whether ref is an acceptable manifest reference: either a
// tag (tagRe) or a content digest (digestRe). Callers must validate ref
// before passing it to Client.Manifest, which interpolates it unescaped into
// a registry request path — an unvalidated ref (e.g. containing "../" or "/")
// would let a caller steer that request to an arbitrary path on the registry
// host.
func ValidRef(ref string) bool {
	if ref == "" {
		return false
	}
	return tagRe.MatchString(ref) || digestRe.MatchString(ref)
}

// IsDigestRef reports whether ref is digest-form (matches digestRe) rather
// than a tag. Delete requires this on top of ValidRef: a tag-form ref must
// never reach the registry's delete endpoint, since deleting by tag removes
// the manifest for every tag that currently shares that digest.
func IsDigestRef(ref string) bool {
	return digestRe.MatchString(ref)
}

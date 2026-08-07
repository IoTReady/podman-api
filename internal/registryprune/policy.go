package registryprune

import (
	"regexp"
	"strings"
	"time"

	"github.com/iotready/podman-api/internal/imgregistry"
)

// Class is the outcome of classifying one digest (an imgregistry.TagGroup —
// every tag currently pointing at that digest, within one repo) against a
// Policy. Deletion happens by digest, so a TagGroup is the unit of
// classification: one class per digest, covering every tag that shares it.
type Class string

const (
	// ClassProtected: at least one tag in the group is an exact name from
	// Policy.ProtectedExact (e.g. "latest", "main").
	ClassProtected Class = "protected"
	// ClassCalVer: at least one tag matches Policy.CalVer.
	ClassCalVer Class = "calver"
	// ClassRepoProtected: at least one tag matches this repo's entry in
	// Policy.ExtraPerRepo.
	ClassRepoProtected Class = "repo-protected"
	// ClassSharesProtected: none of this group's own tags are protected, but
	// its digest is — the same content is also reachable through a protected
	// tag (in this repo or another; digests are shared across repos, and
	// InUseSet/protectedDigests are keyed that way deliberately).
	ClassSharesProtected Class = "shares-protected"
	// ClassInUse: the digest is in the fleet-wide InUseSet (running somewhere,
	// or pinned by a stored spec). Checked before every delete rule, so a
	// dry-run's report of "why did this survive" is never wrong.
	ClassInUse Class = "in-use"
	// ClassFeatRecent: a "feat-*" tag younger than the retention window.
	ClassFeatRecent Class = "feat-recent"
	// ClassShaIsLatest: a bare-hex tag sharing its digest with the repo's own
	// "latest" tag (both land in the same TagGroup, since TagGroup already
	// groups by digest). Distinguished from ClassProtected so a report reads
	// "this is latest's own digest, under an extra alias" rather than the
	// generic "protected" — same disposition (kept), more specific reason.
	ClassShaIsLatest Class = "sha-is-latest"
	// ClassUnclassified: no rule recognises any tag in the group. KEPT by
	// default — see the #66 comment on Deletable.
	ClassUnclassified Class = "unclassified"
	// ClassShaOrphan: every tag in the group is bare-hex-form and nothing
	// above protects it. Deletable.
	ClassShaOrphan Class = "sha-orphan"
	// ClassFeatStale: a "feat-*" tag at or past the retention window (or of
	// unknown age — see the comment in Classify). Deletable.
	ClassFeatStale Class = "feat-stale"
)

// Policy is the classification ruleset. Kept as data, not code, so it stays
// reviewable and testable — ported verbatim from registry-gc.sh's defaults
// (see DefaultPolicy).
type Policy struct {
	// ProtectedExact is a list of tag names that are always kept, matched
	// case-sensitively and exactly.
	ProtectedExact []string
	// CalVer matches calendar-versioned release tags (e.g. "2024.01.01").
	// Nil means no calver protection.
	CalVer *regexp.Regexp
	// ExtraPerRepo adds repo-specific protected-tag patterns on top of
	// ProtectedExact/CalVer. A repo with no entry gets no extra protection.
	ExtraPerRepo map[string]*regexp.Regexp
	// RetentionDays is how long a "feat-*" tag survives before it is
	// eligible for deletion, aged from the digest's config-blob .created.
	RetentionDays int
	// MaxDeletesPerRepo is the per-repo tripwire threshold. Not used by
	// Classify/Deletable directly — it is Task 5's concern — but it lives on
	// Policy because it is part of the same reviewable ruleset.
	MaxDeletesPerRepo int
	// DeleteUnrecognised opts a run into deleting ClassUnclassified tags. The
	// #66 fix: false by default, because some repos have no CI-enforced tag
	// vocabulary and ~13% of their tags are legitimate developer-branch
	// names indistinguishable, by pattern alone, from garbage.
	DeleteUnrecognised bool
}

// DefaultPolicy returns the ruleset ported verbatim from registry-gc.sh.
func DefaultPolicy() Policy {
	return Policy{
		ProtectedExact: []string{"latest", "e2e", "dev", "main", "deps-cache", "scanners-cache"},
		CalVer:         regexp.MustCompile(`^[0-9]{4}\.[0-9]{2}\.[0-9]{2}$`),
		ExtraPerRepo: map[string]*regexp.Regexp{
			"otp": regexp.MustCompile(`^(runtime-base|valvo-fork-v1)`),
		},
		RetentionDays:      30,
		MaxDeletesPerRepo:  100,
		DeleteUnrecognised: false,
	}
}

// bareHexTagRe is SHA_ONLY_PATTERN, read verbatim from registry-gc.sh:
// 7-12 lowercase hex characters. This range is deliberately narrow, not a
// rounding of "looks like a hash" — a 40-char git full-SHA tag (13+ hex
// chars, a common CI convention) does NOT match, so it falls through to
// ClassUnclassified (kept by default) exactly as the script has kept it for
// months. Widening this to admit longer hex strings would make
// ClassShaOrphan — one of only two unconditionally deletable classes —
// unconditionally delete tags the production script protects. Do not widen
// it without re-reading registry-gc.sh's SHA_ONLY_PATTERN first.
var bareHexTagRe = regexp.MustCompile(`^[0-9a-f]{7,12}$`)

func isBareHexTag(tag string) bool { return bareHexTagRe.MatchString(tag) }

func hasBareHexTag(tags []string) bool {
	for _, t := range tags {
		if isBareHexTag(t) {
			return true
		}
	}
	return false
}

func allBareHexTags(tags []string) bool {
	if len(tags) == 0 {
		return false
	}
	for _, t := range tags {
		if !isBareHexTag(t) {
			return false
		}
	}
	return true
}

func hasTag(tags []string, name string) bool {
	for _, t := range tags {
		if t == name {
			return true
		}
	}
	return false
}

func matchesAnyExact(tags []string, names []string) bool {
	for _, t := range tags {
		for _, n := range names {
			if t == n {
				return true
			}
		}
	}
	return false
}

func matchesAnyRegex(tags []string, re *regexp.Regexp) bool {
	if re == nil {
		return false
	}
	for _, t := range tags {
		if re.MatchString(t) {
			return true
		}
	}
	return false
}

func hasFeatTag(tags []string) bool {
	for _, t := range tags {
		if strings.HasPrefix(t, "feat-") {
			return true
		}
	}
	return false
}

// Classify decides the Class of one digest (tg) within repo, given the
// fleet-wide in-use set, a precomputed set of digests already known to be
// protected (e.g. via a protected tag in another repo — digests are shared
// across repos, so protection must be too), the policy, and the current
// time (a parameter, not time.Now, so this stays a pure function callers
// can test exhaustively).
//
// PRECONDITION — the caller MUST invoke this once per digest, i.e. once per
// imgregistry.Tags() entry (a TagGroup already carries every tag sharing
// that digest), and never once per individual tag. ClassShaIsLatest depends
// on this: it fires only because a bare-hex tag sharing digest with the
// repo's own "latest" arrives in tg.Tags alongside "latest" itself. A caller
// that flattens tags and calls Classify per-tag will never observe that
// combination — "latest" and its hex alias would be classified separately —
// so ClassShaIsLatest becomes permanently unreachable. That is not unsafe
// (the hex tag still falls through to ClassProtected and stays kept), but it
// is a silent behaviour loss: fix the call site, not this function, if that
// happens.
//
// Precedence, first match wins:
//
//  1. in-use — checked before every delete rule (KEPT), so a dry-run's
//     reported reason for survival is never wrong.
//  2. sha-is-latest — a bare-hex tag sharing digest with this repo's own
//     "latest" (both are in tg.Tags, since TagGroup already groups by
//     digest). More specific than the generic "protected" below.
//  3. protected (exact name match)
//  4. calver
//  5. repo-protected (per-repo extra)
//  6. shares-protected (digest, not name, matches a protected tag elsewhere)
//  7. feat-* aging rules (feat-recent / feat-stale)
//  8. sha-orphan — every tag in the group is bare-hex and nothing above fired
//  9. unclassified — the default, KEPT unless Policy.DeleteUnrecognised
func Classify(repo string, tg imgregistry.TagGroup, inUse InUseSet, protectedDigests map[string]struct{}, p Policy, now time.Time) Class {
	if inUse.Has(tg.Digest) {
		return ClassInUse
	}

	if hasTag(tg.Tags, "latest") && hasBareHexTag(tg.Tags) {
		return ClassShaIsLatest
	}

	if matchesAnyExact(tg.Tags, p.ProtectedExact) {
		return ClassProtected
	}

	if matchesAnyRegex(tg.Tags, p.CalVer) {
		return ClassCalVer
	}

	if extra, ok := p.ExtraPerRepo[repo]; ok && matchesAnyRegex(tg.Tags, extra) {
		return ClassRepoProtected
	}

	if _, ok := protectedDigests[tg.Digest]; ok {
		return ClassSharesProtected
	}

	if hasFeatTag(tg.Tags) {
		retention := time.Duration(p.RetentionDays) * 24 * time.Hour
		// Deliberate asymmetry, ported from registry-gc.sh: everywhere else
		// in this package, an unknown quantity fails CLOSED (toward keeping
		// / aborting — see ErrUnsafeToPrune). Here it fails the other way:
		// a feat-* tag whose age we could not determine (Created is zero —
		// e.g. a multi-arch index with no single config blob to date) is
		// treated as stale, i.e. deletable. This matches the script's
		// long-standing behaviour for feature-branch cleanup and is NOT a
		// bug — do not "fix" it to fail closed without re-checking the
		// script and this comment first.
		//
		// Do NOT read this as "those tags get deleted", either: the handler
		// fails them closed one layer up. A ClassFeatStale whose Created is
		// zero is pulled back out of the candidate list and recorded as
		// age-unknown rather than deleted (handler.go, the ageUnknown branch),
		// because a zero Created is indistinguishable from a transient blob-GET
		// failure and the blast radius is per-DIGEST. Classification is the
		// honest answer to "what is this tag"; whether to act on it is the
		// handler's call.
		if tg.Created.IsZero() || now.Sub(tg.Created) > retention {
			return ClassFeatStale
		}
		return ClassFeatRecent
	}

	if allBareHexTags(tg.Tags) {
		return ClassShaOrphan
	}

	return ClassUnclassified
}

// Deletable reports whether a Class is eligible for deletion under p.
// Deletable classes are exactly ClassShaOrphan and ClassFeatStale, plus
// ClassUnclassified only when p.DeleteUnrecognised is set (the #66 fix).
// Nothing else is ever deletable — this function, not Classify's caller, is
// the single place that decision is made.
func Deletable(c Class, p Policy) bool {
	switch c {
	case ClassShaOrphan, ClassFeatStale:
		return true
	case ClassUnclassified:
		return p.DeleteUnrecognised
	default:
		return false
	}
}

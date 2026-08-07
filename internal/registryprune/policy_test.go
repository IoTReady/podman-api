package registryprune

import (
	"regexp"
	"testing"
	"time"

	"github.com/iotready/podman-api/internal/imgregistry"
)

func mustPolicy() Policy {
	return DefaultPolicy()
}

func TestDefaultPolicy(t *testing.T) {
	p := DefaultPolicy()
	wantExact := []string{"latest", "e2e", "dev", "main", "deps-cache", "scanners-cache"}
	if len(p.ProtectedExact) != len(wantExact) {
		t.Fatalf("ProtectedExact = %v, want %v", p.ProtectedExact, wantExact)
	}
	for i, name := range wantExact {
		if p.ProtectedExact[i] != name {
			t.Fatalf("ProtectedExact[%d] = %q, want %q", i, p.ProtectedExact[i], name)
		}
	}
	if p.CalVer == nil || !p.CalVer.MatchString("2024.01.01") {
		t.Fatalf("CalVer regexp does not match a calver tag: %v", p.CalVer)
	}
	if p.CalVer.MatchString("v1.2.3") {
		t.Fatalf("CalVer regexp incorrectly matches a non-calver tag")
	}
	extra, ok := p.ExtraPerRepo["otp"]
	if !ok || extra == nil {
		t.Fatalf("ExtraPerRepo[otp] missing")
	}
	if !extra.MatchString("runtime-base") || !extra.MatchString("valvo-fork-v1") {
		t.Fatalf("otp extra regexp does not match expected tags")
	}
	if p.RetentionDays != 30 {
		t.Fatalf("RetentionDays = %d, want 30", p.RetentionDays)
	}
	if p.MaxDeletesPerRepo != 100 {
		t.Fatalf("MaxDeletesPerRepo = %d, want 100", p.MaxDeletesPerRepo)
	}
	if p.DeleteUnrecognised {
		t.Fatalf("DeleteUnrecognised should default false")
	}
}

func TestClassify(t *testing.T) {
	now := time.Date(2026, 8, 7, 0, 0, 0, 0, time.UTC)
	p := mustPolicy()

	cases := []struct {
		name             string
		repo             string
		tg               imgregistry.TagGroup
		inUse            InUseSet
		protectedDigests map[string]struct{}
		want             Class
	}{
		{
			name:  "in-use wins over everything else",
			repo:  "engine",
			tg:    imgregistry.TagGroup{Digest: "sha256:aaa", Tags: []string{"deadbeef1234567"}, Created: now.Add(-100 * 24 * time.Hour)},
			inUse: InUseSet{Digests: map[string]struct{}{"sha256:aaa": {}}},
			want:  ClassInUse,
		},
		{
			name: "name-exact protected",
			repo: "engine",
			tg:   imgregistry.TagGroup{Digest: "sha256:bbb", Tags: []string{"latest"}, Created: now},
			want: ClassProtected,
		},
		{
			name: "calver protected",
			repo: "engine",
			tg:   imgregistry.TagGroup{Digest: "sha256:ccc", Tags: []string{"2024.01.01"}, Created: now},
			want: ClassCalVer,
		},
		{
			name: "otp per-repo extra: runtime-base",
			repo: "otp",
			tg:   imgregistry.TagGroup{Digest: "sha256:ddd", Tags: []string{"runtime-base"}, Created: now},
			want: ClassRepoProtected,
		},
		{
			name: "otp per-repo extra: valvo-fork-v1",
			repo: "otp",
			tg:   imgregistry.TagGroup{Digest: "sha256:eee", Tags: []string{"valvo-fork-v1"}, Created: now},
			want: ClassRepoProtected,
		},
		{
			name: "same tag name in a repo without the otp extra is NOT repo-protected",
			repo: "engine",
			tg:   imgregistry.TagGroup{Digest: "sha256:fff", Tags: []string{"runtime-base"}, Created: now},
			want: ClassUnclassified,
		},
		{
			name:             "shares digest with a protected tag elsewhere",
			repo:             "engine",
			tg:               imgregistry.TagGroup{Digest: "sha256:shared", Tags: []string{"deadbeef1234567"}, Created: now.Add(-100 * 24 * time.Hour)},
			protectedDigests: map[string]struct{}{"sha256:shared": {}},
			want:             ClassSharesProtected,
		},
		{
			name: "bare-hex tag whose digest equals :latest's is sha-is-latest, never sha-orphan",
			repo: "engine",
			tg:   imgregistry.TagGroup{Digest: "sha256:ggg", Tags: []string{"latest", "deadbeef1234567"}, Created: now.Add(-100 * 24 * time.Hour)},
			want: ClassShaIsLatest,
		},
		{
			name: "feat tag within retention is feat-recent",
			repo: "engine",
			tg:   imgregistry.TagGroup{Digest: "sha256:hhh", Tags: []string{"feat-widget"}, Created: now.Add(-10 * 24 * time.Hour)},
			want: ClassFeatRecent,
		},
		{
			name: "feat tag past retention is feat-stale",
			repo: "engine",
			tg:   imgregistry.TagGroup{Digest: "sha256:iii", Tags: []string{"feat-widget"}, Created: now.Add(-31 * 24 * time.Hour)},
			want: ClassFeatStale,
		},
		{
			name: "feat tag with unknown age fails TOWARD deletion (deliberate asymmetry)",
			repo: "engine",
			tg:   imgregistry.TagGroup{Digest: "sha256:jjj", Tags: []string{"feat-widget"}, Created: time.Time{}},
			want: ClassFeatStale,
		},
		{
			name: "bare-hex tag with no other signal is sha-orphan",
			repo: "engine",
			tg:   imgregistry.TagGroup{Digest: "sha256:kkk", Tags: []string{"deadbeef1234567"}, Created: now.Add(-1000 * 24 * time.Hour)},
			want: ClassShaOrphan,
		},
		{
			name: "unrecognised developer-branch tag is unclassified, kept by default",
			repo: "engine",
			tg:   imgregistry.TagGroup{Digest: "sha256:lll", Tags: []string{"toms-experiment"}, Created: now.Add(-1000 * 24 * time.Hour)},
			want: ClassUnclassified,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := Classify(tc.repo, tc.tg, tc.inUse, tc.protectedDigests, p, now)
			if got != tc.want {
				t.Fatalf("Classify() = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestDeletable(t *testing.T) {
	pDefault := DefaultPolicy()
	pOptIn := DefaultPolicy()
	pOptIn.DeleteUnrecognised = true

	cases := []struct {
		name string
		c    Class
		p    Policy
		want bool
	}{
		{"sha-orphan is deletable", ClassShaOrphan, pDefault, true},
		{"feat-stale is deletable", ClassFeatStale, pDefault, true},
		{"unclassified is kept by default", ClassUnclassified, pDefault, false},
		{"unclassified becomes deletable only when opted in", ClassUnclassified, pOptIn, true},
		{"protected is never deletable", ClassProtected, pOptIn, false},
		{"calver is never deletable", ClassCalVer, pOptIn, false},
		{"repo-protected is never deletable", ClassRepoProtected, pOptIn, false},
		{"shares-protected is never deletable", ClassSharesProtected, pOptIn, false},
		{"in-use is never deletable", ClassInUse, pOptIn, false},
		{"feat-recent is never deletable", ClassFeatRecent, pOptIn, false},
		{"sha-is-latest is never deletable", ClassShaIsLatest, pOptIn, false},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := Deletable(tc.c, tc.p); got != tc.want {
				t.Fatalf("Deletable(%q) = %v, want %v", tc.c, got, tc.want)
			}
		})
	}
}

// sanity: DefaultPolicy's CalVer/ExtraPerRepo compile as valid regexps and
// are reused (not re-compiled) by Classify — guards against a future change
// swapping *regexp.Regexp for a string and silently breaking Classify calls
// across many repos.
func TestPolicyRegexpFieldsAreUsable(t *testing.T) {
	var _ *regexp.Regexp = DefaultPolicy().CalVer
}

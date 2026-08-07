package registryprune

import (
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
			tg:   imgregistry.TagGroup{Digest: "sha256:ggg", Tags: []string{"latest", "deadbeef123"}, Created: now.Add(-100 * 24 * time.Hour)},
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
			tg:   imgregistry.TagGroup{Digest: "sha256:kkk", Tags: []string{"deadbee1234"}, Created: now.Add(-1000 * 24 * time.Hour)},
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

// TestBareHexBoundary pins the SHA_ONLY_PATTERN boundary read verbatim from
// registry-gc.sh: ^[0-9a-f]{7,12}$. This is not cosmetic — allBareHexTags
// feeds ClassShaOrphan, one of only two unconditionally deletable classes. A
// 40-char git full-SHA tag (a common CI convention, 13+ hex chars) must NOT
// match: the script does not recognise it as SHA_ONLY, so it falls through
// to unclassified (kept by default). Getting this boundary wrong would
// silently delete manifests the production script has protected for months.
func TestBareHexBoundary(t *testing.T) {
	now := time.Date(2026, 8, 7, 0, 0, 0, 0, time.UTC)
	p := mustPolicy()

	cases := []struct {
		name string
		tag  string
		want Class
	}{
		{"7 hex chars (lower boundary) is sha-orphan", "deadbee", ClassShaOrphan},
		{"12 hex chars (upper boundary) is sha-orphan", "deadbeefcafe", ClassShaOrphan},
		{"13 hex chars is NOT sha-orphan (past the script's SHA_ONLY_PATTERN)", "deadbeefcafe1", ClassUnclassified},
		{"64 hex chars (full git SHA) is NOT sha-orphan", "deadbeefcafe1234deadbeefcafe1234deadbeefcafe1234deadbeefcafe1234", ClassUnclassified},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			tg := imgregistry.TagGroup{Digest: "sha256:boundary-" + tc.tag, Tags: []string{tc.tag}, Created: now.Add(-1000 * 24 * time.Hour)}
			got := Classify("engine", tg, InUseSet{}, nil, p, now)
			if got != tc.want {
				t.Fatalf("Classify(%q) = %q, want %q", tc.tag, got, tc.want)
			}
			if got == ClassShaOrphan || got == ClassShaIsLatest {
				if tc.want != ClassShaOrphan {
					t.Fatalf("tag %q must not be swept as %q", tc.tag, got)
				}
			}
			// The 13/64-char cases must not even be reachable as an opt-in
			// deletable unclassified surprise beyond what Unclassified
			// already, correctly, allows — confirm the class itself, not
			// just Deletable(), is exactly Unclassified.
			if tc.want == ClassUnclassified && got != ClassUnclassified {
				t.Fatalf("tag %q classified %q, want exactly unclassified", tc.tag, got)
			}
		})
	}
}

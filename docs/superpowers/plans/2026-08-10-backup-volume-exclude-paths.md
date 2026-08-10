# Per-volume backup exclude paths — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Let a template declare, per volume, glob patterns whose matching paths are omitted from that volume's backup tar — and from nothing else.

**Architecture:** `render.Volume` gains an `Exclude []string` field, validated at template registration. `instance.Backup` maps each declared volume's patterns onto the podman volume name and hands them to `backupVolume`, which — only when patterns are present — switches from a byte-for-byte `TeeReader` copy to a decode → filter → re-encode pass through the tar reader that already computes the manifest. Applied patterns and drop counts are persisted per volume and exposed by the API.

**Tech Stack:** Go, `archive/tar` (stdlib), `github.com/bmatcuk/doublestar/v4` (new dependency), SQLite store, existing table-driven Go tests.

**Spec:** `docs/superpowers/specs/2026-08-10-backup-volume-exclude-paths-design.md`
**Issue:** #248

## Global Constraints

- **Empty `Exclude` must leave behaviour bit-identical.** When a volume declares no patterns, `backupVolume` runs today's `io.TeeReader(rc, cw)` copy path unchanged. This is the safety property the whole change rests on — every task must preserve it.
- **The filter applies in `internal/instance/backup.go` only.** `internal/instance/rename.go:219`, `internal/instance/service.go:1519`, and `internal/instance/service.go:1537` also call `VolumeExport` and must keep exporting everything; those paths remove the source, so a dropped entry there is unrecoverable.
- **Two different "exclude" concepts must not be conflated.** `excludePath` in `internal/instance/manifest.go:140` omits Litestream shadow-WAL paths from *fingerprint comparison* (#142) while their bytes still ship. The new filter omits *bytes*. A Litestream-excluded entry is still written to the filtered tar. Name the new predicate distinctly (`dropEntry` / `contentFilter`), never `exclude*`.
- **Build tags are required on every Go command:** `go test -tags "containers_image_openpgp exclude_graphdriver_btrfs exclude_graphdriver_devicemapper" ./...`, or just `make test` / `make vet`. A bare `go test` fails to build on a clean machine.
- **`main` is PR-only.** Work on branch `feat/backup-volume-exclude-paths` (already created, carries the spec commit) and open a PR on Forgejo. Never push to GitHub before the PR merges.
- Pattern syntax: `doublestar.Match` semantics, matched against `path.Clean(hdr.Name)`.

---

### Task 1: `Exclude` field on `render.Volume` + validation

**Files:**
- Modify: `internal/render/meta.go:66-69` (the `Volume` struct), and add `ValidateVolumes` near `ValidateIngress:95`
- Modify: `internal/render/meta.go:227-233` (call `ValidateVolumes` in the parse path, beside `ValidateParamDefs`/`ValidateIngress`)
- Modify: `internal/instance/templates.go:215` (call `ValidateVolumes` in `ValidateTemplate`)
- Modify: `go.mod`, `go.sum` (add `github.com/bmatcuk/doublestar/v4`)
- Test: `internal/render/meta_test.go`, `internal/instance/templates_test.go`

**Interfaces:**
- Consumes: nothing
- Produces: `render.Volume.Exclude []string`; `render.ValidateVolumes(m Meta) error`

- [ ] **Step 1: Add the dependency**

```bash
cd ~/projects/podman-api
go get github.com/bmatcuk/doublestar/v4@latest
go mod tidy
```

- [ ] **Step 2: Write the failing validation tests**

In `internal/render/meta_test.go`:

```go
func TestValidateVolumes(t *testing.T) {
	cases := []struct {
		name    string
		vols    []Volume
		wantErr string
	}{
		{name: "no volumes", vols: nil},
		{name: "no patterns", vols: []Volume{{Name: "data"}}},
		{name: "valid pattern", vols: []Volume{{Name: "sites", Exclude: []string{"*/private/backups/**"}}}},
		{name: "empty pattern", vols: []Volume{{Name: "sites", Exclude: []string{""}}}, wantErr: "must not be empty"},
		{name: "whitespace pattern", vols: []Volume{{Name: "sites", Exclude: []string{"  "}}}, wantErr: "must not be empty"},
		{name: "absolute pattern", vols: []Volume{{Name: "sites", Exclude: []string{"/etc/**"}}}, wantErr: "must be relative"},
		{name: "parent escape", vols: []Volume{{Name: "sites", Exclude: []string{"../other/**"}}}, wantErr: "must not contain"},
		{name: "parent escape mid-path", vols: []Volume{{Name: "sites", Exclude: []string{"a/../../b"}}}, wantErr: "must not contain"},
		{name: "malformed glob", vols: []Volume{{Name: "sites", Exclude: []string{"[a-"}}}, wantErr: "invalid pattern"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := ValidateVolumes(Meta{Volumes: tc.vols})
			if tc.wantErr == "" {
				if err != nil {
					t.Fatalf("want nil, got %v", err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("want error containing %q, got %v", tc.wantErr, err)
			}
		})
	}
}

func TestParseMeta_rejectsBadExclude(t *testing.T) {
	src := `# template-meta:
#   id: t1
#   volumes:
#     - { name: data, exclude: ["/abs/**"] }
---
apiVersion: v1
kind: Pod
`
	if _, _, err := ParseMeta(src); err == nil {
		t.Fatal("want ParseMeta to reject an absolute exclude pattern")
	}
}

func TestParseMeta_acceptsExclude(t *testing.T) {
	src := `# template-meta:
#   id: t1
#   volumes:
#     - { name: sites, backup: "s3; interval=24h", exclude: ["*/private/backups/**"] }
---
apiVersion: v1
kind: Pod
`
	m, _, err := ParseMeta(src)
	if err != nil {
		t.Fatal(err)
	}
	if len(m.Volumes) != 1 || len(m.Volumes[0].Exclude) != 1 ||
		m.Volumes[0].Exclude[0] != "*/private/backups/**" {
		t.Fatalf("exclude not parsed: %+v", m.Volumes)
	}
}
```

- [ ] **Step 3: Run the tests to verify they fail**

Run: `go test -tags "containers_image_openpgp exclude_graphdriver_btrfs exclude_graphdriver_devicemapper" ./internal/render/ -run 'Volumes|Exclude' -v`
Expected: FAIL — `undefined: ValidateVolumes`, and `Exclude` is not a field of `Volume`.

- [ ] **Step 4: Add the field**

In `internal/render/meta.go`, replace the `Volume` struct:

```go
type Volume struct {
	Name   string `yaml:"name" json:"name"`
	Backup string `yaml:"backup,omitempty" json:"backup,omitempty"`
	// Exclude lists glob patterns (doublestar syntax, `**` spans separators)
	// matched against each tar entry's path.Clean'ed name relative to the
	// volume root. Matching entries are omitted from the volume's BACKUP tar
	// and from nothing else — rename/migrate/copy always export everything.
	//
	// A trailing "/**" matches a directory's contents but not the directory
	// entry, so the directory restores as empty; a pattern naming the
	// directory itself drops it entirely.
	Exclude []string `yaml:"exclude,omitempty" json:"exclude,omitempty"`
}
```

- [ ] **Step 5: Add the validator**

In `internal/render/meta.go`, after `ValidateIngress`:

```go
// ValidateVolumes checks each volume's exclude patterns: non-empty, relative,
// no ".." segment (so a pattern cannot be read as host-absolute or escape the
// volume root), and compilable. Rejecting at registration means a typo fails
// visibly instead of silently matching nothing at backup time.
func ValidateVolumes(m Meta) error {
	for _, v := range m.Volumes {
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
			if !doublestar.ValidatePattern(p) {
				return fmt.Errorf("template-meta: volume %q: invalid pattern %q", v.Name, p)
			}
		}
	}
	return nil
}
```

Add the import `"github.com/bmatcuk/doublestar/v4"` (and `"fmt"`/`"strings"` if not already present — both are).

- [ ] **Step 6: Wire into both validation entry points**

In `internal/render/meta.go`, after the `ValidateIngress` call at ~231:

```go
	if err := ValidateVolumes(wrapper.Meta); err != nil {
		return Meta{}, "", err
	}
```

In `internal/instance/templates.go`, after the `render.ValidateIngress` block at ~215:

```go
	if err := render.ValidateVolumes(t.Meta); err != nil {
		return fmt.Errorf("%w: %v", ErrInvalidTemplate, err)
	}
```

Add to `ValidateTemplate`'s doc comment (`internal/instance/templates.go:196-199`) a numbered point: `5. Every volume's exclude patterns must be relative, non-empty and compilable (render.ValidateVolumes).`

- [ ] **Step 7: Add the ValidateTemplate test**

In `internal/instance/templates_test.go`:

```go
func TestValidateTemplate_rejectsBadExcludePattern(t *testing.T) {
	tpl := validTemplateFixture(t) // existing helper in this file
	tpl.Meta.Volumes = []render.Volume{{Name: "data", Exclude: []string{"../escape"}}}
	err := ValidateTemplate(tpl)
	if err == nil || !errors.Is(err, ErrInvalidTemplate) {
		t.Fatalf("want ErrInvalidTemplate, got %v", err)
	}
}
```

If `validTemplateFixture` does not exist under that name, use whatever the neighbouring tests in the file use to build a valid `store.Template`.

- [ ] **Step 8: Run the tests**

Run: `make test`
Expected: PASS

- [ ] **Step 9: Commit**

```bash
git add go.mod go.sum internal/render/meta.go internal/render/meta_test.go \
        internal/instance/templates.go internal/instance/templates_test.go
git commit -m "feat(render): validate per-volume backup exclude patterns (#248)"
```

---

### Task 2: Tar filter in the manifest pass

**Files:**
- Modify: `internal/instance/manifest.go:61-101` (`parseTar` gains options; `buildManifest` keeps its signature)
- Create: `internal/instance/tarfilter.go`
- Test: `internal/instance/tarfilter_test.go`

**Interfaces:**
- Consumes: `render.Volume.Exclude` (Task 1)
- Produces:
  - `type dropStats struct { Patterns []string; Entries int; Bytes int64 }`
  - `func newDropper(patterns []string) (func(hdr *tar.Header) bool, error)`
  - `func filterTar(dst io.Writer, src io.Reader, patterns []string) (Manifest, dropStats, error)`

- [ ] **Step 1: Write the failing tests**

Create `internal/instance/tarfilter_test.go`:

```go
package instance

import (
	"archive/tar"
	"bytes"
	"io"
	"testing"
)

// tarEntry is one entry to write into a test archive.
type tarEntry struct {
	name string
	body string
	typ  byte
	link string
}

func makeTar(t *testing.T, entries []tarEntry) []byte {
	t.Helper()
	var buf bytes.Buffer
	tw := tar.NewWriter(&buf)
	for _, e := range entries {
		typ := e.typ
		if typ == 0 {
			typ = tar.TypeReg
		}
		hdr := &tar.Header{Name: e.name, Typeflag: typ, Mode: 0o644, Linkname: e.link}
		if typ == tar.TypeReg {
			hdr.Size = int64(len(e.body))
		}
		if err := tw.WriteHeader(hdr); err != nil {
			t.Fatal(err)
		}
		if typ == tar.TypeReg {
			if _, err := tw.Write([]byte(e.body)); err != nil {
				t.Fatal(err)
			}
		}
	}
	if err := tw.Close(); err != nil {
		t.Fatal(err)
	}
	return buf.Bytes()
}

func tarNames(t *testing.T, b []byte) []string {
	t.Helper()
	var out []string
	tr := tar.NewReader(bytes.NewReader(b))
	for {
		hdr, err := tr.Next()
		if err == io.EOF {
			return out
		}
		if err != nil {
			t.Fatal(err)
		}
		out = append(out, hdr.Name)
		if _, err := io.Copy(io.Discard, tr); err != nil {
			t.Fatal(err)
		}
	}
}

func TestFilterTar_dropsMatchingContents(t *testing.T) {
	src := makeTar(t, []tarEntry{
		{name: "site/private/backups/", typ: tar.TypeDir},
		{name: "site/private/backups/db.sql.gz", body: "DUMPDUMP"},
		{name: "site/private/files/a.pdf", body: "PDF"},
		{name: "site/site_config.json", body: "{}"},
	})
	var out bytes.Buffer
	m, stats, err := filterTar(&out, bytes.NewReader(src), []string{"*/private/backups/**"})
	if err != nil {
		t.Fatal(err)
	}
	got := tarNames(t, out.Bytes())
	want := []string{"site/private/backups/", "site/private/files/a.pdf", "site/site_config.json"}
	if len(got) != len(want) {
		t.Fatalf("entries = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("entries = %v, want %v", got, want)
		}
	}
	if stats.Entries != 1 || stats.Bytes != int64(len("DUMPDUMP")) {
		t.Fatalf("stats = %+v, want 1 entry / 8 bytes", stats)
	}
	if _, ok := m["site/private/backups/db.sql.gz"]; ok {
		t.Fatal("dropped entry must not appear in the manifest")
	}
	if _, ok := m["site/private/files/a.pdf"]; !ok {
		t.Fatal("surviving entry missing from the manifest")
	}
}

func TestFilterTar_noPatternsIsPassThrough(t *testing.T) {
	src := makeTar(t, []tarEntry{
		{name: "a.txt", body: "A"},
		{name: "d/", typ: tar.TypeDir},
		{name: "d/b.txt", body: "B"},
	})
	var out bytes.Buffer
	_, stats, err := filterTar(&out, bytes.NewReader(src), nil)
	if err != nil {
		t.Fatal(err)
	}
	if stats.Entries != 0 || stats.Bytes != 0 {
		t.Fatalf("stats = %+v, want zero", stats)
	}
	if len(tarNames(t, out.Bytes())) != 3 {
		t.Fatalf("entries = %v, want all 3", tarNames(t, out.Bytes()))
	}
}

func TestFilterTar_directoryEntryVsContents(t *testing.T) {
	entries := []tarEntry{
		{name: "site/private/backups", typ: tar.TypeDir},
		{name: "site/private/backups/db.sql.gz", body: "D"},
	}
	// "dir/**" keeps the directory entry itself.
	var keep bytes.Buffer
	if _, _, err := filterTar(&keep, bytes.NewReader(makeTar(t, entries)), []string{"*/private/backups/**"}); err != nil {
		t.Fatal(err)
	}
	if n := len(tarNames(t, keep.Bytes())); n != 1 {
		t.Fatalf("dir/** kept %d entries, want 1 (the directory)", n)
	}
	// "dir" drops the directory entry too.
	var drop bytes.Buffer
	if _, _, err := filterTar(&drop, bytes.NewReader(makeTar(t, entries)), []string{"*/private/backups", "*/private/backups/**"}); err != nil {
		t.Fatal(err)
	}
	if n := len(tarNames(t, drop.Bytes())); n != 0 {
		t.Fatalf("dir pattern kept %d entries, want 0", n)
	}
}

func TestFilterTar_dropsHardlinkToDroppedTarget(t *testing.T) {
	src := makeTar(t, []tarEntry{
		{name: "junk/big.bin", body: "BIG"},
		{name: "keep/link.bin", typ: tar.TypeLink, link: "junk/big.bin"},
		{name: "keep/real.txt", body: "R"},
	})
	var out bytes.Buffer
	_, stats, err := filterTar(&out, bytes.NewReader(src), []string{"junk/**"})
	if err != nil {
		t.Fatal(err)
	}
	got := tarNames(t, out.Bytes())
	if len(got) != 1 || got[0] != "keep/real.txt" {
		t.Fatalf("entries = %v, want only keep/real.txt (link to a dropped target must go too)", got)
	}
	if stats.Entries != 2 {
		t.Fatalf("stats.Entries = %d, want 2 (the file and its link)", stats.Entries)
	}
}

func TestFilterTar_symlinkIsNotResolved(t *testing.T) {
	// A symlink is a name, not a hardlink to an inode — it survives even when
	// its target was dropped, exactly as it would on the source filesystem.
	src := makeTar(t, []tarEntry{
		{name: "junk/big.bin", body: "BIG"},
		{name: "keep/sym", typ: tar.TypeSymlink, link: "../junk/big.bin"},
	})
	var out bytes.Buffer
	if _, _, err := filterTar(&out, bytes.NewReader(src), []string{"junk/**"}); err != nil {
		t.Fatal(err)
	}
	got := tarNames(t, out.Bytes())
	if len(got) != 1 || got[0] != "keep/sym" {
		t.Fatalf("entries = %v, want the symlink kept", got)
	}
}

func TestFilterTar_preservesLongNamesAndXattrs(t *testing.T) {
	long := "site/" + string(bytes.Repeat([]byte("d"), 120)) + "/deep.txt"
	var buf bytes.Buffer
	tw := tar.NewWriter(&buf)
	hdr := &tar.Header{
		Name: long, Typeflag: tar.TypeReg, Mode: 0o644, Size: 3,
		Format:     tar.FormatPAX,
		PAXRecords: map[string]string{"SCHILY.xattr.user.test": "v"},
	}
	if err := tw.WriteHeader(hdr); err != nil {
		t.Fatal(err)
	}
	if _, err := tw.Write([]byte("abc")); err != nil {
		t.Fatal(err)
	}
	if err := tw.Close(); err != nil {
		t.Fatal(err)
	}

	var out bytes.Buffer
	if _, _, err := filterTar(&out, bytes.NewReader(buf.Bytes()), []string{"nothing/**"}); err != nil {
		t.Fatal(err)
	}
	tr := tar.NewReader(bytes.NewReader(out.Bytes()))
	got, err := tr.Next()
	if err != nil {
		t.Fatal(err)
	}
	if got.Name != long {
		t.Fatalf("name = %q, want the full %d-char name", got.Name, len(long))
	}
	if got.PAXRecords["SCHILY.xattr.user.test"] != "v" {
		t.Fatalf("xattr lost: %+v", got.PAXRecords)
	}
}

func TestFilterTar_manifestMatchesBuildManifestWhenNothingDropped(t *testing.T) {
	src := makeTar(t, []tarEntry{
		{name: "a.txt", body: "A"},
		{name: "d/b.txt", body: "BB"},
		{name: "s", typ: tar.TypeSymlink, link: "a.txt"},
	})
	want, err := buildManifest(bytes.NewReader(src))
	if err != nil {
		t.Fatal(err)
	}
	got, _, err := filterTar(io.Discard, bytes.NewReader(src), []string{"nothing/**"})
	if err != nil {
		t.Fatal(err)
	}
	if k, ok := got.firstDiff(want); !ok {
		t.Fatalf("manifests differ at %q", k)
	}
}

func TestNewDropper_rejectsBadPattern(t *testing.T) {
	if _, err := newDropper([]string{"[a-"}); err == nil {
		t.Fatal("want an error for an uncompilable pattern")
	}
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `go test -tags "containers_image_openpgp exclude_graphdriver_btrfs exclude_graphdriver_devicemapper" ./internal/instance/ -run 'FilterTar|NewDropper' -v`
Expected: FAIL — `undefined: filterTar`, `undefined: newDropper`.

- [ ] **Step 3: Generalise `parseTar`**

In `internal/instance/manifest.go`, change `parseTar` to take options. `buildManifest` keeps its exact current signature and behaviour:

```go
// tarOpts configures parseTar. The zero value is manifest-only: read the
// stream, fingerprint it, write nothing.
//
// Note the two distinct notions of "exclude" in this file. excludePath (below)
// omits paths from the fingerprint while their bytes still ship — it exists so
// a copy and its verify pass compare equal (#142). drop omits the bytes
// themselves from w, for backup exclude patterns (#248). An excludePath entry
// is still written to w.
type tarOpts struct {
	w     *tar.Writer                // nil = do not re-emit
	drop  func(hdr *tar.Header) bool // nil = drop nothing
	stats *dropStats                 // nil = do not count
}

func buildManifest(r io.Reader) (Manifest, error) {
	m := Manifest{}
	err := parseTar(r, m, tarOpts{})
	io.Copy(io.Discard, r) //nolint:errcheck // best-effort drain (see doc comment)
	return m, err
}

func parseTar(r io.Reader, m Manifest, opt tarOpts) error {
	tr := tar.NewReader(r)
	for {
		hdr, err := tr.Next()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		cleaned := path.Clean(hdr.Name)

		if opt.drop != nil && opt.drop(hdr) {
			n, err := io.Copy(io.Discard, tr)
			if err != nil {
				return err
			}
			if opt.stats != nil {
				opt.stats.Entries++
				opt.stats.Bytes += n
			}
			continue
		}

		excluded := excludePath(cleaned)

		// Re-emit the header before the body: tar.Writer requires it, and the
		// body below is teed into it.
		var body io.Writer = io.Discard
		if opt.w != nil {
			if err := opt.w.WriteHeader(hdr); err != nil {
				return err
			}
			body = opt.w
		}

		fi := fileInfo{typ: hdr.Typeflag}
		switch hdr.Typeflag {
		case tar.TypeReg:
			if excluded {
				if _, err := io.Copy(body, tr); err != nil {
					return err
				}
			} else {
				h := sha256.New()
				n, err := io.Copy(io.MultiWriter(h, body), tr)
				if err != nil {
					return err
				}
				fi.size = n
				fi.sha256 = hex.EncodeToString(h.Sum(nil))
			}
		case tar.TypeSymlink, tar.TypeLink:
			fi.link = hdr.Linkname
		}
		if excluded {
			continue
		}
		// path.Clean can collapse distinct names (e.g. "./foo" and "foo") to one
		// key, last-writer-wins. That is safe here: the same cleaning is applied
		// to both source and dest manifests, and podman's volume export is
		// deterministic, so a collision cancels out on both sides rather than
		// producing a false "equal".
		m[cleaned] = fi
	}
}
```

- [ ] **Step 4: Write the filter**

Create `internal/instance/tarfilter.go`:

```go
package instance

import (
	"archive/tar"
	"fmt"
	"io"
	"path"

	"github.com/bmatcuk/doublestar/v4"
)

// dropStats records what a backup's exclude patterns removed from a volume's
// tar. Entries == 0 with a non-empty Patterns is the visible signal that a
// pattern is stale or misspelled; it is deliberately not an error, because a
// new instance legitimately has nothing to exclude yet.
type dropStats struct {
	Patterns []string `json:"patterns"`
	Entries  int      `json:"entries"`
	Bytes    int64    `json:"bytes"`
}

// newDropper compiles patterns into a predicate over tar headers. It returns
// nil (not an error) when patterns is empty, so callers can test for "no
// filtering" with a nil check.
//
// Hardlinks: a tar TypeLink entry carries no body — it points at an earlier
// entry's inode. Keeping a link whose target was dropped produces an archive
// that fails to import, so the returned predicate also drops a link whose
// Linkname names an already-dropped path. This relies on the target preceding
// the link in the stream, which is how tar encodes hardlinks; a link that
// somehow precedes its target is kept and would be a broken reference. That is
// the same shape as a pre-existing broken link in the source volume, and the
// import surfaces it rather than silently corrupting data.
//
// Symlinks are NOT resolved: a symlink is a name, and a dangling one is
// exactly what the source filesystem would have.
func newDropper(patterns []string) (func(hdr *tar.Header) bool, error) {
	if len(patterns) == 0 {
		return nil, nil
	}
	for _, p := range patterns {
		if !doublestar.ValidatePattern(p) {
			return nil, fmt.Errorf("invalid exclude pattern %q", p)
		}
	}
	dropped := map[string]bool{}
	return func(hdr *tar.Header) bool {
		cleaned := path.Clean(hdr.Name)
		match := false
		for _, p := range patterns {
			if ok, _ := doublestar.Match(p, cleaned); ok {
				match = true
				break
			}
		}
		if !match && hdr.Typeflag == tar.TypeLink {
			match = dropped[path.Clean(hdr.Linkname)]
		}
		if match {
			dropped[cleaned] = true
		}
		return match
	}, nil
}

// filterTar copies src to dst, omitting entries matched by patterns, and
// returns the manifest of what survived plus what was dropped. Passing no
// patterns copies everything (re-encoded); callers that want a byte-for-byte
// copy must not call filterTar at all.
func filterTar(dst io.Writer, src io.Reader, patterns []string) (Manifest, dropStats, error) {
	drop, err := newDropper(patterns)
	if err != nil {
		return nil, dropStats{}, err
	}
	stats := dropStats{Patterns: patterns}
	m := Manifest{}
	tw := tar.NewWriter(dst)
	if err := parseTar(src, m, tarOpts{w: tw, drop: drop, stats: &stats}); err != nil {
		io.Copy(io.Discard, src) //nolint:errcheck // drain so the caller's Close is clean
		return nil, stats, err
	}
	if err := tw.Close(); err != nil {
		return nil, stats, err
	}
	return m, stats, nil
}
```

- [ ] **Step 5: Run the tests**

Run: `go test -tags "containers_image_openpgp exclude_graphdriver_btrfs exclude_graphdriver_devicemapper" ./internal/instance/ -run 'FilterTar|NewDropper|Manifest' -v`
Expected: PASS

- [ ] **Step 6: Run the whole suite — `parseTar` is shared**

Run: `make test && make vet`
Expected: PASS. `buildManifest` behaviour must be unchanged; any failure in the migrate/verify tests means the `tarOpts{}` default is not equivalent to the old body.

- [ ] **Step 7: Commit**

```bash
git add internal/instance/manifest.go internal/instance/tarfilter.go internal/instance/tarfilter_test.go
git commit -m "feat(instance): tar exclude filter sharing the manifest pass (#248)"
```

---

### Task 3: Persist and expose the drop record

**Files:**
- Modify: `internal/store/backups.go:23-27` (`BackupVolume`)
- Modify: `internal/api/backups.go:77-80` (`BackupVolumeView`) and `toBackupViews:85`
- Modify: `api/openapi.yaml:311` and `api/openapi.yaml:536`
- Test: `internal/api/backups_test.go`, `internal/store` round-trip test

**Interfaces:**
- Consumes: `dropStats` (Task 2)
- Produces: `store.BackupVolume.Excluded *ExcludedPaths`; `api.BackupVolumeView.Excluded *ExcludedView`

- [ ] **Step 1: Write the failing tests**

In `internal/store/backups_test.go` (or the memory-store test file that already exercises `CompleteBackup`):

```go
func TestCompleteBackup_roundTripsExcluded(t *testing.T) {
	st := NewMemory() // use whatever constructor neighbouring tests use
	ctx := context.Background()
	id := NewBackupID()
	if err := st.CreateBackup(ctx, Backup{ID: id, Host: "h", Template: "t", Slug: "s", State: BackupCreating}); err != nil {
		t.Fatal(err)
	}
	vols := []BackupVolume{{
		Name: "t-s-sites", SizeBytes: 10,
		Excluded: &ExcludedPaths{Patterns: []string{"*/private/backups/**"}, Entries: 3, Bytes: 999},
	}}
	if ok, err := st.CompleteBackup(ctx, id, vols); err != nil || !ok {
		t.Fatalf("CompleteBackup ok=%v err=%v", ok, err)
	}
	got, err := st.GetBackup(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	ex := got.Volumes[0].Excluded
	if ex == nil || ex.Entries != 3 || ex.Bytes != 999 || len(ex.Patterns) != 1 {
		t.Fatalf("excluded round-trip lost data: %+v", ex)
	}
}

func TestCompleteBackup_omitsExcludedWhenNil(t *testing.T) {
	b, err := json.Marshal(BackupVolume{Name: "v", SizeBytes: 1})
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(b), "excluded") {
		t.Fatalf("unfiltered volume must not carry an excluded key: %s", b)
	}
}
```

In `internal/api/backups_test.go`:

```go
func TestToBackupViews_carriesExcluded(t *testing.T) {
	in := []store.Backup{{
		ID: "bk_1", Host: "h", Template: "t", Slug: "s", State: store.BackupComplete,
		Volumes: []store.BackupVolume{
			{Name: "t-s-sites", SizeBytes: 10, Excluded: &store.ExcludedPaths{
				Patterns: []string{"*/private/backups/**"}, Entries: 3, Bytes: 999}},
			{Name: "t-s-logs", SizeBytes: 5},
		},
	}}
	out := toBackupViews(in)
	if out[0].Volumes[0].Excluded == nil || out[0].Volumes[0].Excluded.Entries != 3 {
		t.Fatalf("excluded not mapped: %+v", out[0].Volumes[0])
	}
	if out[0].Volumes[1].Excluded != nil {
		t.Fatal("unfiltered volume must have no excluded block")
	}
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `go test -tags "containers_image_openpgp exclude_graphdriver_btrfs exclude_graphdriver_devicemapper" ./internal/store/ ./internal/api/ -run 'Excluded' -v`
Expected: FAIL — `undefined: ExcludedPaths`.

- [ ] **Step 3: Add the store type**

In `internal/store/backups.go`:

```go
// ExcludedPaths records what a volume's backup exclude patterns removed. It is
// nil for a volume that declared no patterns, so an unfiltered backup is
// byte-identical in the row as well as on the wire. Entries == 0 with a
// non-empty Patterns means the patterns matched nothing — usually a stale or
// misspelled pattern. (#248)
type ExcludedPaths struct {
	Patterns []string `json:"patterns"`
	Entries  int      `json:"entries"`
	Bytes    int64    `json:"bytes"`
}

type BackupVolume struct {
	Name      string          `json:"name"`
	SizeBytes int64           `json:"size_bytes"`
	Manifest  json.RawMessage `json:"manifest"`
	Excluded  *ExcludedPaths  `json:"excluded,omitempty"`
}
```

No migration is needed: `Volumes` is already persisted as a JSON blob, and `omitempty` on a nil pointer means existing rows decode unchanged.

- [ ] **Step 4: Add the API view**

In `internal/api/backups.go`:

```go
// ExcludedView reports the backup exclude patterns applied to one volume and
// what they removed. Absent when the volume was exported in full. (#248)
type ExcludedView struct {
	Patterns []string `json:"patterns"`
	Entries  int      `json:"entries"`
	Bytes    int64    `json:"bytes"`
}

// BackupVolumeView is one exported volume's public metadata: name, tar size,
// and — when the template declared exclude patterns — what they dropped.
type BackupVolumeView struct {
	Name      string        `json:"name"`
	SizeBytes int64         `json:"size_bytes"`
	Excluded  *ExcludedView `json:"excluded,omitempty"`
}
```

In `toBackupViews`, where each volume is mapped, add:

```go
		if v.Excluded != nil {
			vv.Excluded = &ExcludedView{
				Patterns: v.Excluded.Patterns,
				Entries:  v.Excluded.Entries,
				Bytes:    v.Excluded.Bytes,
			}
		}
```

(Match the surrounding loop's variable names — read the existing body of `toBackupViews` at `internal/api/backups.go:85` before editing.)

- [ ] **Step 5: Update the OpenAPI document**

In `api/openapi.yaml`, at both volume schemas (lines ~311 and ~536), keep `required: [name, size_bytes]` and add under `properties`:

```yaml
        excluded:
          type: object
          description: >
            Backup exclude patterns applied to this volume and what they
            removed. Absent when the volume was exported in full. entries=0
            with a non-empty patterns list means the patterns matched nothing.
          required: [patterns, entries, bytes]
          properties:
            patterns: {type: array, items: {type: string}}
            entries: {type: integer}
            bytes: {type: integer, format: int64}
```

- [ ] **Step 6: Run the tests**

Run: `make test && make vet`
Expected: PASS. `internal/api/openapi_test.go` validates the document against the handlers — if it fails, the schema indentation or `required` list is wrong.

- [ ] **Step 7: Commit**

```bash
git add internal/store/backups.go internal/api/backups.go api/openapi.yaml \
        internal/store/backups_test.go internal/api/backups_test.go
git commit -m "feat(api): record and expose per-volume backup exclusions (#248)"
```

---

### Task 4: Wire the filter into the backup path

**Files:**
- Modify: `internal/instance/backup.go:126-140` (the volume loop in `Backup`) and `internal/instance/backup.go:171-196` (`backupVolume`)
- Modify: `internal/instance/rename.go:219`, `internal/instance/service.go:1519`, `internal/instance/service.go:1537` (comments only)
- Test: `internal/instance/backup_test.go`

**Interfaces:**
- Consumes: `filterTar`, `dropStats` (Task 2); `store.ExcludedPaths` (Task 3); `render.Volume.Exclude` (Task 1)
- Produces: `backupVolume(ctx, req, name string, patterns []string) (store.BackupVolume, error)`

- [ ] **Step 1: Write the failing tests**

In `internal/instance/backup_test.go`, following the existing fake-podman backup tests in that file:

```go
func TestBackup_excludesDeclaredPaths(t *testing.T) {
	// Build a service whose fake podman exports a tar containing a junk subtree.
	// Follow the existing harness in this file for constructing svc/fake/store.
	svc, fake, st := newBackupTestService(t)
	fake.ExportTars = map[string][]byte{
		"t-s-sites": makeTar(t, []tarEntry{
			{name: "site/private/backups/db.sql.gz", body: "DUMP"},
			{name: "site/private/files/a.pdf", body: "PDF"},
		}),
	}
	setTemplateVolumes(t, st, "t", []render.Volume{
		{Name: "sites", Backup: "s3; interval=24h", Exclude: []string{"*/private/backups/**"}},
	})

	id := store.NewBackupID()
	if err := svc.Backup(ctx, BackupRequest{BackupID: id, Host: "h", Template: "t", Slug: "s"}, nil); err != nil {
		t.Fatal(err)
	}

	b, err := st.GetBackup(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	v := b.Volumes[0]
	if v.Excluded == nil || v.Excluded.Entries != 1 {
		t.Fatalf("excluded = %+v, want 1 entry dropped", v.Excluded)
	}
	names := tarNames(t, blobBytes(t, svc, backupBlobKey("h", "t", "s", id, "t-s-sites")))
	for _, n := range names {
		if strings.Contains(n, "private/backups") {
			t.Fatalf("dropped path present in the blob: %v", names)
		}
	}
}

func TestBackup_noExcludeIsByteForByte(t *testing.T) {
	svc, fake, st := newBackupTestService(t)
	src := makeTar(t, []tarEntry{{name: "a.txt", body: "A"}, {name: "b.txt", body: "B"}})
	fake.ExportTars = map[string][]byte{"t-s-data": src}
	setTemplateVolumes(t, st, "t", []render.Volume{{Name: "data", Backup: "s3; interval=24h"}})

	id := store.NewBackupID()
	if err := svc.Backup(ctx, BackupRequest{BackupID: id, Host: "h", Template: "t", Slug: "s"}, nil); err != nil {
		t.Fatal(err)
	}
	got := blobBytes(t, svc, backupBlobKey("h", "t", "s", id, "t-s-data"))
	if !bytes.Equal(got, src) {
		t.Fatal("a volume with no exclude patterns must be stored byte-for-byte")
	}
	b, _ := st.GetBackup(ctx, id)
	if b.Volumes[0].Excluded != nil {
		t.Fatalf("unfiltered volume must record no exclusions: %+v", b.Volumes[0].Excluded)
	}
}
```

If `newBackupTestService`, `setTemplateVolumes` or `blobBytes` do not already exist, write them as small helpers in this test file using the existing fake podman client (`internal/podman/fake`) and memory store the neighbouring tests use. `makeTar`/`tarNames` come from Task 2's test file (same package).

- [ ] **Step 2: Run the tests to verify they fail**

Run: `go test -tags "containers_image_openpgp exclude_graphdriver_btrfs exclude_graphdriver_devicemapper" ./internal/instance/ -run 'TestBackup_exclude|TestBackup_noExclude' -v`
Expected: FAIL — `backupVolume` takes three arguments; nothing populates `Excluded`.

- [ ] **Step 3: Map declared patterns onto podman volume names**

In `internal/instance/backup.go`, inside `Backup`, replace the volume loop (currently at ~126-140):

```go
	vols, err := s.InstanceVolumes(ctx, req.Host, req.Template, req.Slug)
	if err != nil {
		restart()
		return fail(fmt.Errorf("list volumes: %w", err))
	}
	// Declared exclude patterns are keyed by the template's short volume name;
	// InstanceVolumes returns podman's full <template>-<slug>-<vol> names.
	// Resolve through the same volumeName() the pod manifest uses, so the two
	// cannot drift.
	tpl, err := s.lookup(ctx, req.Host, req.Template)
	if err != nil {
		restart()
		return fail(fmt.Errorf("lookup template: %w", err))
	}
	excludes := map[string][]string{}
	for _, v := range tpl.Meta.Volumes {
		if len(v.Exclude) > 0 {
			excludes[volumeName(req.Template, req.Slug, v.Name)] = v.Exclude
		}
	}
	var bvols []store.BackupVolume
	for _, v := range vols {
		bv, err := s.backupVolume(ctx, req, v.Name, excludes[v.Name])
		if err != nil {
			restart()
			return fail(fmt.Errorf("backup volume %q: %w", v.Name, err))
		}
		bvols = append(bvols, bv)
		step("export-volume", v.Name)
	}
```

- [ ] **Step 4: Branch inside `backupVolume`**

Replace the body of `backupVolume` (`internal/instance/backup.go:171`), keeping its existing doc comment and appending the new paragraph:

```go
// … existing doc comment …
//
// When patterns is non-empty the copy is not byte-for-byte: the tar is decoded,
// filtered and re-encoded in the same pass that builds the manifest (#248).
// With no patterns the original TeeReader copy runs unchanged, so every volume
// that has not opted in is bit-identical to before.
func (s *Service) backupVolume(ctx context.Context, req BackupRequest, name string, patterns []string) (store.BackupVolume, error) {
	rc, err := s.client.VolumeExport(ctx, req.Host, name)
	if err != nil {
		return store.BackupVolume{}, fmt.Errorf("export: %w", err)
	}
	defer rc.Close()

	w, err := s.blobs.Put(ctx, backupBlobKey(req.Host, req.Template, req.Slug, req.BackupID, name))
	if err != nil {
		return store.BackupVolume{}, fmt.Errorf("open blob: %w", err)
	}
	cw := &countingWriter{w: w}

	var (
		m     Manifest
		stats dropStats
	)
	if len(patterns) == 0 {
		m, err = buildManifest(io.TeeReader(rc, cw))
	} else {
		m, stats, err = filterTar(cw, rc, patterns)
	}
	if err != nil {
		_ = w.Abort()
		return store.BackupVolume{}, fmt.Errorf("read tar: %w", err)
	}
	if err := w.Commit(); err != nil {
		return store.BackupVolume{}, fmt.Errorf("commit blob: %w", err)
	}
	raw, err := json.Marshal(m)
	if err != nil {
		return store.BackupVolume{}, fmt.Errorf("marshal manifest: %w", err)
	}
	bv := store.BackupVolume{Name: name, SizeBytes: cw.n, Manifest: raw}
	if len(patterns) > 0 {
		bv.Excluded = &store.ExcludedPaths{
			Patterns: stats.Patterns, Entries: stats.Entries, Bytes: stats.Bytes,
		}
	}
	return bv, nil
}
```

Note `cw.n` now counts the *filtered* tar's bytes, which is correct: `size_bytes` describes the stored blob.

- [ ] **Step 5: Comment the three unfiltered call sites**

At `internal/instance/rename.go:219`, `internal/instance/service.go:1519` and `internal/instance/service.go:1537`, immediately above each `VolumeExport` call:

```go
	// Unfiltered on purpose: backup exclude patterns (#248) apply only to
	// backupVolume. This path removes the source once the copy lands, so a
	// dropped entry would have no second copy to recover from.
```

- [ ] **Step 6: Run the tests**

Run: `make test && make vet`
Expected: PASS

- [ ] **Step 7: Commit**

```bash
git add internal/instance/backup.go internal/instance/backup_test.go \
        internal/instance/rename.go internal/instance/service.go
git commit -m "feat(backup): apply per-volume exclude patterns to the snapshot tar (#248)"
```

---

### Task 5: Document the field

**Files:**
- Modify: the template-authoring doc that documents `volumes:` / `backup:` — find it with `grep -rln 'pre_backup' docs/ README.md`
- Modify: `CLAUDE.md` if it describes template meta fields

**Interfaces:**
- Consumes: everything above
- Produces: no code

- [ ] **Step 1: Find the authoring doc**

```bash
grep -rln "pre_backup" docs/ README.md CLAUDE.md
```

- [ ] **Step 2: Add the section**

Beside the existing `volumes:`/`backup:` documentation:

````markdown
#### `exclude:` — paths omitted from a volume's backup

A volume may declare glob patterns whose matching paths are left out of its
backup tar:

```yaml
volumes:
  - name: sites
    backup: "s3; interval=24h"
    exclude:
      - "*/private/backups/**"
```

`**` spans directory separators; patterns match each entry's path relative to
the volume root, and must be relative (no leading `/`, no `..` segment) or the
template is rejected at registration.

`*/private/backups/**` matches the directory's **contents**, so the directory
itself restores as an empty directory. A pattern naming the directory
(`*/private/backups`) drops it entirely — use that only when the application
recreates the directory itself.

Patterns apply to **backups only**. Instance rename, host migration and volume
copy always export everything, because those remove the source.

Each backup records the patterns applied and what they dropped, visible in
`GET /hosts/{host}/instances/{template}/{slug}/backups`:

```json
"excluded": {"patterns": ["*/private/backups/**"], "entries": 8, "bytes": 3627000000}
```

`"entries": 0` against a non-empty pattern list means the patterns matched
nothing — usually a typo. It does not fail the backup, because a new instance
legitimately has nothing to exclude yet.
````

- [ ] **Step 3: Commit**

```bash
git add docs/ CLAUDE.md README.md
git commit -m "docs: document per-volume backup exclude patterns (#248)"
```

---

### Task 6: Integration check on a real volume, then PR

**Files:** none (verification)

- [ ] **Step 1: Run the full suite and vet**

Run: `make test && make vet`
Expected: PASS, no output from vet.

- [ ] **Step 2: Cross-compile the integration test onto a fleet host**

`dev`'s podman is fine (5.8.4), but the integration suite expects a managed host — follow `docs/` integration-test instructions, or run against `engine-1`. Create a volume holding a `private/backups/` subtree, back it up with and without the pattern, restore both, and diff the trees.

Expected: the filtered restore is missing exactly the excluded subtree and identical everywhere else; the unfiltered blob is byte-identical to `podman volume export`.

- [ ] **Step 3: Open the PR**

```bash
git push -u origin feat/backup-volume-exclude-paths
forgejo pr create IoTReadyNext/podman-api \
  --title="feat(backup): per-volume exclude patterns (#248)" \
  --head=feat/backup-volume-exclude-paths --base=main \
  --body="Closes #248. Design: docs/superpowers/specs/2026-08-10-backup-volume-exclude-paths-design.md"
```

- [ ] **Step 4: After merge — tag and consume**

Tag an OSS release, push it to GitHub (only after the PR is merged on Forgejo), then in `podman-api-pro`:

```bash
make bump V=<new-tag> && make build && make test
```

Then add `exclude: ["*/private/backups/**"]` to the `sites` volume in
`templates/frappe-otp.yaml` and `templates/frappe-bbmeat.yaml` and re-register.
**Templates change last** — a template declaring `exclude:` against a core that
predates it is rejected by validation.

---

## Self-Review

**Spec coverage:**

| Spec section | Task |
|---|---|
| 1. Declaration (`Exclude` field, validation, sibling-of-`backup:` rationale) | 1 |
| 2. Directory-entry convention | 1 (doc comment), 2 (test), 5 (docs) |
| 3. The filter (unchanged empty path, header fidelity, hardlinks, cost) | 2, 4 |
| 4. Recording what was dropped | 3 |
| 5. Scope: backup path only + comments at the three other call sites | 4 |
| Testing (round-trip, filtering, hardlink, dir-vs-contents, accounting, validation, integration) | 1, 2, 4, 6 |
| Rollout | 6 |
| Naming hazard vs `excludePath` | Global constraints, 2 |

No gaps.

**Type consistency:** `dropStats` (Task 2) → `store.ExcludedPaths` (Task 3) → `api.ExcludedView` (Task 3); fields `Patterns`/`Entries`/`Bytes` identical across all three. `backupVolume`'s new fourth parameter (Task 4) matches the `excludes[v.Name]` type `[]string` from `render.Volume.Exclude` (Task 1). `newDropper` returns `(func(*tar.Header) bool, error)` and is consumed only by `filterTar`.

**Known soft spot:** Task 4's tests assume helper names (`newBackupTestService`, `setTemplateVolumes`, `blobBytes`) that may not exist under those names in `internal/instance/backup_test.go`. The step says to read the file and reuse whatever harness is there. This is the one place the implementer must adapt rather than transcribe.

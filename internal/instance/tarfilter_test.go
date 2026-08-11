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
	// "dir" ALSO keeps the directory entry — directory entries are never
	// dropped (Finding 1: dropping one makes the backup unrestorable,
	// because VolumeImport recreates it implicitly and the re-export then
	// carries a key the stored manifest lacks). Only the file underneath,
	// matched by the second pattern, is dropped.
	var drop bytes.Buffer
	if _, _, err := filterTar(&drop, bytes.NewReader(makeTar(t, entries)), []string{"*/private/backups", "*/private/backups/**"}); err != nil {
		t.Fatal(err)
	}
	got := tarNames(t, drop.Bytes())
	want := []string{"site/private/backups"}
	if len(got) != len(want) || got[0] != want[0] {
		t.Fatalf("entries = %v, want %v (the directory entry always survives)", got, want)
	}
}

func TestFilterTar_literalDirPatternNeverDropsTheDirectory(t *testing.T) {
	// A literal (non-"/**") pattern that matches a directory's own name
	// must NOT drop that directory entry — directory entries are never
	// dropped, full stop (Finding 1). The pattern has no globstar to reach
	// the nested file, so that entry survives too: this pattern is a no-op
	// against this tree.
	entries := []tarEntry{
		{name: "site/private/backups", typ: tar.TypeDir},
		{name: "site/private/backups/db.sql.gz", body: "D"},
	}
	var out bytes.Buffer
	if _, _, err := filterTar(&out, bytes.NewReader(makeTar(t, entries)), []string{"*/private/backups"}); err != nil {
		t.Fatal(err)
	}
	got := tarNames(t, out.Bytes())
	want := []string{"site/private/backups", "site/private/backups/db.sql.gz"}
	if len(got) != len(want) || got[0] != want[0] || got[1] != want[1] {
		t.Fatalf("entries = %v, want %v (a bare directory pattern is a no-op: dir and contents both ship)", got, want)
	}
}

// TestFilterTar_dirEntrySurvivesInManifestEvenWhenTargeted pins the
// motivating bug directly: with the directory itself targeted by a bare
// pattern, the manifest returned by filterTar must still contain the
// directory's own key. A re-export of the restored volume recreates the
// directory implicitly (VolumeImport), so any manifest missing this key
// would fail restoreVolume's firstDiff integrity check — after the
// instance has already been torn down for the restore.
func TestFilterTar_dirEntrySurvivesInManifestEvenWhenTargeted(t *testing.T) {
	entries := []tarEntry{
		{name: "site/private/backups", typ: tar.TypeDir},
		{name: "site/private/backups/db.sql.gz", body: "D"},
	}
	m, _, err := filterTar(io.Discard, bytes.NewReader(makeTar(t, entries)), []string{"*/private/backups"})
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := m["site/private/backups"]; !ok {
		t.Fatalf("manifest = %v, missing the directory entry key %q — a restore verify against a re-export would fail", m, "site/private/backups")
	}
}

func TestFilterTar_nestedDirUnderExcludedRootSurvives(t *testing.T) {
	// Directory entries are never dropped (Finding 1) — not just the
	// excluded root's own entry, but every directory beneath it too, even
	// though "*/private/backups/**" matches all of them. Only the file
	// content is actually removed.
	entries := []tarEntry{
		{name: "site/private/backups", typ: tar.TypeDir},
		{name: "site/private/backups/sub", typ: tar.TypeDir},
		{name: "site/private/backups/sub/f.txt", body: "F"},
	}
	var out bytes.Buffer
	if _, _, err := filterTar(&out, bytes.NewReader(makeTar(t, entries)), []string{"*/private/backups/**"}); err != nil {
		t.Fatal(err)
	}
	got := tarNames(t, out.Bytes())
	want := []string{"site/private/backups", "site/private/backups/sub"}
	if len(got) != len(want) || got[0] != want[0] || got[1] != want[1] {
		t.Fatalf("entries = %v, want %v (every directory entry survives; only the file drops)", got, want)
	}
}

func TestFilterTar_excludePathStillWrittenButOmittedFromManifest(t *testing.T) {
	// Locks the #142/#248 separation: a path excludePath() hides from the
	// fingerprint must still ship its bytes in the filtered tar when it is
	// not matched by any drop pattern. Regresses if a future parseTar edit
	// moves the excludePath check above the WriteHeader/body copy.
	src := makeTar(t, []tarEntry{
		{name: "db-litestream/x.wal", body: "WAL"},
		{name: "normal.txt", body: "N"},
	})
	var out bytes.Buffer
	m, _, err := filterTar(&out, bytes.NewReader(src), []string{"nothing/**"})
	if err != nil {
		t.Fatal(err)
	}
	got := tarNames(t, out.Bytes())
	found := false
	for _, n := range got {
		if n == "db-litestream/x.wal" {
			found = true
		}
	}
	if !found {
		t.Fatalf("entries = %v, want db-litestream/x.wal present in the output tar", got)
	}
	if _, ok := m["db-litestream/x.wal"]; ok {
		t.Fatal("db-litestream/x.wal must be excluded from the manifest (#142)")
	}
	if _, ok := m["normal.txt"]; !ok {
		t.Fatal("normal.txt missing from the manifest")
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

func TestFilterTar_globstarSuffixDoesNotDropBarePrefixOfAnyType(t *testing.T) {
	// Finding 2: doublestar.Match("logs/**", "logs") is true (zero-segment
	// "**"), so a naive check would drop a regular file or a symlink named
	// exactly "logs" as a side effect of a "logs/**" pattern meant to
	// target the CONTENTS of a directory named "logs". The zero-segment
	// rescue in newDropper must apply to every entry type, not just
	// directories, or a symlink named after the excluded directory gets
	// dropped while the directory itself (protected by Finding 1's rule)
	// survives — contradicting "symlinks are NOT resolved".
	entries := []tarEntry{
		{name: "logs", typ: tar.TypeReg, body: "L"},
	}
	var regOut bytes.Buffer
	if _, _, err := filterTar(&regOut, bytes.NewReader(makeTar(t, entries)), []string{"logs/**"}); err != nil {
		t.Fatal(err)
	}
	got := tarNames(t, regOut.Bytes())
	if len(got) != 1 || got[0] != "logs" {
		t.Fatalf("regular file: entries = %v, want the bare-named file to survive", got)
	}

	symEntries := []tarEntry{
		{name: "logs", typ: tar.TypeSymlink, link: "/var/log"},
	}
	var symOut bytes.Buffer
	if _, _, err := filterTar(&symOut, bytes.NewReader(makeTar(t, symEntries)), []string{"logs/**"}); err != nil {
		t.Fatal(err)
	}
	got = tarNames(t, symOut.Bytes())
	if len(got) != 1 || got[0] != "logs" {
		t.Fatalf("symlink: entries = %v, want the bare-named symlink to survive", got)
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

// countingReader wraps an io.Reader and tracks how many bytes were actually
// read out of it, so a test can assert a source stream was fully drained.
type countingReader struct {
	r io.Reader
	n int64
}

func (c *countingReader) Read(p []byte) (int, error) {
	n, err := c.r.Read(p)
	c.n += int64(n)
	return n, err
}

// TestFilterTar_drainsSourceOnSuccess guards against filterTar returning
// after tw.Close() without reading the rest of src: podman's HTTP response
// body must be fully drained for the caller's Close to release the
// connection for reuse (see buildManifest's doc comment). The source is
// padded to a 10240-byte record boundary — the blocking factor real tar
// producers use — so unread trailing padding is not masked by an already-
// exact archive/tar minimal encoding.
func TestFilterTar_drainsSourceOnSuccess(t *testing.T) {
	src := padTarToRecordBoundary(makeTar(t, []tarEntry{
		{name: "a.txt", body: "A"},
		{name: "b.txt", body: "B"},
	}))
	cr := &countingReader{r: bytes.NewReader(src)}
	if _, _, err := filterTar(io.Discard, cr, []string{"nothing/**"}); err != nil {
		t.Fatal(err)
	}
	if cr.n != int64(len(src)) {
		t.Fatalf("source not fully drained: read %d of %d bytes", cr.n, len(src))
	}
}

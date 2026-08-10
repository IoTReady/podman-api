package instance

import (
	"archive/tar"
	"fmt"
	"io"
	"path"
	"strings"

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
//
// A directory entry gets one extra rule. doublestar's "**" matches zero or
// more path segments, so a pattern like "dir/**" matches the literal name
// "dir" as well as everything under it — which would drop the directory
// entry itself, not just its contents. We want "dir/**" to mean "contents
// of dir", matching bash's globstar rather than doublestar's zero-segment
// reading: for a TypeDir header, a "/**"-suffixed pattern is checked against
// its own prefix first, and skipped (for that pattern only) when the
// directory's cleaned name is exactly what the prefix matches — i.e. the
// only reason it matched was the trailing globstar consuming nothing. A
// literal pattern with no "/**" suffix (e.g. "dir" itself) still drops the
// directory entry as expected.
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
			ok, _ := doublestar.Match(p, cleaned)
			if !ok {
				continue
			}
			if hdr.Typeflag == tar.TypeDir {
				if prefix, isGlobstar := strings.CutSuffix(p, "/**"); isGlobstar {
					if pOk, _ := doublestar.Match(prefix, cleaned); pOk {
						// Matched only because "**" consumed zero segments;
						// this pattern means "contents of", not the dir itself.
						continue
					}
				}
			}
			match = true
			break
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

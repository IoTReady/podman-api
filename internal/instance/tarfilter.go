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

// maxPromotableLinkTarget bounds how much of an excluded regular file's body
// dropFilter will buffer on the chance that a later, non-excluded hardlink
// still needs it (#250). Volumes routinely stream in the hundreds-of-MB-to-
// low-GB range (#223), so buffering every dropped file unconditionally risks
// real memory pressure on the control plane for the sake of a rare case
// (hardlink-based snapshot trees). Above this size, a surviving link to the
// target falls back to the pre-#250 behaviour: dropped, same as before this
// fix. That is a graceful degradation, not a bug — it only gives up
// promotion for content large enough that buffering it is itself a cost
// worth avoiding.
const maxPromotableLinkTarget = 64 * 1024 * 1024 // 64MiB

// maxTotalBufferedBytes bounds the SUM of every excluded regular file's body
// dropFilter is holding onto at once, across the whole tar stream — not just
// a single entry (that's maxPromotableLinkTarget's job). Without this, a
// backup with many excluded files each individually under the per-entry cap
// still accumulates all of that content in RAM for the duration of parseTar
// (#250 review finding 2), and a control plane running several such backups
// concurrently can hit real memory pressure. Volumes routinely stream in the
// hundreds-of-MB-to-low-GB range (#223); 512MiB sits below a single volume's
// typical size (so it can't itself dominate a backup's footprint) while
// still comfortably covering the common case (a handful of promotable
// hardlink targets, not thousands). Once buffering a newly-dropped entry
// would push the running total over this bound, that entry falls back to
// the pre-#250 behaviour instead: dropped, without a chance at promotion.
// That is a var, not a const, so tests can shrink it rather than construct
// gigabytes of tar data.
var maxTotalBufferedBytes int64 = 512 * 1024 * 1024 // 512MiB

// dropFilter compiles exclude patterns into a stateful, stream-order decision
// maker for filterTar/parseTar. It returns nil (not an error) when patterns
// is empty, so callers can test for "no filtering" with a nil check.
//
// dropFilter is single-use per tar stream and must not be reused across
// streams or called concurrently: decide and storeBody mutate its internal
// maps as entries are seen.
//
// Hardlinks: a tar TypeLink entry carries no body — it points at an earlier
// entry's inode. Keeping a link whose target was dropped produces an archive
// that fails to import, so decide drops a link whose Linkname names an
// already-dropped path — UNLESS the link itself survives every pattern (#250):
// in that case content no pattern named would otherwise vanish silently, so
// the FIRST such surviving link is promoted from TypeLink into TypeReg,
// carrying the target's actual bytes (buffered by the caller at drop time via
// storeBody, up to maxPromotableLinkTarget). Any LATER surviving link to the
// same target is not itself promoted — decide rewrites its Linkname to point
// at the promoted entry instead, so it stays a cheap TypeLink rather than
// duplicating the content. This relies on the target preceding its links in
// the stream, which is how tar encodes hardlinks; a link that somehow
// precedes its target is kept and would be a broken reference. That is the
// same shape as a pre-existing broken link in the source volume, and the
// import surfaces it rather than silently corrupting data.
//
// Symlinks are NOT resolved: a symlink is a name, and a dangling one is
// exactly what the source filesystem would have.
//
// A directory entry is NEVER dropped, regardless of which pattern matches
// it or how. VolumeImport recreates directories implicitly from the paths
// beneath them, so a tar that ships a file but omits its parent directory
// entry produces a re-export whose manifest carries a key the stored
// manifest lacks — restoreVolume's firstDiff then fails integrity
// verification on a backup that was actually fine, after the instance has
// already been torn down for the restore. A pattern can therefore empty a
// directory but never remove it; the cost is one 512-byte header per
// surviving directory, which is negligible next to the risk.
//
// Independently of that, doublestar's "**" matches zero or more path
// segments, so a pattern like "dir/**" matches the literal name "dir" as
// well as everything under it. We want "dir/**" to mean "contents of dir"
// only, matching bash's globstar rather than doublestar's zero-segment
// reading — for ANY entry type, not just directories (a regular file or a
// symlink named "logs" must not be swept up by a "logs/**" pattern aimed at
// a directory's contents). A "/**"-suffixed pattern is therefore checked
// against its own prefix first, and skipped (for that pattern only) when
// the entry's cleaned name is exactly what the prefix matches — i.e. the
// only reason it matched was the trailing globstar consuming nothing. A
// literal pattern with no "/**" suffix (e.g. "dir" itself) still matches a
// non-directory entry of that exact name as expected.
type dropFilter struct {
	patterns []string
	dropped  map[string]bool   // cleaned path -> true once that entry has been dropped
	promoted map[string]string // original (dropped) target path -> path of the entry now carrying its content
	bodies   map[string][]byte // original (dropped) target path -> buffered content, pending a possible promotion
	buffered int64             // sum of len(bodies[*]) currently held, bounded by maxTotalBufferedBytes
}

func newDropper(patterns []string) (*dropFilter, error) {
	if len(patterns) == 0 {
		return nil, nil
	}
	for _, p := range patterns {
		if !doublestar.ValidatePattern(p) {
			return nil, fmt.Errorf("invalid exclude pattern %q", p)
		}
	}
	return &dropFilter{
		patterns: patterns,
		dropped:  map[string]bool{},
		promoted: map[string]string{},
		bodies:   map[string][]byte{},
	}, nil
}

// matches reports whether cleaned is named by any pattern, applying the
// zero-segment "**" rescue and the "never drop a directory" rule documented
// on dropFilter.
func (f *dropFilter) matches(cleaned string, typ byte) bool {
	for _, p := range f.patterns {
		ok, _ := doublestar.Match(p, cleaned)
		if !ok {
			continue
		}
		if prefix, isGlobstar := strings.CutSuffix(p, "/**"); isGlobstar {
			if pOk, _ := doublestar.Match(prefix, cleaned); pOk {
				// Matched only because "**" consumed zero segments;
				// this pattern means "contents of", not the entry itself.
				continue
			}
		}
		if typ == tar.TypeDir {
			// Directory entries are never dropped (see doc comment).
			continue
		}
		return true
	}
	return false
}

// decide reports what parseTar should do with hdr, in stream order. It may
// mutate hdr in place:
//   - a link entry whose target was already promoted gets its Linkname
//     rewritten to the promoted entry's path;
//   - a link entry that is the first survivor of a dropped target is
//     rewritten from TypeLink into TypeReg, and promotedBody carries the
//     bytes parseTar must write as its content (hdr.Size is set to match).
//
// When drop is true, needsBuffer tells parseTar whether it must capture the
// entry's body (via storeBody) instead of discarding it, because a later
// link may still promote it.
//
// unwindStats reports how many previously-recorded dropStats entries/bytes
// parseTar must subtract because this decide call just promoted them: a
// promoted target's bytes ship intact in the output under the surviving
// link's name, so they were never actually excluded (#250 review finding 1)
// even though parseTar provisionally counted them as dropped when the
// target itself was seen, before it knew a later link would rescue it.
func (f *dropFilter) decide(hdr *tar.Header, cleaned string) (drop, needsBuffer bool, promotedBody []byte, unwindEntries int, unwindBytes int64) {
	match := f.matches(cleaned, hdr.Typeflag)

	if !match && hdr.Typeflag == tar.TypeLink {
		target := path.Clean(hdr.Linkname)
		if promotedPath, ok := f.promoted[target]; ok {
			// A later surviving link to an already-promoted target: point
			// it at the promoted entry instead of the dropped original.
			hdr.Linkname = promotedPath
		} else if f.dropped[target] {
			if body, ok := f.bodies[target]; ok {
				// First surviving link to a dropped target: promote it to
				// carry the content directly, instead of dropping it.
				hdr.Typeflag = tar.TypeReg
				hdr.Linkname = ""
				hdr.Size = int64(len(body))
				f.promoted[target] = cleaned
				promotedBody = body
				delete(f.bodies, target) // later links redirect via f.promoted now
				f.buffered -= int64(len(body))
				unwindEntries = 1
				unwindBytes = int64(len(body))
			} else {
				// Target's body was never buffered (not a regular file, over
				// maxPromotableLinkTarget, or the cumulative buffer cap was
				// already full) — fall back to dropping, the pre-#250
				// behaviour.
				match = true
			}
		}
	}

	if match {
		f.dropped[cleaned] = true
		needsBuffer = hdr.Typeflag == tar.TypeReg &&
			hdr.Size <= maxPromotableLinkTarget &&
			f.buffered+hdr.Size <= maxTotalBufferedBytes
	}
	return match, needsBuffer, promotedBody, unwindEntries, unwindBytes
}

// storeBody records the buffered body of a dropped regular file, keyed by
// its cleaned path, so a later surviving hardlink can promote it.
func (f *dropFilter) storeBody(cleaned string, body []byte) {
	f.bodies[cleaned] = body
	f.buffered += int64(len(body))
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
	io.Copy(io.Discard, src) //nolint:errcheck // drain so the caller's Close is clean
	return m, stats, nil
}

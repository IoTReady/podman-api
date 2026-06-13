package instance

import (
	"archive/tar"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"path"
	"sort"
	"strings"
)

// fileInfo is the content fingerprint of one tar entry. It deliberately omits
// mtime/uid/gid/mode so a volume compares equal across hosts that don't preserve
// those identically.
type fileInfo struct {
	typ    byte   // tar.Header.Typeflag
	size   int64  // regular files only
	sha256 string // hex sha256 of contents; regular files only
	link   string // symlink/hardlink target only
}

// Manifest fingerprints a volume's tar export, keyed by cleaned path.
type Manifest map[string]fileInfo

// fileInfoJSON is fileInfo's serialized form, used to persist a backup's
// manifest in the backups table (#66). Field set mirrors fileInfo exactly.
type fileInfoJSON struct {
	Type   byte   `json:"type"`
	Size   int64  `json:"size,omitempty"`
	Sha256 string `json:"sha256,omitempty"`
	Link   string `json:"link,omitempty"`
}

func (f fileInfo) MarshalJSON() ([]byte, error) {
	return json.Marshal(fileInfoJSON{Type: f.typ, Size: f.size, Sha256: f.sha256, Link: f.link})
}

func (f *fileInfo) UnmarshalJSON(b []byte) error {
	var j fileInfoJSON
	if err := json.Unmarshal(b, &j); err != nil {
		return err
	}
	*f = fileInfo{typ: j.Type, size: j.Size, sha256: j.Sha256, link: j.Link}
	return nil
}

// buildManifest parses an uncompressed tar stream (as produced by VolumeExport)
// into a Manifest. It always drains r to EOF — even after a parse error — so the
// caller's deferred Close releases the connection cleanly rather than on a
// half-read body. (This also keeps the function safe to feed from an in-process
// pipe, should a future caller tee the copy stream into it.)
func buildManifest(r io.Reader) (Manifest, error) {
	m := Manifest{}
	err := parseTar(r, m)
	io.Copy(io.Discard, r) //nolint:errcheck // best-effort drain (see doc comment)
	return m, err
}

func parseTar(r io.Reader, m Manifest) error {
	tr := tar.NewReader(r)
	for {
		hdr, err := tr.Next()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		fi := fileInfo{typ: hdr.Typeflag}
		switch hdr.Typeflag {
		case tar.TypeReg:
			h := sha256.New()
			n, err := io.Copy(h, tr)
			if err != nil {
				return err
			}
			fi.size = n
			fi.sha256 = hex.EncodeToString(h.Sum(nil))
		case tar.TypeSymlink, tar.TypeLink:
			fi.link = hdr.Linkname
		}
		cleaned := path.Clean(hdr.Name)
		if excludePath(cleaned) {
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

// firstDiff returns ("", true) when the two manifests are equal, otherwise
// (path, false) naming the first path (sorted) that is present on only one side
// or whose content differs. fileInfo is comparable, so == covers all fields.
func (m Manifest) firstDiff(other Manifest) (string, bool) {
	seen := map[string]bool{}
	var keys []string
	for k := range m {
		if !seen[k] {
			seen[k] = true
			keys = append(keys, k)
		}
	}
	for k := range other {
		if !seen[k] {
			seen[k] = true
			keys = append(keys, k)
		}
	}
	sort.Strings(keys)
	for _, k := range keys {
		a, oka := m[k]
		b, okb := other[k]
		if oka != okb || a != b {
			return k, false
		}
	}
	return "", true
}

// excludePath returns true for paths that should be excluded from volume
// integrity comparison. Litestream writes shadow WAL files into a
// <name>-litestream/ directory during SQLite shutdown. This happens in the
// brief window between pod stop and volume export, causing the source state
// captured during the copy phase to differ from the state re-exported during
// the verify phase. (#142)
func excludePath(name string) bool {
	for _, part := range strings.Split(name, "/") {
		if strings.HasSuffix(part, "-litestream") {
			return true
		}
	}
	return false
}

package instance

import (
	"archive/tar"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"path"
	"sort"
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

// buildManifest parses an uncompressed tar stream (as produced by VolumeExport)
// into a Manifest. It always drains r to EOF — even after a parse error — so a
// writer feeding r through a pipe can never block on a short read.
func buildManifest(r io.Reader) (Manifest, error) {
	m := Manifest{}
	err := parseTar(r, m)
	io.Copy(io.Discard, r) //nolint:errcheck // best-effort drain so a tee'd writer never blocks
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
		m[path.Clean(hdr.Name)] = fi
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

package podman

import (
	"errors"
	"fmt"

	"github.com/blang/semver/v4"
)

// MinPodmanVersion is the floor for managed hosts. Cold-copy migrate streams
// volumes through libpod's GET /volumes/{name}/export and volumes.Import,
// which first shipped in podman 5.6.0 — on older hosts the export 404s
// mid-migration. The preflight turns that into a clear setup error (#85).
const MinPodmanVersion = "5.6.0"

var minPodmanVersion = semver.MustParse(MinPodmanVersion)

// ErrHostVersionUnsupported marks a host whose podman is below
// MinPodmanVersion (or whose version cannot be parsed, which fails closed).
var ErrHostVersionUnsupported = errors.New("host podman version unsupported")

// checkVersion enforces MinPodmanVersion against a host-reported version
// string. Parsing is tolerant ("v" prefixes, partial versions) because podman
// builds report suffixed versions like "5.8.2-dev"; per semver, a pre-release
// of the floor (5.6.0-rc1) sorts below the floor and is rejected.
func checkVersion(hostID, version string) error {
	v, err := semver.ParseTolerant(version)
	if err != nil {
		return fmt.Errorf("host %q: cannot parse podman version %q (floor %s unconfirmed): %w",
			hostID, version, MinPodmanVersion, ErrHostVersionUnsupported)
	}
	if v.LT(minPodmanVersion) {
		return fmt.Errorf("host %q: podman %s < minimum %s (volume export/import requires >= %s): %w",
			hostID, version, MinPodmanVersion, MinPodmanVersion, ErrHostVersionUnsupported)
	}
	return nil
}

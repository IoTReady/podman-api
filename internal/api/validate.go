package api

import (
	"fmt"
	"net/http"

	"github.com/iotready/podman-api/internal/render"
)

// nameRe is the DNS-label-style allowlist used for slug, template-id, and
// secret-name path/body inputs. These strings flow into pod, secret, and
// volume names and into rendered YAML — anything outside this charset
// (newlines, YAML metacharacters, path separators, template delimiters)
// must be rejected at the edge before reaching the renderer or podman.
//
// Constraints:
//   - 2-40 characters
//   - lowercase ASCII letters, digits, hyphen
//   - must start and end with [a-z0-9]
//
// It aliases render.NameRe so the API edge and the template validator share
// one definition.
var nameRe = render.NameRe

func validName(s string) bool { return render.ValidName(s) }

// writeInvalidName writes a 400 invalid_parameters response describing
// which named field had an invalid value.
func writeInvalidName(w http.ResponseWriter, field, value string) {
	WriteJSON(w, http.StatusBadRequest, ErrorBody{
		Code: "invalid_parameters",
		Message: fmt.Sprintf(
			"%s %q is invalid: must match %s",
			field, value, nameRe.String(),
		),
	})
}

// validInstancePath validates the {template} and {slug} path params shared by
// most instance handlers. Returns true if both pass; otherwise writes the
// 400 response and returns false so the caller can return immediately.
func validInstancePath(w http.ResponseWriter, tmpl, slug string) bool {
	if !validName(tmpl) {
		writeInvalidName(w, "template", tmpl)
		return false
	}
	if !validName(slug) {
		writeInvalidName(w, "slug", slug)
		return false
	}
	return true
}

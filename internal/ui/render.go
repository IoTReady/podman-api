package ui

import (
	"bytes"
	"errors"
	"html/template"
	"log"
	"net/http"

	"github.com/iotready/podman-api/internal/instance"
)

// render writes block either wrapped in the layout (normal navigation) or bare
// (HTMX fragment, when HX-Request is set). data is shallow-augmented with the
// CSRF token used by the layout's hx-headers attribute.
//
// The block name is validated against the parsed template set up front: every
// caller passes a compile-time constant, so an unknown block is a programmer
// error, and validating turns it into a clean 500 instead of a partially-written
// 200. We render into a buffer and only write once execution fully succeeds, so
// a mid-template failure can't emit a partial body under a 200 status.
func (u *UI) render(w http.ResponseWriter, r *http.Request, block string, data map[string]any) {
	if data == nil {
		data = map[string]any{}
	}
	data["CSRF"] = csrfFromRequest(r)

	if u.tmpl.Lookup(block) == nil {
		log.Printf("ui: render: unknown template block %q", block)
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}

	var buf bytes.Buffer
	var err error
	if r.Header.Get("HX-Request") == "true" {
		err = u.tmpl.ExecuteTemplate(&buf, block, data)
	} else {
		// The layout renders a template named "body"; define it per-request to
		// delegate to the (validated) block. Clone so concurrent requests don't
		// race on redefining "body".
		var t *template.Template
		if t, err = u.tmpl.Clone(); err == nil {
			if _, err = t.New("body").Parse(`{{template "` + block + `" .}}`); err == nil {
				err = t.ExecuteTemplate(&buf, "layout", data)
			}
		}
	}
	if err != nil {
		log.Printf("ui: render %q: %v", block, err)
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	_, _ = w.Write(buf.Bytes())
}

// csrfFromRequest derives the CSRF token from the request's session cookie, or
// "" when unauthenticated.
func csrfFromRequest(r *http.Request) string {
	c, err := r.Cookie(sessionCookie)
	if err != nil {
		return ""
	}
	return csrfToken(c.Value)
}

// renderError writes an inline HTML error fragment with the mapped status.
func (u *UI) renderError(w http.ResponseWriter, err error) {
	status := errorStatus(err)
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.WriteHeader(status)
	_, _ = w.Write([]byte(`<div class="error">` + template.HTMLEscapeString(err.Error()) + `</div>`))
}

// errorStatus maps instance sentinel errors to HTTP status codes, mirroring the
// JSON API's taxonomy.
func errorStatus(err error) int {
	switch {
	case errors.Is(err, instance.ErrUnknownHost),
		errors.Is(err, instance.ErrUnknownTemplate),
		errors.Is(err, instance.ErrInstanceNotFound):
		return http.StatusNotFound
	case errors.Is(err, instance.ErrInstanceExists),
		errors.Is(err, instance.ErrHostDraining),
		errors.Is(err, instance.ErrPortConflict):
		return http.StatusConflict
	case errors.Is(err, instance.ErrHostSecretMissing):
		return http.StatusBadRequest
	default:
		return http.StatusInternalServerError
	}
}

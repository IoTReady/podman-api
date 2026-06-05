package ui

import (
	"bytes"
	"errors"
	"html/template"
	"log"
	"net/http"

	"github.com/iotready/podman-api/internal/instance"
	"github.com/iotready/podman-api/internal/render"
	"github.com/iotready/podman-api/internal/store"
)

// render writes block either wrapped in the layout (normal navigation) or bare
// (HTMX fragment, when HX-Request is set), with the given HTTP status. data is
// shallow-augmented with the CSRF token used by the layout's hx-headers
// attribute. Most callers pass http.StatusOK; error re-renders (e.g. a failed
// login or an invalid form) pass the appropriate 4xx.
//
// The block name is validated against the parsed template set up front: every
// caller passes a compile-time constant, so an unknown block is a programmer
// error, and validating turns it into a clean 500 instead of a partially-written
// response. We render into a buffer and only write once execution fully
// succeeds, and we set Content-Type + WriteHeader(status) before the body so the
// status and headers are always correct (writing them after WriteHeader is a
// silent no-op on a real ResponseWriter).
func (u *UI) render(w http.ResponseWriter, r *http.Request, status int, block string, data map[string]any) {
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
	w.WriteHeader(status)
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

// renderError renders the "error" block with the mapped status. Like render it
// honours HX-Request: an HTMX swap gets a bare error fragment (it lands in
// #main), while a full-page navigation gets the error wrapped in the layout
// chrome — including the sidebar (via pageData), since renderError is only
// reached from authenticated handlers.
func (u *UI) renderError(w http.ResponseWriter, r *http.Request, err error) {
	u.render(w, r, errorStatus(err), "error", u.pageData(map[string]any{"Error": err.Error()}))
}

// errorStatus maps instance/store/render sentinel errors to HTTP status codes,
// mirroring the JSON API's classify() taxonomy (internal/api/errors.go).
func errorStatus(err error) int {
	switch {
	case errors.Is(err, instance.ErrUnknownHost),
		errors.Is(err, instance.ErrUnknownTemplate),
		errors.Is(err, instance.ErrInstanceNotFound),
		errors.Is(err, store.ErrNotFound):
		return http.StatusNotFound
	case errors.Is(err, instance.ErrInstanceExists),
		errors.Is(err, instance.ErrPortConflict):
		return http.StatusConflict
	case errors.Is(err, instance.ErrHostDraining):
		return http.StatusLocked
	case errors.Is(err, instance.ErrHostSecretMissing):
		return http.StatusUnprocessableEntity
	case errors.Is(err, instance.ErrImagePull):
		return http.StatusBadGateway
	case errors.Is(err, instance.ErrStoreDisabled):
		return http.StatusNotImplemented
	case errors.Is(err, render.ErrInvalidParameters),
		errors.Is(err, instance.ErrSameHost),
		errors.Is(err, store.ErrSecretsNeedKey):
		return http.StatusBadRequest
	default:
		return http.StatusInternalServerError
	}
}

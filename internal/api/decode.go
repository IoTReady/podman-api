package api

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
)

// decodeBody decodes r.Body's JSON into v with DisallowUnknownFields, writing
// a 400 ErrorBody and returning false on failure. A mistyped or extra field
// (e.g. "volume" for "volumes") is otherwise silently ignored by the zero
// value it leaves behind, which can widen a request's effect in ways the
// caller never asked for (#253, #254) — so every handler that decodes a
// request body should route through this helper rather than calling
// json.NewDecoder(r.Body).Decode directly.
//
// An entirely absent body (io.EOF) is NOT an error here: it decodes to v's
// zero value and reports success, matching the historical behaviour of the
// plain Decode this replaces. Handlers that need a body are responsible for
// validating the zero value themselves (as they already do for required
// fields).
func decodeBody(w http.ResponseWriter, r *http.Request, v any) bool {
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	if err := dec.Decode(v); err != nil && !errors.Is(err, io.EOF) {
		WriteJSON(w, http.StatusBadRequest, ErrorBody{Code: "invalid_body", Message: err.Error()})
		return false
	}
	return true
}

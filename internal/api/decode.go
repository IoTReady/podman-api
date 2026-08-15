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
// zero value and reports success. This is a deliberate behaviour CHANGE from
// the plain json.NewDecoder(r.Body).Decode(v) every migrated call site used
// before: that decoder treated io.EOF as an error too, so every one of these
// handlers previously 400'd "invalid_body" on an empty request. Decoding to
// the zero value and letting the handler proceed means a handler that skips
// its own zero-value validation can now reach downstream logic with an
// under-specified request and answer with a different status/code than
// before (e.g. POST /evacuate with no body used to 400; now FromHost=="" and
// it 404s as "unknown_host"). Handlers that need a body are responsible for
// validating the zero value themselves — check each new call site rather
// than assuming this helper 400s an empty body for you.
func decodeBody(w http.ResponseWriter, r *http.Request, v any) bool {
	return decodeBodyErr(w, r, v, "invalid_body", "")
}

// decodeBodyErr is decodeBody with a caller-chosen error code and message
// prefix, for handlers whose bespoke error code/message predates this shared
// helper and is pinned by tests — e.g. postBackup's "invalid_request" code
// and "body must be a JSON object with an optional ..." message (#254 review
// finding 3).
func decodeBodyErr(w http.ResponseWriter, r *http.Request, v any, code, msgPrefix string) bool {
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	if err := dec.Decode(v); err != nil && !errors.Is(err, io.EOF) {
		WriteJSON(w, http.StatusBadRequest, ErrorBody{Code: code, Message: msgPrefix + err.Error()})
		return false
	}
	return true
}

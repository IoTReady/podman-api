package api

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
)

// decodeJSON is the shared low-level decode: DisallowUnknownFields, decoding
// r.Body's JSON into v. It does not write a response — callers classify the
// returned error (in particular, whether it is io.EOF, meaning the body was
// entirely absent) and respond accordingly.
func decodeJSON(r *http.Request, v any) error {
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	return dec.Decode(v)
}

// decodeBody decodes r.Body's JSON into v with DisallowUnknownFields, writing
// a 400 ErrorBody and returning false on any decode error — including an
// entirely absent body (io.EOF), which is rejected with "request body is
// required" rather than silently decoding to v's zero value. A mistyped or
// extra field (e.g. "volume" for "volumes") is otherwise silently ignored by
// the zero value it leaves behind, which can widen a request's effect in
// ways the caller never asked for (#253, #254) — so every handler that
// decodes a request body should route through this helper rather than
// calling json.NewDecoder(r.Body).Decode directly.
//
// Every one of the ~17 call sites this replaced a plain
// json.NewDecoder(r.Body).Decode(v) at (#254) independently turned out to
// need its own explicit "the zero value is not an acceptable request"
// rejection — evacuate, migrate, updateTemplate, and now applyInstance all
// needed a hand-added guard after an EOF-tolerant decodeBody let a bodyless
// request reach downstream logic that had no sane interpretation for it. That
// pattern repeating three review rounds in a row is a sign the tolerant
// behaviour should not have been the default: EOF-as-400 is now decodeBody's
// behaviour, and a handler that genuinely wants an absent body honored (only
// postBackup does, today) must opt in explicitly via decodeBodyErr. A
// handler can still return a more specific message for a "technically
// present but semantically empty" body (e.g. `{}`, or specific-field-absent)
// by checking the decoded zero value itself after decodeBody succeeds — that
// case is indistinguishable from EOF once decoded, so decodeBody cannot do
// it for you.
func decodeBody(w http.ResponseWriter, r *http.Request, v any) bool {
	err := decodeJSON(r, v)
	if err == nil {
		return true
	}
	if errors.Is(err, io.EOF) {
		WriteJSON(w, http.StatusBadRequest, ErrorBody{Code: "invalid_body", Message: "request body is required"})
		return false
	}
	WriteJSON(w, http.StatusBadRequest, ErrorBody{Code: "invalid_body", Message: err.Error()})
	return false
}

// decodeBodyErr is the EOF-tolerant primitive decodeBody is built on, with a
// caller-chosen error code and message prefix, for a handler whose bespoke
// error code/message predates the shared helper and is pinned by tests, or
// which genuinely wants an absent body to decode to v's zero value rather
// than be rejected — today, only postBackup's "invalid_request" code and
// "body must be a JSON object with an optional ..." message (#254 review
// finding 3), which treats an absent body as "back up everything".
func decodeBodyErr(w http.ResponseWriter, r *http.Request, v any, code, msgPrefix string) bool {
	if err := decodeJSON(r, v); err != nil && !errors.Is(err, io.EOF) {
		WriteJSON(w, http.StatusBadRequest, ErrorBody{Code: code, Message: msgPrefix + err.Error()})
		return false
	}
	return true
}

package api

import (
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestDecodeBody_AbsentBodyRejected pins decodeBody's default: an entirely
// absent body (io.EOF) is a 400, not a silent decode to v's zero value. This
// default exists so a future handler that forgets to add its own
// zero-value guard fails safe, rather than repeating the #254 review pattern
// (evacuate, migrate, updateTemplate, applyInstance each needed a hand-added
// guard against the previous EOF-tolerant default).
func TestDecodeBody_AbsentBodyRejected(t *testing.T) {
	r := httptest.NewRequest("POST", "/x", nil)
	w := httptest.NewRecorder()

	var v struct {
		Name string `json:"name"`
	}
	ok := decodeBody(w, r, &v)

	assert.False(t, ok)
	assert.Equal(t, 400, w.Code)
	assert.Contains(t, w.Body.String(), `"invalid_body"`)
}

// TestDecodeBody_EmptyJSONObjectDecodesToZeroValue confirms `{}` is NOT
// rejected by decodeBody itself — it decodes successfully to v's zero value,
// same as before. Distinguishing "meaningfully empty" from "absent" for a
// `{}` body remains each handler's own responsibility (decodeBody cannot
// tell them apart post-decode), which is why handlers still carry their own
// guards after this default flipped.
func TestDecodeBody_EmptyJSONObjectDecodesToZeroValue(t *testing.T) {
	r := httptest.NewRequest("POST", "/x", strings.NewReader("{}"))
	w := httptest.NewRecorder()

	var v struct {
		Name string `json:"name"`
	}
	ok := decodeBody(w, r, &v)

	require.True(t, ok)
	assert.Equal(t, "", v.Name)
}

// TestDecodeBodyErr_AbsentBodyStillTolerated confirms the low-level
// EOF-tolerant primitive decodeBody is built on is unchanged: postBackup
// relies on this to treat an absent body as "back up everything".
func TestDecodeBodyErr_AbsentBodyStillTolerated(t *testing.T) {
	r := httptest.NewRequest("POST", "/x", nil)
	w := httptest.NewRecorder()

	var v struct {
		Name string `json:"name"`
	}
	ok := decodeBodyErr(w, r, &v, "invalid_request", "")

	assert.True(t, ok)
	assert.Equal(t, "", v.Name)
}

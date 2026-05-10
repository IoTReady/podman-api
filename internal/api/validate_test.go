package api

import (
	"bytes"
	"net/http"
	"net/url"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestValidName_AcceptsAndRejects(t *testing.T) {
	good := []string{"ab", "postgres", "a1", "1a", "lite-engine", "abcdefghij0123456789abcdefghij0123456789"}
	for _, s := range good {
		assert.True(t, validName(s), "expected %q to be valid", s)
	}
	bad := []string{
		"",
		"a",                  // too short
		"-ab",                // leading dash
		"ab-",                // trailing dash
		"AB",                 // uppercase
		"ab_cd",              // underscore
		"ab.cd",              // dot
		"ab cd",              // space
		"ab\ncd",             // newline
		"ab/cd",              // slash
		"..",                 // path traversal
		"{{ab}}",             // template delimiter
		"ab*cd",              // glob
		"ab:cd",              // colon
		strings.Repeat("a", 41), // too long
	}
	for _, s := range bad {
		assert.False(t, validName(s), "expected %q to be invalid", s)
	}
}

// TestAPI_RejectsBadSlugAtEdge covers each handler that has a {slug} or
// {template} path param: a slug containing a forbidden character must
// produce 400 invalid_parameters before any service call.
func TestAPI_RejectsBadSlugAtEdge(t *testing.T) {
	srv, tok, _ := newSrvFull(t)

	cases := []struct {
		name   string
		method string
		path   string
		body   string
	}{
		{"get newline slug", "GET", "/hosts/h1/instances/app/" + url.PathEscape("ab\ncd"), ""},
		{"get colon slug", "GET", "/hosts/h1/instances/app/" + url.PathEscape("ab:cd"), ""},
		{"delete slash slug", "DELETE", "/hosts/h1/instances/app/" + url.PathEscape("ab/cd"), ""},
		{"start glob slug", "POST", "/hosts/h1/instances/app/" + url.PathEscape("a*b") + "/start", ""},
		// `..` is collapsed by Go's path-cleaning before reaching our handler — that's
		// equally safe (the request never matches the route). We assert 4xx, not 400.
		{"upgrade dotdot slug", "POST", "/hosts/h1/instances/app/" + url.PathEscape("..") + "/upgrade", `{"image":"i:2"}`},
		{"logs template tmpl injection", "GET", "/hosts/h1/instances/" + url.PathEscape("{{x}}") + "/ok/logs?container=app", ""},
		{"put bad template via path", "PUT", "/hosts/h1/instances/" + url.PathEscape("BAD") + "/ok", `{}`},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			var body *bytes.Buffer
			if c.body != "" {
				body = bytes.NewBufferString(c.body)
			} else {
				body = bytes.NewBuffer(nil)
			}
			req, err := http.NewRequest(c.method, srv.URL+c.path, body)
			require.NoError(t, err)
			req.Header.Set("Authorization", "Bearer "+tok)
			req.Header.Set("Content-Type", "application/json")
			resp, err := http.DefaultClient.Do(req)
			require.NoError(t, err)
			resp.Body.Close()
			// Either our 400 invalid_parameters or a 4xx from path-cleaning/routing
			// is acceptable — the request must not reach business logic.
			assert.GreaterOrEqual(t, resp.StatusCode, 400, "case %s", c.name)
			assert.Less(t, resp.StatusCode, 500, "case %s", c.name)
		})
	}
}

// TestAPI_RejectsBadSlugInBody covers the JSON body path: createInstance
// reads template and slug from the body, not the URL. Forbidden chars in
// the body must still produce 400 invalid_parameters.
func TestAPI_RejectsBadSlugInBody(t *testing.T) {
	srv, tok, _ := newSrvFull(t)
	body := `{"template":"app","slug":"a/b","parameters":{"slug":"a/b","image":"i"},"secrets":{"auth_secret":"s"}}`
	req, _ := http.NewRequest("POST", srv.URL+"/hosts/h1/instances", bytes.NewBufferString(body))
	req.Header.Set("Authorization", "Bearer "+tok)
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	resp.Body.Close()
	assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
}

// TestAPI_RejectsBadSecretName covers the PUT/DELETE secrets endpoints.
func TestAPI_RejectsBadSecretName(t *testing.T) {
	srv, tok, _ := newSrvFull(t)

	for _, name := range []string{"a", "AB", "..", "a/b", "a*b", "a\nb", "{{x}}"} {
		path := "/hosts/h1/secrets/" + url.PathEscape(name)
		req, _ := http.NewRequest("PUT", srv.URL+path, bytes.NewBufferString(`{"value":"v"}`))
		req.Header.Set("Authorization", "Bearer "+tok)
		req.Header.Set("Content-Type", "application/json")
		resp, err := http.DefaultClient.Do(req)
		require.NoError(t, err, "PUT name=%q", name)
		resp.Body.Close()
		assert.GreaterOrEqual(t, resp.StatusCode, 400, "PUT name=%q", name)
		assert.Less(t, resp.StatusCode, 500, "PUT name=%q", name)

		req, _ = http.NewRequest("DELETE", srv.URL+path, nil)
		req.Header.Set("Authorization", "Bearer "+tok)
		resp, err = http.DefaultClient.Do(req)
		require.NoError(t, err, "DELETE name=%q", name)
		resp.Body.Close()
		assert.GreaterOrEqual(t, resp.StatusCode, 400, "DELETE name=%q", name)
		assert.Less(t, resp.StatusCode, 500, "DELETE name=%q", name)
	}
}

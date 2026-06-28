package ui_test

import (
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/iotready/podman-api/internal/auth"
	"github.com/iotready/podman-api/internal/config"
	"github.com/iotready/podman-api/internal/ui"
)

func newTokenUI(t *testing.T) (*httptest.Server, *auth.TokenManager) {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "keys.yaml")
	hash, _ := config.HashToken("existingsecret")
	os.WriteFile(path, []byte("keys:\n  - id: existing\n    secret_hash: "+hash+"\n    scopes: [instances:read]\n    description: existing token\n"), 0o600)
	store := auth.NewKeyStore(nil)
	mgr := auth.NewTokenManager(path, store)

	u, err := ui.New(ui.Config{
		Auth: ui.AuthenticatorFunc(func(user, pass string) (ui.Identity, error) {
			return ui.Identity{Subject: "admin", Scopes: []string{"*"}}, nil
		}),
		TokenMgr: mgr,
	})
	if err != nil {
		t.Fatal(err)
	}
	return httptest.NewServer(u.Handler()), mgr
}

func loginAndGetSession(t *testing.T, srv *httptest.Server) *http.Cookie {
	t.Helper()
	jar := &singleCookieJar{}
	client := &http.Client{Jar: jar, CheckRedirect: func(req *http.Request, via []*http.Request) error { return http.ErrUseLastResponse }}
	resp, err := client.PostForm(srv.URL+"/ui/login", url.Values{"username": {"admin"}, "password": {"any"}})
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	return jar.cookie
}

// singleCookieJar captures the first Set-Cookie value.
type singleCookieJar struct{ cookie *http.Cookie }

func (j *singleCookieJar) SetCookies(_ *url.URL, cookies []*http.Cookie) {
	if j.cookie == nil && len(cookies) > 0 {
		j.cookie = cookies[0]
	}
}
func (j *singleCookieJar) Cookies(_ *url.URL) []*http.Cookie {
	if j.cookie == nil {
		return nil
	}
	return []*http.Cookie{j.cookie}
}

func TestTokensListPage(t *testing.T) {
	srv, _ := newTokenUI(t)
	defer srv.Close()
	session := loginAndGetSession(t, srv)

	req, _ := http.NewRequest("GET", srv.URL+"/ui/tokens", nil)
	req.AddCookie(session)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("want 200, got %d", resp.StatusCode)
	}
	body := readBody(t, resp)
	if !strings.Contains(body, "existing") {
		t.Error("existing token not shown in list")
	}
}

func TestTokensCreateAndRevoke(t *testing.T) {
	srv, mgr := newTokenUI(t)
	defer srv.Close()
	session := loginAndGetSession(t, srv)

	// GET a CSRF token.
	req, _ := http.NewRequest("GET", srv.URL+"/ui/tokens", nil)
	req.AddCookie(session)
	resp, _ := http.DefaultClient.Do(req)
	body := readBody(t, resp)
	csrf := extractCSRF(body)

	// POST to create a token.
	form := url.Values{
		"id":          {"newci"},
		"description": {"CI token"},
		"scopes":      {"instances:write"},
		"csrf_token":  {csrf},
	}
	req2, _ := http.NewRequest("POST", srv.URL+"/ui/tokens", strings.NewReader(form.Encode()))
	req2.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req2.AddCookie(session)
	resp2, err := http.DefaultClient.Do(req2)
	if err != nil {
		t.Fatal(err)
	}
	body2 := readBody(t, resp2)
	if resp2.StatusCode != http.StatusOK {
		t.Fatalf("create: want 200, got %d\n%s", resp2.StatusCode, body2)
	}
	if !strings.Contains(body2, "newci") {
		t.Error("created token id not in response")
	}
	// Body should contain the plaintext token (one-time reveal).
	// The token is base64url, 43 chars. We can't predict it, just confirm it's present.
	if !strings.Contains(body2, "Copy your token") && !strings.Contains(body2, "token") {
		t.Error("one-time token reveal not in response")
	}

	// Revoke it.
	keys, _ := mgr.List()
	var found bool
	for _, k := range keys {
		if k.ID == "newci" {
			found = true
		}
	}
	if !found {
		t.Fatal("token not created")
	}

	req3, _ := http.NewRequest("GET", srv.URL+"/ui/tokens", nil)
	req3.AddCookie(session)
	resp3, _ := http.DefaultClient.Do(req3)
	body3 := readBody(t, resp3)
	csrf2 := extractCSRF(body3)

	form2 := url.Values{"csrf_token": {csrf2}}
	req4, _ := http.NewRequest("POST", srv.URL+"/ui/tokens/newci/revoke", strings.NewReader(form2.Encode()))
	req4.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req4.AddCookie(session)
	noFollow := &http.Client{CheckRedirect: func(_ *http.Request, _ []*http.Request) error { return http.ErrUseLastResponse }}
	resp4, err := noFollow.Do(req4)
	if err != nil {
		t.Fatal(err)
	}
	resp4.Body.Close()
	// Expect redirect back to /ui/tokens.
	if resp4.StatusCode != http.StatusSeeOther {
		t.Fatalf("revoke: want 303, got %d", resp4.StatusCode)
	}
}

func readBody(t *testing.T, resp *http.Response) string {
	t.Helper()
	defer resp.Body.Close()
	b, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

func extractCSRF(body string) string {
	// Extract value from: <input type="hidden" name="csrf_token" value="...">
	const needle = `name="csrf_token" value="`
	idx := strings.Index(body, needle)
	if idx < 0 {
		return ""
	}
	rest := body[idx+len(needle):]
	end := strings.Index(rest, `"`)
	if end < 0 {
		return ""
	}
	return rest[:end]
}

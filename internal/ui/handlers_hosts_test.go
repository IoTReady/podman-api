package ui

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/iotready/podman-api/internal/config"
	"github.com/iotready/podman-api/internal/instance"
	"github.com/iotready/podman-api/internal/podman"
	"github.com/iotready/podman-api/internal/podman/fake"
	"github.com/iotready/podman-api/internal/render"
	"github.com/iotready/podman-api/internal/store"
)

// uiWithService builds a UI backed by a real instance.Service over the fake
// podman client, with one host "edge-1" and an authenticated operator.
func uiWithService(t *testing.T) *UI {
	t.Helper()
	fc := fake.New()
	hosts := []config.Host{{ID: "edge-1"}}
	svc := instance.NewService(fc, hosts)
	svc.SetStore(store.NewMemory())
	hash, _ := config.HashToken("pw")
	u, err := New(Config{
		Svc:  svc,
		Auth: NewOperatorAuthenticator(config.Operator{Username: "op", PasswordHash: hash}),
	})
	if err != nil {
		t.Fatal(err)
	}
	return u
}

// authedGet drives a GET through a real session.
func authedGet(t *testing.T, u *UI, path string) *httptest.ResponseRecorder {
	t.Helper()
	tok, _ := u.cfg.Sessions.Create(Identity{Subject: "op", Scopes: []string{"*"}})
	r := httptest.NewRequest("GET", path, nil)
	r.AddCookie(&http.Cookie{Name: sessionCookie, Value: tok})
	w := httptest.NewRecorder()
	u.Handler().ServeHTTP(w, r)
	return w
}

// newTestUIWithHosts builds a UI backed by a real instance.Service over the
// fake podman client, with one configured host per key in counts and that
// many running instances (pods of a single seeded template) on each. Used to
// exercise dashboard's per-host fan-out with differing instance counts.
func newTestUIWithHosts(t *testing.T, counts map[string]int) *UI {
	t.Helper()
	f := fake.New()
	hosts := make([]config.Host, 0, len(counts))
	for id := range counts {
		hosts = append(hosts, config.Host{ID: id})
	}
	tmpl := store.Template{
		Meta:   render.Meta{ID: "tmpl-a"},
		Body:   "apiVersion: v1\nkind: Pod\nmetadata:\n  name: tmpl-a\n",
		Origin: "seed",
	}
	mem := store.NewMemory()
	if err := mem.PutTemplate(context.Background(), tmpl); err != nil {
		t.Fatal(err)
	}
	svc := instance.NewService(f, hosts)
	svc.SetStore(mem)
	for host, n := range counts {
		for i := 0; i < n; i++ {
			slug := fmt.Sprintf("slug-%d", i)
			name := "tmpl-a-" + host + "-" + slug
			f.AddPod(host, podman.Pod{
				ID:   name,
				Name: name,
				Labels: map[string]string{
					"podman-api/template": "tmpl-a",
					"podman-api/slug":     slug,
				},
				Status: "Running",
			})
		}
	}
	hash, _ := config.HashToken("pw")
	u, err := New(Config{
		Svc:  svc,
		Auth: NewOperatorAuthenticator(config.Operator{Username: "op", PasswordHash: hash}),
	})
	if err != nil {
		t.Fatal(err)
	}
	return u
}

// withSession stamps req with a valid session cookie for an authenticated
// operator against u, mirroring authedGet's session setup for tests that
// invoke a handler method directly (bypassing the router/auth middleware).
func withSession(t *testing.T, u *UI, req *http.Request) {
	t.Helper()
	tok, err := u.cfg.Sessions.Create(Identity{Subject: "op", Scopes: []string{"*"}})
	if err != nil {
		t.Fatal(err)
	}
	req.AddCookie(&http.Cookie{Name: sessionCookie, Value: tok})
}

func TestDashboard_SummariesSortedAndComplete(t *testing.T) {
	// Build a UI whose Svc has 3 hosts (h3, h1, h2) with 2,0,1 instances.
	u := newTestUIWithHosts(t, map[string]int{"h3": 2, "h1": 0, "h2": 1})
	rec := httptest.NewRecorder()
	req := httptest.NewRequest("GET", "/ui", nil)
	req.Header.Set("HX-Request", "true") // fragment, simpler to assert
	withSession(t, u, req)
	u.dashboard(rec, req)

	body := rec.Body.String()
	// Sorted by host id: h1 before h2 before h3.
	i1, i2, i3 := strings.Index(body, "h1"), strings.Index(body, "h2"), strings.Index(body, "h3")
	if !(i1 >= 0 && i1 < i2 && i2 < i3) {
		t.Errorf("hosts not sorted: h1=%d h2=%d h3=%d", i1, i2, i3)
	}
	if !strings.Contains(body, "3 instance(s) across 3 host(s)") {
		t.Errorf("totals wrong; body:\n%s", body)
	}
}

func TestDashboardListsHosts(t *testing.T) {
	u := uiWithService(t)
	w := authedGet(t, u, "/ui")
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d", w.Code)
	}
	if !strings.Contains(w.Body.String(), "edge-1") {
		t.Error("dashboard should list host edge-1")
	}
}

func TestZeroHostsStillRendersShell(t *testing.T) {
	svc := instance.NewService(fake.New(), nil) // zero configured hosts
	svc.SetStore(store.NewMemory())
	hash, _ := config.HashToken("pw")
	u, err := New(Config{
		Svc:  svc,
		Auth: NewOperatorAuthenticator(config.Operator{Username: "op", PasswordHash: hash}),
	})
	if err != nil {
		t.Fatal(err)
	}
	w := authedGet(t, u, "/ui")
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d", w.Code)
	}
	if !strings.Contains(w.Body.String(), "Sign out") {
		t.Error("an authenticated page with zero hosts should still render the shell chrome")
	}
}

func TestHostInstancesUnknownHostIs404(t *testing.T) {
	u := uiWithService(t)
	w := authedGet(t, u, "/ui/hosts/does-not-exist")
	if w.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want 404 for unknown host", w.Code)
	}
	// A full-page authenticated error still renders the shell (sidebar hosts),
	// not a chrome-free naked error.
	if !strings.Contains(w.Body.String(), "edge-1") {
		t.Error("full-page error should keep the sidebar shell")
	}
}

func TestHostInstancesPageRenders(t *testing.T) {
	u := uiWithService(t)
	w := authedGet(t, u, "/ui/hosts/edge-1")
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d", w.Code)
	}
	body := w.Body.String()
	if !strings.Contains(body, "edge-1") {
		t.Error("host page should name the host")
	}
	if !strings.Contains(body, "Deploy") {
		t.Error("host page should have a Deploy action")
	}
}

func TestHostPageHasPollingFragmentAndFreshnessCue(t *testing.T) {
	u := uiWithService(t)
	w := authedGet(t, u, "/ui/hosts/edge-1")
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d", w.Code)
	}
	body := w.Body.String()
	// The data region polls the fragment endpoint.
	if !strings.Contains(body, `hx-get="/ui/hosts/edge-1/fragment"`) {
		t.Errorf("host page missing polling fragment hx-get\n%s", body)
	}
	if !strings.Contains(body, `hx-trigger="every 10s"`) {
		t.Errorf("host page missing 10s poll trigger\n%s", body)
	}
	// Freshness cue is present on a reachable host (edge-1 is up in the fake).
	if !strings.Contains(body, "updated") {
		t.Errorf("host page missing freshness cue\n%s", body)
	}
}

func TestHostInstancesFragmentReturnsBareBody(t *testing.T) {
	u := uiWithService(t)
	tok, _ := u.cfg.Sessions.Create(Identity{Subject: "op", Scopes: []string{"*"}})
	r := httptest.NewRequest("GET", "/ui/hosts/edge-1/fragment", nil)
	r.AddCookie(&http.Cookie{Name: sessionCookie, Value: tok})
	r.Header.Set("HX-Request", "true") // htmx poll
	w := httptest.NewRecorder()
	u.Handler().ServeHTTP(w, r)

	if w.Code != http.StatusOK {
		t.Fatalf("status = %d", w.Code)
	}
	body := w.Body.String()
	if strings.Contains(body, "<!DOCTYPE html>") {
		t.Errorf("fragment must not include the layout\n%s", body)
	}
	if !strings.Contains(body, `class="tbl"`) {
		t.Errorf("fragment should contain the instance table\n%s", body)
	}
}

func TestHostFetchTimeoutIsBounded(t *testing.T) {
	// Both the dashboard fan-out and the host page / fragment bound their
	// per-host live fetch with hostFetchTimeout so one cold/unreachable host
	// can't stall the render. We assert the bound is small; the renders
	// themselves are exercised by other tests. Guards against the bound being
	// removed or set too high.
	if hostFetchTimeout <= 0 || hostFetchTimeout > 10*time.Second {
		t.Fatalf("hostFetchTimeout = %s, want a small positive bound", hostFetchTimeout)
	}
}

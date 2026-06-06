package ui

import (
	"context"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"github.com/iotready/podman-api/internal/config"
	"github.com/iotready/podman-api/internal/instance"
	"github.com/iotready/podman-api/internal/podman"
	"github.com/iotready/podman-api/internal/podman/fake"
	"github.com/iotready/podman-api/internal/render"
	"github.com/iotready/podman-api/internal/store"
)

// uiWithSecretInstance builds a UI with a running "demo/main" instance whose
// template declares two per-instance secrets ("password" set, "apikey" unset).
// Returns the backing store so rotation tests can assert on persisted secrets.
func uiWithSecretInstance(t *testing.T) (*UI, *store.Memory) {
	t.Helper()
	fc := fake.New()
	hosts := []config.Host{{ID: "edge-1"}}
	fc.AddPod("edge-1", podman.Pod{
		Name:   "demo-main",
		Status: "Running",
		Containers: []podman.Container{
			{Name: "demo-main-app", Image: "demo:1", Status: "Running"},
		},
	})
	mem := store.NewMemory()
	_ = mem.PutTemplate(context.Background(), store.Template{Meta: render.Meta{
		ID:         "demo",
		Parameters: []render.ParamDef{{Name: "image", Required: true}},
		Secrets:    render.Secrets{PerInstance: []string{"password", "apikey"}},
	}})
	_ = mem.PutSpec(context.Background(), store.Spec{
		Host: "edge-1", Template: "demo", Slug: "main",
		Parameters: map[string]any{"image": "demo:1"},
		Secrets:    map[string]string{"password": "stored"}, // apikey unset
	})
	svc := instance.NewService(fc, hosts)
	svc.SetStore(mem)
	hash, _ := config.HashToken("pw")
	u, err := New(Config{
		Svc:  svc,
		Jobs: mem,
		Auth: NewOperatorAuthenticator(config.Operator{Username: "op", PasswordHash: hash}),
	})
	if err != nil {
		t.Fatal(err)
	}
	return u, mem
}

func TestInstanceDetailShowsManageSecretsWhenDeclared(t *testing.T) {
	u, _ := uiWithSecretInstance(t)
	body := authedGet(t, u, "/ui/hosts/edge-1/instances/demo/main").Body.String()
	if !strings.Contains(body, "/ui/hosts/edge-1/instances/demo/main/secrets") {
		t.Error("instance detail should link to the manage-secrets page when the template declares per-instance secrets")
	}
	if !strings.Contains(body, "Manage secrets") {
		t.Error("instance detail should show a 'Manage secrets' control")
	}
}

func TestInstanceDetailHidesManageSecretsWhenNoneDeclared(t *testing.T) {
	// uiWithStoredInstance's "demo" template declares no per-instance secrets.
	u := uiWithStoredInstance(t)
	body := authedGet(t, u, "/ui/hosts/edge-1/instances/demo/main").Body.String()
	if strings.Contains(body, "Manage secrets") {
		t.Error("instance detail must NOT show 'Manage secrets' when the template declares no per-instance secrets")
	}
}

func TestSecretsFormListsDeclaredNamesWithStatus(t *testing.T) {
	u, _ := uiWithSecretInstance(t)
	w := authedGet(t, u, "/ui/hosts/edge-1/instances/demo/main/secrets")
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d", w.Code)
	}
	body := w.Body.String()
	if !strings.Contains(body, `name="secret.password"`) || !strings.Contains(body, `name="secret.apikey"`) {
		t.Error("form should render a password input per declared per-instance secret")
	}
	if !strings.Contains(body, "(set)") || !strings.Contains(body, "(not set)") {
		t.Error("form should show set/not-set status (password set, apikey not set)")
	}
	if !strings.Contains(body, `hx-post="/ui/hosts/edge-1/instances/demo/main/secrets"`) {
		t.Error("the rotate form must POST (secrets must never ride a GET URL)")
	}
}

func TestSecretsFormUnknownHostIs404(t *testing.T) {
	u, _ := uiWithSecretInstance(t)
	w := authedGet(t, u, "/ui/hosts/nope/instances/demo/main/secrets")
	if w.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want 404", w.Code)
	}
}

func TestSecretsRotateRequiresCSRF(t *testing.T) {
	u, _ := uiWithSecretInstance(t)
	tok, _ := u.cfg.Sessions.Create(Identity{Subject: "op", Scopes: []string{"*"}})
	form := url.Values{"secret.password": {"new"}} // no csrf_token
	r := httptest.NewRequest("POST", "/ui/hosts/edge-1/instances/demo/main/secrets", strings.NewReader(form.Encode()))
	r.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	r.AddCookie(&http.Cookie{Name: sessionCookie, Value: tok})
	w := httptest.NewRecorder()
	u.Handler().ServeHTTP(w, r)
	if w.Code != http.StatusForbidden {
		t.Fatalf("status = %d, want 403 (missing CSRF)", w.Code)
	}
}

func TestSecretsRotatePersistsNewValue(t *testing.T) {
	u, mem := uiWithSecretInstance(t)
	w := authedPost(t, u, "/ui/hosts/edge-1/instances/demo/main/secrets",
		url.Values{"secret.password": {"rotated"}})
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", w.Code)
	}
	sp, err := mem.GetSpec(context.Background(), "edge-1", "demo", "main")
	if err != nil {
		t.Fatal(err)
	}
	if sp.Secrets["password"] != "rotated" {
		t.Errorf("password = %q, want rotated", sp.Secrets["password"])
	}
}

func TestSecretsRotateEmptyIsRejected(t *testing.T) {
	u, _ := uiWithSecretInstance(t)
	// authedPost adds csrf_token but no secret.* fields → nothing to rotate.
	w := authedPost(t, u, "/ui/hosts/edge-1/instances/demo/main/secrets", url.Values{})
	if w.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400 (no secrets to rotate)", w.Code)
	}
	if !strings.Contains(w.Body.String(), `name="secret.password"`) {
		t.Error("an empty submit should re-render the form, not drop it")
	}
}

func TestSecretsFormCorruptSpecDegrades(t *testing.T) {
	u, mem := uiWithSecretInstance(t)
	u.cfg.Svc.SetStore(corruptSpecStore{mem})
	body := authedGet(t, u, "/ui/hosts/edge-1/instances/demo/main/secrets").Body.String()
	if !strings.Contains(body, "manual cleanup") {
		t.Error("a corrupt/undecryptable spec should degrade to a cleanup notice")
	}
	if strings.Contains(body, `type="password"`) {
		t.Error("no rotate inputs should render for a corrupt spec")
	}
}

// corruptSpecStore makes GetSpec report a permanently-corrupt (malformed) row —
// distinct from the recoverable, wrong-key undecryptableSpecStore below.
type corruptSpecStore struct{ *store.Memory }

func (corruptSpecStore) GetSpec(context.Context, string, string, string) (store.Spec, error) {
	return store.Spec{}, store.ErrSpecCorrupt
}

func TestSecretsFormUndecryptableSpecDegrades(t *testing.T) {
	u, mem := uiWithSecretInstance(t)
	u.cfg.Svc.SetStore(undecryptableSpecStore{mem})
	body := authedGet(t, u, "/ui/hosts/edge-1/instances/demo/main/secrets").Body.String()
	if !strings.Contains(body, "manual cleanup") {
		t.Error("an undecryptable spec should degrade to a cleanup notice")
	}
	if strings.Contains(body, `type="password"`) {
		t.Error("no rotate inputs should render for an undecryptable spec")
	}
}

// undecryptableSpecStore makes GetSpec report a wrong/missing-key blob.
type undecryptableSpecStore struct{ *store.Memory }

func (undecryptableSpecStore) GetSpec(context.Context, string, string, string) (store.Spec, error) {
	return store.Spec{}, store.ErrSecretsUndecryptable
}

// TestSecretsFormHidesRotateWhenNoDeclaredSecrets: reaching the form for an
// instance whose template declares no per-instance secrets (e.g. a direct URL —
// the gated control wouldn't link here) shows an explanation and no Rotate button
// that could only ever 400.
func TestSecretsFormHidesRotateWhenNoDeclaredSecrets(t *testing.T) {
	u := uiWithStoredInstance(t) // "demo" template declares no per-instance secrets
	body := authedGet(t, u, "/ui/hosts/edge-1/instances/demo/main/secrets").Body.String()
	if strings.Contains(body, ">Rotate<") {
		t.Error("Rotate button should be hidden when the template declares no per-instance secrets")
	}
	if !strings.Contains(body, "declares no per-instance secrets") {
		t.Error("the form should explain there are no per-instance secrets to manage")
	}
}

// TestSecretsRotateIgnoresQueryStringSecret locks the body-only invariant (#99):
// a secret smuggled into the query string must be ignored (the handler reads
// r.PostForm), so the body — carrying only the CSRF token — is an empty rotation.
func TestSecretsRotateIgnoresQueryStringSecret(t *testing.T) {
	u, mem := uiWithSecretInstance(t)
	w := authedPost(t, u,
		"/ui/hosts/edge-1/instances/demo/main/secrets?secret.password=fromquery",
		url.Values{})
	if w.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400 (query-string secret ignored → empty submit)", w.Code)
	}
	if strings.Contains(w.Body.String(), "fromquery") {
		t.Error("a query-string secret value must never be reflected into the response")
	}
	sp, err := mem.GetSpec(context.Background(), "edge-1", "demo", "main")
	if err != nil {
		t.Fatal(err)
	}
	if sp.Secrets["password"] != "stored" {
		t.Errorf("password = %q, want unchanged 'stored' (a query-string secret must not rotate)", sp.Secrets["password"])
	}
}

// TestSecretsRotateFailureNeverEchoesSubmittedValue locks down the most
// security-sensitive branch: when a rotation fails and the form is re-rendered
// with the error, the submitted secret value must never appear in the response.
// A secret name the template doesn't declare makes RotateInstanceSecrets re-apply
// and render.Validate reject it ("unknown secret"), so secretsFormData still
// succeeds and the rotate-error re-render path runs.
func TestSecretsRotateFailureNeverEchoesSubmittedValue(t *testing.T) {
	u, _ := uiWithSecretInstance(t)
	w := authedPost(t, u, "/ui/hosts/edge-1/instances/demo/main/secrets",
		url.Values{"secret.bogus": {"leakcanary"}})
	if w.Code == http.StatusOK {
		t.Fatalf("status = %d, want a non-OK status for an invalid rotation", w.Code)
	}
	body := w.Body.String()
	if !strings.Contains(body, `name="secret.password"`) {
		t.Error("a rotate failure should re-render the form")
	}
	if strings.Contains(body, "leakcanary") {
		t.Error("a submitted secret value must never be echoed into the re-rendered form")
	}
}

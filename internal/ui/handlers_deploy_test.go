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

// uiWithStoredInstance builds a UI whose service has the desired-state store
// enabled and a "demo/main" instance both seeded as a running pod and persisted
// as a spec, so the image-only upgrade flow can resolve and prefill.
func uiWithStoredInstance(t *testing.T) *UI {
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
	_ = mem.PutTemplate(context.Background(), store.Template{
		Meta: render.Meta{
			ID: "demo",
			Parameters: []render.ParamDef{
				{Name: "image", Required: true},
			},
		},
	})
	_ = mem.PutSpec(context.Background(), store.Spec{
		Host: "edge-1", Template: "demo", Slug: "main",
		Parameters: map[string]any{"image": "demo:1"},
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
	return u
}

// uiWithTemplateMeta registers a single "demo" template described by meta and
// returns a UI wired to a memory store, so meta-driven form tests can vary the
// parameter contract without re-deriving the fixture each time.
func uiWithTemplateMeta(t *testing.T, meta render.Meta) *UI {
	t.Helper()
	fc := fake.New()
	hosts := []config.Host{{ID: "edge-1"}}
	mem := store.NewMemory()
	_ = mem.PutTemplate(context.Background(), store.Template{Meta: meta})
	svc := instance.NewService(fc, hosts)
	svc.SetStore(mem)
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

// uiWithTemplate registers a "demo" template (one required param "version", one
// per-instance secret "password") so the meta-driven form has fields to render.
func uiWithTemplate(t *testing.T) *UI {
	t.Helper()
	return uiWithTemplateMeta(t, render.Meta{
		ID:         "demo",
		Parameters: []render.ParamDef{{Name: "version", Required: true}},
		Secrets:    render.Secrets{PerInstance: []string{"password"}},
	})
}

func TestDeployFormRendersMetaFields(t *testing.T) {
	u := uiWithTemplate(t)
	w := authedGet(t, u, "/ui/hosts/edge-1/deploy")
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d", w.Code)
	}
	body := w.Body.String()
	if !strings.Contains(body, `name="param.version"`) {
		t.Error("required param 'version' should render an input")
	}
	if !strings.Contains(body, `name="secret.password"`) {
		t.Error("per-instance secret 'password' should render a password input")
	}
}

func TestDeployCreateMissingRequiredParamRerendersForm(t *testing.T) {
	u := uiWithTemplate(t)
	tok, _ := u.cfg.Sessions.Create(Identity{Subject: "op", Scopes: []string{"*"}})
	form := url.Values{
		csrfField:  {csrfToken(tok)},
		"template": {"demo"},
		"slug":     {"main"},
		// no param.version, no secret.password → validation must fail
	}
	r := httptest.NewRequest("POST", "/ui/hosts/edge-1/deploy", strings.NewReader(form.Encode()))
	r.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	r.AddCookie(&http.Cookie{Name: sessionCookie, Value: tok})
	w := httptest.NewRecorder()
	u.Handler().ServeHTTP(w, r)
	if w.Code == http.StatusOK || w.Code == http.StatusSeeOther {
		t.Fatalf("expected a non-success status for invalid deploy, got %d", w.Code)
	}
	if !strings.Contains(w.Body.String(), `name="param.version"`) {
		t.Error("form should re-render with its fields on validation error")
	}
}

func TestDeployFormUnknownHostIs404(t *testing.T) {
	u := uiWithTemplate(t)
	w := authedGet(t, u, "/ui/hosts/nope/deploy")
	if w.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want 404 for unknown host", w.Code)
	}
}

// TestUpgradeFormAlwaysAvailable verifies that the upgrade form is always
// accessible now that the store is always present (HasStore gating is removed).
func TestUpgradeFormAlwaysAvailable(t *testing.T) {
	u := uiWithStoredInstance(t)
	w := authedGet(t, u, "/ui/hosts/edge-1/instances/demo/main/upgrade")
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200 (upgrade always available)", w.Code)
	}
}

func TestUpgradeFormRendersImageField(t *testing.T) {
	u := uiWithStoredInstance(t)
	w := authedGet(t, u, "/ui/hosts/edge-1/instances/demo/main/upgrade")
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d", w.Code)
	}
	body := w.Body.String()
	if !strings.Contains(body, `name="image"`) {
		t.Error("upgrade form should have an image field")
	}
	if !strings.Contains(body, "demo:1") {
		t.Error("upgrade form should prefill the current image")
	}
	// Image-only: no param/secret inputs.
	if strings.Contains(body, `name="param.`) || strings.Contains(body, `name="secret.`) {
		t.Error("image-only upgrade form must not render param/secret inputs")
	}
}

func TestDeployFormPreservesTypedValuesOnTemplateSwitch(t *testing.T) {
	u := uiWithTemplate(t)
	w := authedGet(t, u, "/ui/hosts/edge-1/deploy?template=demo&slug=web&param.version=1.2.3&secret.password=hunter2")
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d", w.Code)
	}
	body := w.Body.String()
	if !strings.Contains(body, `value="1.2.3"`) {
		t.Error("typed param 'version' should be preserved across template switch")
	}
	if !strings.Contains(body, `value="hunter2"`) {
		t.Error("typed secret 'password' should be preserved across template switch")
	}
	if !strings.Contains(body, `value="web"`) {
		t.Error("typed slug should still round-trip")
	}
}

func TestDeployFormDropsValuesForFieldsNotInTemplate(t *testing.T) {
	u := uiWithTemplate(t)
	w := authedGet(t, u, "/ui/hosts/edge-1/deploy?template=demo&param.bogus=keepme")
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d", w.Code)
	}
	if strings.Contains(w.Body.String(), "keepme") {
		t.Error("a value for a field not declared by the selected template must not render")
	}
}

// defaultedParamUI is a UI whose "demo" template has a single param carrying a
// one-click default, for exercising the default-vs-typed precedence.
func defaultedParamUI(t *testing.T) *UI {
	return uiWithTemplateMeta(t, render.Meta{
		ID:         "demo",
		Parameters: []render.ParamDef{{Name: "version", Default: "9.9"}},
	})
}

// TestDeployFormTypedValueBeatsParamDefault locks in the precedence introduced by
// the #98 typed-parameter model: a one-click default shows on a fresh form, but a
// value the operator already typed wins over it across a template switch.
func TestDeployFormTypedValueBeatsParamDefault(t *testing.T) {
	u := defaultedParamUI(t)

	// Fresh form: the parameter default is shown.
	if body := authedGet(t, u, "/ui/hosts/edge-1/deploy?template=demo").Body.String(); !strings.Contains(body, `value="9.9"`) {
		t.Error("fresh form should show the parameter default")
	}

	// Typed value present: it wins over the default.
	body := authedGet(t, u, "/ui/hosts/edge-1/deploy?template=demo&param.version=1.2.3").Body.String()
	if !strings.Contains(body, `value="1.2.3"`) {
		t.Error("typed value should be rendered")
	}
	if strings.Contains(body, `value="9.9"`) {
		t.Error("typed value must win over the parameter default")
	}
}

// TestDeployFormClearedDefaultedFieldStaysEmpty guards the missing-vs-empty
// distinction: a field the operator deliberately cleared is submitted present
// but empty (hx-include sends param.version=), and must NOT silently revert to
// the default — otherwise a resubmit would re-apply a value the operator removed.
func TestDeployFormClearedDefaultedFieldStaysEmpty(t *testing.T) {
	u := defaultedParamUI(t)
	body := authedGet(t, u, "/ui/hosts/edge-1/deploy?template=demo&param.version=").Body.String()
	if !strings.Contains(body, `name="param.version"`) {
		t.Fatal("version field should still render")
	}
	if strings.Contains(body, `value="9.9"`) {
		t.Error("a cleared defaulted field must not revert to the default")
	}
}

// TestDeployFormDefaultedFieldShowsDefaultPlaceholder documents that a defaulted
// param advertises its default via the placeholder, so an empty (e.g. cleared)
// field communicates that deploying it empty applies the default — matching the
// apply path's render.ApplyDefaults behavior.
func TestDeployFormDefaultedFieldShowsDefaultPlaceholder(t *testing.T) {
	u := defaultedParamUI(t)
	body := authedGet(t, u, "/ui/hosts/edge-1/deploy?template=demo").Body.String()
	if !strings.Contains(body, `placeholder="default: 9.9"`) {
		t.Error("a defaulted param should advertise its default via the placeholder")
	}
}

// TestDeployFormSetsNoStore verifies rendered pages are non-cacheable, since they
// now re-populate typed per-instance secrets into the HTML.
func TestDeployFormSetsNoStore(t *testing.T) {
	u := uiWithTemplate(t)
	w := authedGet(t, u, "/ui/hosts/edge-1/deploy")
	if got := w.Header().Get("Cache-Control"); got != "no-store" {
		t.Errorf("Cache-Control = %q, want no-store", got)
	}
}

func TestDeployCreateErrorPreservesTypedValues(t *testing.T) {
	u := uiWithTemplate(t)
	tok, _ := u.cfg.Sessions.Create(Identity{Subject: "op", Scopes: []string{"*"}})
	form := url.Values{
		csrfField:         {csrfToken(tok)},
		"template":        {"demo"},
		"slug":            {"web"},
		"secret.password": {"hunter2"},
		// no param.version → validation fails, form re-renders
	}
	r := httptest.NewRequest("POST", "/ui/hosts/edge-1/deploy", strings.NewReader(form.Encode()))
	r.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	r.AddCookie(&http.Cookie{Name: sessionCookie, Value: tok})
	w := httptest.NewRecorder()
	u.Handler().ServeHTTP(w, r)
	if w.Code == http.StatusOK || w.Code == http.StatusSeeOther {
		t.Fatalf("expected a non-success status for invalid deploy, got %d", w.Code)
	}
	if !strings.Contains(w.Body.String(), `value="hunter2"`) {
		t.Error("typed secret should be preserved on a failed-deploy re-render")
	}
}

func TestUpgradeApplyMissingImageRerendersForm(t *testing.T) {
	u := uiWithStoredInstance(t)
	tok, _ := u.cfg.Sessions.Create(Identity{Subject: "op", Scopes: []string{"*"}})
	form := url.Values{csrfField: {csrfToken(tok)}} // no image
	r := httptest.NewRequest("POST", "/ui/hosts/edge-1/instances/demo/main/upgrade", strings.NewReader(form.Encode()))
	r.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	r.AddCookie(&http.Cookie{Name: sessionCookie, Value: tok})
	w := httptest.NewRecorder()
	u.Handler().ServeHTTP(w, r)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400 for upgrade with no image", w.Code)
	}
	if !strings.Contains(w.Body.String(), `name="image"`) {
		t.Error("upgrade form should re-render with the image field on error")
	}
}

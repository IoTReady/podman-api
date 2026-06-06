package ui

import (
	"context"
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

package instance

import (
	"context"
	"testing"

	"github.com/iotready/podman-api/internal/config"
	"github.com/iotready/podman-api/internal/podman"
	"github.com/iotready/podman-api/internal/podman/fake"
	"github.com/iotready/podman-api/internal/render"
	"github.com/iotready/podman-api/internal/store"
)

func newRefreshSvc(t *testing.T) (*Service, *fake.Fake) {
	t.Helper()
	fc := fake.New()
	svc := NewService(fc, []config.Host{{ID: "h1"}})
	mem := store.NewMemory()
	if err := mem.PutTemplate(context.Background(), store.Template{
		Meta: render.Meta{ID: "tmpl-a"},
		Body: "apiVersion: v1\nkind: Pod\nmetadata:\n  name: tmpl-a\n",
	}); err != nil {
		t.Fatal(err)
	}
	svc.SetStore(mem)
	fc.AddPod("h1", podman.Pod{
		ID:   "tmpl-a-h1-x",
		Name: "tmpl-a-h1-x",
		Labels: map[string]string{
			"podman-api/template": "tmpl-a",
			"podman-api/slug":     "x",
		},
		Status: "Running",
	})
	return svc, fc
}

func TestRefreshHostWarmsCache(t *testing.T) {
	svc, _ := newRefreshSvc(t)
	svc.EnableWarmInventory()

	if err := svc.RefreshHost(context.Background(), "h1"); err != nil {
		t.Fatalf("RefreshHost: %v", err)
	}

	obs, fresh, err := svc.ListAllInstancesWithMeta(context.Background(), "h1")
	if err != nil {
		t.Fatalf("ListAllInstancesWithMeta: %v", err)
	}
	if len(obs) != 1 || obs[0].Slug != "x" {
		t.Fatalf("obs = %+v", obs)
	}
	if !fresh.Reachable || !fresh.HasData {
		t.Fatalf("fresh = %+v", fresh)
	}
}

func TestRefreshHostUnknownHostErrors(t *testing.T) {
	svc, _ := newRefreshSvc(t)
	if err := svc.RefreshHost(context.Background(), "nope"); err == nil {
		t.Fatal("expected error for unknown host")
	}
}

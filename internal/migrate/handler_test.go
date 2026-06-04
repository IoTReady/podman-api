package migrate

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/iotready/podman-api/internal/config"
	"github.com/iotready/podman-api/internal/instance"
	"github.com/iotready/podman-api/internal/jobs"
	"github.com/iotready/podman-api/internal/podman"
	"github.com/iotready/podman-api/internal/podman/fake"
	"github.com/iotready/podman-api/internal/render"
	"github.com/iotready/podman-api/internal/store"
)

// pgFixture mirrors the instance package's postgres test template.
func pgFixture() config.Template {
	return config.Template{
		Meta: render.Meta{
			ID:         "postgres",
			Parameters: render.Parameters{Required: []string{"slug", "image", "port", "db", "user"}},
			Secrets:    render.Secrets{PerInstance: []string{"password"}},
			Volumes:    []render.Volume{{Name: "data"}},
		},
		Body:   "apiVersion: v1\nkind: Pod\nmetadata:\n  name: postgres-{{.slug}}\nspec:\n  containers:\n    - name: db\n      image: {{.image}}\n",
		Source: "postgres.yaml",
	}
}

func TestHandler_Run_MigratesAndRecordsSteps(t *testing.T) {
	ctx := context.Background()
	f := fake.New()
	mem := store.NewMemory()
	hosts := []config.Host{{ID: "h1", Addr: "unix", Socket: "/x"}, {ID: "h2", Addr: "unix", Socket: "/y"}}
	svc := instance.NewService(f, hosts, []config.Template{pgFixture()})
	svc.SetStore(mem)

	params := map[string]any{"slug": "db1", "image": "x", "port": 5432, "db": "d", "user": "u"}
	require.NoError(t, mem.PutSpec(ctx, store.Spec{
		Host: "h1", Template: "postgres", Slug: "db1",
		Parameters: params, Secrets: map[string]string{"password": "p"},
	}))
	f.AddPod("h1", podman.Pod{Name: "postgres-db1", Status: "Running"})

	args, _ := json.Marshal(instance.MigrateRequest{FromHost: "h1", ToHost: "h2", Template: "postgres", Slug: "db1"})
	job, err := mem.Enqueue(ctx, "migrate", args, "")
	require.NoError(t, err)

	h := &Handler{Svc: svc}
	jc := jobs.NewJobContext(mem, job.ID)
	require.NoError(t, h.Run(ctx, job, jc))

	// Migration happened: dest pod running.
	p, err := f.PodInspect(ctx, "h2", "postgres-db1")
	require.NoError(t, err)
	assert.Equal(t, "Running", p.Status)

	// Progress steps recorded on the job.
	got, err := mem.GetJob(ctx, job.ID)
	require.NoError(t, err)
	assert.NotEmpty(t, got.Steps)
}

func TestHandler_Run_BadArgs(t *testing.T) {
	h := &Handler{Svc: nil}
	job := store.Job{ID: "j1", Kind: "migrate", Args: []byte("{not json")}
	err := h.Run(context.Background(), job, jobs.NewJobContext(store.NewMemory(), "j1"))
	require.Error(t, err)
}

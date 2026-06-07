package backup

import (
	"archive/tar"
	"bytes"
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

// tarBytes builds an uncompressed tar (one regular file per map entry) for use
// as fake volume contents.
func tarBytes(t *testing.T, files map[string]string) []byte {
	t.Helper()
	var buf bytes.Buffer
	tw := tar.NewWriter(&buf)
	for name, content := range files {
		require.NoError(t, tw.WriteHeader(&tar.Header{
			Name: name, Mode: 0o644, Size: int64(len(content)), Typeflag: tar.TypeReg,
		}))
		_, err := tw.Write([]byte(content))
		require.NoError(t, err)
	}
	require.NoError(t, tw.Close())
	return buf.Bytes()
}

// pgTemplate is the postgres-shaped fixture: pod postgres-<slug>, one "data"
// volume, NO declared secrets (so the spec needs none — avoids key-less store
// issues and keeps these adapter tests focused on the job plumbing).
func pgTemplate() store.Template {
	return store.Template{
		Meta: render.Meta{
			ID: "postgres",
			Parameters: []render.ParamDef{
				{Name: "slug", Type: "string", Required: true},
				{Name: "image", Type: "string", Required: true},
			},
			Volumes: []render.Volume{{Name: "data", Backup: "none"}},
		},
		Body: `apiVersion: v1
kind: Pod
metadata:
  name: postgres-{{.slug}}
  labels:
    podman-api/template: postgres
    podman-api/slug: {{.slug}}
spec:
  containers:
    - name: db
      image: {{.image}}
`,
		Origin: "seed",
	}
}

// contentA is the seeded volume content for restore tests.
func contentA() map[string]string { return map[string]string{"f": "v1"} }

// seedSvc builds a runnable instance.Service over a fake podman client, a
// Memory store seeded with the postgres template + the postgres/a spec, a
// running pod postgres-a with a tar-bearing volume postgres-a-data, and a
// LocalDir blob store rooted at a temp dir. It returns the service and the
// backing pieces.
func seedSvc(t *testing.T) (*instance.Service, *fake.Fake, *store.Memory, *LocalDir) {
	t.Helper()
	hosts := []config.Host{{ID: "h1", Addr: "unix", Socket: "/x"}}
	f := fake.New()

	mem := store.NewMemory()
	require.NoError(t, mem.PutTemplate(context.Background(), pgTemplate()))
	require.NoError(t, mem.PutSpec(context.Background(), store.Spec{
		Host: "h1", Template: "postgres", Slug: "a",
		Parameters: map[string]any{"slug": "a", "image": "pg:16"},
	}))

	svc := instance.NewService(f, hosts)
	svc.SetStore(mem)

	f.AddPod("h1", podman.Pod{
		Name: "postgres-a", ID: "postgres-a", Status: "Running",
		Containers: []podman.Container{{Name: "postgres-a-db", Image: "pg:16", Status: "Running"}},
		Labels:     map[string]string{"podman-api/template": "postgres", "podman-api/slug": "a"},
	})
	f.SetVolumeData("h1", "postgres-a-data", tarBytes(t, contentA()))

	blob, err := NewLocalDir(t.TempDir())
	require.NoError(t, err)
	svc.SetBlobStore(blob)
	return svc, f, mem, blob
}

func TestHandler_RunDecodesArgsAndBacksUp(t *testing.T) {
	svc, _, mem, _ := seedSvc(t)
	ctx := context.Background()

	id := store.NewBackupID()
	args, err := json.Marshal(instance.BackupRequest{
		BackupID: id, Host: "h1", Template: "postgres", Slug: "a",
	})
	require.NoError(t, err)
	job, err := mem.Enqueue(ctx, "backup", args, "")
	require.NoError(t, err)

	h := &Handler{Svc: svc}
	require.NoError(t, h.Run(ctx, job, jobs.NewJobContext(mem, job.ID)))

	b, err := mem.GetBackup(ctx, id)
	require.NoError(t, err)
	assert.Equal(t, store.BackupComplete, b.State)
}

func TestHandler_BadArgsFails(t *testing.T) {
	h := &Handler{}
	err := h.Run(context.Background(), store.Job{Args: json.RawMessage(`{`)}, jobs.NewJobContext(store.NewMemory(), "j1"))
	assert.Error(t, err)
}

func TestRestoreHandler_RunRoundTrip(t *testing.T) {
	svc, f, mem, _ := seedSvc(t)
	ctx := context.Background()

	// Take a backup of content A.
	id := store.NewBackupID()
	require.NoError(t, svc.Backup(ctx, instance.BackupRequest{
		BackupID: id, Host: "h1", Template: "postgres", Slug: "a",
	}, nil))

	// Mutate the live volume so a successful restore is observable.
	f.SetVolumeData("h1", "postgres-a-data", tarBytes(t, map[string]string{"f": "v2", "extra": "x"}))

	args, err := json.Marshal(instance.RestoreRequest{BackupID: id})
	require.NoError(t, err)
	job, err := mem.Enqueue(ctx, "restore", args, "")
	require.NoError(t, err)

	h := &RestoreHandler{Svc: svc}
	require.NoError(t, h.Run(ctx, job, jobs.NewJobContext(mem, job.ID)))

	// Volume content restored byte-for-byte.
	assert.Equal(t, tarBytes(t, contentA()), f.VolumeData("h1", "postgres-a-data"))

	// Pod running again.
	p, err := f.PodInspect(ctx, "h1", "postgres-a")
	require.NoError(t, err)
	assert.Equal(t, "Running", p.Status)
}

func TestRestoreHandler_BadArgsFails(t *testing.T) {
	h := &RestoreHandler{}
	err := h.Run(context.Background(), store.Job{Args: json.RawMessage(`{`)}, jobs.NewJobContext(store.NewMemory(), "j1"))
	assert.Error(t, err)
}

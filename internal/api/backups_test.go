package api

import (
	"archive/tar"
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/iotready/podman-api/internal/auth"
	"github.com/iotready/podman-api/internal/backup"
	"github.com/iotready/podman-api/internal/config"
	"github.com/iotready/podman-api/internal/instance"
	"github.com/iotready/podman-api/internal/podman"
	"github.com/iotready/podman-api/internal/podman/fake"
	"github.com/iotready/podman-api/internal/render"
	"github.com/iotready/podman-api/internal/store"
)

// backupTarBytes builds a one-file uncompressed tar for fake volume contents.
func backupTarBytes(t *testing.T, name, content string) []byte {
	t.Helper()
	var buf bytes.Buffer
	tw := tar.NewWriter(&buf)
	require.NoError(t, tw.WriteHeader(&tar.Header{
		Name: name, Mode: 0o644, Size: int64(len(content)), Typeflag: tar.TypeReg,
	}))
	_, err := tw.Write([]byte(content))
	require.NoError(t, err)
	require.NoError(t, tw.Close())
	return buf.Bytes()
}

// backupTmpl is the postgres-shaped fixture: pod postgres-<slug>, one "data"
// volume. The template ID matches the rendered pod name prefix.
func backupTmpl() store.Template {
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

// newBackupSrv builds a backup-capable API server: one host h1, the postgres
// template + a deployed postgres/a instance (spec + running pod + tar-bearing
// volume on the fake), a LocalDir blob store at t.TempDir(), and the Memory
// store wired as the JobStore. Returns server, full token, fake, store, blob.
func newBackupSrv(t *testing.T) (*httptest.Server, string, *fake.Fake, *store.Memory, *backup.LocalDir) {
	t.Helper()
	tok := "t"
	hash, err := config.HashToken(tok)
	require.NoError(t, err)
	keys := []config.APIKey{{ID: "k", SecretHash: hash, Scopes: []string{"instances:*"}}}
	hosts := []config.Host{{ID: "h1", Addr: "unix", Socket: "/x"}}

	f := fake.New()
	mem := store.NewMemory()
	require.NoError(t, mem.PutTemplate(context.Background(), backupTmpl()))
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
	f.SetVolumeData("h1", "postgres-a-data", backupTarBytes(t, "f", "v1"))

	blob, err := backup.NewLocalDir(t.TempDir())
	require.NoError(t, err)
	svc.SetBlobStore(blob)

	srv := httptest.NewServer(NewRouter(svc, mem, auth.NewKeyStore(keys), nil, nil, nil))
	t.Cleanup(srv.Close)
	return srv, tok, f, mem, blob
}

func backupReq(t *testing.T, method, url, tok string) *http.Response {
	t.Helper()
	req, _ := http.NewRequest(method, url, nil)
	req.Header.Set("Authorization", "Bearer "+tok)
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	return resp
}

func TestAPI_PostBackup_EnqueuesAndReturnsIDs(t *testing.T) {
	srv, tok, _, mem, _ := newBackupSrv(t)
	ctx := context.Background()

	resp := backupReq(t, "POST", srv.URL+"/hosts/h1/instances/postgres/a/backup", tok)
	require.Equal(t, http.StatusAccepted, resp.StatusCode)
	var acc struct {
		JobID    string `json:"job_id"`
		BackupID string `json:"backup_id"`
	}
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&acc))
	require.NotEmpty(t, acc.JobID)
	require.NotEmpty(t, acc.BackupID)

	job, err := mem.GetJob(ctx, acc.JobID)
	require.NoError(t, err)
	assert.Equal(t, "backup", job.Kind)
	assert.Contains(t, string(job.Args), acc.BackupID)
}

func TestAPI_PostBackup_UnknownInstance404(t *testing.T) {
	srv, tok, _, _, _ := newBackupSrv(t)
	resp := backupReq(t, "POST", srv.URL+"/hosts/h1/instances/postgres/missing/backup", tok)
	require.Equal(t, http.StatusNotFound, resp.StatusCode)
	var body ErrorBody
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&body))
	assert.Equal(t, "instance_not_found", body.Code)
}

func TestAPI_ListBackups(t *testing.T) {
	srv, tok, _, mem, _ := newBackupSrv(t)
	ctx := context.Background()

	// Seed two complete rows for the same instance.
	for _, id := range []string{store.NewBackupID(), store.NewBackupID()} {
		require.NoError(t, mem.CreateBackup(ctx, store.Backup{
			ID: id, Host: "h1", Template: "postgres", Slug: "a",
		}))
		ok, err := mem.CompleteBackup(ctx, id, []store.BackupVolume{
			{Name: "postgres-a-data", SizeBytes: 42, Manifest: json.RawMessage(`{"secretmanifest":true}`)},
		})
		require.NoError(t, err)
		require.True(t, ok)
	}

	resp := backupReq(t, "GET", srv.URL+"/hosts/h1/instances/postgres/a/backups", tok)
	require.Equal(t, http.StatusOK, resp.StatusCode)
	raw, _ := io.ReadAll(resp.Body)

	// Manifests are internal verification metadata and must not be exposed.
	assert.NotContains(t, string(raw), "secretmanifest")
	assert.NotContains(t, string(raw), "manifest")

	var out struct {
		Backups []BackupView `json:"backups"`
	}
	require.NoError(t, json.Unmarshal(raw, &out))
	require.Len(t, out.Backups, 2)
	// Newest first: IDs are sortable; index 0 >= index 1.
	assert.True(t, out.Backups[0].ID > out.Backups[1].ID)
	assert.Equal(t, "complete", out.Backups[0].State)
	require.Len(t, out.Backups[0].Volumes, 1)
	assert.Equal(t, "postgres-a-data", out.Backups[0].Volumes[0].Name)
	assert.Equal(t, int64(42), out.Backups[0].Volumes[0].SizeBytes)
	assert.NotEmpty(t, out.Backups[0].Created)
	assert.NotEmpty(t, out.Backups[0].Finished)

	// ?limit=1 returns a single row.
	resp = backupReq(t, "GET", srv.URL+"/hosts/h1/instances/postgres/a/backups?limit=1", tok)
	require.Equal(t, http.StatusOK, resp.StatusCode)
	out.Backups = nil
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&out))
	require.Len(t, out.Backups, 1)
}

func TestAPI_PostRestore_Enqueues(t *testing.T) {
	srv, tok, _, mem, _ := newBackupSrv(t)
	ctx := context.Background()

	id := store.NewBackupID()
	require.NoError(t, mem.CreateBackup(ctx, store.Backup{ID: id, Host: "h1", Template: "postgres", Slug: "a"}))
	ok, err := mem.CompleteBackup(ctx, id, []store.BackupVolume{{Name: "postgres-a-data", SizeBytes: 1}})
	require.NoError(t, err)
	require.True(t, ok)

	resp := backupReq(t, "POST", srv.URL+"/backups/"+id+"/restore", tok)
	require.Equal(t, http.StatusAccepted, resp.StatusCode)
	var acc struct {
		JobID string `json:"job_id"`
	}
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&acc))
	require.NotEmpty(t, acc.JobID)
	job, err := mem.GetJob(ctx, acc.JobID)
	require.NoError(t, err)
	assert.Equal(t, "restore", job.Kind)
}

func TestAPI_PostRestore_NotRestorable422(t *testing.T) {
	srv, tok, _, mem, _ := newBackupSrv(t)
	ctx := context.Background()

	id := store.NewBackupID()
	// Creating state — not restorable.
	require.NoError(t, mem.CreateBackup(ctx, store.Backup{ID: id, Host: "h1", Template: "postgres", Slug: "a"}))

	resp := backupReq(t, "POST", srv.URL+"/backups/"+id+"/restore", tok)
	require.Equal(t, http.StatusUnprocessableEntity, resp.StatusCode)
	var body ErrorBody
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&body))
	assert.Equal(t, "backup_not_restorable", body.Code)
}

func TestAPI_PostRestore_Missing404(t *testing.T) {
	srv, tok, _, _, _ := newBackupSrv(t)
	resp := backupReq(t, "POST", srv.URL+"/backups/bk_nope/restore", tok)
	require.Equal(t, http.StatusNotFound, resp.StatusCode)
	var body ErrorBody
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&body))
	assert.Equal(t, "backup_not_found", body.Code)
}

func TestAPI_DeleteBackup_RemovesRowAndBlobs(t *testing.T) {
	srv, tok, _, mem, blob := newBackupSrv(t)
	ctx := context.Background()

	id := store.NewBackupID()
	require.NoError(t, mem.CreateBackup(ctx, store.Backup{ID: id, Host: "h1", Template: "postgres", Slug: "a"}))
	ok, err := mem.CompleteBackup(ctx, id, []store.BackupVolume{{Name: "postgres-a-data", SizeBytes: 1}})
	require.NoError(t, err)
	require.True(t, ok)

	// Commit a blob via LocalDir under this backup's key prefix.
	key := "h1/postgres/a/" + id + "/postgres-a-data.tar"
	w, err := blob.Put(ctx, key)
	require.NoError(t, err)
	_, err = w.Write([]byte("payload"))
	require.NoError(t, err)
	require.NoError(t, w.Commit())
	// Sanity: blob is readable.
	rc, err := blob.Get(ctx, key)
	require.NoError(t, err)
	rc.Close()

	resp := backupReq(t, "DELETE", srv.URL+"/backups/"+id, tok)
	require.Equal(t, http.StatusNoContent, resp.StatusCode)

	// Row gone.
	_, err = mem.GetBackup(ctx, id)
	assert.ErrorIs(t, err, store.ErrNotFound)
	// Blob gone.
	_, err = blob.Get(ctx, key)
	assert.Error(t, err)

	// Second DELETE → 404.
	resp = backupReq(t, "DELETE", srv.URL+"/backups/"+id, tok)
	require.Equal(t, http.StatusNotFound, resp.StatusCode)
	var body ErrorBody
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&body))
	assert.Equal(t, "backup_not_found", body.Code)
}

func TestAPI_DeleteBackup_RestoreInFlight409(t *testing.T) {
	srv, tok, _, mem, _ := newBackupSrv(t)
	ctx := context.Background()

	id := store.NewBackupID()
	require.NoError(t, mem.CreateBackup(ctx, store.Backup{ID: id, Host: "h1", Template: "postgres", Slug: "a"}))
	ok, err := mem.CompleteBackup(ctx, id, []store.BackupVolume{{Name: "postgres-a-data", SizeBytes: 1}})
	require.NoError(t, err)
	require.True(t, ok)

	// Enqueue (don't run) a restore job for this backup.
	args, err := json.Marshal(instance.RestoreRequest{BackupID: id})
	require.NoError(t, err)
	_, err = mem.Enqueue(ctx, "restore", args, "")
	require.NoError(t, err)

	resp := backupReq(t, "DELETE", srv.URL+"/backups/"+id, tok)
	require.Equal(t, http.StatusConflict, resp.StatusCode)
	var body ErrorBody
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&body))
	assert.Equal(t, "backup_busy", body.Code)
}

func TestAPI_Backup_ScopeEnforced(t *testing.T) {
	// A token with only instances:read cannot enqueue a backup (write scope).
	hash, err := config.HashToken("t")
	require.NoError(t, err)
	hosts := []config.Host{{ID: "h1", Addr: "unix", Socket: "/x"}}
	mem := store.NewMemory()
	require.NoError(t, mem.PutTemplate(context.Background(), backupTmpl()))
	require.NoError(t, mem.PutSpec(context.Background(), store.Spec{
		Host: "h1", Template: "postgres", Slug: "a",
		Parameters: map[string]any{"slug": "a", "image": "pg:16"},
	}))
	f := fake.New()
	svc := instance.NewService(f, hosts)
	svc.SetStore(mem)
	blob, err := backup.NewLocalDir(t.TempDir())
	require.NoError(t, err)
	svc.SetBlobStore(blob)

	keys := []config.APIKey{{ID: "ro", SecretHash: hash, Scopes: []string{"instances:read"}}}
	srv := httptest.NewServer(NewRouter(svc, mem, auth.NewKeyStore(keys), nil, nil, nil))
	t.Cleanup(srv.Close)

	resp := backupReq(t, "POST", srv.URL+"/hosts/h1/instances/postgres/a/backup", "t")
	assert.Equal(t, http.StatusForbidden, resp.StatusCode)
}

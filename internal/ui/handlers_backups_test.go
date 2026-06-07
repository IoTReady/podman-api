package ui

import (
	"archive/tar"
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"testing"

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
	if err := tw.WriteHeader(&tar.Header{
		Name: name, Mode: 0o644, Size: int64(len(content)), Typeflag: tar.TypeReg,
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := tw.Write([]byte(content)); err != nil {
		t.Fatal(err)
	}
	if err := tw.Close(); err != nil {
		t.Fatal(err)
	}
	return buf.Bytes()
}

// uiWithBackups builds a backup-capable UI: host edge-1, the postgres template +
// a deployed postgres/main instance (spec + running pod + tar-bearing volume on
// the fake), a LocalDir blob store, and the Memory store wired as the JobStore.
// Returns the UI and the Memory store so tests can seed/inspect rows and jobs.
func uiWithBackups(t *testing.T) (*UI, *store.Memory) {
	t.Helper()
	hosts := []config.Host{{ID: "edge-1"}}

	fc := fake.New()
	mem := store.NewMemory()
	if err := mem.PutTemplate(context.Background(), store.Template{
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
	}); err != nil {
		t.Fatal(err)
	}
	if err := mem.PutSpec(context.Background(), store.Spec{
		Host: "edge-1", Template: "postgres", Slug: "main",
		Parameters: map[string]any{"slug": "main", "image": "pg:16"},
	}); err != nil {
		t.Fatal(err)
	}

	svc := instance.NewService(fc, hosts)
	svc.SetStore(mem)

	fc.AddPod("edge-1", podman.Pod{
		Name: "postgres-main", ID: "postgres-main", Status: "Running",
		Containers: []podman.Container{{Name: "postgres-main-db", Image: "pg:16", Status: "Running"}},
		Labels:     map[string]string{"podman-api/template": "postgres", "podman-api/slug": "main"},
	})
	fc.SetVolumeData("edge-1", "postgres-main-data", backupTarBytes(t, "f", "v1"))

	blob, err := backup.NewLocalDir(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	svc.SetBlobStore(blob)

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

// seedCompleteBackup inserts a complete backup row for postgres/main and
// returns its id.
func seedCompleteBackup(t *testing.T, mem *store.Memory) string {
	t.Helper()
	ctx := context.Background()
	id := store.NewBackupID()
	if err := mem.CreateBackup(ctx, store.Backup{ID: id, Host: "edge-1", Template: "postgres", Slug: "main"}); err != nil {
		t.Fatal(err)
	}
	ok, err := mem.CompleteBackup(ctx, id, []store.BackupVolume{{Name: "postgres-main-data", SizeBytes: 1}})
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("CompleteBackup CAS failed")
	}
	return id
}

func TestUI_InstanceDetailShowsBackups(t *testing.T) {
	u, mem := uiWithBackups(t)
	id := seedCompleteBackup(t, mem)

	w := authedGet(t, u, "/ui/hosts/edge-1/instances/postgres/main")
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", w.Code)
	}
	body := w.Body.String()
	if !strings.Contains(body, id) {
		t.Errorf("detail should list backup id %q", id)
	}
	if !strings.Contains(body, "Back up now") {
		t.Error("detail should render the Back up now control")
	}
}

func TestUI_BackupNow(t *testing.T) {
	u, mem := uiWithBackups(t)

	w := authedAction(t, u, "/ui/hosts/edge-1/instances/postgres/main/backup")
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200\n%s", w.Code, w.Body.String())
	}
	if !strings.Contains(w.Body.String(), "Backup started") {
		t.Error("backup should re-render with a started notice")
	}

	jobs, err := mem.ListJobs(context.Background(), store.JobFilter{Kind: "backup"})
	if err != nil {
		t.Fatal(err)
	}
	if len(jobs) == 0 {
		t.Error("a job of kind backup should be enqueued")
	}
}

func TestUI_RestoreEnqueues(t *testing.T) {
	u, mem := uiWithBackups(t)
	id := seedCompleteBackup(t, mem)

	w := authedAction(t, u, "/ui/backups/"+id+"/restore")
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200\n%s", w.Code, w.Body.String())
	}
	if !strings.Contains(w.Body.String(), "Restore started") {
		t.Error("restore should re-render with a started notice")
	}

	jobs, err := mem.ListJobs(context.Background(), store.JobFilter{Kind: "restore"})
	if err != nil {
		t.Fatal(err)
	}
	if len(jobs) == 0 {
		t.Error("a job of kind restore should be enqueued")
	}
}

func TestUI_DeleteBackup(t *testing.T) {
	u, mem := uiWithBackups(t)
	id := seedCompleteBackup(t, mem)

	w := authedAction(t, u, "/ui/backups/"+id+"/delete")
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200\n%s", w.Code, w.Body.String())
	}
	if !strings.Contains(w.Body.String(), "deleted") {
		t.Error("delete should re-render with a deleted notice")
	}
	if _, err := mem.GetBackup(context.Background(), id); err == nil {
		t.Error("backup row should be gone after delete")
	}
}

func TestUI_DeleteBusyShowsError(t *testing.T) {
	u, mem := uiWithBackups(t)
	id := seedCompleteBackup(t, mem)

	// Enqueue (don't run) a restore job for this backup.
	args, err := json.Marshal(instance.RestoreRequest{BackupID: id})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := mem.Enqueue(context.Background(), "restore", args, ""); err != nil {
		t.Fatal(err)
	}

	w := authedAction(t, u, "/ui/backups/"+id+"/delete")
	if !strings.Contains(w.Body.String(), instance.ErrBackupBusy.Error()) {
		t.Errorf("delete of a busy backup should surface the busy error\n%s", w.Body.String())
	}
	if _, err := mem.GetBackup(context.Background(), id); err != nil {
		t.Error("busy backup row should still be present")
	}
}

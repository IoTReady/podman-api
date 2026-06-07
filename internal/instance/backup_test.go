package instance

import (
	"bytes"
	"context"
	"errors"
	"io"
	"io/fs"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/iotready/podman-api/internal/config"
	"github.com/iotready/podman-api/internal/podman"
	"github.com/iotready/podman-api/internal/podman/fake"
	"github.com/iotready/podman-api/internal/store"
)

// memBlob is an in-memory BlobStore: committed blobs land in data; aborted
// writes vanish. PutErr forces Put to fail.
type memBlob struct {
	mu     sync.Mutex
	data   map[string][]byte
	PutErr error
}

func newMemBlob() *memBlob { return &memBlob{data: map[string][]byte{}} }

func (m *memBlob) Put(_ context.Context, key string) (BlobWriter, error) {
	if m.PutErr != nil {
		return nil, m.PutErr
	}
	return &memBlobWriter{m: m, key: key}, nil
}

type memBlobWriter struct {
	m   *memBlob
	key string
	buf bytes.Buffer
}

func (w *memBlobWriter) Write(p []byte) (int, error) { return w.buf.Write(p) }
func (w *memBlobWriter) Commit() error {
	w.m.mu.Lock()
	defer w.m.mu.Unlock()
	w.m.data[w.key] = w.buf.Bytes()
	return nil
}
func (w *memBlobWriter) Abort() error { return nil }

func (m *memBlob) Get(_ context.Context, key string) (io.ReadCloser, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	b, ok := m.data[key]
	if !ok {
		return nil, fs.ErrNotExist
	}
	return io.NopCloser(bytes.NewReader(b)), nil
}

func (m *memBlob) DeleteAll(_ context.Context, prefix string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	for k := range m.data {
		if strings.HasPrefix(k, prefix+"/") || k == prefix {
			delete(m.data, k)
		}
	}
	return nil
}

func (m *memBlob) len() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.data)
}

// newBackupSvc seeds host "h1", the postgres-shaped "pg" template with one
// "data" volume, a deployed instance pg/a (spec + running pod + volume with
// real tar bytes), and wires an in-memory blob store. It returns the service,
// the fake, the memory store and the blob store.
func newBackupSvc(t *testing.T) (*Service, *fake.Fake, *store.Memory, *memBlob) {
	t.Helper()
	hosts := []config.Host{{ID: "h1", Addr: "unix", Socket: "/x"}}
	f := fake.New()

	tmpl := pgTemplate()
	tmpl.Meta.ID = "pg"
	svc, mem := newSvcWith(t, f, hosts, tmpl)

	// Stored desired-state spec for pg/a.
	require.NoError(t, mem.PutSpec(context.Background(), store.Spec{
		Host: "h1", Template: "pg", Slug: "a",
		Parameters: map[string]any{"slug": "a", "image": "pg:16", "port": 5432, "db": "d", "user": "u"},
	}))

	// Running pod pg-a with one container carrying an image ref.
	f.AddPod("h1", podman.Pod{
		Name: "pg-a", ID: "pg-a", Status: "Running",
		Containers: []podman.Container{{Name: "pg-a-db", Image: "pg:16", Status: "Running"}},
		Labels:     map[string]string{"podman-api/template": "pg", "podman-api/slug": "a"},
	})

	// Volume pg-a-data with real tar contents.
	f.SetVolumeData("h1", "pg-a-data", tarBytes(t, map[string]string{"PG_VERSION": "16", "base/1": "data"}))

	blob := newMemBlob()
	svc.SetBlobStore(blob)
	return svc, f, mem, blob
}

func newBackupReq() BackupRequest {
	return BackupRequest{BackupID: store.NewBackupID(), Host: "h1", Template: "pg", Slug: "a"}
}

func recordSteps(steps *[]string) func(string, string) {
	return func(step, _ string) { *steps = append(*steps, step) }
}

func TestBackup_HappyPath(t *testing.T) {
	svc, f, _, blob := newBackupSvc(t)
	ctx := context.Background()
	req := newBackupReq()

	var steps []string
	require.NoError(t, svc.Backup(ctx, req, recordSteps(&steps)))

	b, err := svc.store.GetBackup(ctx, req.BackupID)
	require.NoError(t, err)
	assert.Equal(t, store.BackupComplete, b.State)
	require.Len(t, b.Volumes, 1)
	assert.Equal(t, "pg-a-data", b.Volumes[0].Name)
	assert.Greater(t, b.Volumes[0].SizeBytes, int64(0))
	assert.NotEmpty(t, b.Volumes[0].Manifest)

	key := "h1/pg/a/" + req.BackupID + "/pg-a-data.tar"
	rc, err := blob.Get(ctx, key)
	require.NoError(t, err)
	_ = rc.Close()

	// Pod is running again.
	p, err := f.PodInspect(ctx, "h1", "pg-a")
	require.NoError(t, err)
	assert.Equal(t, "Running", p.Status)

	assert.Contains(t, steps, "stop")
	assert.Contains(t, steps, "export-volume")
}

func TestBackup_StoppedInstanceStaysStopped(t *testing.T) {
	svc, f, _, _ := newBackupSvc(t)
	ctx := context.Background()
	require.NoError(t, f.PodStop(ctx, "h1", "pg-a"))

	req := newBackupReq()
	require.NoError(t, svc.Backup(ctx, req, nil))

	b, err := svc.store.GetBackup(ctx, req.BackupID)
	require.NoError(t, err)
	assert.Equal(t, store.BackupComplete, b.State)

	p, err := f.PodInspect(ctx, "h1", "pg-a")
	require.NoError(t, err)
	assert.NotEqual(t, "Running", p.Status, "a stopped instance must stay stopped")
}

func TestBackup_ExportFailureMarksFailedRestartsAndCleansBlobs(t *testing.T) {
	svc, f, _, blob := newBackupSvc(t)
	ctx := context.Background()
	f.ExportErr = errors.New("export boom")

	req := newBackupReq()
	err := svc.Backup(ctx, req, nil)
	require.Error(t, err)

	b, gerr := svc.store.GetBackup(ctx, req.BackupID)
	require.NoError(t, gerr)
	assert.Equal(t, store.BackupFailed, b.State)

	assert.Equal(t, 0, blob.len(), "partial blobs must be cleaned up")

	p, perr := f.PodInspect(ctx, "h1", "pg-a")
	require.NoError(t, perr)
	assert.Equal(t, "Running", p.Status, "instance must be restarted after failure")
}

func TestBackup_UnknownHost(t *testing.T) {
	svc, _, _, _ := newBackupSvc(t)
	req := newBackupReq()
	req.Host = "nope"
	err := svc.Backup(context.Background(), req, nil)
	require.ErrorIs(t, err, ErrUnknownHost)
}

func TestBackup_NoSpec(t *testing.T) {
	svc, _, _, _ := newBackupSvc(t)
	req := newBackupReq()
	req.Slug = "ghost"
	err := svc.Backup(context.Background(), req, nil)
	require.ErrorIs(t, err, ErrInstanceNotFound)
}

func TestBackup_NoBlobStore(t *testing.T) {
	svc, _, _, _ := newBackupSvc(t)
	svc.SetBlobStore(nil)
	req := newBackupReq()
	err := svc.Backup(context.Background(), req, nil)
	require.ErrorIs(t, err, ErrBackupsDisabled)
}

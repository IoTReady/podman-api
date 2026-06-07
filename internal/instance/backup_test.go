package instance

import (
	"bytes"
	"context"
	"encoding/json"
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
	"github.com/iotready/podman-api/internal/render"
	"github.com/iotready/podman-api/internal/store"
)

// memBlob is an in-memory BlobStore: committed blobs land in data; aborted
// writes vanish. PutErr forces Put to fail. WriteErrAfter + WriteErr inject a
// write failure mid-stream (after WriteErrAfter bytes have been accepted).
type memBlob struct {
	mu            sync.Mutex
	data          map[string][]byte
	PutErr        error
	WriteErrAfter int   // 0 = no error injection
	WriteErr      error // error to return once WriteErrAfter bytes written
}

func newMemBlob() *memBlob { return &memBlob{data: map[string][]byte{}} }

func (m *memBlob) Put(_ context.Context, key string) (BlobWriter, error) {
	if m.PutErr != nil {
		return nil, m.PutErr
	}
	return &memBlobWriter{m: m, key: key}, nil
}

type memBlobWriter struct {
	m       *memBlob
	key     string
	buf     bytes.Buffer
	written int
}

func (w *memBlobWriter) Write(p []byte) (int, error) {
	if w.m.WriteErrAfter > 0 && w.m.WriteErr != nil && w.written >= w.m.WriteErrAfter {
		return 0, w.m.WriteErr
	}
	n, err := w.buf.Write(p)
	w.written += n
	return n, err
}
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

func TestBackup_BlobWriteFailureMarksFailedRestartsAndCleansBlobs(t *testing.T) {
	svc, f, _, blob := newBackupSvc(t)
	ctx := context.Background()

	// Inject a write error after the first byte — forces backupVolume to abort.
	blob.WriteErrAfter = 1
	blob.WriteErr = errors.New("disk full")

	req := newBackupReq()
	err := svc.Backup(ctx, req, nil)
	require.Error(t, err)

	b, gerr := svc.store.GetBackup(ctx, req.BackupID)
	require.NoError(t, gerr)
	assert.Equal(t, store.BackupFailed, b.State)

	assert.Equal(t, 0, blob.len(), "aborted blob must not appear in store")

	p, perr := f.PodInspect(ctx, "h1", "pg-a")
	require.NoError(t, perr)
	assert.Equal(t, "Running", p.Status, "instance must be restarted after failure")
}

// newBackupSvcTwoVols is newBackupSvc extended with a second "logs" volume so
// tests can exercise multi-volume cleanup paths.
func newBackupSvcTwoVols(t *testing.T) (*Service, *fake.Fake, *store.Memory, *memBlob) {
	t.Helper()
	hosts := []config.Host{{ID: "h1", Addr: "unix", Socket: "/x"}}
	f := fake.New()

	tmpl := pgTemplate()
	tmpl.Meta.ID = "pg"
	// Add a second volume declaration alongside the existing "data" volume.
	tmpl.Meta.Volumes = append(tmpl.Meta.Volumes, render.Volume{Name: "logs", Backup: "none"})
	svc, mem := newSvcWith(t, f, hosts, tmpl)

	require.NoError(t, mem.PutSpec(context.Background(), store.Spec{
		Host: "h1", Template: "pg", Slug: "a",
		Parameters: map[string]any{"slug": "a", "image": "pg:16", "port": 5432, "db": "d", "user": "u"},
	}))

	f.AddPod("h1", podman.Pod{
		Name: "pg-a", ID: "pg-a", Status: "Running",
		Containers: []podman.Container{{Name: "pg-a-db", Image: "pg:16", Status: "Running"}},
		Labels:     map[string]string{"podman-api/template": "pg", "podman-api/slug": "a"},
	})

	// Volume pg-a-data has valid tar; pg-a-logs will be made to fail via ExportReader.
	f.SetVolumeData("h1", "pg-a-data", tarBytes(t, map[string]string{"PG_VERSION": "16", "base/1": "data"}))
	f.SetVolumeData("h1", "pg-a-logs", tarBytes(t, map[string]string{"app.log": "ok"}))

	blob := newMemBlob()
	svc.SetBlobStore(blob)
	return svc, f, mem, blob
}

// contentA is the seeded volume content for restore tests.
func contentA() map[string]string { return map[string]string{"f": "v1"} }

// newRestoreSvc seeds host "h1", the unmodified postgres template (so the
// rendered pod name, the seeded pod, and waitRunning all agree on
// "postgres-<slug>"), a deployed instance postgres/a (spec WITH the
// per-instance password secret, running pod, one "data" volume with contentA),
// then runs a Backup so a complete restorable row exists. It returns the
// service, the fake, the memory store, the blob store and the backup id.
func newRestoreSvc(t *testing.T) (*Service, *fake.Fake, *store.Memory, *memBlob, string) {
	t.Helper()
	hosts := []config.Host{{ID: "h1", Addr: "unix", Socket: "/x"}}
	f := fake.New()
	svc, mem := newSvcWith(t, f, hosts, pgTemplate())

	require.NoError(t, mem.PutSpec(context.Background(), store.Spec{
		Host: "h1", Template: "postgres", Slug: "a",
		Parameters: map[string]any{"slug": "a", "image": "pg:16", "port": 5432, "db": "d", "user": "u"},
		Secrets:    map[string]string{"password": "p"},
	}))
	f.AddPod("h1", podman.Pod{
		Name: "postgres-a", ID: "postgres-a", Status: "Running",
		Containers: []podman.Container{{Name: "postgres-a-db", Image: "pg:16", Status: "Running"}},
		Labels:     map[string]string{"podman-api/template": "postgres", "podman-api/slug": "a"},
	})
	f.SetVolumeData("h1", "postgres-a-data", tarBytes(t, contentA()))

	blob := newMemBlob()
	svc.SetBlobStore(blob)

	req := BackupRequest{BackupID: store.NewBackupID(), Host: "h1", Template: "postgres", Slug: "a"}
	require.NoError(t, svc.Backup(context.Background(), req, nil))
	return svc, f, mem, blob, req.BackupID
}

func TestRestore_HappyPath(t *testing.T) {
	svc, f, mem, _, id := newRestoreSvc(t)
	ctx := context.Background()

	// Mutate the live volume to content B so a successful restore is observable.
	f.SetVolumeData("h1", "postgres-a-data", tarBytes(t, map[string]string{"f": "v2", "extra": "x"}))

	var steps []string
	require.NoError(t, svc.Restore(ctx, RestoreRequest{BackupID: id}, recordSteps(&steps)))

	// Volume content is back to A (byte-for-byte the backed-up tar).
	assert.Equal(t, tarBytes(t, contentA()), f.VolumeData("h1", "postgres-a-data"))

	// Pod is running again.
	p, err := f.PodInspect(ctx, "h1", "postgres-a")
	require.NoError(t, err)
	assert.Equal(t, "Running", p.Status)

	// Spec still present.
	_, err = mem.GetSpec(ctx, "h1", "postgres", "a")
	require.NoError(t, err)

	assert.Contains(t, steps, "teardown")
	assert.Contains(t, steps, "restore-volume")
	assert.Contains(t, steps, "apply")
	assert.Contains(t, steps, "verify")
}

func TestRestore_VerifyMismatchFails(t *testing.T) {
	svc, f, mem, blob, id := newRestoreSvc(t)
	ctx := context.Background()

	// Replace the complete row with one whose stored manifest disagrees with the
	// blob's actual content. Rows are immutable through the public API, so delete
	// and re-create: same id, same blob, but a manifest claiming a different
	// sha256 for file "f" — verification must reject it.
	orig, err := mem.GetBackup(ctx, id)
	require.NoError(t, err)
	require.NoError(t, mem.DeleteBackup(ctx, id))

	var m Manifest
	require.NoError(t, json.Unmarshal(orig.Volumes[0].Manifest, &m))
	fi := m["f"]
	fi.sha256 = "deadbeef" // corrupt the recorded digest
	m["f"] = fi
	bad, err := json.Marshal(m)
	require.NoError(t, err)

	require.NoError(t, mem.CreateBackup(ctx, store.Backup{
		ID: id, Host: orig.Host, Template: orig.Template, Slug: orig.Slug,
	}))
	ok, err := mem.CompleteBackup(ctx, id, []store.BackupVolume{
		{Name: orig.Volumes[0].Name, SizeBytes: orig.Volumes[0].SizeBytes, Manifest: bad},
	})
	require.NoError(t, err)
	require.True(t, ok)
	_ = blob // blob still holds the real content

	err = svc.Restore(ctx, RestoreRequest{BackupID: id}, nil)
	require.ErrorIs(t, err, ErrVolumeIntegrity)

	// Verification failed before Apply: the pod was torn down and never recreated.
	_, perr := f.PodInspect(ctx, "h1", "postgres-a")
	require.ErrorIs(t, perr, podman.ErrNotFound)
}

func TestRestore_NotComplete(t *testing.T) {
	svc, _, mem, _, id := newRestoreSvc(t)
	ctx := context.Background()

	// Drop the complete row, replace with a creating-state row of the same id.
	require.NoError(t, mem.DeleteBackup(ctx, id))
	require.NoError(t, mem.CreateBackup(ctx, store.Backup{
		ID: id, Host: "h1", Template: "postgres", Slug: "a",
	}))

	_, err := svc.CheckRestorable(ctx, id)
	require.ErrorIs(t, err, ErrBackupNotRestorable)
	err = svc.Restore(ctx, RestoreRequest{BackupID: id}, nil)
	require.ErrorIs(t, err, ErrBackupNotRestorable)
}

func TestRestore_MissingBlob(t *testing.T) {
	svc, _, _, blob, id := newRestoreSvc(t)
	ctx := context.Background()

	require.NoError(t, blob.DeleteAll(ctx, backupBlobPrefix("h1", "postgres", "a", id)))

	err := svc.Restore(ctx, RestoreRequest{BackupID: id}, nil)
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrBackupNotRestorable)
}

func TestRestore_MissingBackup(t *testing.T) {
	svc, _, _, _, _ := newRestoreSvc(t)
	err := svc.Restore(context.Background(), RestoreRequest{BackupID: "bk_nope"}, nil)
	require.ErrorIs(t, err, ErrBackupNotFound)
}

func TestRestore_InstanceGone(t *testing.T) {
	svc, _, mem, _, id := newRestoreSvc(t)
	ctx := context.Background()
	require.NoError(t, mem.DeleteSpec(ctx, "h1", "postgres", "a"))

	err := svc.Restore(ctx, RestoreRequest{BackupID: id}, nil)
	require.ErrorIs(t, err, ErrInstanceNotFound)
}

func TestRestore_DrainingHostRefusedUpfront(t *testing.T) {
	svc, f, _, _, id := newRestoreSvc(t)
	ctx := context.Background()
	svc.SetHosts([]config.Host{{ID: "h1", Addr: "unix", Socket: "/x", Drain: true}})

	err := svc.Restore(ctx, RestoreRequest{BackupID: id}, nil)
	require.ErrorIs(t, err, ErrHostDraining)

	// Refused before any teardown: the pod is untouched.
	p, perr := f.PodInspect(ctx, "h1", "postgres-a")
	require.NoError(t, perr)
	assert.Equal(t, "Running", p.Status)
}

func TestRestoreInFlight(t *testing.T) {
	ctx := context.Background()
	mem := store.NewMemory()

	args, err := json.Marshal(RestoreRequest{BackupID: "bk_X"})
	require.NoError(t, err)
	job, err := mem.Enqueue(ctx, "restore", args, "")
	require.NoError(t, err)

	in, err := RestoreInFlight(ctx, mem, "bk_X")
	require.NoError(t, err)
	assert.True(t, in)

	in, err = RestoreInFlight(ctx, mem, "bk_other")
	require.NoError(t, err)
	assert.False(t, in)

	// Claim then finish (terminal) — no longer in flight.
	claimed, ok, err := mem.ClaimNext(ctx)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, job.ID, claimed.ID)
	require.NoError(t, mem.Finish(ctx, job.ID, store.JobFailed, "x"))

	in, err = RestoreInFlight(ctx, mem, "bk_X")
	require.NoError(t, err)
	assert.False(t, in)
}

func TestBackup_SecondVolumeFailureCleansFirstVolumeBlob(t *testing.T) {
	svc, f, _, blob := newBackupSvcTwoVols(t)
	ctx := context.Background()

	boom := errors.New("logs export failed")
	// ExportReader lets us fail only the second volume by name.
	f.ExportReader = func(host, name string) io.ReadCloser {
		if name == "pg-a-logs" {
			return &midStreamReader{err: boom}
		}
		// First volume: serve the real seeded bytes.
		data := f.VolumeData(host, name)
		return io.NopCloser(bytes.NewReader(data))
	}

	req := newBackupReq()
	err := svc.Backup(ctx, req, nil)
	require.Error(t, err)

	b, gerr := svc.store.GetBackup(ctx, req.BackupID)
	require.NoError(t, gerr)
	assert.Equal(t, store.BackupFailed, b.State)

	// The first volume's committed blob must have been cleaned up by DeleteAll.
	assert.Equal(t, 0, blob.len(), "all blobs including the first volume must be cleaned up")

	p, perr := f.PodInspect(ctx, "h1", "pg-a")
	require.NoError(t, perr)
	assert.Equal(t, "Running", p.Status, "instance must be restarted after failure")
}

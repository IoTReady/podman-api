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
	// pgTemplate's Body hardcodes "postgres" (pod name, template label) rather
	// than templating off the Meta.ID above — harmless for the other consumers
	// of this fixture, which never call Apply/PlayKube, but a Restore (which
	// does) would render a pod named "postgres-a" while every other lookup
	// here (AddPod, VolumeRemove/Create, waitRunning) expects "pg-a" derived
	// from Meta.ID. Keep the rendered body consistent with the overridden ID.
	tmpl.Body = strings.ReplaceAll(tmpl.Body, "postgres", "pg")
	svc, mem := newSvcWith(t, f, hosts, tmpl)

	// Stored desired-state spec for pg/a. Secrets carries the template's
	// required "password" so a Restore (which re-applies the spec through
	// validation) succeeds — newBackupSvc was originally Backup-only fixture
	// and never needed this until Restore started using it too.
	require.NoError(t, mem.PutSpec(context.Background(), store.Spec{
		Host: "h1", Template: "pg", Slug: "a",
		Parameters: map[string]any{"slug": "a", "image": "pg:16", "port": 5432, "db": "d", "user": "u"},
		Secrets:    map[string]string{"password": "p"},
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

// TestBackup_DegradedInstanceRestarts verifies that an instance in Degraded
// state (some containers up, some crashed) is treated as running: wasRunning
// must be true so the instance is restarted after a successful backup rather
// than left fully stopped, which would be a downgrade from its pre-backup state.
func TestBackup_DegradedInstanceRestarts(t *testing.T) {
	svc, f, _, _ := newBackupSvc(t)
	ctx := context.Background()

	// Mutate the seeded pod to Degraded status (one container crashed).
	p, err := f.PodInspect(ctx, "h1", "pg-a")
	require.NoError(t, err)
	p.Status = "Degraded"
	f.AddPod("h1", p)

	req := newBackupReq()
	require.NoError(t, svc.Backup(ctx, req, nil))

	b, err := svc.store.GetBackup(ctx, req.BackupID)
	require.NoError(t, err)
	assert.Equal(t, store.BackupComplete, b.State)

	// The instance must be running again — Degraded counts as "was running".
	pod, err := f.PodInspect(ctx, "h1", "pg-a")
	require.NoError(t, err)
	assert.Equal(t, "Running", pod.Status, "Degraded instance must be restarted after backup")
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
	// pgTemplate() already declares "logs" as backup: none (vetoed, never
	// exported) — this fixture needs a SECOND volume that IS exported so its
	// mid-export failure is reachable, so replace that declaration in place
	// with a real marker rather than appending a duplicate — a real template
	// cannot declare the same volume name twice.
	for i, v := range tmpl.Meta.Volumes {
		if v.Name == "logs" {
			tmpl.Meta.Volumes[i].Backup = "s3; interval=24h"
		}
	}
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

// TestRestore_MissingBlobLeavesExistingVolumeIntact: a restore may never delete
// a volume it has no copy of. The blob is opened BEFORE the remove/create, so a
// missing blob fails without touching the live content — the alternative leaves
// the instance down with the volume recreated empty and its contents gone for
// good.
func TestRestore_MissingBlobLeavesExistingVolumeIntact(t *testing.T) {
	svc, f, _, blob, id := newRestoreSvc(t)
	ctx := context.Background()

	// Live content diverged from the backup since it was taken; it is the only
	// copy of these bytes once the blob is gone.
	live := tarBytes(t, map[string]string{"f": "v2", "extra": "x"})
	f.SetVolumeData("h1", "postgres-a-data", live)

	require.NoError(t, blob.DeleteAll(ctx, backupBlobPrefix("h1", "postgres", "a", id)))

	err := svc.Restore(ctx, RestoreRequest{BackupID: id}, nil)
	require.ErrorIs(t, err, ErrBackupNotRestorable)

	assert.Equal(t, live, f.VolumeData("h1", "postgres-a-data"),
		"a missing blob must not destroy the volume it cannot restore")
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

func TestBackup_RunsPreBackupBeforeStop(t *testing.T) {
	svc, f, _, _ := newBackupSvc(t)
	ctx := context.Background()
	tmpl, err := svc.store.GetTemplate(ctx, "pg")
	require.NoError(t, err)
	tmpl.Meta.PreBackup = &render.PreBackup{Container: "db", Command: "echo dump {{.slug}}"}
	require.NoError(t, svc.store.PutTemplate(ctx, tmpl))

	var steps []string
	require.NoError(t, svc.Backup(ctx, newBackupReq(), recordSteps(&steps)))

	require.Len(t, f.ExecCalls, 1)
	assert.Equal(t, "pg-a-db", f.ExecCalls[0].Container)
	assert.Contains(t, strings.Join(f.ExecCalls[0].Cmd, " "), "echo dump a")
	assert.Less(t, indexOf(steps, "pre-backup"), indexOf(steps, "stop"))
}

func TestBackup_PreBackupNonZeroExitFailsBeforeStop(t *testing.T) {
	svc, f, _, _ := newBackupSvc(t)
	ctx := context.Background()
	tmpl, err := svc.store.GetTemplate(ctx, "pg")
	require.NoError(t, err)
	tmpl.Meta.PreBackup = &render.PreBackup{Container: "db", Command: "false"}
	require.NoError(t, svc.store.PutTemplate(ctx, tmpl))
	f.ExecFunc = func(host, container string, cmd []string) (podman.ExecResult, error) {
		return podman.ExecResult{ExitCode: 1, Output: "dump failed"}, nil
	}

	req := newBackupReq()
	var steps []string
	err = svc.Backup(ctx, req, recordSteps(&steps))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "pre-backup")
	assert.NotContains(t, steps, "stop")
	assert.NotContains(t, steps, "export-volume")
	p, _ := f.PodInspect(ctx, "h1", "pg-a")
	assert.Equal(t, "Running", p.Status)
	b, _ := svc.store.GetBackup(ctx, req.BackupID)
	assert.Equal(t, store.BackupFailed, b.State)
}

func TestBackup_PreBackupTransportErrorFailsBeforeStop(t *testing.T) {
	svc, f, _, _ := newBackupSvc(t)
	ctx := context.Background()
	tmpl, err := svc.store.GetTemplate(ctx, "pg")
	require.NoError(t, err)
	tmpl.Meta.PreBackup = &render.PreBackup{Container: "db", Command: "bench backup"}
	require.NoError(t, svc.store.PutTemplate(ctx, tmpl))
	f.ExecFunc = func(host, container string, cmd []string) (podman.ExecResult, error) {
		return podman.ExecResult{}, errors.New("connection refused")
	}

	req := newBackupReq()
	var steps []string
	err = svc.Backup(ctx, req, recordSteps(&steps))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "pre-backup: exec in")
	assert.NotContains(t, steps, "stop")
	assert.NotContains(t, steps, "export-volume")
	p, _ := f.PodInspect(ctx, "h1", "pg-a")
	assert.Equal(t, "Running", p.Status)
	b, _ := svc.store.GetBackup(ctx, req.BackupID)
	assert.Equal(t, store.BackupFailed, b.State)
}

// indexOf returns the position of v in s, or len(s) if absent.
func indexOf(s []string, v string) int {
	for i, x := range s {
		if x == v {
			return i
		}
	}
	return len(s)
}

func TestBackupInFlight(t *testing.T) {
	ctx := context.Background()
	mem := store.NewMemory()

	args, err := json.Marshal(BackupRequest{BackupID: "bk_X", Host: "h1", Template: "pg", Slug: "a"})
	require.NoError(t, err)
	job, err := mem.Enqueue(ctx, "backup", args, "")
	require.NoError(t, err)

	in, err := BackupInFlight(ctx, mem, "bk_X")
	require.NoError(t, err)
	assert.True(t, in)

	in, err = BackupInFlight(ctx, mem, "bk_other")
	require.NoError(t, err)
	assert.False(t, in)

	// Claim then finish (terminal) — no longer in flight.
	claimed, ok, err := mem.ClaimNext(ctx)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, job.ID, claimed.ID)
	require.NoError(t, mem.Finish(ctx, job.ID, store.JobFailed, "x"))

	in, err = BackupInFlight(ctx, mem, "bk_X")
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

// TestRestore_ImportFailureKeepsSpecRetryable verifies that a failure in the
// post-teardown phase (VolumeImport here) does not strand the instance
// spec-less: the spec row must survive so CheckRestorable succeeds and the
// restore can be retried once the underlying fault is cleared.
func TestRestore_ImportFailureKeepsSpecRetryable(t *testing.T) {
	svc, f, mem, _, id := newRestoreSvc(t)
	ctx := context.Background()

	// Inject a VolumeImport failure: restore will fail mid-volume.
	f.ImportErr = errors.New("import boom")

	var steps []string
	err := svc.Restore(ctx, RestoreRequest{BackupID: id}, recordSteps(&steps))
	require.Error(t, err)

	// The spec row must survive so the instance is still retryable.
	_, serr := mem.GetSpec(ctx, "h1", "postgres", "a")
	require.NoError(t, serr, "spec row must be preserved after import failure")

	// CheckRestorable must succeed (retry possible).
	_, cerr := svc.CheckRestorable(ctx, id)
	require.NoError(t, cerr, "CheckRestorable must not fail after import failure (retry must be possible)")

	// Confirm the respec breadcrumb was emitted.
	assert.Contains(t, steps, "respec")

	// Retry after clearing the fault: should succeed.
	f.ImportErr = nil
	// Re-seed the volume so the fake has content to export during the retry.
	f.SetVolumeData("h1", "postgres-a-data", tarBytes(t, contentA()))
	require.NoError(t, svc.Restore(ctx, RestoreRequest{BackupID: id}, nil))

	// Pod running after successful retry.
	p, perr := f.PodInspect(ctx, "h1", "postgres-a")
	require.NoError(t, perr)
	assert.Equal(t, "Running", p.Status)
}

// TestReconcileBackup_RowNeverCreated verifies that a BackupID with no row
// resolves immediately as terminal failed — the job died before CreateBackup,
// so there is nothing to clean up.
func TestReconcileBackup_RowNeverCreated(t *testing.T) {
	svc, _, _, _ := newBackupSvc(t)
	req := BackupRequest{BackupID: store.NewBackupID(), Host: "h1", Template: "pg", Slug: "a"}

	resolved, ok, msg, err := svc.ReconcileBackup(context.Background(), req, nil)

	require.NoError(t, err)
	assert.True(t, resolved)
	assert.False(t, ok)
	assert.Contains(t, msg, "before the backup row was created")
}

// TestReconcileBackup_HostGoneResolvesTerminal verifies that when the host is
// removed from config after a backup row is created, ReconcileBackup marks the
// row failed (store-local) and returns a terminal result rather than retrying
// forever. Without the host guard, Start → lookup returns ErrUnknownHost,
// which the old else-branch would propagate as a non-nil err, causing the
// runner to retry every sweep in an infinite loop.
func TestReconcileBackup_HostGoneResolvesTerminal(t *testing.T) {
	svc, _, mem, _ := newBackupSvc(t)
	ctx := context.Background()

	id := store.NewBackupID()
	require.NoError(t, mem.CreateBackup(ctx, store.Backup{
		ID: id, Host: "h1", Template: "pg", Slug: "a", State: store.BackupCreating,
	}))

	// Remove all hosts from config so the guard triggers.
	svc.SetHosts(nil)

	req := BackupRequest{BackupID: id, Host: "h1", Template: "pg", Slug: "a"}
	resolved, ok, msg, err := svc.ReconcileBackup(ctx, req, nil)

	require.NoError(t, err)
	assert.True(t, resolved)
	assert.False(t, ok)
	assert.Contains(t, msg, "no longer configured")

	// Row must be marked failed even though the host is gone (FailBackup is
	// store-local and runs before the host guard).
	b, gerr := mem.GetBackup(ctx, id)
	require.NoError(t, gerr)
	assert.Equal(t, store.BackupFailed, b.State)
}

// TestReconcileBackup_InstanceGoneSkipsRestart verifies that when the pod has
// been removed from the host, ReconcileBackup still resolves terminally (row
// failed, blobs cleaned) without returning an error — the "instance gone" path
// is intentional and not a retryable fault.
func TestReconcileBackup_InstanceGoneSkipsRestart(t *testing.T) {
	svc, f, mem, blob := newBackupSvc(t)
	ctx := context.Background()

	id := store.NewBackupID()
	require.NoError(t, mem.CreateBackup(ctx, store.Backup{
		ID: id, Host: "h1", Template: "pg", Slug: "a", State: store.BackupCreating,
	}))

	// Plant a stray blob so we can verify DeleteAll ran.
	key := "h1/pg/a/" + id + "/pg-a-data.tar"
	w, err := blob.Put(ctx, key)
	require.NoError(t, err)
	_, err = w.Write([]byte("partial"))
	require.NoError(t, err)
	require.NoError(t, w.Commit())

	// Remove the pod so Start returns ErrInstanceNotFound.
	require.NoError(t, f.PodRemove(ctx, "h1", "pg-a", false))

	var steps []string
	req := BackupRequest{BackupID: id, Host: "h1", Template: "pg", Slug: "a"}
	resolved, ok, _, err := svc.ReconcileBackup(ctx, req, recordSteps(&steps))

	require.NoError(t, err)
	assert.True(t, resolved)
	assert.False(t, ok)
	assert.Contains(t, steps, "reconcile-restart-skipped")

	// Partial blob must be cleaned up.
	assert.Equal(t, 0, blob.len())

	// Row must be marked failed.
	b, gerr := mem.GetBackup(ctx, id)
	require.NoError(t, gerr)
	assert.Equal(t, store.BackupFailed, b.State)
}

// newBackupTestService seeds host "h", a minimal single-volume template "t"
// (volumes populated per-test via setTemplateVolumes), a deployed instance
// t/s (spec + running pod), and wires an in-memory blob store. It is the
// harness for the exclude-filter tests below, deliberately smaller than
// newBackupSvc's postgres fixture since these tests only care about the
// volume-export path, not the app container shape.
func newBackupTestService(t *testing.T) (*Service, *fake.Fake, *store.Memory) {
	t.Helper()
	hosts := []config.Host{{ID: "h", Addr: "unix", Socket: "/x"}}
	f := fake.New()

	tmpl := store.Template{
		Meta: render.Meta{
			ID:         "t",
			Parameters: requiredParams("slug"),
		},
		Body: `apiVersion: v1
kind: Pod
metadata:
  name: t-{{.slug}}
  labels:
    podman-api/template: t
    podman-api/slug: {{.slug}}
spec:
  containers:
    - name: app
      image: busybox
`,
		Origin: "seed",
	}
	svc, mem := newSvcWith(t, f, hosts, tmpl)

	require.NoError(t, mem.PutSpec(context.Background(), store.Spec{
		Host: "h", Template: "t", Slug: "s",
		Parameters: map[string]any{"slug": "s"},
	}))

	f.AddPod("h", podman.Pod{
		Name: "t-s", ID: "t-s", Status: "Running",
		Containers: []podman.Container{{Name: "t-s-app", Image: "busybox", Status: "Running"}},
		Labels:     map[string]string{"podman-api/template": "t", "podman-api/slug": "s"},
	})

	svc.SetBlobStore(newMemBlob())
	return svc, f, mem
}

// setTemplateVolumes overwrites template tmplID's declared volumes — the
// exclude-filter tests need per-test volume declarations (including Exclude
// patterns) on top of newBackupTestService's minimal fixture.
func setTemplateVolumes(t *testing.T, st *store.Memory, tmplID string, vols []render.Volume) {
	t.Helper()
	tmpl, err := st.GetTemplate(context.Background(), tmplID)
	require.NoError(t, err)
	tmpl.Meta.Volumes = vols
	require.NoError(t, st.PutTemplate(context.Background(), tmpl))
}

// blobBytes reads a committed blob straight from svc's blob store.
func blobBytes(t *testing.T, svc *Service, key string) []byte {
	t.Helper()
	rc, err := svc.blobs.Get(context.Background(), key)
	require.NoError(t, err)
	defer rc.Close()
	b, err := io.ReadAll(rc)
	require.NoError(t, err)
	return b
}

// TestBackup_excludesDeclaredPaths verifies that a volume's declared exclude
// patterns are applied to the exported tar and recorded on the backup row.
func TestBackup_excludesDeclaredPaths(t *testing.T) {
	svc, f, st := newBackupTestService(t)
	ctx := context.Background()
	f.SetVolumeData("h", "t-s-sites", makeTar(t, []tarEntry{
		{name: "site/private/backups/db.sql.gz", body: "DUMP"},
		{name: "site/private/files/a.pdf", body: "PDF"},
	}))
	setTemplateVolumes(t, st, "t", []render.Volume{
		{Name: "sites", Backup: "s3; interval=24h", Exclude: []string{"*/private/backups/**"}},
	})

	id := store.NewBackupID()
	require.NoError(t, svc.Backup(ctx, BackupRequest{BackupID: id, Host: "h", Template: "t", Slug: "s"}, nil))

	b, err := st.GetBackup(ctx, id)
	require.NoError(t, err)
	require.Len(t, b.Volumes, 1)
	v := b.Volumes[0]
	if v.Excluded == nil || v.Excluded.Entries != 1 {
		t.Fatalf("excluded = %+v, want 1 entry dropped", v.Excluded)
	}
	names := tarNames(t, blobBytes(t, svc, backupBlobKey("h", "t", "s", id, "t-s-sites")))
	for _, n := range names {
		if strings.Contains(n, "private/backups") {
			t.Fatalf("dropped path present in the blob: %v", names)
		}
	}
}

// TestBackup_excludePatternMatchesNothing verifies the stale-pattern signal:
// a volume whose exclude pattern matches nothing in the exported tar still
// backs up successfully, and the backup row records Excluded with the
// declared patterns and Entries: 0 — the visible signal (per dropStats' doc
// comment) that a pattern is stale or misspelled, deliberately not an error.
func TestBackup_excludePatternMatchesNothing(t *testing.T) {
	svc, f, st := newBackupTestService(t)
	ctx := context.Background()
	f.SetVolumeData("h", "t-s-sites", makeTar(t, []tarEntry{
		{name: "site/files/a.pdf", body: "PDF"},
	}))
	setTemplateVolumes(t, st, "t", []render.Volume{
		{Name: "sites", Backup: "s3; interval=24h", Exclude: []string{"*/private/backups/**"}},
	})

	id := store.NewBackupID()
	require.NoError(t, svc.Backup(ctx, BackupRequest{BackupID: id, Host: "h", Template: "t", Slug: "s"}, nil))

	b, err := st.GetBackup(ctx, id)
	require.NoError(t, err)
	require.Len(t, b.Volumes, 1)
	v := b.Volumes[0]
	if v.Excluded == nil {
		t.Fatal("want Excluded recorded even when the pattern matches nothing")
	}
	assert.Equal(t, 0, v.Excluded.Entries)
	assert.Equal(t, []string{"*/private/backups/**"}, v.Excluded.Patterns)
}

// TestBackup_noExcludeIsByteForByte is the safety-property guard: a volume
// declaring no exclude patterns must still take the original TeeReader copy
// path, producing a blob byte-for-byte identical to the exported tar, and
// must record no Excluded metadata.
//
// The source is padded to a 10240-byte record boundary (the blocking factor
// GNU tar and podman's own export actually use) rather than left at
// archive/tar's minimal per-entry block alignment. Without the padding this
// test is vacuous: buildManifest's post-parse io.Copy(io.Discard, r) drain
// happens to copy the same number of trailing bytes that filterTar's
// tar.Writer re-encode independently produces for this fixture, so routing
// every volume through filterTar (a real #248 regression) would still pass.
// Padded, the two diverge — buildManifest's drain preserves the trailing
// padding byte-for-byte, filterTar's re-encode does not — so this is what
// makes the guard real. (Confirmed live: see the mutation check in the task-4
// fix report.)
func TestBackup_noExcludeIsByteForByte(t *testing.T) {
	svc, f, st := newBackupTestService(t)
	ctx := context.Background()
	src := padTarToRecordBoundary(makeTar(t, []tarEntry{{name: "a.txt", body: "A"}, {name: "b.txt", body: "B"}}))
	f.SetVolumeData("h", "t-s-data", src)
	setTemplateVolumes(t, st, "t", []render.Volume{{Name: "data", Backup: "s3; interval=24h"}})

	id := store.NewBackupID()
	require.NoError(t, svc.Backup(ctx, BackupRequest{BackupID: id, Host: "h", Template: "t", Slug: "s"}, nil))

	got := blobBytes(t, svc, backupBlobKey("h", "t", "s", id, "t-s-data"))
	if !bytes.Equal(got, src) {
		t.Fatal("a volume with no exclude patterns must be stored byte-for-byte")
	}
	b, err := st.GetBackup(ctx, id)
	require.NoError(t, err)
	require.Len(t, b.Volumes, 1)
	if b.Volumes[0].Excluded != nil {
		t.Fatalf("unfiltered volume must record no exclusions: %+v", b.Volumes[0].Excluded)
	}
	assert.Equal(t, int64(len(src)), b.Volumes[0].SizeBytes)
}

// padTarToRecordBoundary pads a tar stream with zero bytes to the next
// 10240-byte record boundary (blocking factor 20 x 512), matching what GNU
// tar / podman's own volume export actually emits. archive/tar's Writer only
// pads to 512-byte block alignment plus the two zero end-of-archive blocks,
// so a synthetic makeTar() fixture left unpadded is too short to distinguish
// a true byte-for-byte copy from a filter-and-reencode round-trip that
// happens to produce the same length.
func padTarToRecordBoundary(b []byte) []byte {
	const record = 10240
	if rem := len(b) % record; rem != 0 {
		b = append(b, make([]byte, record-rem)...)
	}
	return b
}

// TestRestore_VerifyMismatchKeepsSpec extends TestRestore_VerifyMismatchFails
// to also assert that the spec row survives after an ErrVolumeIntegrity
// failure so the restore is retryable.
func TestRestore_VerifyMismatchKeepsSpec(t *testing.T) {
	svc, _, mem, blob, id := newRestoreSvc(t)
	ctx := context.Background()

	// Replace the complete row with one whose stored manifest disagrees with
	// the blob's actual content (same surgery as TestRestore_VerifyMismatchFails).
	orig, err := mem.GetBackup(ctx, id)
	require.NoError(t, err)
	require.NoError(t, mem.DeleteBackup(ctx, id))

	var m Manifest
	require.NoError(t, json.Unmarshal(orig.Volumes[0].Manifest, &m))
	fi := m["f"]
	fi.sha256 = "deadbeef"
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
	_ = blob

	err = svc.Restore(ctx, RestoreRequest{BackupID: id}, nil)
	require.ErrorIs(t, err, ErrVolumeIntegrity)

	// Spec row must survive so the operator can retry with a corrected backup.
	_, serr := mem.GetSpec(ctx, "h1", "postgres", "a")
	require.NoError(t, serr, "spec row must be preserved after verify-mismatch failure")
}

// TestRestore_LeavesVolumesOutsideTheBackupAlone locks the invariant that a
// restore may never delete a volume it has no copy of. Before this, Restore
// tore down every DECLARED volume via PruneVolumes:true and recreated only the
// ones the backup recorded — harmless while every backup held every volume, and
// data loss the moment one is narrower.
//
// The volume must be DECLARED by the template to reproduce: pruneInstanceResources
// derives what to remove from t.Meta.Volumes, so an undeclared volume was never
// at risk and would make this test pass vacuously. The scenario here is the
// ordinary one that gets there today — a template that gained a volume after the
// backup was taken.
func TestRestore_LeavesVolumesOutsideTheBackupAlone(t *testing.T) {
	svc, f, mem, _ := newBackupSvc(t)
	ctx := context.Background()

	req := newBackupReq()
	require.NoError(t, svc.Backup(ctx, req, nil))

	b, err := svc.store.GetBackup(ctx, req.BackupID)
	require.NoError(t, err)
	require.Len(t, b.Volumes, 1)
	require.Equal(t, "pg-a-data", b.Volumes[0].Name)

	// The template gains a volume AFTER the backup, and the instance populates
	// it. The backup has no copy of it.
	tmpl, err := mem.GetTemplate(ctx, "pg")
	require.NoError(t, err)
	tmpl.Meta.Volumes = append(tmpl.Meta.Volumes, render.Volume{Name: "extra"})
	require.NoError(t, mem.PutTemplate(ctx, tmpl))
	f.SetVolumeData("h1", "pg-a-extra", tarBytes(t, map[string]string{"keep": "me"}))

	require.NoError(t, svc.Restore(ctx, RestoreRequest{BackupID: req.BackupID}, nil))

	assert.NotEmpty(t, f.VolumeData("h1", "pg-a-extra"),
		"restore deleted a declared volume the backup had no copy of")
}

// TestBackup_SkipsNoneMarkedVolumes proves `backup: none` is a veto rather than
// documentation. The `logs` volume exists on the host and is exported today.
func TestBackup_SkipsNoneMarkedVolumes(t *testing.T) {
	svc, f, _, blob := newBackupSvc(t)
	ctx := context.Background()
	f.SetVolumeData("h1", "pg-a-logs", tarBytes(t, map[string]string{"app.log": "noise"}))

	req := newBackupReq()
	var steps []string
	require.NoError(t, svc.Backup(ctx, req, recordSteps(&steps)))

	b, err := svc.store.GetBackup(ctx, req.BackupID)
	require.NoError(t, err)
	require.Len(t, b.Volumes, 1)
	assert.Equal(t, "pg-a-data", b.Volumes[0].Name)

	_, err = blob.Get(ctx, "h1/pg/a/"+req.BackupID+"/pg-a-logs.tar")
	assert.ErrorIs(t, err, fs.ErrNotExist, "a none-marked volume must write no blob")

	assert.Contains(t, steps, "skip-volume")
}

// TestBackup_ExportsUnmarkedVolumes locks the other half of the rule: only an
// explicit `none` vetoes. A volume with no backup: field at all is still
// captured — reading "unmarked" as "excluded" would silently shrink every
// existing backup in the fleet.
func TestBackup_ExportsUnmarkedVolumes(t *testing.T) {
	svc, f, mem, _ := newBackupSvc(t)
	ctx := context.Background()

	tmpl, err := mem.GetTemplate(ctx, "pg")
	require.NoError(t, err)
	tmpl.Meta.Volumes = append(tmpl.Meta.Volumes, render.Volume{Name: "cache"})
	require.NoError(t, mem.PutTemplate(ctx, tmpl))
	f.SetVolumeData("h1", "pg-a-cache", tarBytes(t, map[string]string{"c": "1"}))

	req := newBackupReq()
	require.NoError(t, svc.Backup(ctx, req, nil))

	b, err := svc.store.GetBackup(ctx, req.BackupID)
	require.NoError(t, err)
	names := []string{}
	for _, v := range b.Volumes {
		names = append(names, v.Name)
	}
	assert.ElementsMatch(t, []string{"pg-a-data", "pg-a-cache"}, names)
}

// TestBackup_EmitsExportStepBeforeExporting is the #135 fix: the step trail must
// name the volume being exported for the DURATION of the export, not only once
// it finishes. Without it the whole multi-minute stop window reads as dead air
// between `stop` and the first sign of progress, and a live job is
// indistinguishable from a hung one.
func TestBackup_EmitsExportStepBeforeExporting(t *testing.T) {
	svc, f, _, _ := newBackupSvc(t)
	ctx := context.Background()

	var steps []string
	f.ExportReader = func(_, name string) io.ReadCloser {
		// Assert mid-export: by the time bytes are being read, the step must
		// already have been emitted.
		assert.Contains(t, steps, "export-volume",
			"export-volume must be emitted BEFORE the export begins")
		return io.NopCloser(bytes.NewReader(tarBytes(t, map[string]string{"x": "1"})))
	}

	require.NoError(t, svc.Backup(ctx, newBackupReq(), recordSteps(&steps)))
	assert.Contains(t, steps, "export-volume-done")
}

func TestCheckBackupable_RejectsUndeclaredVolume(t *testing.T) {
	svc, _, _, _ := newBackupSvc(t)
	err := svc.CheckBackupable(context.Background(), "h1", "pg", "a", []string{"sitez"})
	assert.ErrorIs(t, err, ErrInvalidBackupScope)
}

func TestCheckBackupable_RejectsNoneMarkedVolume(t *testing.T) {
	svc, _, _, _ := newBackupSvc(t)
	err := svc.CheckBackupable(context.Background(), "h1", "pg", "a", []string{"logs"})
	assert.ErrorIs(t, err, ErrInvalidBackupScope)
}

func TestCheckBackupable_AcceptsEmptyAndDeclaredScope(t *testing.T) {
	svc, _, _, _ := newBackupSvc(t)
	ctx := context.Background()
	assert.NoError(t, svc.CheckBackupable(ctx, "h1", "pg", "a", nil))
	assert.NoError(t, svc.CheckBackupable(ctx, "h1", "pg", "a", []string{"data"}))
}

// TestCheckBackupable_RejectsEmptyScopeWhenEveryDeclaredVolumeIsNone: with
// every declared volume vetoed, an empty scope ("everything not vetoed")
// resolves to nothing exportable — reject rather than let a request through
// that stops the pod, exports zero volumes, and records a green backup.
func TestCheckBackupable_RejectsEmptyScopeWhenEveryDeclaredVolumeIsNone(t *testing.T) {
	svc, _, mem, _ := newBackupSvc(t)
	ctx := context.Background()

	tmpl, err := mem.GetTemplate(ctx, "pg")
	require.NoError(t, err)
	for i := range tmpl.Meta.Volumes {
		tmpl.Meta.Volumes[i].Backup = BackupMarkerNone
	}
	require.NoError(t, mem.PutTemplate(ctx, tmpl))

	err = svc.CheckBackupable(ctx, "h1", "pg", "a", nil)
	assert.ErrorIs(t, err, ErrInvalidBackupScope)
	// The message must describe the case that actually happened — volumes ARE
	// declared, and every one of them is vetoed — rather than sending an
	// operator hunting for a marker on a template that declares nothing.
	assert.Contains(t, err.Error(), "every one of them is marked")
}

// TestCheckBackupable_AcceptsTemplateDeclaringNoVolumes: a stateless template
// (the bundled `basic-web`) has nothing to back up. That is a valid no-op, not
// an error — rejecting it would regress every caller that backs one up today,
// with a message pointing at a `none` marker that does not exist.
func TestCheckBackupable_AcceptsTemplateDeclaringNoVolumes(t *testing.T) {
	svc, _, mem, _ := newBackupSvc(t)
	ctx := context.Background()

	tmpl, err := mem.GetTemplate(ctx, "pg")
	require.NoError(t, err)
	tmpl.Meta.Volumes = nil
	require.NoError(t, mem.PutTemplate(ctx, tmpl))

	assert.NoError(t, svc.CheckBackupable(ctx, "h1", "pg", "a", nil))
}

// TestBackup_ScopeNarrowsTheExportSet: with two exportable volumes present, a
// scope of {data} captures only data.
func TestBackup_ScopeNarrowsTheExportSet(t *testing.T) {
	svc, f, mem, _ := newBackupSvc(t)
	ctx := context.Background()

	tmpl, err := mem.GetTemplate(ctx, "pg")
	require.NoError(t, err)
	tmpl.Meta.Volumes = append(tmpl.Meta.Volumes, render.Volume{Name: "cache"})
	require.NoError(t, mem.PutTemplate(ctx, tmpl))
	f.SetVolumeData("h1", "pg-a-cache", tarBytes(t, map[string]string{"c": "1"}))

	req := newBackupReq()
	req.Volumes = []string{"data"}
	require.NoError(t, svc.Backup(ctx, req, nil))

	b, err := svc.store.GetBackup(ctx, req.BackupID)
	require.NoError(t, err)
	require.Len(t, b.Volumes, 1)
	assert.Equal(t, "pg-a-data", b.Volumes[0].Name)
}

// TestBackup_ExplicitScopeCapturingNothingFails: an EXPLICIT scope naming a
// declared volume that does not exist on the host captures nothing, and must
// never record a green, empty backup the caller would later restore from and
// get nothing back.
//
// Round-3 finding 4 moved WHERE that is caught: CheckBackupable now resolves
// existence itself, so the rejection is synchronous and the pod is never
// stopped. Failing after the export instead made a template edit on a
// not-yet-re-applied fleet a permanent stop/restart loop.
func TestBackup_ExplicitScopeCapturingNothingFails(t *testing.T) {
	svc, f, mem, blob := newBackupSvc(t)
	ctx := context.Background()

	tmpl, err := mem.GetTemplate(ctx, "pg")
	require.NoError(t, err)
	tmpl.Meta.Volumes = append(tmpl.Meta.Volumes, render.Volume{Name: "cache"})
	require.NoError(t, mem.PutTemplate(ctx, tmpl))
	// Note: no SetVolumeData for pg-a-cache — it is declared but absent.

	req := newBackupReq()
	req.Volumes = []string{"cache"}
	err = svc.Backup(ctx, req, nil)
	require.ErrorIs(t, err, ErrInvalidBackupScope)

	// Rejected before anything happened: no row, no blobs, pod untouched.
	_, gerr := svc.store.GetBackup(ctx, req.BackupID)
	require.ErrorIs(t, gerr, store.ErrNotFound)
	assert.Equal(t, 0, blob.len())
	p, perr := f.PodInspect(ctx, "h1", "pg-a")
	require.NoError(t, perr)
	assert.Equal(t, "Running", p.Status)
}

// TestBackup_UnscopedCapturingNothingSucceeds: the other half of the same rule.
// An UNSCOPED backup resolving to zero exportable volumes is the brand-new
// instance case — nothing has been written yet — and must still succeed.
func TestBackup_UnscopedCapturingNothingSucceeds(t *testing.T) {
	svc, f, _, _ := newBackupSvc(t)
	ctx := context.Background()

	// No volume exists on the host yet: the declared `data` was never created.
	require.NoError(t, f.VolumeRemove(ctx, "h1", "pg-a-data", true))

	req := newBackupReq() // no Volumes -> unscoped
	require.NoError(t, svc.Backup(ctx, req, nil))

	b, err := svc.store.GetBackup(ctx, req.BackupID)
	require.NoError(t, err)
	assert.Equal(t, store.BackupComplete, b.State)
	assert.Empty(t, b.Volumes)
}

// TestBackup_InvalidScopeNeverStopsThePod: validation is synchronous and runs
// before the stop, so a scheduler bug or a typo cannot cost an outage.
func TestBackup_InvalidScopeNeverStopsThePod(t *testing.T) {
	svc, f, _, _ := newBackupSvc(t)
	ctx := context.Background()

	req := newBackupReq()
	req.Volumes = []string{"nope"}
	err := svc.Backup(ctx, req, nil)
	assert.ErrorIs(t, err, ErrInvalidBackupScope)

	p, perr := f.PodInspect(ctx, "h1", "pg-a")
	require.NoError(t, perr)
	assert.Equal(t, "Running", p.Status, "an invalid scope must not stop the pod")
}

// TestBackup_PartialScopeMissFails (re-review A) is the case an emptiness check
// misses: a scope naming two volumes, one of which no longer exists on the
// host, resolves to a NON-empty export set. Without a set comparison the run
// records a green `complete` row containing only the volume that happened to
// exist, and a later restore tears the pod down and brings the other one back
// unrestored while the operator believes it was captured.
func TestBackup_PartialScopeMissFails(t *testing.T) {
	svc, f, mem, blob := newBackupSvc(t)
	ctx := context.Background()

	tmpl, err := mem.GetTemplate(ctx, "pg")
	require.NoError(t, err)
	tmpl.Meta.Volumes = append(tmpl.Meta.Volumes, render.Volume{Name: "cache"})
	require.NoError(t, mem.PutTemplate(ctx, tmpl))
	// `data` exists (newBackupSvc populated it); `cache` is declared but absent.

	req := newBackupReq()
	req.Volumes = []string{"data", "cache"}
	err = svc.Backup(ctx, req, nil)
	require.ErrorIs(t, err, ErrInvalidBackupScope)
	// The message must name the volume that was not captured, not just say the
	// scope failed — the operator's next action depends on which one it was.
	assert.Contains(t, err.Error(), "cache")
	assert.NotContains(t, err.Error(), "[data cache]", "only the MISSING volumes belong in the message")

	// Since round-3 finding 4 this is caught up front, so there is no row to
	// fail and no partial blob to reap (both asserted here, since a regression
	// that moved the check back would produce exactly those).
	_, gerr := svc.store.GetBackup(ctx, req.BackupID)
	require.ErrorIs(t, gerr, store.ErrNotFound)
	assert.Equal(t, 0, blob.len())

	p, perr := f.PodInspect(ctx, "h1", "pg-a")
	require.NoError(t, perr)
	assert.Equal(t, "Running", p.Status)
}

// TestBackup_PartialScopeMissRaceStillFailsAfterExport keeps the post-export
// set comparison honest as the DEFENSIVE BACKSTOP it became: the forward check
// in CheckBackupable makes it unreachable in the ordinary case, but a volume
// removed between that check and the export is still a partial capture, and it
// must still fail rather than record a green row.
//
// The race is driven through the template's pre_backup exec, which runs after
// CheckBackupable and before Backup resolves InstanceVolumes.
func TestBackup_PartialScopeMissRaceStillFailsAfterExport(t *testing.T) {
	svc, f, mem, blob := newBackupSvc(t)
	ctx := context.Background()

	tmpl, err := mem.GetTemplate(ctx, "pg")
	require.NoError(t, err)
	tmpl.Meta.Volumes = append(tmpl.Meta.Volumes, render.Volume{Name: "cache"})
	tmpl.Meta.PreBackup = &render.PreBackup{Container: "db", Command: "true"}
	require.NoError(t, mem.PutTemplate(ctx, tmpl))
	f.SetVolumeData("h1", "pg-a-cache", tarBytes(t, map[string]string{"c": "1"}))

	// `cache` passes the up-front existence check, then vanishes.
	f.ExecFunc = func(host, container string, cmd []string) (podman.ExecResult, error) {
		_ = f.VolumeRemove(ctx, "h1", "pg-a-cache", true)
		return podman.ExecResult{}, nil
	}

	req := newBackupReq()
	req.Volumes = []string{"data", "cache"}
	err = svc.Backup(ctx, req, nil)
	require.ErrorIs(t, err, ErrInvalidBackupScope)
	assert.Contains(t, err.Error(), "cache")

	b, err := svc.store.GetBackup(ctx, req.BackupID)
	require.NoError(t, err)
	assert.Equal(t, store.BackupFailed, b.State, "a partially-satisfied scope must not record a complete backup")
	assert.Equal(t, 0, blob.len(), "the partial blob must be reaped")

	p, perr := f.PodInspect(ctx, "h1", "pg-a")
	require.NoError(t, perr)
	assert.Equal(t, "Running", p.Status)
}

// TestBackup_FullyResolvedScopeSucceeds is the other half of A: the set
// comparison must not reject a scope every one of whose volumes WAS captured.
func TestBackup_FullyResolvedScopeSucceeds(t *testing.T) {
	svc, f, mem, _ := newBackupSvc(t)
	ctx := context.Background()

	tmpl, err := mem.GetTemplate(ctx, "pg")
	require.NoError(t, err)
	tmpl.Meta.Volumes = append(tmpl.Meta.Volumes, render.Volume{Name: "cache"})
	require.NoError(t, mem.PutTemplate(ctx, tmpl))
	f.SetVolumeData("h1", "pg-a-cache", tarBytes(t, map[string]string{"c": "1"}))

	req := newBackupReq()
	req.Volumes = []string{"data", "cache"}
	require.NoError(t, svc.Backup(ctx, req, nil))

	b, err := svc.store.GetBackup(ctx, req.BackupID)
	require.NoError(t, err)
	require.Len(t, b.Volumes, 2)
}

// TestBackup_ScopeExcludedVolumeEmitsItsOwnStep (re-review J): a volume left
// out because it was not in the requested scope must be distinguishable, in the
// job trail, from one left out by a `backup: none` veto. Both produce a backup
// missing a volume and the trail is the only after-the-fact record.
func TestBackup_ScopeExcludedVolumeEmitsItsOwnStep(t *testing.T) {
	svc, f, mem, _ := newBackupSvc(t)
	ctx := context.Background()

	tmpl, err := mem.GetTemplate(ctx, "pg")
	require.NoError(t, err)
	tmpl.Meta.Volumes = append(tmpl.Meta.Volumes, render.Volume{Name: "cache"})
	require.NoError(t, mem.PutTemplate(ctx, tmpl))
	f.SetVolumeData("h1", "pg-a-cache", tarBytes(t, map[string]string{"c": "1"}))
	// pgTemplate declares `logs` as backup: none; give it content so it is a
	// live volume the loop actually reaches.
	f.SetVolumeData("h1", "pg-a-logs", tarBytes(t, map[string]string{"app.log": "noise"}))

	req := newBackupReq()
	req.Volumes = []string{"data"}
	var steps []string
	require.NoError(t, svc.Backup(ctx, req, recordSteps(&steps)))

	assert.Contains(t, steps, "skip-volume-scope", "a scope-excluded volume must be recorded")
	assert.Contains(t, steps, "skip-volume", "the veto keeps its own step name")
}

// TestBackup_NearMissNoneMarkerOnAStoredRowStillVetoes (re-review D):
// registration rejects `None`, but nothing re-validates a template row
// persisted before that validator existed — so the comparison itself must fail
// CLOSED. A row carrying `None` must veto exactly like `none`, not export the
// volume against the operator's explicit instruction.
func TestBackup_NearMissNoneMarkerOnAStoredRowStillVetoes(t *testing.T) {
	svc, f, mem, blob := newBackupSvc(t)
	ctx := context.Background()

	// PutTemplate directly, as a pre-validator row would have been stored.
	tmpl, err := mem.GetTemplate(ctx, "pg")
	require.NoError(t, err)
	tmpl.Meta.Volumes = append(tmpl.Meta.Volumes, render.Volume{Name: "cache", Backup: "None"})
	require.NoError(t, mem.PutTemplate(ctx, tmpl))
	f.SetVolumeData("h1", "pg-a-cache", tarBytes(t, map[string]string{"c": "1"}))

	req := newBackupReq()
	require.NoError(t, svc.Backup(ctx, req, nil))

	b, err := svc.store.GetBackup(ctx, req.BackupID)
	require.NoError(t, err)
	require.Len(t, b.Volumes, 1)
	assert.Equal(t, "pg-a-data", b.Volumes[0].Name)
	_, err = blob.Get(ctx, "h1/pg/a/"+req.BackupID+"/pg-a-cache.tar")
	assert.ErrorIs(t, err, fs.ErrNotExist, "a `None`-marked volume must write no blob")

	// The same veto must hold on the scope-validation path.
	assert.ErrorIs(t, svc.CheckBackupable(ctx, "h1", "pg", "a", []string{"cache"}), ErrInvalidBackupScope)
}

// TestCheckBackupable_NearMissNoneOnEveryVolumeRejectsEmptyScope: the
// all-vetoed rule reads the markers through the same fail-closed comparison, so
// a catalog of `NONE` markers is still "every declared volume is vetoed" rather
// than a green, empty backup.
func TestCheckBackupable_NearMissNoneOnEveryVolumeRejectsEmptyScope(t *testing.T) {
	svc, _, mem, _ := newBackupSvc(t)
	ctx := context.Background()

	tmpl, err := mem.GetTemplate(ctx, "pg")
	require.NoError(t, err)
	for i := range tmpl.Meta.Volumes {
		tmpl.Meta.Volumes[i].Backup = " NONE "
	}
	require.NoError(t, mem.PutTemplate(ctx, tmpl))

	assert.ErrorIs(t, svc.CheckBackupable(ctx, "h1", "pg", "a", nil), ErrInvalidBackupScope)
}

// TestRestore_MissingThirdBlobLeavesEveryVolumeUntouched (re-review E): the
// "never delete a volume we have no copy of" invariant is instance-wide, not
// per volume. Opening each blob immediately before its own remove makes each
// volume individually safe but still rolls volumes 1 and 2 back to
// backup-epoch content before discovering blob 3 is gone — leaving the instance
// down AND mixed-epoch, with those two overwrites unrecoverable.
func TestRestore_MissingThirdBlobLeavesEveryVolumeUntouched(t *testing.T) {
	svc, f, mem, blob := newBackupSvc(t)
	ctx := context.Background()

	tmpl, err := mem.GetTemplate(ctx, "pg")
	require.NoError(t, err)
	tmpl.Meta.Volumes = append(tmpl.Meta.Volumes,
		render.Volume{Name: "cache"}, render.Volume{Name: "extra"})
	require.NoError(t, mem.PutTemplate(ctx, tmpl))
	f.SetVolumeData("h1", "pg-a-cache", tarBytes(t, map[string]string{"c": "1"}))
	f.SetVolumeData("h1", "pg-a-extra", tarBytes(t, map[string]string{"e": "1"}))

	req := newBackupReq()
	require.NoError(t, svc.Backup(ctx, req, nil))
	b, err := svc.store.GetBackup(ctx, req.BackupID)
	require.NoError(t, err)
	require.Len(t, b.Volumes, 3)

	// Every volume has diverged since the backup; the live bytes are the only
	// copy of this epoch.
	live := map[string][]byte{}
	for _, bv := range b.Volumes {
		data := tarBytes(t, map[string]string{"newer": bv.Name})
		f.SetVolumeData("h1", bv.Name, data)
		live[bv.Name] = data
	}

	// Drop ONLY the last volume's blob, so the first two would restore fine.
	last := b.Volumes[len(b.Volumes)-1].Name
	require.NoError(t, blob.DeleteAll(ctx, backupBlobKey("h1", "pg", "a", req.BackupID, last)))

	err = svc.Restore(ctx, RestoreRequest{BackupID: req.BackupID}, nil)
	require.ErrorIs(t, err, ErrBackupNotRestorable)

	for name, data := range live {
		assert.Equal(t, data, f.VolumeData("h1", name),
			"volume %q was rolled back before the restore discovered it could not finish", name)
	}
}

// TestRestore_MissingBlobLeavesPodRunning (round-3 finding 2): the whole-set
// blob preflight is right, but it used to run at the top of
// restorePostTeardown — AFTER Restore had already deleted the pod. So a
// missing blob left every volume untouched (the goal) while the instance was
// down, Apply was never reached, and it stayed down until a human noticed a
// failed job. The check is a pure read over the backup row, so it belongs in
// CheckRestorable, where the caller gets a synchronous rejection and the
// instance never stops serving.
func TestRestore_MissingBlobLeavesPodRunning(t *testing.T) {
	svc, f, _, blob, id := newRestoreSvc(t)
	ctx := context.Background()

	require.NoError(t, blob.DeleteAll(ctx, backupBlobPrefix("h1", "postgres", "a", id)))

	// The synchronous check rejects it before Restore is even called.
	_, err := svc.CheckRestorable(ctx, id)
	require.ErrorIs(t, err, ErrBackupNotRestorable)

	err = svc.Restore(ctx, RestoreRequest{BackupID: id}, nil)
	require.ErrorIs(t, err, ErrBackupNotRestorable)

	p, perr := f.PodInspect(ctx, "h1", "postgres-a")
	require.NoError(t, perr, "the pod must still exist: a missing blob may not tear the instance down")
	assert.Equal(t, "Running", p.Status,
		"a restore rejected for a missing blob must leave the instance serving")
}

// TestCheckBackupable_ExplicitScopeRejectsUnmaterialisedVolume (round-3
// finding 4): the post-export set comparison catches a declared-but-absent
// volume only AFTER the pod has been stopped and every other volume exported —
// so a template edit that marks a new volume on a fleet that has not been
// re-applied turns every scheduler tick into a stop/export/fail/restart cycle,
// forever, recording nothing. For an EXPLICIT scope the existence check moves
// forward into CheckBackupable: the failure is synchronous, and no pod is ever
// stopped.
func TestCheckBackupable_ExplicitScopeRejectsUnmaterialisedVolume(t *testing.T) {
	svc, f, mem, _ := newBackupSvc(t)
	ctx := context.Background()

	// The template gains a `cache` volume; the running instance predates the
	// edit, so pg-a-cache does not exist on the host.
	tmpl, err := mem.GetTemplate(ctx, "pg")
	require.NoError(t, err)
	tmpl.Meta.Volumes = append(tmpl.Meta.Volumes, render.Volume{Name: "cache", Backup: "s3; interval=24h"})
	require.NoError(t, mem.PutTemplate(ctx, tmpl))

	err = svc.CheckBackupable(ctx, "h1", "pg", "a", []string{"data", "cache"})
	require.ErrorIs(t, err, ErrInvalidBackupScope)
	assert.Contains(t, err.Error(), "cache")

	// A scope naming only materialised volumes still passes.
	require.NoError(t, svc.CheckBackupable(ctx, "h1", "pg", "a", []string{"data"}))

	// And the whole run refuses before anything is stopped.
	req := newBackupReq()
	req.Volumes = []string{"data", "cache"}
	var steps []string
	err = svc.Backup(ctx, req, recordSteps(&steps))
	require.ErrorIs(t, err, ErrInvalidBackupScope)
	assert.NotContains(t, steps, "stop", "the pod must not be stopped for a scope that cannot succeed")
	assert.NotContains(t, steps, "export-volume")

	p, perr := f.PodInspect(ctx, "h1", "pg-a")
	require.NoError(t, perr)
	assert.Equal(t, "Running", p.Status)

	// No row was created, so nothing is left half-recorded either.
	_, gerr := svc.store.GetBackup(ctx, req.BackupID)
	require.ErrorIs(t, gerr, store.ErrNotFound)
}

// TestBackup_UnscopedExportingNothingFailsWhenVolumesExist (round-3 finding
// 5, tightened in the unscoped-path loop-closure fix): the "did we capture
// what was asked for" guard only ran for an explicit scope, so an UNSCOPED run
// that exported nothing recorded a green `complete` row with an empty blob
// set. Reachable whenever a template declares a vetoed volume alongside one
// existing instances have not materialised: `data` is skipped as vetoed,
// `cache` does not exist.
//
// This used to be caught only AFTER the pod was stopped and the (empty)
// export ran — which reintroduced, on the unscoped path, exactly the
// stop/export/fail/restart outage loop CheckBackupable's explicit-scope
// existence check exists to prevent: since this scenario recurs every
// scheduler tick, it would never resolve on its own. CheckBackupable now
// resolves the same question up front by checking what is MATERIALISED on
// the host (mirroring the explicit-scope branch), so this is rejected
// synchronously and the pod is never touched.
func TestBackup_UnscopedExportingNothingFailsWhenVolumesExist(t *testing.T) {
	svc, f, mem, blob := newBackupSvc(t)
	ctx := context.Background()

	// data: vetoed (and the only volume that exists on the host).
	// cache: backupable, but declared by a template edit the instance predates.
	tmpl, err := mem.GetTemplate(ctx, "pg")
	require.NoError(t, err)
	tmpl.Meta.Volumes = []render.Volume{
		{Name: "data", Backup: "none"},
		{Name: "cache", Backup: "s3; interval=24h"},
	}
	require.NoError(t, mem.PutTemplate(ctx, tmpl))

	err = svc.CheckBackupable(ctx, "h1", "pg", "a", nil)
	require.ErrorIs(t, err, ErrInvalidBackupScope)
	assert.Contains(t, err.Error(), "backup: none")

	req := newBackupReq()
	var steps []string
	err = svc.Backup(ctx, req, recordSteps(&steps))
	require.Error(t, err, "an unscoped backup that captures nothing must not report success")
	assert.ErrorIs(t, err, ErrInvalidBackupScope)
	assert.NotContains(t, steps, "stop", "the pod must not be stopped for an unscoped run that cannot capture anything")
	assert.NotContains(t, steps, "export-volume")

	// Rejected before anything happened: no row, no blobs, pod untouched.
	_, gerr := svc.store.GetBackup(ctx, req.BackupID)
	require.ErrorIs(t, gerr, store.ErrNotFound)
	assert.Zero(t, blob.len())

	p, perr := f.PodInspect(ctx, "h1", "pg-a")
	require.NoError(t, perr)
	assert.Equal(t, "Running", p.Status)
}

// TestCheckBackupable_UnscopedHostErrorNotMisclassified: a genuine host
// failure while resolving materialised volumes for an unscoped request must
// surface as a host error, never as ErrInvalidBackupScope — a transient blip
// misread as a permanent scope error would stop a scheduler retrying.
func TestCheckBackupable_UnscopedHostErrorNotMisclassified(t *testing.T) {
	svc, f, _, _ := newBackupSvc(t)
	ctx := context.Background()

	// pgTemplate declares "data" (exportable) so the cheap all-vetoed
	// short-circuit does not fire and the host call is actually reached.
	f.VolumeInspectErr = errors.New("ssh: connection reset")

	err := svc.CheckBackupable(ctx, "h1", "pg", "a", nil)
	require.Error(t, err)
	assert.NotErrorIs(t, err, ErrInvalidBackupScope)
}

// TestBackup_UnscopedWithNoVolumesYetStillSucceeds is the other half of
// finding 5: the carve-out is "no volume exists yet" (a brand-new instance
// taking its first backup), NOT "no scope was given". It must keep succeeding
// with an empty volume list.
func TestBackup_UnscopedWithNoVolumesYetStillSucceeds(t *testing.T) {
	svc, _, mem, _ := newBackupSvc(t)
	ctx := context.Background()

	// Declared, backupable, and not created on the host yet.
	tmpl, err := mem.GetTemplate(ctx, "pg")
	require.NoError(t, err)
	tmpl.Meta.Volumes = []render.Volume{{Name: "cache", Backup: "s3; interval=24h"}}
	require.NoError(t, mem.PutTemplate(ctx, tmpl))

	req := newBackupReq()
	require.NoError(t, svc.Backup(ctx, req, nil))

	b, err := svc.store.GetBackup(ctx, req.BackupID)
	require.NoError(t, err)
	assert.Equal(t, store.BackupComplete, b.State)
	assert.Empty(t, b.Volumes)
}

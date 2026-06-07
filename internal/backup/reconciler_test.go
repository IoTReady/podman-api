package backup

import (
	"context"
	"encoding/json"
	"io/fs"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/iotready/podman-api/internal/instance"
	"github.com/iotready/podman-api/internal/jobs"
	"github.com/iotready/podman-api/internal/store"
)

// backupBlobKey mirrors the instance package's blob layout so the reconciler
// test can plant a stray blob under a backup's prefix.
func backupBlobKey(host, tmpl, slug, id, volume string) string {
	return host + "/" + tmpl + "/" + slug + "/" + id + "/" + volume + ".tar"
}

func TestReconciler_MarksCreatingFailedCleansBlobsAndRestarts(t *testing.T) {
	svc, f, mem, blob := seedSvc(t)
	ctx := context.Background()

	id := store.NewBackupID()
	require.NoError(t, mem.CreateBackup(ctx, store.Backup{
		ID: id, Host: "h1", Template: "postgres", Slug: "a", State: store.BackupCreating,
	}))

	// Commit a stray blob under the backup's prefix.
	key := backupBlobKey("h1", "postgres", "a", id, "postgres-a-data")
	w, err := blob.Put(ctx, key)
	require.NoError(t, err)
	_, err = w.Write([]byte("partial"))
	require.NoError(t, err)
	require.NoError(t, w.Commit())

	// Stop the pod to prove the reconciler restarts it.
	require.NoError(t, f.PodStop(ctx, "h1", "postgres-a"))

	args, err := json.Marshal(instance.BackupRequest{
		BackupID: id, Host: "h1", Template: "postgres", Slug: "a",
	})
	require.NoError(t, err)
	job, err := mem.Enqueue(ctx, "backup", args, "")
	require.NoError(t, err)

	r := &Reconciler{Svc: svc}
	state, _, resolved, rerr := r.Reconcile(ctx, job, jobs.NewJobContext(mem, job.ID))
	require.NoError(t, rerr)
	assert.True(t, resolved)
	assert.Equal(t, store.JobFailed, state)

	b, err := mem.GetBackup(ctx, id)
	require.NoError(t, err)
	assert.Equal(t, store.BackupFailed, b.State)

	_, gerr := blob.Get(ctx, key)
	assert.ErrorIs(t, gerr, fs.ErrNotExist, "partial blob must be deleted")

	p, err := f.PodInspect(ctx, "h1", "postgres-a")
	require.NoError(t, err)
	assert.Equal(t, "Running", p.Status, "instance must be restarted")
}

func TestReconciler_CompletedRowResolvesSucceeded(t *testing.T) {
	svc, _, mem, _ := seedSvc(t)
	ctx := context.Background()

	id := store.NewBackupID()
	require.NoError(t, mem.CreateBackup(ctx, store.Backup{
		ID: id, Host: "h1", Template: "postgres", Slug: "a", State: store.BackupCreating,
	}))
	ok, err := mem.CompleteBackup(ctx, id, []store.BackupVolume{{Name: "postgres-a-data"}})
	require.NoError(t, err)
	require.True(t, ok)

	args, err := json.Marshal(instance.BackupRequest{
		BackupID: id, Host: "h1", Template: "postgres", Slug: "a",
	})
	require.NoError(t, err)
	job, err := mem.Enqueue(ctx, "backup", args, "")
	require.NoError(t, err)

	r := &Reconciler{Svc: svc}
	state, _, resolved, rerr := r.Reconcile(ctx, job, jobs.NewJobContext(mem, job.ID))
	require.NoError(t, rerr)
	assert.True(t, resolved)
	assert.Equal(t, store.JobSucceeded, state)

	b, err := mem.GetBackup(ctx, id)
	require.NoError(t, err)
	assert.Equal(t, store.BackupComplete, b.State, "completed row stays complete")
}

func TestReconciler_BadArgsResolvesFailed(t *testing.T) {
	r := &Reconciler{}
	mem := store.NewMemory()
	job := store.Job{ID: "j1", Kind: "backup", Args: json.RawMessage(`{`)}
	state, message, resolved, err := r.Reconcile(context.Background(), job, jobs.NewJobContext(mem, "j1"))
	require.NoError(t, err)
	assert.True(t, resolved)
	assert.Equal(t, store.JobFailed, state)
	assert.Contains(t, message, "decode")
}

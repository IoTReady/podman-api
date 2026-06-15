package backupctl

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/iotready/podman-api/internal/config"
	"github.com/iotready/podman-api/internal/instance"
	"github.com/iotready/podman-api/internal/render"
	"github.com/iotready/podman-api/internal/store"
)

// fakeSvc is a minimal Service for driving the controller without a real
// instance.Service (which needs a podman client + hosts).
type fakeSvc struct {
	hosts         []config.Host
	instances     map[string][]instance.Observed // host id -> observed
	templates     map[string]store.Template      // template id -> template
	backups       map[string][]store.Backup      // "host/tmpl/slug" -> newest-first
	backupableErr error
	listErr       error
}

func key(host, tmpl, slug string) string { return host + "/" + tmpl + "/" + slug }

func (f *fakeSvc) Hosts() []config.Host { return f.hosts }

func (f *fakeSvc) ListAllInstances(_ context.Context, host string) ([]instance.Observed, error) {
	if f.listErr != nil {
		return nil, f.listErr
	}
	return f.instances[host], nil
}

func (f *fakeSvc) GetTemplate(_ context.Context, id string) (store.Template, error) {
	t, ok := f.templates[id]
	if !ok {
		return store.Template{}, store.ErrNotFound
	}
	return t, nil
}

func (f *fakeSvc) ListBackups(_ context.Context, host, tmpl, slug string, _ int) ([]store.Backup, error) {
	return f.backups[key(host, tmpl, slug)], nil
}

func (f *fakeSvc) CheckBackupable(_ context.Context, _, _, _ string) error { return f.backupableErr }

func tmpl(id string, vols ...render.Volume) store.Template {
	return store.Template{Meta: render.Meta{ID: id, Volumes: vols}}
}

func TestListBackupInstances_filtersToBackupMarkedVolumes(t *testing.T) {
	svc := &fakeSvc{
		hosts: []config.Host{{ID: "h1"}},
		instances: map[string][]instance.Observed{
			"h1": {
				{Template: "web", Slug: "a"},
				{Template: "web", Slug: "b"},
				{Template: "plain", Slug: "c"},
			},
		},
		templates: map[string]store.Template{
			"web":   tmpl("web", render.Volume{Name: "data", Backup: "s3; interval=6h"}, render.Volume{Name: "cache"}),
			"plain": tmpl("plain", render.Volume{Name: "d"}),
		},
	}
	c := &Controller{Svc: svc, Jobs: store.NewMemory()}

	got, err := c.ListBackupInstances(context.Background())
	require.NoError(t, err)

	// plain/c has no backup-marked volume → excluded. web/a and web/b included,
	// each carrying only the marked "data" volume with its raw marker.
	require.Len(t, got, 2)
	for _, bi := range got {
		assert.Equal(t, "h1", bi.Host)
		assert.Equal(t, "web", bi.Template)
		require.Len(t, bi.Volumes, 1)
		assert.Equal(t, "data", bi.Volumes[0].Name)
		assert.Equal(t, "s3; interval=6h", bi.Volumes[0].Backup)
	}
	slugs := []string{got[0].Slug, got[1].Slug}
	assert.ElementsMatch(t, []string{"a", "b"}, slugs)
}

func TestLastBackupAt_newestComplete(t *testing.T) {
	t1 := time.Date(2026, 6, 10, 0, 0, 0, 0, time.UTC)
	t2 := time.Date(2026, 6, 12, 0, 0, 0, 0, time.UTC)
	svc := &fakeSvc{
		backups: map[string][]store.Backup{
			// newest-first; the newest is still creating (not complete)
			key("h1", "web", "a"): {
				{ID: "bk_3", State: store.BackupCreating},
				{ID: "bk_2", State: store.BackupComplete, Finished: t2},
				{ID: "bk_1", State: store.BackupComplete, Finished: t1},
			},
		},
	}
	c := &Controller{Svc: svc, Jobs: store.NewMemory()}

	got, err := c.LastBackupAt(context.Background(), "h1", "web", "a")
	require.NoError(t, err)
	assert.Equal(t, t2, got, "should return the newest *complete* backup's finish time")

	// no backups → zero time
	zero, err := c.LastBackupAt(context.Background(), "h1", "web", "missing")
	require.NoError(t, err)
	assert.True(t, zero.IsZero())
}

func TestEnqueueBackup_enqueuesBackupJob(t *testing.T) {
	mem := store.NewMemory()
	c := &Controller{Svc: &fakeSvc{}, Jobs: mem}

	id, err := c.EnqueueBackup(context.Background(), "h1", "web", "a")
	require.NoError(t, err)
	assert.NotEmpty(t, id)

	jobs, err := mem.ListJobs(context.Background(), store.JobFilter{Kind: "backup"})
	require.NoError(t, err)
	require.Len(t, jobs, 1)
	assert.Equal(t, id, jobs[0].ID)
	var req instance.BackupRequest
	require.NoError(t, json.Unmarshal(jobs[0].Args, &req))
	assert.Equal(t, "h1", req.Host)
	assert.Equal(t, "web", req.Template)
	assert.Equal(t, "a", req.Slug)
	assert.NotEmpty(t, req.BackupID)
}

func TestEnqueueBackup_dedupesInFlight(t *testing.T) {
	mem := store.NewMemory()
	// pre-enqueue a backup job for the same instance → already in flight (queued)
	pre := instance.BackupRequest{BackupID: store.NewBackupID(), Host: "h1", Template: "web", Slug: "a"}
	args, _ := json.Marshal(pre)
	_, err := mem.Enqueue(context.Background(), "backup", args, "")
	require.NoError(t, err)

	c := &Controller{Svc: &fakeSvc{}, Jobs: mem}
	id, err := c.EnqueueBackup(context.Background(), "h1", "web", "a")
	require.NoError(t, err)
	assert.Empty(t, id, "should enqueue nothing when a backup is already in flight")

	jobs, err := mem.ListJobs(context.Background(), store.JobFilter{Kind: "backup"})
	require.NoError(t, err)
	assert.Len(t, jobs, 1, "no second backup job should be enqueued")
}

func TestEnqueueBackup_differentInstanceNotDeduped(t *testing.T) {
	mem := store.NewMemory()
	pre := instance.BackupRequest{BackupID: store.NewBackupID(), Host: "h1", Template: "web", Slug: "a"}
	args, _ := json.Marshal(pre)
	_, err := mem.Enqueue(context.Background(), "backup", args, "")
	require.NoError(t, err)

	c := &Controller{Svc: &fakeSvc{}, Jobs: mem}
	id, err := c.EnqueueBackup(context.Background(), "h1", "web", "b") // different slug
	require.NoError(t, err)
	assert.NotEmpty(t, id, "a different instance must not be deduped against an in-flight one")
}

func TestEnqueueBackup_propagatesCheckError(t *testing.T) {
	c := &Controller{Svc: &fakeSvc{backupableErr: instance.ErrInstanceNotFound}, Jobs: store.NewMemory()}
	id, err := c.EnqueueBackup(context.Background(), "h1", "web", "gone")
	require.ErrorIs(t, err, instance.ErrInstanceNotFound)
	assert.Empty(t, id)
}

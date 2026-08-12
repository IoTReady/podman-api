package backupctl

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/iotready/podman-api/extension"
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

func (f *fakeSvc) CheckBackupable(_ context.Context, _, _, _ string, _ []string) error {
	return f.backupableErr
}

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

	id, err := c.EnqueueBackup(context.Background(), "h1", "web", "a", extension.BackupOptions{})
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
	id, err := c.EnqueueBackup(context.Background(), "h1", "web", "a", extension.BackupOptions{})
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
	id, err := c.EnqueueBackup(context.Background(), "h1", "web", "b", extension.BackupOptions{}) // different slug
	require.NoError(t, err)
	assert.NotEmpty(t, id, "a different instance must not be deduped against an in-flight one")
}

// TestEnqueueBackup_ValidatesScopeBeforeDedupe: a bad scope must surface even
// when a backup for the same instance is already in flight. The other order
// answers a misconfigured scheduler with ("", nil) — "already in flight,
// nothing to do" — which it records as a handled tick, so the typo'd or newly
// vetoed volume name stays invisible for as long as backups keep overlapping.
func TestEnqueueBackup_ValidatesScopeBeforeDedupe(t *testing.T) {
	mem := store.NewMemory()
	pre := instance.BackupRequest{BackupID: store.NewBackupID(), Host: "h1", Template: "web", Slug: "a"}
	args, _ := json.Marshal(pre)
	_, err := mem.Enqueue(context.Background(), "backup", args, "")
	require.NoError(t, err)

	c := &Controller{Svc: &fakeSvc{backupableErr: instance.ErrInvalidBackupScope}, Jobs: mem}
	id, err := c.EnqueueBackup(context.Background(), "h1", "web", "a",
		extension.BackupOptions{Volumes: &[]string{"typo"}})
	require.ErrorIs(t, err, instance.ErrInvalidBackupScope)
	assert.Empty(t, id)
}

func TestEnqueueBackup_propagatesCheckError(t *testing.T) {
	c := &Controller{Svc: &fakeSvc{backupableErr: instance.ErrInstanceNotFound}, Jobs: store.NewMemory()}
	id, err := c.EnqueueBackup(context.Background(), "h1", "web", "gone", extension.BackupOptions{})
	require.ErrorIs(t, err, instance.ErrInstanceNotFound)
	assert.Empty(t, id)
}

// TestListBackupInstances_DropsNoneMarkedVolumes: the core now interprets `none`, so
// a vetoed volume must not be projected to a scheduler that would then have to
// re-derive the same veto — and an instance whose ONLY marked volume is `none`
// is not backup-eligible at all.
func TestListBackupInstances_DropsNoneMarkedVolumes(t *testing.T) {
	svc := &fakeSvc{
		hosts: []config.Host{{ID: "h1"}},
		instances: map[string][]instance.Observed{
			"h1": {
				{Template: "web", Slug: "a"},
				{Template: "vetoed", Slug: "b"},
			},
		},
		templates: map[string]store.Template{
			"web": tmpl("web",
				render.Volume{Name: "sites", Backup: "s3; interval=24h"},
				render.Volume{Name: "logs", Backup: "none"},
				render.Volume{Name: "cache"}),
			// Every marked volume is vetoed: not backup-eligible at all.
			"vetoed": tmpl("vetoed", render.Volume{Name: "logs", Backup: "none"}),
		},
	}
	c := &Controller{Svc: svc}

	got, err := c.ListBackupInstances(context.Background())
	require.NoError(t, err)

	require.Len(t, got, 1, "an instance whose only marker is `none` is not eligible")
	assert.Equal(t, "web", got[0].Template)
	require.Len(t, got[0].Volumes, 1, "a `none` marker must not be projected")
	assert.Equal(t, "sites", got[0].Volumes[0].Name)
	assert.Equal(t, "s3; interval=24h", got[0].Volumes[0].Backup)
}

// TestEnqueueBackup_ExplicitlyEmptyScopeIsAnError (re-review F): a scheduler
// whose filter produced an empty list must not receive a job id for a
// FULL-INSTANCE backup it never asked for. nil means unscoped; an explicitly
// empty slice is a request for nothing, which is a bug at the caller.
func TestEnqueueBackup_ExplicitlyEmptyScopeIsAnError(t *testing.T) {
	mem := store.NewMemory()
	c := &Controller{Svc: &fakeSvc{}, Jobs: mem}

	id, err := c.EnqueueBackup(context.Background(), "h1", "web", "a",
		extension.BackupOptions{Volumes: &[]string{}})
	require.ErrorIs(t, err, instance.ErrInvalidBackupScope)
	assert.Empty(t, id)

	jobs, err := mem.ListJobs(context.Background(), store.JobFilter{Kind: "backup"})
	require.NoError(t, err)
	assert.Empty(t, jobs, "an empty scope must never escalate to a full-instance backup")
}

// TestEnqueueBackup_NilScopeIsUnscoped is the other half of F: absent still
// means "every declared volume not marked none", and the persisted args carry
// no scope.
func TestEnqueueBackup_NilScopeIsUnscoped(t *testing.T) {
	mem := store.NewMemory()
	c := &Controller{Svc: &fakeSvc{}, Jobs: mem}

	id, err := c.EnqueueBackup(context.Background(), "h1", "web", "a", extension.BackupOptions{})
	require.NoError(t, err)
	require.NotEmpty(t, id)

	jobs, err := mem.ListJobs(context.Background(), store.JobFilter{Kind: "backup"})
	require.NoError(t, err)
	require.Len(t, jobs, 1)
	var req instance.BackupRequest
	require.NoError(t, json.Unmarshal(jobs[0].Args, &req))
	assert.Empty(t, req.Volumes)
}

// enqueueInFlight puts a backup job for h1/web/a with the given scope into the
// queue, as an already-running backup would have left it.
func enqueueInFlight(t *testing.T, mem *store.Memory, volumes []string) {
	t.Helper()
	pre := instance.BackupRequest{
		BackupID: store.NewBackupID(), Host: "h1", Template: "web", Slug: "a", Volumes: volumes,
	}
	args, err := json.Marshal(pre)
	require.NoError(t, err)
	_, err = mem.Enqueue(context.Background(), "backup", args, "")
	require.NoError(t, err)
}

// TestEnqueueBackup_DedupeOnlyWhenTheInFlightScopeCovers (re-review G): the
// dedupe was safe while every enqueue for an instance requested the same work.
// With scopes, swallowing a request the in-flight run will NOT satisfy drops
// that window's snapshot entirely — never taken, never retried, no error.
func TestEnqueueBackup_DedupeOnlyWhenTheInFlightScopeCovers(t *testing.T) {
	tests := []struct {
		name        string
		inFlight    []string // nil = unscoped
		want        *[]string
		wantEnqueue bool
	}{
		{name: "unscoped in flight covers a scoped request", inFlight: nil, want: &[]string{"wal"}},
		{name: "unscoped in flight covers an unscoped request", inFlight: nil, want: nil},
		{name: "same scope is covered", inFlight: []string{"wal"}, want: &[]string{"wal"}},
		{name: "a subset is covered", inFlight: []string{"wal", "data"}, want: &[]string{"wal"}},
		{
			name: "a disjoint scope is NOT covered", inFlight: []string{"data"},
			want: &[]string{"wal"}, wantEnqueue: true,
		},
		{
			name: "a superset is NOT covered", inFlight: []string{"wal"},
			want: &[]string{"wal", "data"}, wantEnqueue: true,
		},
		{
			// The in-flight run captures only `data`; an unscoped request asks
			// for every volume, which that run will not deliver.
			name: "a scoped run does NOT cover an unscoped request", inFlight: []string{"data"},
			want: nil, wantEnqueue: true,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			mem := store.NewMemory()
			enqueueInFlight(t, mem, tc.inFlight)
			c := &Controller{Svc: &fakeSvc{}, Jobs: mem}

			id, err := c.EnqueueBackup(context.Background(), "h1", "web", "a",
				extension.BackupOptions{Volumes: tc.want})
			require.NoError(t, err)

			jobs, err := mem.ListJobs(context.Background(), store.JobFilter{Kind: "backup"})
			require.NoError(t, err)
			if tc.wantEnqueue {
				assert.NotEmpty(t, id)
				assert.Len(t, jobs, 2, "an uncovered request must be enqueued, not swallowed")
			} else {
				assert.Empty(t, id)
				assert.Len(t, jobs, 1)
			}
		})
	}
}

// TestBackupMarkers_NearMissNoneStillVetoes (re-review D): the projection reads
// the marker through the same fail-closed comparison, so a `None` stored before
// the registration validator existed is not handed to a scheduler as an opaque
// commercial marker.
func TestBackupMarkers_NearMissNoneStillVetoes(t *testing.T) {
	svc := &fakeSvc{
		hosts:     []config.Host{{ID: "h1"}},
		instances: map[string][]instance.Observed{"h1": {{Template: "web", Slug: "a"}}},
		templates: map[string]store.Template{
			"web": tmpl("web",
				render.Volume{Name: "logs", Backup: "None"},
				render.Volume{Name: "data", Backup: "s3; interval=24h"},
			),
		},
	}
	c := &Controller{Svc: svc, Jobs: store.NewMemory()}

	got, err := c.ListBackupInstances(context.Background())
	require.NoError(t, err)
	require.Len(t, got, 1)
	require.Len(t, got[0].Volumes, 1)
	assert.Equal(t, "data", got[0].Volumes[0].Name)
}

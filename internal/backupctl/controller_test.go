package backupctl

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
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

	// hostErr is returned by CheckBackupable only — the half that touches the
	// host — so a test can tell the declarative check apart from the existence
	// one. backupableErr is returned by both, as a scope error would be.
	hostErr         error
	declaredCalls   int
	backupableCalls int

	// existingBackupIDs, when non-nil, gates DeleteBackup: an id not in the
	// set is reported ErrBackupNotFound, mirroring the real Service. A nil map
	// means "anything is deletable" for tests that don't care.
	existingBackupIDs map[string]bool
	deleteErr         error
	deletedIDs        []string
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
	f.backupableCalls++
	if f.hostErr != nil {
		return f.hostErr
	}
	return f.backupableErr
}

func (f *fakeSvc) CheckBackupScopeDeclared(_ context.Context, _, _, _ string, _ []string) error {
	f.declaredCalls++
	return f.backupableErr
}

func (f *fakeSvc) DeleteBackup(_ context.Context, id string) error {
	if f.deleteErr != nil {
		return f.deleteErr
	}
	if f.existingBackupIDs != nil && !f.existingBackupIDs[id] {
		return fmt.Errorf("%w: %s", instance.ErrBackupNotFound, id)
	}
	f.deletedIDs = append(f.deletedIDs, id)
	return nil
}

func tmpl(id string, vols ...render.Volume) store.Template {
	return store.Template{Meta: render.Meta{ID: id, Volumes: vols}}
}

// TestListBackupInstances_filtersToBackupMarkedVolumes: eligibility is
// per-INSTANCE ("does this template declare at least one non-`none` volume"),
// not per-marker — an unmarked volume ("cache") is eligible too (#255) and is
// projected alongside an explicitly marked one, carrying its raw (empty)
// marker unchanged. "plain" declares only a `none`-marked volume, so it alone
// is excluded.
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
			"plain": tmpl("plain", render.Volume{Name: "d", Backup: "none"}),
		},
	}
	c := &Controller{Svc: svc, Jobs: store.NewMemory()}

	got, err := c.ListBackupInstances(context.Background())
	require.NoError(t, err)

	// plain/c's only declared volume is `none`-vetoed → excluded. web/a and
	// web/b included, each carrying BOTH declared volumes: "data" with its raw
	// marker, and the unmarked "cache" with an empty one.
	require.Len(t, got, 2)
	for _, bi := range got {
		assert.Equal(t, "h1", bi.Host)
		assert.Equal(t, "web", bi.Template)
		require.Len(t, bi.Volumes, 2)
		names := []string{bi.Volumes[0].Name, bi.Volumes[1].Name}
		assert.ElementsMatch(t, []string{"data", "cache"}, names)
		for _, v := range bi.Volumes {
			if v.Name == "data" {
				assert.Equal(t, "s3; interval=6h", v.Backup)
			} else {
				assert.Equal(t, "", v.Backup, "an unmarked volume must project an empty (not synthesised) marker")
			}
		}
	}
	slugs := []string{got[0].Slug, got[1].Slug}
	assert.ElementsMatch(t, []string{"a", "b"}, slugs)
}

// TestListBackupInstances_UnmarkedVolumeMakesInstanceEligible pins #255: a
// template shaped `[data: none, logs: unmarked]` used to be entirely invisible
// to ListBackupInstances (both volumes were dropped before the eligibility
// test), while CheckBackupable's unscoped-empty-scope guard would gladly admit
// an unscoped backup of the same instance — "logs" is declared and not
// `none`-vetoed, which is exactly what that guard accepts. The two halves of
// the core must agree.
func TestListBackupInstances_UnmarkedVolumeMakesInstanceEligible(t *testing.T) {
	svc := &fakeSvc{
		hosts: []config.Host{{ID: "h1"}},
		instances: map[string][]instance.Observed{
			"h1": {{Template: "app", Slug: "a"}},
		},
		templates: map[string]store.Template{
			"app": tmpl("app",
				render.Volume{Name: "data", Backup: "none"},
				render.Volume{Name: "logs"}), // unmarked
		},
	}
	c := &Controller{Svc: svc, Jobs: store.NewMemory()}

	got, err := c.ListBackupInstances(context.Background())
	require.NoError(t, err)

	require.Len(t, got, 1, "an instance with an unmarked, non-vetoed volume must be eligible")
	require.Len(t, got[0].Volumes, 1, "the `none`-vetoed volume must still be excluded")
	assert.Equal(t, "logs", got[0].Volumes[0].Name)
	assert.Equal(t, "", got[0].Volumes[0].Backup)
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

// TestEnqueueBackup_ThreadsModeIntoTheBackupRequest guards against Mode being
// dropped between extension.BackupOptions and the instance.BackupRequest this
// enqueues — the two are otherwise easy to let drift since Mode is opaque to
// this controller (#135): it is set by a commercial scheduler's own marker
// grammar and interpreted only by instance.Service.
func TestEnqueueBackup_ThreadsModeIntoTheBackupRequest(t *testing.T) {
	mem := store.NewMemory()
	c := &Controller{Svc: &fakeSvc{}, Jobs: mem}
	volumes := []string{"sites"}

	id, err := c.EnqueueBackup(context.Background(), "h1", "web", "a", extension.BackupOptions{Volumes: &volumes, Mode: "live"})
	require.NoError(t, err)
	require.NotEmpty(t, id)

	jobs, err := mem.ListJobs(context.Background(), store.JobFilter{Kind: "backup"})
	require.NoError(t, err)
	require.Len(t, jobs, 1)
	var req instance.BackupRequest
	require.NoError(t, json.Unmarshal(jobs[0].Args, &req))
	assert.Equal(t, "live", req.Mode)
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

// TestEnqueueBackup_HostResolutionRunsOnlyForATickThatEnqueues: the declarative
// scope check runs before dedupe, so a bad scope is never masked by a run in
// flight; the existence check runs AFTER, so a covered tick costs no host round
// trips at all. Before this split, a sweep paid a VolumeInspect per declared
// volume for every instance it was about to skip (review-6 finding 5).
func TestEnqueueBackup_HostResolutionRunsOnlyForATickThatEnqueues(t *testing.T) {
	mem := store.NewMemory()
	pre := instance.BackupRequest{BackupID: store.NewBackupID(), Host: "h1", Template: "web", Slug: "a"}
	args, _ := json.Marshal(pre)
	_, err := mem.Enqueue(context.Background(), "backup", args, "")
	require.NoError(t, err)

	svc := &fakeSvc{}
	c := &Controller{Svc: svc, Jobs: mem}

	// Covered by the unscoped job already in flight: dedupes, and never asks
	// the host anything.
	id, err := c.EnqueueBackup(context.Background(), "h1", "web", "a", extension.BackupOptions{})
	require.NoError(t, err)
	assert.Empty(t, id)
	assert.Equal(t, 1, svc.declaredCalls, "declarative check must still run, so a bad scope is not masked by dedupe")
	assert.Zero(t, svc.backupableCalls, "a deduped tick must not touch the host")

	// A different instance is not covered, so this one does resolve.
	_, err = c.EnqueueBackup(context.Background(), "h1", "web", "b", extension.BackupOptions{})
	require.NoError(t, err)
	assert.Equal(t, 1, svc.backupableCalls, "a tick that enqueues resolves exactly once")
}

// TestEnqueueBackup_HostErrorSurfacesFromTheEnqueueingTick: the existence check
// moved after dedupe, so its errors must still reach the caller — a scheduler
// has to tell a transient host blip from a permanent scope problem.
func TestEnqueueBackup_HostErrorSurfacesFromTheEnqueueingTick(t *testing.T) {
	svc := &fakeSvc{hostErr: errors.New("list volumes on h1: dial: connection refused")}
	c := &Controller{Svc: svc, Jobs: store.NewMemory()}
	id, err := c.EnqueueBackup(context.Background(), "h1", "web", "a", extension.BackupOptions{})
	require.Error(t, err)
	assert.NotErrorIs(t, err, instance.ErrInvalidBackupScope, "a host error must never read as a scope error")
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
// re-derive the same veto — and an instance whose ONLY declared volume is
// `none` is not backup-eligible at all. An unmarked volume is NOT vetoed
// (#255) and stays projected alongside a marked one.
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
			// Every declared volume is `none`-vetoed: not backup-eligible at all.
			"vetoed": tmpl("vetoed", render.Volume{Name: "logs", Backup: "none"}),
		},
	}
	c := &Controller{Svc: svc}

	got, err := c.ListBackupInstances(context.Background())
	require.NoError(t, err)

	require.Len(t, got, 1, "an instance whose only declared volume is `none`-vetoed is not eligible")
	assert.Equal(t, "web", got[0].Template)
	require.Len(t, got[0].Volumes, 2, "a `none` marker must not be projected, but the unmarked volume must be")
	names := []string{got[0].Volumes[0].Name, got[0].Volumes[1].Name}
	assert.ElementsMatch(t, []string{"sites", "cache"}, names)
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

// TestEnqueueBackup_ReconcilingJobDefersTheTick (round-3 finding 9, corrected
// by review-4 finding 6): a reconciling backup job is non-terminal, and
// ReconcileBackup only fails the row, reaps partial blobs and restarts the
// instance — it never exports. So it is not COVERAGE. But it IS concurrency:
// the crash left the instance STOPPED, and a backup started now records
// wasRunning=false and therefore never restarts the pod afterwards, leaving the
// instance down with two rows for one window and a green backup on something
// that is not serving. The tick is deferred regardless of scope, including a
// scope the reconciling job would not have covered.
//
// The deferral is reported as extension.ErrBackupDeferred, NOT as the `("", nil)`
// a covered tick returns (review-5 finding 6): those two answers mean opposite
// things to a scheduler's interval gate, and returning the "handled" one here
// would silently drop every window a multi-sweep reconcile spans.
func TestEnqueueBackup_ReconcilingJobDefersTheTick(t *testing.T) {
	reconciling := func(t *testing.T, pre instance.BackupRequest) *store.Memory {
		t.Helper()
		ctx := context.Background()
		mem := store.NewMemory()
		args, err := json.Marshal(pre)
		require.NoError(t, err)
		_, err = mem.Enqueue(ctx, "backup", args, "")
		require.NoError(t, err)
		// queued -> running -> reconciling (the daemon-restart sweep's own path).
		_, ok, err := mem.ClaimNext(ctx)
		require.NoError(t, err)
		require.True(t, ok)
		n, err := mem.MarkReconciling(ctx, []string{"backup"})
		require.NoError(t, err)
		require.Equal(t, 1, n)
		return mem
	}

	t.Run("unscoped tick", func(t *testing.T) {
		mem := reconciling(t, instance.BackupRequest{BackupID: store.NewBackupID(), Host: "h1", Template: "web", Slug: "a"})
		c := &Controller{Svc: &fakeSvc{}, Jobs: mem}
		id, err := c.EnqueueBackup(context.Background(), "h1", "web", "a", extension.BackupOptions{})
		require.ErrorIs(t, err, extension.ErrBackupDeferred,
			"a deferral must be distinguishable from a satisfied tick, or the scheduler re-arms its gate on an unhandled window")
		assert.Empty(t, id, "the instance is mid-recovery and very likely stopped; nothing may start against it")
	})

	t.Run("a scope the reconciling job would not have covered", func(t *testing.T) {
		// The reconciling job is scoped to ["data"], so scopeCovers would say it
		// does not cover a ["wal"] tick — which is true, and beside the point:
		// coverage is not the question, concurrency is.
		mem := reconciling(t, instance.BackupRequest{BackupID: store.NewBackupID(), Host: "h1", Template: "web", Slug: "a", Volumes: []string{"data"}})
		c := &Controller{Svc: &fakeSvc{}, Jobs: mem}
		want := []string{"wal"}
		id, err := c.EnqueueBackup(context.Background(), "h1", "web", "a", extension.BackupOptions{Volumes: &want})
		require.ErrorIs(t, err, extension.ErrBackupDeferred)
		assert.Empty(t, id, "a reconciling job of any scope defers a tick of any scope")
	})

	t.Run("a different instance is unaffected", func(t *testing.T) {
		mem := reconciling(t, instance.BackupRequest{BackupID: store.NewBackupID(), Host: "h1", Template: "web", Slug: "a"})
		c := &Controller{Svc: &fakeSvc{}, Jobs: mem}
		id, err := c.EnqueueBackup(context.Background(), "h1", "web", "b", extension.BackupOptions{})
		require.NoError(t, err)
		assert.NotEmpty(t, id, "the deferral is per instance, not global")
	})
}

// TestListBackups_projectsWithoutManifests (#292): a retention policy needs
// state/timing/size, never the per-file sha256 manifest — the field that makes
// a bulk listing expensive. The projection sums each row's per-volume sizes
// into one SizeBytes total and carries the state through as a plain string.
func TestListBackups_projectsWithoutManifests(t *testing.T) {
	created := time.Date(2026, 8, 24, 3, 0, 0, 0, time.UTC)
	finished := time.Date(2026, 8, 24, 3, 5, 0, 0, time.UTC)
	svc := &fakeSvc{
		backups: map[string][]store.Backup{
			key("h1", "glitchtip", "main"): {
				{
					ID: "bk_1", Host: "h1", Template: "glitchtip", Slug: "main",
					State: store.BackupComplete, Created: created, Finished: finished,
					Volumes: []store.BackupVolume{
						{Name: "pgdata", SizeBytes: 1000, Manifest: []byte(`{"a":1}`)},
						{Name: "uploads", SizeBytes: 234, Manifest: []byte(`{"b":2}`)},
					},
				},
				{
					ID: "bk_2", Host: "h1", Template: "glitchtip", Slug: "main",
					State: store.BackupFailed, Created: created,
				},
			},
		},
	}
	c := &Controller{Svc: svc, Jobs: store.NewMemory()}

	got, err := c.ListBackups(context.Background(), "h1", "glitchtip", "main", 0)
	require.NoError(t, err)
	require.Len(t, got, 2)

	assert.Equal(t, extension.Backup{
		ID: "bk_1", Host: "h1", Template: "glitchtip", Slug: "main",
		State: "complete", Created: created, Finished: finished, SizeBytes: 1234,
	}, got[0])
	assert.Equal(t, extension.Backup{
		ID: "bk_2", Host: "h1", Template: "glitchtip", Slug: "main",
		State: "failed", Created: created, SizeBytes: 0,
	}, got[1])
}

func TestListBackups_propagatesStoreError(t *testing.T) {
	wantErr := errors.New("store unreachable")
	c := &Controller{Svc: &erroringListBackupsSvc{err: wantErr}, Jobs: store.NewMemory()}
	_, err := c.ListBackups(context.Background(), "h1", "web", "a", 0)
	require.ErrorIs(t, err, wantErr)
}

// erroringListBackupsSvc wraps fakeSvc but forces ListBackups to fail,
// isolating that error path without overloading fakeSvc's existing listErr
// (which already means something else: ListAllInstances failing).
type erroringListBackupsSvc struct {
	fakeSvc
	err error
}

func (e *erroringListBackupsSvc) ListBackups(context.Context, string, string, string, int) ([]store.Backup, error) {
	return nil, e.err
}

// TestDeleteBackup_removesBlobsAndRow (#292): the controller's DeleteBackup
// delegates straight to the same Service.DeleteBackup the HTTP handler calls
// (blobs then row), once BackupDeletable has cleared it.
func TestDeleteBackup_removesBlobsAndRow(t *testing.T) {
	svc := &fakeSvc{existingBackupIDs: map[string]bool{"bk_1": true}}
	c := &Controller{Svc: svc, Jobs: store.NewMemory()}

	err := c.DeleteBackup(context.Background(), "bk_1")
	require.NoError(t, err)
	assert.Equal(t, []string{"bk_1"}, svc.deletedIDs)
}

// TestDeleteBackup_NoopWhenAbsent: a retention pass retried, or racing another
// deleter, must not treat "already gone" as a failure.
func TestDeleteBackup_NoopWhenAbsent(t *testing.T) {
	svc := &fakeSvc{existingBackupIDs: map[string]bool{}}
	c := &Controller{Svc: svc, Jobs: store.NewMemory()}

	err := c.DeleteBackup(context.Background(), "bk_gone")
	require.NoError(t, err)
	assert.Empty(t, svc.deletedIDs)
}

// TestDeleteBackup_RefusesBackupInFlight: a backup job still queued/running
// for this id must block the delete — retention must never delete a run that
// is still being written. Service.DeleteBackup is never reached.
func TestDeleteBackup_RefusesBackupInFlight(t *testing.T) {
	mem := store.NewMemory()
	req := instance.BackupRequest{BackupID: "bk_1", Host: "h1", Template: "web", Slug: "a"}
	args, err := json.Marshal(req)
	require.NoError(t, err)
	_, err = mem.Enqueue(context.Background(), "backup", args, "")
	require.NoError(t, err)

	svc := &fakeSvc{existingBackupIDs: map[string]bool{"bk_1": true}}
	c := &Controller{Svc: svc, Jobs: mem}

	err = c.DeleteBackup(context.Background(), "bk_1")
	require.ErrorIs(t, err, instance.ErrBackupBusy)
	assert.Empty(t, svc.deletedIDs, "an in-flight backup must never reach Service.DeleteBackup")
}

// TestDeleteBackup_RefusesRestoreInFlight mirrors the backup case for a
// restore job — the two jobs BackupDeletable checks.
func TestDeleteBackup_RefusesRestoreInFlight(t *testing.T) {
	mem := store.NewMemory()
	req := instance.RestoreRequest{BackupID: "bk_1"}
	args, err := json.Marshal(req)
	require.NoError(t, err)
	_, err = mem.Enqueue(context.Background(), "restore", args, "")
	require.NoError(t, err)

	svc := &fakeSvc{existingBackupIDs: map[string]bool{"bk_1": true}}
	c := &Controller{Svc: svc, Jobs: mem}

	err = c.DeleteBackup(context.Background(), "bk_1")
	require.ErrorIs(t, err, instance.ErrBackupBusy)
	assert.Empty(t, svc.deletedIDs)
}

func TestDeleteBackup_propagatesOtherErrors(t *testing.T) {
	wantErr := errors.New("blob store: connection refused")
	svc := &fakeSvc{deleteErr: wantErr}
	c := &Controller{Svc: svc, Jobs: store.NewMemory()}

	err := c.DeleteBackup(context.Background(), "bk_1")
	require.ErrorIs(t, err, wantErr)
}

package instance

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/iotready/podman-api/internal/config"
	"github.com/iotready/podman-api/internal/podman/fake"
	"github.com/iotready/podman-api/internal/store"
)

// noopRewrite is the rewriteFile argument for the tests that are not about the
// config file at all: it succeeds and its revert is a no-op. The file-rewrite
// path proper is exercised at the API layer, against a real hosts dir.
func noopRewrite() (func() error, error) { return func() error { return nil }, nil }

// blockingRenameStore parks inside the store's rename transaction until the
// test releases it, so the whole locked section of Service.RenameHost is
// observably in flight while another goroutine tries to Apply.
type blockingRenameStore struct {
	*store.Memory
	entered chan struct{}
	release chan struct{}
}

func (b *blockingRenameStore) RenameHost(ctx context.Context, oldID, newID string) error {
	close(b.entered)
	<-b.release
	return b.Memory.RenameHost(ctx, oldID, newID)
}

// A concurrent Apply for an EXISTING instance on the host being renamed must
// block until the rename completes — including an apply that carries no
// domains, which takes no host lock of its own. Without RenameHost holding the
// host lock plus every instance lock, such an Apply can persist its spec under
// the OLD host id after `UPDATE specs SET host = newID` has already run,
// permanently orphaning the row under an id no live host claims.
func TestServiceRenameHostBlocksConcurrentApply(t *testing.T) {
	hosts := []config.Host{{ID: "h1", Addr: "unix", Socket: "/x"}}
	svc, _ := newSvcWith(t, fake.New(), hosts, noIngressTemplate())
	st := &blockingRenameStore{
		Memory:  seedStore(t, noIngressTemplate()),
		entered: make(chan struct{}),
		release: make(chan struct{}),
	}
	svc.SetStore(st)
	ctx := context.Background()

	// An instance that already exists on h1 — this is the one RenameHost must
	// learn about via ListSpecKeys and hold the instance lock for.
	require.NoError(t, st.PutSpec(ctx, store.Spec{
		Host: "h1", Template: "db", Slug: "a",
		Parameters: map[string]any{"slug": "a", "image": "docker.io/library/postgres:16"},
	}))

	renameDone := make(chan error, 1)
	go func() { renameDone <- svc.RenameHost(ctx, "h1", "h2", noopRewrite) }()

	select {
	case <-st.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("RenameHost never reached the store transaction")
	}

	applyDone := make(chan error, 1)
	go func() {
		applyDone <- svc.Apply(ctx, "h1", ApplyRequest{
			Template:   "db",
			Slug:       "a",
			Parameters: map[string]any{"slug": "a", "image": "docker.io/library/postgres:17"},
		}, ApplyOptions{Replace: true})
	}()

	// The rename holds this instance's lock, so the apply must not make
	// progress while the rename is parked.
	select {
	case err := <-applyDone:
		t.Fatalf("Apply completed during the rename (err=%v): it was not serialized", err)
	case <-time.After(250 * time.Millisecond):
	}

	close(st.release)
	select {
	case err := <-renameDone:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("RenameHost did not return")
	}
	select {
	case <-applyDone:
		// The apply now resolves h1, which no longer exists; whatever it
		// returns, the point is that it ran after the migration, so it cannot
		// have written a spec row under the old id.
	case <-time.After(5 * time.Second):
		t.Fatal("Apply stayed blocked after the rename completed")
	}

	// No orphan: nothing left under the old id, and the instance is intact
	// under the new one.
	keys, err := st.ListSpecKeys(ctx, "h1")
	require.NoError(t, err)
	require.Empty(t, keys, "a spec row under the old host id is an orphan the API can never reach")
	_, err = st.GetSpec(ctx, "h2", "db", "a")
	require.NoError(t, err)

	// And an apply against the NEW id lands under the new id, proving the
	// instance is reachable again once the rename is done.
	require.NoError(t, svc.Apply(ctx, "h2", ApplyRequest{
		Template:   "db",
		Slug:       "a",
		Parameters: map[string]any{"slug": "a", "image": "docker.io/library/postgres:17"},
	}, ApplyOptions{Replace: true}))
	got, err := st.GetSpec(ctx, "h2", "db", "a")
	require.NoError(t, err)
	require.Equal(t, "docker.io/library/postgres:17", got.Parameters["image"])
}

// A host carrying several instances renames cleanly: RenameHost acquires one
// instance lock per key returned by ListSpecKeys, and a duplicate or mis-keyed
// acquisition there would deadlock rather than fail.
func TestServiceRenameHostManyInstances(t *testing.T) {
	hosts := []config.Host{{ID: "h1", Addr: "unix", Socket: "/x"}}
	svc := NewService(fake.New(), hosts)
	st := store.NewMemory()
	svc.SetStore(st)
	ctx := context.Background()

	keys := []store.SpecKey{{Template: "app", Slug: "b"}, {Template: "app", Slug: "a"}, {Template: "db", Slug: "a"}}
	for _, k := range keys {
		require.NoError(t, st.PutSpec(ctx, store.Spec{Host: "h1", Template: k.Template, Slug: k.Slug, Parameters: map[string]any{}}))
	}

	require.NoError(t, svc.RenameHost(ctx, "h1", "h2", noopRewrite))

	for _, k := range keys {
		_, err := st.GetSpec(ctx, "h2", k.Template, k.Slug)
		require.NoError(t, err, "%s/%s must move to the new host id", k.Template, k.Slug)
	}
	old, err := st.ListSpecKeys(ctx, "h1")
	require.NoError(t, err)
	require.Empty(t, old)
}

func TestServiceRenameHost(t *testing.T) {
	hosts := []config.Host{{ID: "h1", Addr: "unix", Socket: "/x"}}
	svc := NewService(fake.New(), hosts)
	st := store.NewMemory()
	svc.SetStore(st)
	ctx := context.Background()

	require.NoError(t, st.PutSpec(ctx, store.Spec{Host: "h1", Template: "app", Slug: "a", Parameters: map[string]any{}}))

	require.NoError(t, svc.RenameHost(ctx, "h1", "h2", noopRewrite))

	got := svc.Hosts()
	require.Len(t, got, 1)
	require.Equal(t, "h2", got[0].ID)

	_, err := st.GetSpec(ctx, "h2", "app", "a")
	require.NoError(t, err)
}

func TestServiceRenameHostUnknown(t *testing.T) {
	svc := NewService(fake.New(), nil)
	svc.SetStore(store.NewMemory())

	err := svc.RenameHost(context.Background(), "nope", "h2", noopRewrite)
	require.ErrorIs(t, err, ErrUnknownHost)
}

func TestServiceRenameHostConflict(t *testing.T) {
	hosts := []config.Host{{ID: "h1"}, {ID: "h2"}}
	svc := NewService(fake.New(), hosts)
	svc.SetStore(store.NewMemory())

	err := svc.RenameHost(context.Background(), "h1", "h2", noopRewrite)
	require.ErrorIs(t, err, ErrHostAlreadyExists)
}

func TestServiceRenameHostHasBackups(t *testing.T) {
	hosts := []config.Host{{ID: "h1"}}
	svc := NewService(fake.New(), hosts)
	st := store.NewMemory()
	svc.SetStore(st)
	ctx := context.Background()
	require.NoError(t, st.PutSpec(ctx, store.Spec{Host: "h1", Template: "app", Slug: "a", Parameters: map[string]any{}}))
	require.NoError(t, st.CreateBackup(ctx, store.Backup{ID: "b1", Host: "h1", Template: "app", Slug: "a", State: store.BackupCreating, Created: time.Now()}))

	err := svc.RenameHost(ctx, "h1", "h2", noopRewrite)
	require.ErrorIs(t, err, ErrHostHasBackups)
}

// A rename must propagate the new host list to the podman client too, not just
// to the Service's own host map — otherwise every podman operation against the
// new id fails with `unknown host` until a SIGHUP (final-review finding #1).
func TestServiceRenameHostUpdatesPodmanClient(t *testing.T) {
	hosts := []config.Host{{ID: "h1", Addr: "unix", Socket: "/x"}}
	cl := fake.New()
	svc := NewService(cl, hosts)
	st := store.NewMemory()
	svc.SetStore(st)
	ctx := context.Background()

	before := len(cl.SetHostsCalls)
	require.NoError(t, svc.RenameHost(ctx, "h1", "h2", noopRewrite))

	calls := cl.SetHostsCalls
	require.Greater(t, len(calls), before, "RenameHost must call client.SetHosts")
	last := calls[len(calls)-1]
	ids := make([]string, 0, len(last))
	for _, h := range last {
		ids = append(ids, h.ID)
	}
	require.Contains(t, ids, "h2")
	require.NotContains(t, ids, "h1")
	// The rest of the host record travels with the id.
	require.Equal(t, "/x", last[0].Socket)
}

// Per-host caches keyed by the old id would otherwise be exported as metrics
// under a host id that no longer exists, forever (final-review finding #3).
func TestServiceRenameHostEvictsPerHostCaches(t *testing.T) {
	hosts := []config.Host{{ID: "h1", Addr: "unix", Socket: "/x"}}
	svc := NewService(fake.New(), hosts)
	svc.SetStore(store.NewMemory())
	ctx := context.Background()

	svc.statsCache.put("h1", HostStats{FetchedAt: time.Now()})
	svc.volCache.put("h1", HostVolumeUsage{Sizes: map[string]int64{"v": 1}, FetchedAt: time.Now()})
	svc.instCache.put("h1", svc.instCache.beginRefresh("h1"), []Observed{{Template: "app", Slug: "a"}}, time.Now())

	require.Contains(t, svc.StatsSnapshot(), "h1")
	require.Contains(t, svc.VolumeUsageSnapshot(), "h1")
	require.Contains(t, svc.InventorySnapshot(), "h1")

	require.NoError(t, svc.RenameHost(ctx, "h1", "h2", noopRewrite))

	require.NotContains(t, svc.StatsSnapshot(), "h1")
	require.NotContains(t, svc.VolumeUsageSnapshot(), "h1")
	require.NotContains(t, svc.InventorySnapshot(), "h1")
}

// Two concurrent RenameHost calls both targeting the SAME old host id (to
// DIFFERENT new ids) must not both silently no-op. Before the fix,
// checkRenameHosts ran against a pre-lock Hosts() snapshot in which h1 was
// still live for both callers; the loser then serialized on hostLock("h1"),
// proceeded anyway once unblocked, matched zero rows in store.RenameHost
// (h1 already renamed by the winner), and returned nil — a 200 for a rename
// that never happened. With the check moved inside the lock, the loser
// re-observes that h1 is no longer live and returns a real error.
func TestServiceRenameHostConcurrentSameOldHost(t *testing.T) {
	hosts := []config.Host{{ID: "h1", Addr: "unix", Socket: "/x"}}
	svc := NewService(fake.New(), hosts)
	st := &blockingRenameStore{
		Memory:  store.NewMemory(),
		entered: make(chan struct{}),
		release: make(chan struct{}),
	}
	svc.SetStore(st)
	ctx := context.Background()

	require.NoError(t, st.PutSpec(ctx, store.Spec{Host: "h1", Template: "app", Slug: "a", Parameters: map[string]any{}}))

	firstDone := make(chan error, 1)
	go func() { firstDone <- svc.RenameHost(ctx, "h1", "h2", noopRewrite) }()

	select {
	case <-st.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("first RenameHost never reached the store transaction")
	}

	// The second rename targets a DIFFERENT new id but the SAME old id, so
	// it must block on hostLock("h1") behind the first call rather than
	// racing it.
	secondDone := make(chan error, 1)
	go func() { secondDone <- svc.RenameHost(ctx, "h1", "h3", noopRewrite) }()

	select {
	case err := <-secondDone:
		t.Fatalf("second RenameHost completed before the first (err=%v): it was not serialized on the host lock", err)
	case <-time.After(250 * time.Millisecond):
	}

	close(st.release)

	select {
	case err := <-firstDone:
		require.NoError(t, err, "the winner must succeed")
	case <-time.After(5 * time.Second):
		t.Fatal("first RenameHost did not return")
	}

	select {
	case err := <-secondDone:
		require.ErrorIs(t, err, ErrUnknownHost, "the loser must see h1 is no longer live, not silently no-op")
	case <-time.After(5 * time.Second):
		t.Fatal("second RenameHost did not return")
	}

	// The winner's rename actually took effect; the loser did not disturb it.
	got := svc.Hosts()
	require.Len(t, got, 1)
	require.Equal(t, "h2", got[0].ID)
	_, err := st.GetSpec(ctx, "h2", "app", "a")
	require.NoError(t, err)
}

// renameFailingStore accepts every pre-check but fails the migration itself,
// so the revert half of the locked sequence is reachable.
type renameFailingStore struct {
	*store.Memory
	err error
}

func (s renameFailingStore) RenameHost(_ context.Context, _, _ string) error { return s.err }

// A migration failure must run the revert, and must run it before RenameHost
// returns — i.e. still inside the host lock, not left to the caller.
func TestServiceRenameHostRevertsOnStoreFailure(t *testing.T) {
	hosts := []config.Host{{ID: "h1"}}
	svc := NewService(fake.New(), hosts)
	svc.SetStore(renameFailingStore{Memory: store.NewMemory(), err: errors.New("disk on fire")})

	rewritten, reverted := 0, 0
	err := svc.RenameHost(context.Background(), "h1", "h2", func() (func() error, error) {
		rewritten++
		return func() error { reverted++; return nil }, nil
	})
	require.Error(t, err)
	require.Equal(t, 1, rewritten)
	require.Equal(t, 1, reverted, "the revert must run inside RenameHost, not be left to the caller")
	require.Equal(t, "h1", svc.Hosts()[0].ID)
}

// A rename the checks refuse must never touch the config file: the whole point
// of moving the rewrite inside the lock is that the loser of a race writes
// nothing and therefore has nothing to revert over the winner's work.
func TestServiceRenameHostRefusedNeverRewritesFile(t *testing.T) {
	svc := NewService(fake.New(), []config.Host{{ID: "h1"}, {ID: "h2"}})
	svc.SetStore(store.NewMemory())

	called := false
	err := svc.RenameHost(context.Background(), "h1", "h2", func() (func() error, error) {
		called = true
		return func() error { return nil }, nil
	})
	require.ErrorIs(t, err, ErrHostAlreadyExists)
	require.False(t, called, "a refused rename must not rewrite the host config")
}

// A config rewrite that fails leaves the store untouched — it runs before the
// migration, so there is nothing to revert.
func TestServiceRenameHostRewriteFailureLeavesStoreUntouched(t *testing.T) {
	svc := NewService(fake.New(), []config.Host{{ID: "h1"}})
	st := store.NewMemory()
	svc.SetStore(st)
	ctx := context.Background()
	require.NoError(t, st.PutSpec(ctx, store.Spec{Host: "h1", Template: "app", Slug: "a", Parameters: map[string]any{}}))

	wantErr := errors.New("disk full")
	err := svc.RenameHost(ctx, "h1", "h2", func() (func() error, error) { return nil, wantErr })
	require.ErrorIs(t, err, wantErr)

	require.Equal(t, "h1", svc.Hosts()[0].ID)
	_, err = st.GetSpec(ctx, "h1", "app", "a")
	require.NoError(t, err, "the spec must stay under the old host id")
}

func TestServiceCanRenameHost(t *testing.T) {
	hosts := []config.Host{{ID: "h1"}, {ID: "other"}}
	svc := NewService(fake.New(), hosts)
	st := store.NewMemory()
	svc.SetStore(st)
	ctx := context.Background()

	require.NoError(t, svc.CanRenameHost(ctx, "h1", "h2"))
	require.ErrorIs(t, svc.CanRenameHost(ctx, "nope", "h2"), ErrUnknownHost)
	require.ErrorIs(t, svc.CanRenameHost(ctx, "h1", "other"), ErrHostAlreadyExists)

	require.NoError(t, st.CreateBackup(ctx, store.Backup{ID: "b1", Host: "h1", Template: "app", Slug: "a", State: store.BackupComplete, Created: time.Now()}))
	require.ErrorIs(t, svc.CanRenameHost(ctx, "h1", "h2"), ErrHostHasBackups)

	// Read-only: the host list is untouched by a preflight. (Hosts() is
	// map-derived, so assert on membership, never on order.)
	ids := make([]string, 0, 2)
	for _, h := range svc.Hosts() {
		ids = append(ids, h.ID)
	}
	require.ElementsMatch(t, []string{"h1", "other"}, ids)
}

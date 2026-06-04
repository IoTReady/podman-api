//go:build integration

package podman

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/containers/podman/v5/pkg/bindings/containers"
	"github.com/containers/podman/v5/pkg/bindings/images"
	"github.com/containers/podman/v5/pkg/bindings/volumes"
	"github.com/containers/podman/v5/pkg/domain/entities"
	"github.com/containers/podman/v5/pkg/specgen"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/iotready/podman-api/internal/config"
)

// pruneDeadline bounds a single prune call. A prune on a healthy daemon returns
// in well under a second; a multi-second stall means the target daemon is
// wedged on an object lock (observed on busy podman 4.9.3 hosts) rather than a
// test-logic failure. 60s is far above any healthy run yet well below the
// suite's go-test -timeout.
const pruneDeadline = 60 * time.Second

// runWithin runs fn (a prune call) in a goroutine and fails the test loudly if
// it does not return within pruneDeadline, instead of hanging the whole
// integration suite until go test's -timeout fires with a cryptic dump.
//
// A per-call context.WithTimeout would NOT work here: Real.ctxFor returns the
// cached, Background-rooted connection context and discards the per-call ctx, so
// the deadline never reaches the HTTP request. A wall-clock guard is the only
// effective bound. On timeout the prune goroutine is abandoned (it holds a wedged
// HTTP connection) — acceptable, the run is already failing.
func runWithin(t *testing.T, what string, fn func() error) {
	t.Helper()
	done := make(chan error, 1)
	go func() { done <- fn() }()
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(pruneDeadline):
		t.Fatalf("%s did not return within %s — the target podman daemon is likely wedged on a lock (not a test-logic failure)", what, pruneDeadline)
	}
}

// protectLabel mirrors prune.ProtectLabel. The podman package cannot import
// prune (prune already imports podman), so the constant is duplicated here —
// if prune.ProtectLabel ever changes, this must change with it.
const protectLabel = "podman-api.protect"

// itestPruneLabel scopes the volume-prune test to its own fixtures, so the
// prune call only ever touches the two volumes this test creates and can never
// reap unrelated volumes on the runner's podman host.
const itestPruneLabel = "podman-api.itest-prune"

// TestReal_VolumePrune_ProtectLabel_LocalOnly is the safety-critical guarantee:
// the `label!` protect filter must exclude volumes carrying
// podman-api.protect=true from a volume prune. The unit tests only assert the
// filter map is *recorded* (the fake doesn't enforce it); this exercises the
// real libpod GeneratePruneVolumeFilters path against a live daemon.
//
// Hermetic: both fixtures also carry an itest label, and the prune additionally
// filters on that label, so it reaps only these fixtures.
func TestReal_VolumePrune_ProtectLabel_LocalOnly(t *testing.T) {
	sock := localSocket(t)
	c, err := NewReal([]config.Host{{ID: "local", Addr: "unix", Socket: sock}})
	require.NoError(t, err)
	ctx := context.Background()
	conn, err := c.ctxFor(ctx, "local")
	require.NoError(t, err)

	const plain = "podman-api-itest-prune-vol-plain"
	const protected = "podman-api-itest-prune-vol-protected"

	// Register teardown before creating; pre-remove self-heals a prior crashed run.
	t.Cleanup(func() {
		_ = c.VolumeRemove(context.Background(), "local", plain, true)
		_ = c.VolumeRemove(context.Background(), "local", protected, true)
	})
	_ = c.VolumeRemove(ctx, "local", plain, true)
	_ = c.VolumeRemove(ctx, "local", protected, true)

	// plain: itest-scoped, unprotected -> must be reaped.
	_, err = volumes.Create(conn, entities.VolumeCreateOptions{
		Name:   plain,
		Labels: map[string]string{itestPruneLabel: "1"},
	}, nil)
	require.NoError(t, err)
	// protected: itest-scoped AND protect=true -> must survive.
	_, err = volumes.Create(conn, entities.VolumeCreateOptions{
		Name:   protected,
		Labels: map[string]string{itestPruneLabel: "1", protectLabel: "true"},
	}, nil)
	require.NoError(t, err)

	// The same protect-exclusion the handler issues, additionally scoped to the
	// itest label so this prune can only ever touch the two fixtures above.
	var rep PruneReport
	runWithin(t, "VolumePrune", func() error {
		var err error
		rep, err = c.VolumePrune(ctx, "local", map[string][]string{
			"label":  {itestPruneLabel + "=1"},
			"label!": {protectLabel + "=true"},
		})
		return err
	})

	// The protected volume survives.
	_, err = c.VolumeInspect(ctx, "local", protected)
	require.NoError(t, err, "protected volume must survive the prune")
	// The unprotected volume is reaped.
	_, err = c.VolumeInspect(ctx, "local", plain)
	require.ErrorIs(t, err, ErrNotFound, "unprotected volume should be reaped")
	// The report names the reaped volume and not the protected one.
	assert.Contains(t, rep.Items, plain)
	assert.NotContains(t, rep.Items, protected)
}

// TestReal_Prune_HostWide_LocalOnly exercises the real ImagePrune /
// ContainerPrune / BuildCachePrune code paths. These bindings take no filter —
// they reap ALL stopped containers, ALL dangling images, and the entire build
// cache on the target host — so the test is opt-in. Run it only against a
// disposable podman host (CI), never a daily-driver machine.
func TestReal_Prune_HostWide_LocalOnly(t *testing.T) {
	if os.Getenv("PODMAN_API_ITEST_PRUNE_DESTRUCTIVE") == "" {
		t.Skip("set PODMAN_API_ITEST_PRUNE_DESTRUCTIVE=1 to run; this reaps ALL stopped containers / dangling images / build cache on the target host")
	}
	sock := localSocket(t)
	c, err := NewReal([]config.Host{{ID: "local", Addr: "unix", Socket: sock}})
	require.NoError(t, err)
	ctx := context.Background()
	conn, err := c.ctxFor(ctx, "local")
	require.NoError(t, err)

	const img = "docker.io/library/alpine:latest"
	_, err = images.Pull(conn, img, new(images.PullOptions).WithPolicy("missing").WithQuiet(true))
	require.NoError(t, err)

	rmCtr := func(name string) {
		_, _ = containers.Remove(context.Background(), name,
			new(containers.RemoveOptions).WithForce(true).WithIgnore(true))
	}

	t.Run("containers", func(t *testing.T) {
		const name = "podman-api-itest-prune-ctr"
		spec := specgen.NewSpecGenerator(img, false)
		spec.Name = name
		spec.Command = []string{"true"}
		t.Cleanup(func() { rmCtr(name) })
		rmCtr(name)

		// Created but never started => stopped => prunable.
		created, err := containers.CreateWithSpec(conn, spec, nil)
		require.NoError(t, err)

		var rep PruneReport
		runWithin(t, "ContainerPrune", func() error {
			var err error
			rep, err = c.ContainerPrune(ctx, "local")
			return err
		})
		assert.Contains(t, rep.Items, created.ID, "stopped container should be reaped")

		exists, err := containers.Exists(ctx, created.ID, nil)
		require.NoError(t, err)
		assert.False(t, exists, "pruned container should no longer exist")
	})

	t.Run("dangling-image", func(t *testing.T) {
		// Commit a container twice to the same repo:tag with a distinct change
		// each time. The first commit's image loses its tag to the second and
		// becomes dangling (<none>), which ImagePrune(all=false) reaps.
		const seed = "podman-api-itest-prune-img-seed"
		const repo = "localhost/podman-api-itest-dangling"
		spec := specgen.NewSpecGenerator(img, false)
		spec.Name = seed
		spec.Command = []string{"true"}
		// firstImageID is the dangling image we expect the prune to reap. Capture
		// it so cleanup self-heals even if the test fails before the prune runs
		// (otherwise the now-untagged first commit would leak on the host).
		var firstImageID string
		t.Cleanup(func() {
			rmCtr(seed)
			if firstImageID != "" {
				_, _ = images.Remove(context.Background(), []string{firstImageID},
					new(images.RemoveOptions).WithForce(true))
			}
			_, _ = images.Remove(context.Background(), []string{repo + ":v1"},
				new(images.RemoveOptions).WithForce(true))
		})
		rmCtr(seed)
		_, err := containers.CreateWithSpec(conn, spec, nil)
		require.NoError(t, err)

		first, err := containers.Commit(conn, seed, new(containers.CommitOptions).
			WithRepo(repo).WithTag("v1").WithChanges([]string{"LABEL podman-api-itest-seq=1"}))
		require.NoError(t, err)
		firstImageID = first.ID
		_, err = containers.Commit(conn, seed, new(containers.CommitOptions).
			WithRepo(repo).WithTag("v1").WithChanges([]string{"LABEL podman-api-itest-seq=2"}))
		require.NoError(t, err)

		var rep PruneReport
		runWithin(t, "ImagePrune", func() error {
			var err error
			rep, err = c.ImagePrune(ctx, "local", false) // dangling only
			return err
		})
		assert.Contains(t, rep.Items, firstImageID, "dangling image should be reaped")
	})

	t.Run("build-cache", func(t *testing.T) {
		// Build cache can't be seeded without a real build; this verifies the
		// binding path executes against a live daemon and returns cleanly.
		runWithin(t, "BuildCachePrune", func() error {
			_, err := c.BuildCachePrune(ctx, "local")
			return err
		})
	})
}

package instance

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// PITRRestore recreates the instance once with a one-shot RestoreIntent that
// reaches the injector but is NEVER persisted: a subsequent reconcile re-injects
// with nil, so a point-in-time rollback fires exactly once instead of repeating
// on every pod restart.
func TestService_PITRRestore_PassesIntentOnceAndNeverReplays(t *testing.T) {
	defer setVerifyKnobs(50*time.Millisecond, 5*time.Millisecond)()
	svc, f := newSvc(t)
	ctx := context.Background()

	// Create the instance first (no injector yet, so call counts stay clean).
	require.NoError(t, svc.Apply(ctx, "h1", pgApply("demo"), ApplyOptions{Replace: true}))

	inj := &recordingInjector{}
	svc.SetSidecarInjector(inj)

	require.NoError(t, svc.PITRRestore(ctx, PITRRestoreRequest{
		Host: "h1", Template: "postgres", Slug: "demo",
		Timestamp: "2026-06-14T09:30:00Z", Volumes: []string{"data"},
	}, nil))

	// The restore recreate carried the intent to the injector.
	require.Equal(t, 1, inj.calls)
	require.NotNil(t, inj.gotRestore, "PITRRestore must hand the injector a RestoreIntent")
	assert.Equal(t, "2026-06-14T09:30:00Z", inj.gotRestore.Timestamp)
	assert.Equal(t, []string{"data"}, inj.gotRestore.Volumes)

	// Simulate a pod restart (the exact scenario that would replay a persisted
	// rollback): drop the pod, then reconcile. The re-converge must re-inject
	// with NO intent — proof the rollback was never persisted into the spec.
	require.NoError(t, f.PodRemove(ctx, "h1", "postgres-demo", true))
	svc.ReconcileSpecsOnHost(ctx, "h1")
	require.Equal(t, 2, inj.calls, "reconcile must re-inject after the pod was lost")
	assert.Nil(t, inj.gotRestore, "reconcile after a restore must carry no intent")
}

// PITRRestore on an instance with no stored spec returns an error rather than
// silently recreating nothing.
func TestService_PITRRestore_MissingInstance(t *testing.T) {
	svc, _ := newSvc(t)
	err := svc.PITRRestore(context.Background(), PITRRestoreRequest{
		Host: "h1", Template: "postgres", Slug: "ghost", Timestamp: "2026-06-14T09:30:00Z",
	}, nil)
	require.Error(t, err)
}

// CheckInstanceExists is PITR's precondition: it validates host + template +
// stored spec WITHOUT gating on the tarball blob store (PITR restores from the
// Litestream S3 replica, a different subsystem). newSvc wires no blob store, so a
// nil error here proves the check does not require one.
func TestService_CheckInstanceExists(t *testing.T) {
	svc, _ := newSvc(t)
	ctx := context.Background()
	require.NoError(t, svc.Apply(ctx, "h1", pgApply("demo"), ApplyOptions{Replace: true}))

	require.NoError(t, svc.CheckInstanceExists(ctx, "h1", "postgres", "demo"),
		"a deployed instance must pass without a blob store wired")
	assert.ErrorIs(t, svc.CheckInstanceExists(ctx, "h1", "postgres", "ghost"), ErrInstanceNotFound)
	assert.ErrorIs(t, svc.CheckInstanceExists(ctx, "nohost", "postgres", "demo"), ErrUnknownHost)
}

package instance

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/iotready/podman-api/internal/config"
	"github.com/iotready/podman-api/internal/podman"
	"github.com/iotready/podman-api/internal/podman/fake"
	"github.com/iotready/podman-api/internal/render"
	"github.com/iotready/podman-api/internal/store"
)

// specReconcileSvc builds a Service with one host (h1) and a "web" template
// that declares one per-instance secret, backed by a fake client and memory
// store. The template body includes a {{.slug}} parameter so we can verify
// the rendered YAML is correct.
func specReconcileSvc(t *testing.T) (*Service, *fake.Fake, *store.Memory) {
	t.Helper()
	fc := fake.New()
	hosts := []config.Host{{ID: "h1", Addr: "unix", Socket: "/x"}}
	webTmpl := store.Template{
		Meta: render.Meta{
			ID:         "web",
			Parameters: requiredParams("slug"),
			Secrets:    render.Secrets{PerInstance: []string{"password"}},
		},
		Body: `apiVersion: v1
kind: Pod
metadata:
  name: web-{{.slug}}
  labels:
    podman-api/template: web
    podman-api/slug: {{.slug}}
spec:
  containers:
    - name: app
      image: nginx:latest
`,
		Origin: "seed",
	}
	svc, st := newSvcWith(t, fc, hosts, webTmpl)
	return svc, fc, st
}

// seedBootSpec writes a stored spec for (host, tmpl, slug) into the memory store
// with the given optional secrets and a slug parameter matching the name.
func seedBootSpec(t *testing.T, st *store.Memory, host, tmpl, slug string, secrets map[string]string) {
	t.Helper()
	var sec map[string]string
	if secrets != nil {
		sec = secrets
	}
	err := st.PutSpec(context.Background(), store.Spec{
		Host:       host,
		Template:   tmpl,
		Slug:       slug,
		Parameters: map[string]any{"slug": slug},
		Secrets:    sec,
	})
	require.NoError(t, err)
}

// fakeIngressController is a test double for ingress.Controller that
// records every Reconcile call.
type fakeIngressController struct {
	reconcileCalls int
	lastHost       string
}

func (f *fakeIngressController) Reconcile(_ context.Context, host string) error {
	f.reconcileCalls++
	f.lastHost = host
	return nil
}

func TestReconcileSpecsOnHost_AlreadyRunning(t *testing.T) {
	svc, fc, st := specReconcileSvc(t)
	ctx := context.Background()

	// Seed a spec and pre-create the pod.
	seedBootSpec(t, st, "h1", "web", "my-app", nil)
	fc.AddPod("h1", podman.Pod{Name: "web-my-app", Status: "Running",
		Labels: map[string]string{"podman-api/template": "web", "podman-api/slug": "my-app"}})

	svc.ReconcileSpecsOnHost(ctx, "h1")

	// PlayKube should NOT have been called — the pod was already running.
	assert.Empty(t, fc.PlayCalls, "should not re-play an already-running pod")
}

func TestReconcileSpecsOnHost_MissingPod(t *testing.T) {
	svc, fc, st := specReconcileSvc(t)
	ctx := context.Background()

	seedBootSpec(t, st, "h1", "web", "my-app", nil)

	svc.ReconcileSpecsOnHost(ctx, "h1")

	// PlayKube should have been called once with the correct pod name.
	require.Len(t, fc.PlayCalls, 1, "should re-play a missing pod")
	assert.Equal(t, "h1", fc.PlayCalls[0].Host)
	assert.Contains(t, fc.PlayCalls[0].YAML, "name: web-my-app")
	assert.Contains(t, fc.PlayCalls[0].YAML, "podman-api/slug: my-app")
	assert.False(t, fc.PlayCalls[0].Replace, "replace should be false for boot converge")
}

func TestReconcileSpecsOnHost_WithSecrets(t *testing.T) {
	svc, fc, st := specReconcileSvc(t)
	ctx := context.Background()

	seedBootSpec(t, st, "h1", "web", "my-app", map[string]string{"password": "s3cret"})

	svc.ReconcileSpecsOnHost(ctx, "h1")

	// PlayKube should have been called.
	require.Len(t, fc.PlayCalls, 1, "should re-play a missing pod with secrets")

	// The secret should have been created on the host.
	secrets, err := fc.SecretList(ctx, "h1")
	require.NoError(t, err)
	found := false
	for _, s := range secrets {
		if s.Name == "web-my-app-password" {
			found = true
			break
		}
	}
	assert.True(t, found, "per-instance secret should exist on host after boot converge")
}

func TestReconcileSpecsOnHost_TemplateDeleted(t *testing.T) {
	svc, fc, st := specReconcileSvc(t)
	ctx := context.Background()

	// Seed a spec, then delete the template from the store.
	seedBootSpec(t, st, "h1", "web", "my-app", nil)
	require.NoError(t, st.DeleteTemplate(ctx, "web"))

	svc.ReconcileSpecsOnHost(ctx, "h1")

	// PlayKube should NOT have been called — template is gone.
	assert.Empty(t, fc.PlayCalls, "should not re-play when template is deleted")
}

// TestReconcileSpecsOnHost_RenamedAppliedVolumeSkipsReplayInsteadOfForkingData
// (round-3 review, "related, same root cause" as critical finding 1): boot
// converge replays the pod from the CURRENT template body, whose
// persistentVolumeClaim.claimName is authored against the CURRENT declared
// volume name. If the applied volume set names a volume the template no
// longer declares, and that volume's real data still exists under its OLD
// applied name, blindly replaying would bind a brand-new, empty volume under
// the new claim name while the real data sits orphaned and untouched. This
// must be refused, not silently forked.
func TestReconcileSpecsOnHost_RenamedAppliedVolumeSkipsReplayInsteadOfForkingData(t *testing.T) {
	svc, fc, st := specReconcileSvc(t)
	ctx := context.Background()

	seedBootSpec(t, st, "h1", "web", "my-app", nil)
	sp, err := st.GetSpec(ctx, "h1", "web", "my-app")
	require.NoError(t, err)
	sp.AppliedVolumes = []string{"data"} // applied under the OLD name
	require.NoError(t, st.PutSpec(ctx, sp))

	// The real data lives under the OLD applied name.
	fc.SetVolumeData("h1", "web-my-app-data", []byte("real data"))

	// The template renames "data" -> "pgdata" without this instance being
	// re-applied.
	tmpl, err := st.GetTemplate(ctx, "web")
	require.NoError(t, err)
	tmpl.Meta.Volumes = []render.Volume{{Name: "pgdata"}}
	require.NoError(t, st.PutTemplate(ctx, tmpl))

	svc.ReconcileSpecsOnHost(ctx, "h1")

	// PlayKube must NOT have run: replaying the current body would bind a
	// fresh, empty volume under "web-my-app-pgdata" while "web-my-app-data"
	// (the real data) sits untouched and unreferenced.
	assert.Empty(t, fc.PlayCalls, "must not silently fork data by replaying under the renamed claim")
}

// TestReconcileOneSpec_MixedAppliedSetReportsEveryRenamedVolume (#256 review
// round-4, addendum finding 2): the applied set names TWO volumes the
// template no longer declares, and BOTH are still present on the host under
// their old applied names (both genuinely renamed, not lost). Before the
// fix, reconcileOneSpec's guard returned on the FIRST confirmed rename and
// never inspected the rest of the set, so the refusal message named only one
// of the two renamed volumes even though evaluating the whole set was cheap
// and already in progress. This pins that the message covers every renamed
// volume, not just the first one found.
func TestReconcileOneSpec_MixedAppliedSetReportsEveryRenamedVolume(t *testing.T) {
	svc, fc, st := specReconcileSvc(t)
	ctx := context.Background()

	seedBootSpec(t, st, "h1", "web", "my-app", nil)
	sp, err := st.GetSpec(ctx, "h1", "web", "my-app")
	require.NoError(t, err)
	sp.AppliedVolumes = []string{"data", "wal"} // both applied under OLD names
	require.NoError(t, st.PutSpec(ctx, sp))

	// Both volumes' real data still exist on the host under their old applied
	// names — neither was actually lost.
	fc.SetVolumeData("h1", "web-my-app-data", []byte("real data"))
	fc.SetVolumeData("h1", "web-my-app-wal", []byte("real wal"))

	// The template renames both "data" and "wal" without this instance being
	// re-applied.
	tmpl, err := st.GetTemplate(ctx, "web")
	require.NoError(t, err)
	tmpl.Meta.Volumes = []render.Volume{{Name: "pgdata"}, {Name: "pgwal"}}
	require.NoError(t, st.PutTemplate(ctx, tmpl))

	_, err = svc.reconcileOneSpec(ctx, "h1", "web", "my-app")
	require.ErrorIs(t, err, errVolumeRenamePending)
	assert.Contains(t, err.Error(), "data", "must name every renamed volume, not just the first found")
	assert.Contains(t, err.Error(), "wal", "must name every renamed volume, not just the first found")
	assert.Empty(t, fc.PlayCalls, "must not replay while any renamed volume is pending re-apply")
}

// TestReconcileOneSpec_TransientInspectErrorDoesNotBlockReconcile (#256
// review round-4, addendum finding 2): a TRANSIENT VolumeInspect error (not
// ErrNotFound) on an applied volume the template no longer declares must not
// hard-abort the whole reconcile. Before the fix, any instance with a renamed
// volume depended on a live, error-free VolumeInspect round trip just to come
// back up after a routine restart; a flaky host call would leave the
// instance down indefinitely instead of just being unable to confirm this
// one ambiguous volume.
func TestReconcileOneSpec_TransientInspectErrorDoesNotBlockReconcile(t *testing.T) {
	svc, fc, st := specReconcileSvc(t)
	ctx := context.Background()

	seedBootSpec(t, st, "h1", "web", "my-app", nil)
	sp, err := st.GetSpec(ctx, "h1", "web", "my-app")
	require.NoError(t, err)
	sp.AppliedVolumes = []string{"data"} // applied under the OLD name
	require.NoError(t, st.PutSpec(ctx, sp))

	// The template renames "data" -> "pgdata" without this instance being
	// re-applied, and inspecting the old volume name transiently fails.
	tmpl, err := st.GetTemplate(ctx, "web")
	require.NoError(t, err)
	tmpl.Meta.Volumes = []render.Volume{{Name: "pgdata"}}
	require.NoError(t, st.PutTemplate(ctx, tmpl))
	fc.FailVolumeInspectFor("web-my-app-data", assert.AnError)

	reconciled, err := svc.reconcileOneSpec(ctx, "h1", "web", "my-app")
	require.NoError(t, err, "a transient inspect error must not hard-abort the reconcile")
	assert.True(t, reconciled)
	require.Len(t, fc.PlayCalls, 1, "reconcile must proceed despite the undetermined rename")
}

func TestReconcileSpecsOnHost_HostUnreachable(t *testing.T) {
	svc, fc, st := specReconcileSvc(t)
	ctx := context.Background()

	seedBootSpec(t, st, "h1", "web", "my-app", nil)

	// Make PodInspect return an error (simulates unreachable host).
	fc.PodInspectErr = assert.AnError

	svc.ReconcileSpecsOnHost(ctx, "h1")

	// PlayKube should NOT have been called — host was unreachable.
	assert.Empty(t, fc.PlayCalls, "should not re-play when host is unreachable")
}

func TestReconcileSpecsOnHost_NoSpecs(t *testing.T) {
	svc, fc, _ := specReconcileSvc(t)
	ctx := context.Background()

	// No specs seeded — nothing to reconcile.
	svc.ReconcileSpecsOnHost(ctx, "h1")

	assert.Empty(t, fc.PlayCalls, "should not play when there are no specs")
}

func TestReconcileSpecsOnHost_MultipleInstances(t *testing.T) {
	svc, fc, st := specReconcileSvc(t)
	ctx := context.Background()

	// Seed two specs: one already running, one missing.
	seedBootSpec(t, st, "h1", "web", "app-a", nil)
	seedBootSpec(t, st, "h1", "web", "app-b", nil)
	fc.AddPod("h1", podman.Pod{Name: "web-app-a", Status: "Running",
		Labels: map[string]string{"podman-api/template": "web", "podman-api/slug": "app-a"}})

	svc.ReconcileSpecsOnHost(ctx, "h1")

	// Only the missing pod should be re-played.
	require.Len(t, fc.PlayCalls, 1, "should re-play only the missing pod")
	assert.Contains(t, fc.PlayCalls[0].YAML, "name: web-app-b")
}

func TestReconcileSpecsOnHost_NonRunningPod(t *testing.T) {
	svc, fc, st := specReconcileSvc(t)
	ctx := context.Background()

	// Seed a spec where the pod exists but is not running (e.g. Exited).
	seedBootSpec(t, st, "h1", "web", "my-app", nil)
	fc.AddPod("h1", podman.Pod{Name: "web-my-app", Status: "Exited",
		Labels: map[string]string{"podman-api/template": "web", "podman-api/slug": "my-app"}})

	svc.ReconcileSpecsOnHost(ctx, "h1")

	// Should re-play because the pod is not running, with replace=true so
	// podman replaces the stale pod rather than failing with "pod exists".
	require.Len(t, fc.PlayCalls, 1, "should re-play a non-running pod")
	assert.Contains(t, fc.PlayCalls[0].YAML, "name: web-my-app")
	assert.True(t, fc.PlayCalls[0].Replace, "should use replace=true for a non-running pod")
}

// errStore wraps a store.Memory and injects errors on GetSpec for a specific
// (template, slug) pair.
type errStore struct {
	*store.Memory
	corruptTmpl string
	corruptSlug string
}

func (e *errStore) GetSpec(ctx context.Context, host, template, slug string) (store.Spec, error) {
	if template == e.corruptTmpl && slug == e.corruptSlug {
		return store.Spec{}, store.ErrSpecCorrupt
	}
	return e.Memory.GetSpec(ctx, host, template, slug)
}

// listErrStore wraps a store.Memory and fails ListSpecKeys.
type listErrStore struct {
	*store.Memory
}

func (l *listErrStore) ListSpecKeys(_ context.Context, _ string) ([]store.SpecKey, error) {
	return nil, assert.AnError
}

func TestReconcileSpecsOnHost_SpecCorrupt(t *testing.T) {
	svc, fc, st := specReconcileSvc(t)
	ctx := context.Background()

	// Replace the store with one that returns ErrSpecCorrupt for "web/my-app".
	es := &errStore{Memory: st, corruptTmpl: "web", corruptSlug: "my-app"}
	svc.SetStore(es)

	seedBootSpec(t, st, "h1", "web", "my-app", nil)

	svc.ReconcileSpecsOnHost(ctx, "h1")

	// PlayKube should NOT have been called — spec was corrupt.
	assert.Empty(t, fc.PlayCalls, "should not re-play when spec is corrupt")
}

func TestReconcileSpecsOnHost_StoreListError(t *testing.T) {
	fc := fake.New()
	ctx := context.Background()

	// Service with no templates — ListSpecKeys on the error store returns error
	// before any template lookup is needed.
	svc := NewService(fc, []config.Host{{ID: "h1", Addr: "unix", Socket: "/x"}})
	svc.SetStore(&listErrStore{Memory: store.NewMemory()})

	svc.ReconcileSpecsOnHost(ctx, "h1")

	// Should not error or panic; just log and return.
	assert.Empty(t, fc.PlayCalls, "should not play when store list fails")
}

func TestReconcileSpecsOnHost_IngressEnabled(t *testing.T) {
	svc, fc, st := specReconcileSvc(t)
	ctx := context.Background()

	// Enable ingress on the service.
	ingCtl := &fakeIngressController{}
	svc.SetIngress(ingCtl, "test-net")

	// Seed a spec with a domain so ingress would have routes to manage.
	seedBootSpec(t, st, "h1", "web", "my-app", nil)

	svc.ReconcileSpecsOnHost(ctx, "h1")

	// PlayKube should have been called.
	require.Len(t, fc.PlayCalls, 1)
	assert.Contains(t, fc.PlayCalls[0].YAML, "name: web-my-app")

	// Ingress should have been reconciled exactly once.
	assert.Equal(t, 1, ingCtl.reconcileCalls)
	assert.Equal(t, "h1", ingCtl.lastHost)
}

// TestReconcileSpecsOnHost_InvalidatesInstanceCache proves boot converge
// (which re-creates a missing pod via PlayKube outside the normal
// Apply/Delete/lifecycle funnels) drops the host's cached instance list, so a
// read served from a pre-converge cache does not stay stale for up to a TTL
// after the pod comes back up.
func TestReconcileSpecsOnHost_InvalidatesInstanceCache(t *testing.T) {
	svc, fc, st := specReconcileSvc(t)
	svc.SetInstanceCacheTTL(time.Minute)
	ctx := context.Background()

	// Seed a spec whose pod is missing, so ReconcileSpecsOnHost re-converges it.
	seedBootSpec(t, st, "h1", "web", "my-app", nil)

	// Prime the cache (pod missing, so it sweeps empty and caches that).
	if _, err := svc.ListAllInstances(ctx, "h1"); err != nil {
		t.Fatal(err)
	}
	before := fc.PodListCallCount()

	svc.ReconcileSpecsOnHost(ctx, "h1")

	if _, err := svc.ListAllInstances(ctx, "h1"); err != nil {
		t.Fatal(err)
	}
	if after := fc.PodListCallCount(); after == before {
		t.Error("ListAllInstances hit stale cache after boot converge; expected invalidation + refetch")
	}
}

// TestReconcileOneSpec_DroppedUnprunedVolumeDoesNotBlockReplay (#265 review
// finding 3): "applied name no longer declared, but still on the host" is
// the signature of a rename AND of an ordinary volume that was simply removed
// from the template while the old podman volume was never pruned — a routine
// template edit. The guard used to treat both as a pending rename and left
// the pod down on every boot-reconcile sweep until someone manually
// re-applied, converting a template cleanup into a fleet-wide outage. With no
// corroborating rename target (no newly declared volume that is still absent
// from the host), the replay must proceed.
func TestReconcileOneSpec_DroppedUnprunedVolumeDoesNotBlockReplay(t *testing.T) {
	svc, fc, st := specReconcileSvc(t)
	ctx := context.Background()

	seedBootSpec(t, st, "h1", "web", "my-app", nil)
	sp, err := st.GetSpec(ctx, "h1", "web", "my-app")
	require.NoError(t, err)
	sp.AppliedVolumes = []string{"data"}
	require.NoError(t, st.PutSpec(ctx, sp))

	// The old volume was never pruned after the template dropped it.
	fc.SetVolumeData("h1", "web-my-app-data", []byte("stale, unpruned"))

	tmpl, err := st.GetTemplate(ctx, "web")
	require.NoError(t, err)
	tmpl.Meta.Volumes = nil // `data` dropped outright, nothing renamed to
	require.NoError(t, st.PutTemplate(ctx, tmpl))

	reconciled, err := svc.reconcileOneSpec(ctx, "h1", "web", "my-app")
	require.NoError(t, err, "a dropped-and-unpruned volume must not block boot converge")
	assert.True(t, reconciled)
	require.Len(t, fc.PlayCalls, 1, "the pod must come back up")
}

// TestReconcileOneSpec_DropMixedWithGenuineRenameBlocksOnlyTheRename (#265
// review, round-2 blocking finding): `renamed` and `targets` used to be two
// flat, uncorrelated lists, and the guard refused for the WHOLE `renamed` list
// as soon as ANY target existed anywhere. So one genuine rename elsewhere in
// the same template blocked an unrelated, harmless drop that shared the
// applied set: `logs` dropped outright (old volume left unpruned) plus
// `cache` -> `cache2` renamed left the pod down on every boot sweep and named
// `logs` in the refusal, for which no target exists at all.
//
// The refusal itself is still correct — `cache` genuinely could fork — but it
// must name only the volumes an available target can account for.
func TestReconcileOneSpec_DropMixedWithGenuineRenameBlocksOnlyTheRename(t *testing.T) {
	svc, fc, st := specReconcileSvc(t)
	ctx := context.Background()

	seedBootSpec(t, st, "h1", "web", "my-app", nil)
	sp, err := st.GetSpec(ctx, "h1", "web", "my-app")
	require.NoError(t, err)
	sp.AppliedVolumes = []string{"logs", "cache"}
	require.NoError(t, st.PutSpec(ctx, sp))

	// Both old volumes still exist on the host: `logs` because nobody pruned
	// it after the template dropped it, `cache` because the rename target has
	// not been created yet.
	fc.SetVolumeData("h1", "web-my-app-logs", []byte("stale, unpruned"))
	fc.SetVolumeData("h1", "web-my-app-cache", []byte("real cache"))

	// `logs` dropped outright; `cache` renamed to `cache2` (not yet created).
	tmpl, err := st.GetTemplate(ctx, "web")
	require.NoError(t, err)
	tmpl.Meta.Volumes = []render.Volume{{Name: "cache2"}}
	require.NoError(t, st.PutTemplate(ctx, tmpl))

	_, err = svc.reconcileOneSpec(ctx, "h1", "web", "my-app")
	require.ErrorIs(t, err, errVolumeRenamePending)
	assert.Contains(t, err.Error(), "cache", "the genuine rename must still be refused")
	assert.NotContains(t, err.Error(), "logs",
		"a dropped volume with no plausible rename target must not be blocked by an unrelated rename")
	assert.Empty(t, fc.PlayCalls)
}

// TestReconcileOneSpec_UnmatchableTargetStillBlocksEveryRename (#265 review,
// round-2 blocking finding, safe side): when a target cannot be paired with
// any renamed name by the name heuristic, it could be the destination of ANY
// of them — a rename is free to change a name beyond recognition. The guard's
// whole purpose is refusing to fork data, so an unaccounted-for target must
// still block every otherwise-unmatched renamed volume.
func TestReconcileOneSpec_UnmatchableTargetStillBlocksEveryRename(t *testing.T) {
	svc, fc, st := specReconcileSvc(t)
	ctx := context.Background()

	seedBootSpec(t, st, "h1", "web", "my-app", nil)
	sp, err := st.GetSpec(ctx, "h1", "web", "my-app")
	require.NoError(t, err)
	sp.AppliedVolumes = []string{"logs"}
	require.NoError(t, st.PutSpec(ctx, sp))

	fc.SetVolumeData("h1", "web-my-app-logs", []byte("real logs"))

	// `logs` -> `journal`: nothing in the names correlates them, but the
	// target exists and is not yet created, so the rename is plausible.
	tmpl, err := st.GetTemplate(ctx, "web")
	require.NoError(t, err)
	tmpl.Meta.Volumes = []render.Volume{{Name: "journal"}}
	require.NoError(t, st.PutTemplate(ctx, tmpl))

	_, err = svc.reconcileOneSpec(ctx, "h1", "web", "my-app")
	require.ErrorIs(t, err, errVolumeRenamePending)
	assert.Contains(t, err.Error(), "logs")
	assert.Empty(t, fc.PlayCalls)
}

// TestReconcileOneSpec_DroppedVolumeWithOtherDeclaredVolumesPresentReplays
// (#265 review finding 3): same shape, but the template still declares a
// volume — one that ALREADY exists on the host, so it cannot be the target of
// a rename from the dropped name. Still no evidence of a rename; still must
// replay.
func TestReconcileOneSpec_DroppedVolumeWithOtherDeclaredVolumesPresentReplays(t *testing.T) {
	svc, fc, st := specReconcileSvc(t)
	ctx := context.Background()

	seedBootSpec(t, st, "h1", "web", "my-app", nil)
	sp, err := st.GetSpec(ctx, "h1", "web", "my-app")
	require.NoError(t, err)
	sp.AppliedVolumes = []string{"data", "cache"}
	require.NoError(t, st.PutSpec(ctx, sp))

	fc.SetVolumeData("h1", "web-my-app-data", []byte("stale, unpruned"))
	fc.SetVolumeData("h1", "web-my-app-cache", []byte("still declared and present"))

	tmpl, err := st.GetTemplate(ctx, "web")
	require.NoError(t, err)
	tmpl.Meta.Volumes = []render.Volume{{Name: "cache"}} // `data` dropped
	require.NoError(t, st.PutTemplate(ctx, tmpl))

	reconciled, err := svc.reconcileOneSpec(ctx, "h1", "web", "my-app")
	require.NoError(t, err)
	assert.True(t, reconciled)
	require.Len(t, fc.PlayCalls, 1)
}

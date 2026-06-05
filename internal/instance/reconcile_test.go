package instance

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/iotready/podman-api/internal/config"
	"github.com/iotready/podman-api/internal/podman"
	"github.com/iotready/podman-api/internal/podman/fake"
	"github.com/iotready/podman-api/internal/render"
	"github.com/iotready/podman-api/internal/store"
)

// reconcileSvc builds a Service with two hosts (h1 source, h2 dest) and template
// "web", backed by a fake client and a memory store. The verify timeout is set to
// a sub-tick value so the present-but-unhealthy path returns on waitRunning's first
// iteration instead of blocking on its 2s poll ticker; the healthy path returns
// immediately (podReady) and is unaffected.
func reconcileSvc(t *testing.T) (*Service, *fake.Fake, *store.Memory) {
	t.Helper()
	SetVerifyTimeout(time.Nanosecond)
	t.Cleanup(func() { SetVerifyTimeout(60 * time.Second) })
	fc := fake.New()
	hosts := []config.Host{
		{ID: "h1", Addr: "unix", Socket: "/a"},
		{ID: "h2", Addr: "unix", Socket: "/b"},
	}
	tmpls := []config.Template{{Meta: render.Meta{
		ID:      "web",
		Volumes: []render.Volume{{Name: "data"}},
		Secrets: render.Secrets{PerInstance: []string{"password"}},
	}}}
	svc := NewService(fc, hosts, tmpls)
	st := store.NewMemory()
	svc.SetStore(st)
	return svc, fc, st
}

// healthyPod returns a Running pod with one Running container and no healthcheck.
func healthyPod(name string) podman.Pod {
	return podman.Pod{Name: name, Status: "Running", Containers: []podman.Container{{Status: "Running"}}}
}

func unhealthyPod(name string) podman.Pod {
	return podman.Pod{Name: name, Status: "Degraded", Containers: []podman.Container{{Status: "Exited"}}}
}

func req() MigrateRequest {
	return MigrateRequest{FromHost: "h1", ToHost: "h2", Template: "web", Slug: "x"}
}

func TestReconcileMigrate_RollForward(t *testing.T) {
	svc, fc, st := reconcileSvc(t)
	fc.AddPod("h1", healthyPod("web-x")) // source present
	fc.AddPod("h2", healthyPod("web-x")) // dest healthy
	// Seed dest spec so destSpecPersisted returns true (Apply completed fully).
	st.PutSpec(context.Background(), store.Spec{Host: "h2", Template: "web", Slug: "x"})

	resolved, ok, _, err := svc.ReconcileMigrate(context.Background(), req(), nil)
	if err != nil {
		t.Fatal(err)
	}
	if !resolved || !ok {
		t.Fatalf("got resolved=%v ok=%v, want true/true (roll forward)", resolved, ok)
	}
	// Source reaped.
	if _, err := fc.PodInspect(context.Background(), "h1", "web-x"); !errors.Is(err, podman.ErrNotFound) {
		t.Fatalf("source still present after roll-forward: %v", err)
	}
}

func TestReconcileMigrate_RollForward_SourceGone(t *testing.T) {
	svc, fc, st := reconcileSvc(t)
	fc.AddPod("h2", healthyPod("web-x")) // dest healthy, no source
	// Seed dest spec so destSpecPersisted returns true (Apply completed fully).
	st.PutSpec(context.Background(), store.Spec{Host: "h2", Template: "web", Slug: "x"})

	resolved, ok, _, err := svc.ReconcileMigrate(context.Background(), req(), nil)
	if err != nil {
		t.Fatal(err)
	}
	if !resolved || !ok {
		t.Fatalf("got resolved=%v ok=%v, want true/true (already committed)", resolved, ok)
	}
}

func TestReconcileMigrate_RollBack(t *testing.T) {
	svc, fc, _ := reconcileSvc(t)
	fc.AddPod("h1", healthyPod("web-x"))   // source present (stopped or running)
	fc.AddPod("h2", unhealthyPod("web-x")) // dest unhealthy

	resolved, ok, _, err := svc.ReconcileMigrate(context.Background(), req(), nil)
	if err != nil {
		t.Fatal(err)
	}
	if !resolved || ok {
		t.Fatalf("got resolved=%v ok=%v, want true/false (roll back)", resolved, ok)
	}
	// Dest reaped.
	if _, err := fc.PodInspect(context.Background(), "h2", "web-x"); !errors.Is(err, podman.ErrNotFound) {
		t.Fatalf("dest still present after roll-back: %v", err)
	}
}

func TestReconcileMigrate_DestAbsent_SourcePresent_RollsBack(t *testing.T) {
	svc, fc, _ := reconcileSvc(t)
	fc.AddPod("h1", healthyPod("web-x")) // source present, dest absent

	resolved, ok, _, err := svc.ReconcileMigrate(context.Background(), req(), nil)
	if err != nil {
		t.Fatal(err)
	}
	if !resolved || ok {
		t.Fatalf("got resolved=%v ok=%v, want true/false (roll back, nothing to reap)", resolved, ok)
	}
}

func TestReconcileMigrate_OrphanDest_SourceGone_NeverReaps(t *testing.T) {
	svc, fc, _ := reconcileSvc(t)
	fc.AddPod("h2", unhealthyPod("web-x")) // dest unhealthy, source gone

	resolved, ok, msg, err := svc.ReconcileMigrate(context.Background(), req(), nil)
	if err != nil {
		t.Fatal(err)
	}
	if !resolved || ok {
		t.Fatalf("got resolved=%v ok=%v, want true/false (orphan dest)", resolved, ok)
	}
	if !strings.Contains(msg, "manual cleanup") {
		t.Fatalf("orphan message %q should contain 'manual cleanup'", msg)
	}
	// Safety: dest must NOT be reaped — it is the only copy.
	if _, err := fc.PodInspect(context.Background(), "h2", "web-x"); err != nil {
		t.Fatalf("dest was removed in orphan case (data loss): %v", err)
	}
}

func TestReconcileMigrate_Inconclusive_DestUnreachable(t *testing.T) {
	svc, fc, _ := reconcileSvc(t)
	fc.PodInspectErr = errors.New("dial tcp: connection refused") // all inspects fail

	resolved, _, _, err := svc.ReconcileMigrate(context.Background(), req(), nil)
	if err != nil {
		t.Fatal(err)
	}
	if resolved {
		t.Fatal("got resolved=true, want false (host unreachable -> inconclusive)")
	}
}

// TestReconcileMigrate_CompensationFailure_Inconclusive verifies that when the
// roll-back Start (restoring the source) fails, the result is inconclusive
// (resolved=false) so the reconcile loop retries rather than producing a false
// terminal. fake.LifecycleErr makes PodStart return an error without removing
// the pod, which is the path s.Start takes.
func TestReconcileMigrate_CompensationFailure_Inconclusive(t *testing.T) {
	svc, fc, _ := reconcileSvc(t)
	fc.AddPod("h1", healthyPod("web-x"))   // source present
	fc.AddPod("h2", unhealthyPod("web-x")) // dest unhealthy -> roll back
	fc.LifecycleErr = errors.New("boom")   // Start(source) will fail

	resolved, _, _, err := svc.ReconcileMigrate(context.Background(), req(), nil)
	if err != nil {
		t.Fatal(err)
	}
	if resolved {
		t.Fatal("got resolved=true, want false (failed compensation must retry, not falsely terminate)")
	}
}

// TestReconcileMigrate_DestHealthyButSpecMissing_RollsBack verifies Fix #1: a
// dest pod that is healthy but whose spec was never persisted (Apply interrupted
// between PlayKube and PutSpec) is treated as uncommitted and rolled back to the
// intact source, preventing data loss.
func TestReconcileMigrate_DestHealthyButSpecMissing_RollsBack(t *testing.T) {
	svc, fc, _ := reconcileSvc(t)
	fc.AddPod("h1", healthyPod("web-x")) // source present
	fc.AddPod("h2", healthyPod("web-x")) // dest pod healthy, but NO spec seeded

	resolved, ok, _, err := svc.ReconcileMigrate(context.Background(), req(), nil)
	if err != nil {
		t.Fatal(err)
	}
	if !resolved || ok {
		t.Fatalf("got resolved=%v ok=%v, want true/false (roll back: spec not persisted)", resolved, ok)
	}
	// Dest pod must be reaped (it was the uncommitted copy).
	if _, err := fc.PodInspect(context.Background(), "h2", "web-x"); !errors.Is(err, podman.ErrNotFound) {
		t.Fatalf("dest still present after roll-back of unpersisted dest: %v", err)
	}
}

// TestReconcileMigrate_UnknownHost_Terminal verifies Fix #4: a FromHost no
// longer in config produces a terminal resolved=true/ok=false result rather than
// looping as inconclusive forever.
func TestReconcileMigrate_UnknownHost_Terminal(t *testing.T) {
	svc, _, _ := reconcileSvc(t)
	badReq := MigrateRequest{FromHost: "h9", ToHost: "h2", Template: "web", Slug: "x"}

	resolved, ok, msg, err := svc.ReconcileMigrate(context.Background(), badReq, nil)
	if err != nil {
		t.Fatal(err)
	}
	if !resolved || ok {
		t.Fatalf("got resolved=%v ok=%v, want true/false (unknown host must be terminal)", resolved, ok)
	}
	if !strings.Contains(msg, "no longer configured") {
		t.Fatalf("message %q should contain 'no longer configured'", msg)
	}
}

// TestReconcileMigrate_BothAbsent_NotFound verifies Fix #5: when neither host
// has the instance, the message accurately says "not found on either host"
// rather than the misleading orphan-dest message.
func TestReconcileMigrate_BothAbsent_NotFound(t *testing.T) {
	svc, _, _ := reconcileSvc(t)
	// No pods added on either host; no specs.

	resolved, ok, msg, err := svc.ReconcileMigrate(context.Background(), req(), nil)
	if err != nil {
		t.Fatal(err)
	}
	if !resolved || ok {
		t.Fatalf("got resolved=%v ok=%v, want true/false (both absent)", resolved, ok)
	}
	if !strings.Contains(msg, "not found on either host") {
		t.Fatalf("message %q should contain 'not found on either host'", msg)
	}
}

// TestReconcileMigrate_RollForward_SourceGone_ReapsOrphanedResources verifies the
// round-4 fix: when the source pod is already gone, roll-forward also reaps the
// instance's orphaned per-instance volumes/secrets (left by a commit interrupted
// mid-Delete after PodRemove), not just the spec row — otherwise a future deploy
// of the same slug would silently remount the stale named volume.
func TestReconcileMigrate_RollForward_SourceGone_ReapsOrphanedResources(t *testing.T) {
	svc, fc, st := reconcileSvc(t)
	ctx := context.Background()
	fc.AddPod("h2", healthyPod("web-x"))                                // dest healthy, source pod gone
	st.PutSpec(ctx, store.Spec{Host: "h2", Template: "web", Slug: "x"}) // dest committed
	st.PutSpec(ctx, store.Spec{Host: "h1", Template: "web", Slug: "x"}) // orphaned source spec
	// Orphaned source resources left by an interrupted commit.
	fc.AddVolume("h1", podman.Volume{Name: "web-x-data"})
	_ = fc.SecretCreate(ctx, "h1", instanceSecretName("web", "x", "password"), nil)

	resolved, ok, _, err := svc.ReconcileMigrate(ctx, req(), nil)
	if err != nil {
		t.Fatal(err)
	}
	if !resolved || !ok {
		t.Fatalf("got resolved=%v ok=%v, want true/true (roll forward)", resolved, ok)
	}
	if _, err := fc.VolumeInspect(ctx, "h1", "web-x-data"); !errors.Is(err, podman.ErrNotFound) {
		t.Fatalf("orphaned source volume not reaped: %v", err)
	}
	if _, err := fc.SecretInspect(ctx, "h1", instanceSecretName("web", "x", "password")); !errors.Is(err, podman.ErrNotFound) {
		t.Fatalf("orphaned source secret not reaped: %v", err)
	}
}

// hostErrIngress is a fake ingress.Controller whose Reconcile fails only for a
// chosen host, used to prove roll-forward does not couple to source-proxy health.
type hostErrIngress struct{ failHost string }

func (h hostErrIngress) Reconcile(_ context.Context, host string) error {
	if host == h.failHost {
		return errors.New("caddy wedged")
	}
	return nil
}

// TestReconcileMigrate_RollForward_SourceIngressWedged_StillSucceeds verifies the
// round-3 fix: when the source pod is already gone, roll-forward clears the
// orphaned source spec row and refreshes source ingress best-effort — a wedged
// source caddy must NOT strand a committed, healthy migrate in reconciling.
func TestReconcileMigrate_RollForward_SourceIngressWedged_StillSucceeds(t *testing.T) {
	svc, fc, st := reconcileSvc(t)
	svc.SetIngress(hostErrIngress{failHost: "h1"}, "ingressnet") // source (h1) caddy wedged
	ctx := context.Background()
	fc.AddPod("h2", healthyPod("web-x"))                                // dest healthy, source pod gone
	st.PutSpec(ctx, store.Spec{Host: "h2", Template: "web", Slug: "x"}) // dest committed
	st.PutSpec(ctx, store.Spec{Host: "h1", Template: "web", Slug: "x"}) // orphaned source spec

	resolved, ok, _, err := svc.ReconcileMigrate(ctx, req(), nil)
	if err != nil {
		t.Fatal(err)
	}
	if !resolved || !ok {
		t.Fatalf("got resolved=%v ok=%v, want true/true (source caddy health must not block roll-forward)", resolved, ok)
	}
	// The orphaned source spec row is still cleaned despite the wedged source caddy.
	if _, err := st.GetSpec(ctx, "h1", "web", "x"); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("orphaned source spec not cleaned: %v", err)
	}
}

// getSpecErrStore wraps a memory store but fails GetSpec with a transient
// (non-ErrNotFound) error, to exercise the inconclusive path of destSpecState.
type getSpecErrStore struct {
	*store.Memory
	err error
}

func (g *getSpecErrStore) GetSpec(context.Context, string, string, string) (store.Spec, error) {
	return store.Spec{}, g.err
}

// TestReconcileMigrate_DestSpecLookupError_Inconclusive verifies the round-2 #1
// fix: a transient GetSpec error (BUSY / decrypt / cancellation) must be treated
// as inconclusive, NOT as "spec not persisted" — otherwise a committed, healthy
// dest would be wrongly rolled back and deleted.
func TestReconcileMigrate_DestSpecLookupError_Inconclusive(t *testing.T) {
	svc, fc, st := reconcileSvc(t)
	svc.SetStore(&getSpecErrStore{Memory: st, err: errors.New("database is locked (SQLITE_BUSY)")})
	fc.AddPod("h1", healthyPod("web-x")) // source present
	fc.AddPod("h2", healthyPod("web-x")) // dest healthy and (notionally) committed

	resolved, _, _, err := svc.ReconcileMigrate(context.Background(), req(), nil)
	if err != nil {
		t.Fatal(err)
	}
	if resolved {
		t.Fatal("got resolved=true, want false (transient spec-lookup error must be inconclusive)")
	}
	// Neither host may be mutated on an inconclusive result.
	if _, err := fc.PodInspect(context.Background(), "h2", "web-x"); err != nil {
		t.Fatalf("dest was deleted on a transient lookup error (data loss): %v", err)
	}
	if _, err := fc.PodInspect(context.Background(), "h1", "web-x"); err != nil {
		t.Fatalf("source was mutated on a transient lookup error: %v", err)
	}
}

// TestReconcileMigrate_UnknownTemplate_Terminal verifies the round-2 #2 fix: a
// template removed from config terminates the job rather than looping inconclusive.
func TestReconcileMigrate_UnknownTemplate_Terminal(t *testing.T) {
	svc, _, _ := reconcileSvc(t)
	badReq := MigrateRequest{FromHost: "h1", ToHost: "h2", Template: "ghost", Slug: "x"}

	resolved, ok, msg, err := svc.ReconcileMigrate(context.Background(), badReq, nil)
	if err != nil {
		t.Fatal(err)
	}
	if !resolved || ok {
		t.Fatalf("got resolved=%v ok=%v, want true/false (unknown template must be terminal)", resolved, ok)
	}
	if !strings.Contains(msg, "template") || !strings.Contains(msg, "no longer configured") {
		t.Fatalf("message %q should mention the template is no longer configured", msg)
	}
}

// TestReconcileMigrate_RollForward_CleansOrphanedSourceSpec verifies the round-2
// #3 fix: roll-forward reaps the source's persisted state even when the source
// pod is already gone, so a crash inside the original commit's Delete (between
// PodRemove and DeleteSpec) cannot leave an orphaned source spec row that the
// ingress loop would re-derive into a permanent dead route.
func TestReconcileMigrate_RollForward_CleansOrphanedSourceSpec(t *testing.T) {
	svc, fc, st := reconcileSvc(t)
	ctx := context.Background()
	fc.AddPod("h2", healthyPod("web-x"))                                // dest healthy, NO source pod
	st.PutSpec(ctx, store.Spec{Host: "h2", Template: "web", Slug: "x"}) // dest committed
	st.PutSpec(ctx, store.Spec{Host: "h1", Template: "web", Slug: "x"}) // orphaned source spec row

	resolved, ok, _, err := svc.ReconcileMigrate(ctx, req(), nil)
	if err != nil {
		t.Fatal(err)
	}
	if !resolved || !ok {
		t.Fatalf("got resolved=%v ok=%v, want true/true (roll forward)", resolved, ok)
	}
	// The orphaned source spec row must be gone.
	if _, err := st.GetSpec(ctx, "h1", "web", "x"); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("orphaned source spec row not cleaned on roll-forward: %v", err)
	}
}

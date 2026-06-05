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
	tmpls := []config.Template{{Meta: render.Meta{ID: "web"}}}
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

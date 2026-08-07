package registryprune

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/iotready/podman-api/internal/jobs"
	"github.com/iotready/podman-api/internal/podman"
	"github.com/iotready/podman-api/internal/store"
)

// fakeRunner is a hand-written PodRunner. It records the exact call ORDER, not
// just the calls, because the property this stage exists to protect ("the
// registry is running again afterwards") is an ordering claim.
type fakeRunner struct {
	mu sync.Mutex

	calls []string // ordered: "stop", "play", "wait", "remove", "start", "exec"
	yamls []string

	stopErr error
	// startErrs is popped one per PodStart call, the last entry repeating.
	// Lets a test drive a restart that fails once and then succeeds.
	startErrs []error
	playErr   error
	playPanic bool

	// startCtxDone records, per PodStart call, whether the context it was
	// handed was already cancelled. The restart must survive a cancelled job
	// context; nothing else proves it does.
	startCtxDone []bool
	// execCmds records the argv of every ContainerExec.
	execCmds [][]string

	waitCode int
	waitErr  error

	// execOut maps call index -> du output. Successive exec calls pop the front
	// of execOut so a test can give different before/after sizes.
	execOut []podman.ExecResult
	execErr error
}

func (f *fakeRunner) record(name string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, name)
}

func (f *fakeRunner) PodStop(_ context.Context, _, _ string) error {
	f.record("stop")
	return f.stopErr
}

func (f *fakeRunner) PodStart(ctx context.Context, _, _ string) error {
	f.record("start")
	f.mu.Lock()
	defer f.mu.Unlock()
	f.startCtxDone = append(f.startCtxDone, ctx.Err() != nil)
	if len(f.startErrs) == 0 {
		return nil
	}
	err := f.startErrs[0]
	if len(f.startErrs) > 1 {
		f.startErrs = f.startErrs[1:]
	}
	return err
}

func (f *fakeRunner) PlayKube(_ context.Context, _, raw string, _ bool, _ ...string) error {
	f.record("play")
	f.mu.Lock()
	f.yamls = append(f.yamls, raw)
	f.mu.Unlock()
	if f.playPanic {
		panic("simulated podman client panic mid-GC")
	}
	return f.playErr
}

func (f *fakeRunner) WaitForPodCompletion(_ context.Context, _, _ string, _ time.Duration) (int, error) {
	f.record("wait")
	return f.waitCode, f.waitErr
}

func (f *fakeRunner) PodRemove(_ context.Context, _, _ string, _ bool) error {
	f.record("remove")
	return nil
}

func (f *fakeRunner) ContainerExec(_ context.Context, _, _ string, cmd []string) (podman.ExecResult, error) {
	f.record("exec")
	f.mu.Lock()
	f.execCmds = append(f.execCmds, cmd)
	f.mu.Unlock()
	if f.execErr != nil {
		return podman.ExecResult{}, f.execErr
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.execOut) == 0 {
		return podman.ExecResult{}, nil
	}
	out := f.execOut[0]
	f.execOut = f.execOut[1:]
	return out, nil
}

func (f *fakeRunner) callsJoined() string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return strings.Join(f.calls, ",")
}

func (f *fakeRunner) count(name string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	n := 0
	for _, c := range f.calls {
		if c == name {
			n++
		}
	}
	return n
}

// blobGCVerify is a real podman.Client assertion: the concrete client must
// satisfy PodRunner, or this stage cannot be wired in production at all.
var _ PodRunner = (podman.Client)(nil)

func testBlobGC(r *fakeRunner) *BlobGC {
	return &BlobGC{
		Podman:            r,
		HostID:            "otp-infra-1",
		RegistryPod:       "registry-main",
		RegistryContainer: "registry-main-registry",
		StoragePath:       "/srv/registry",
	}
}

func runBlobGC(t *testing.T, g *BlobGC, p Payload) (store.Job, error) {
	t.Helper()
	job, _, err := runBlobGCCtx(t, context.Background(), g, p)
	return job, err
}

func runBlobGCCtx(t *testing.T, ctx context.Context, g *BlobGC, p Payload) (store.Job, Result, error) {
	t.Helper()
	mem := store.NewMemory()
	j, err := mem.Enqueue(context.Background(), JobKind, []byte(`{}`), "")
	if err != nil {
		t.Fatal(err)
	}
	jc := jobs.NewJobContext(mem, j.ID)
	res, runErr := g.Reclaim(ctx, jc, p)
	got, err := mem.GetJob(context.Background(), j.ID)
	if err != nil {
		t.Fatal(err)
	}
	return got, res, runErr
}

// noRetryDelay collapses the restart backoff so retry tests do not sleep.
func noRetryDelay(t *testing.T) {
	t.Helper()
	prev := restartRetryDelay
	restartRetryDelay = 0
	t.Cleanup(func() { restartRetryDelay = prev })
}

// --- the safety property: the registry comes back --------------------------

func TestBlobGC_RestartsRegistryWhenGCExitsNonZero(t *testing.T) {
	r := &fakeRunner{waitCode: 3}
	job, err := runBlobGC(t, testBlobGC(r), Payload{})
	if err == nil {
		t.Fatal("expected a job failure for a non-zero GC exit, got nil")
	}
	if !strings.Contains(err.Error(), "3") {
		t.Fatalf("error should name the exit code, got %v", err)
	}
	if r.count("start") != 1 {
		t.Fatalf("registry was NOT restarted after a failing GC; calls: %s", r.callsJoined())
	}
	// The registry must be started again BEFORE anything slower or more
	// failure-prone (GC pod teardown) — a defer registered after the restart
	// would run first and sit between an aborted GC and the registry coming
	// back.
	if !strings.Contains(r.callsJoined(), "stop,play,wait") ||
		!strings.Contains(r.callsJoined(), "start,remove") {
		t.Fatalf("unexpected call order: %s", r.callsJoined())
	}
	wantStepContaining(t, job, "registry restarted")
}

func TestBlobGC_RestartsRegistryWhenWaitTimesOut(t *testing.T) {
	r := &fakeRunner{waitErr: podman.ErrWaitTimeout}
	_, err := runBlobGC(t, testBlobGC(r), Payload{})
	if err == nil {
		t.Fatal("expected a job failure when the wait timed out, got nil")
	}
	if !errors.Is(err, podman.ErrWaitTimeout) {
		t.Fatalf("timeout must stay distinguishable from a genuine exit code, got %v", err)
	}
	if r.count("start") != 1 {
		t.Fatalf("registry was NOT restarted after a wait timeout; calls: %s", r.callsJoined())
	}
}

// A vanished container is a hard error from WaitForPodCompletion, not a silent
// success. If this stage collapsed it into "exit 0" the job would report a GC
// that it never observed finish.
func TestBlobGC_VanishedContainerIsAFailureNotSuccess(t *testing.T) {
	r := &fakeRunner{waitErr: fmt.Errorf("inspecting container abc: %w", podman.ErrNotFound)}
	_, err := runBlobGC(t, testBlobGC(r), Payload{})
	if err == nil {
		t.Fatal("a wait error must fail the job, never read as success")
	}
	if errors.Is(err, podman.ErrWaitTimeout) {
		t.Fatalf("a vanished container must not be reported as a timeout: %v", err)
	}
	if r.count("start") != 1 {
		t.Fatalf("registry was NOT restarted; calls: %s", r.callsJoined())
	}
}

func TestBlobGC_RestartsRegistryOnPanic(t *testing.T) {
	r := &fakeRunner{playPanic: true}
	func() {
		defer func() {
			if rec := recover(); rec == nil {
				t.Error("panic should propagate, not be swallowed")
			}
		}()
		_, _ = runBlobGC(t, testBlobGC(r), Payload{})
	}()
	if r.count("start") != 1 {
		t.Fatalf("registry was NOT restarted after a panic; calls: %s", r.callsJoined())
	}
}

// A stop that fails still gets a restart attempt: the failure mode we cannot
// afford is a half-stopped registry left down. It must NOT proceed to GC,
// because GC against a live registry is the blob-corruption race.
func TestBlobGC_StopFailureAbortsGCButStillStarts(t *testing.T) {
	r := &fakeRunner{stopErr: errors.New("boom")}
	_, err := runBlobGC(t, testBlobGC(r), Payload{})
	if err == nil {
		t.Fatal("a failed stop must fail the job")
	}
	if r.count("play") != 0 {
		t.Fatalf("GC pod must not run while the registry may still be live; calls: %s", r.callsJoined())
	}
	if r.count("start") != 1 {
		t.Fatalf("registry start was not attempted after a failed stop; calls: %s", r.callsJoined())
	}
}

func TestBlobGC_RestartFailureFailsTheJob(t *testing.T) {
	r := &fakeRunner{startErrs: []error{errors.New("start refused")}}
	noRetryDelay(t)
	job, err := runBlobGC(t, testBlobGC(r), Payload{})
	if err == nil {
		t.Fatal("a registry that did not come back must fail the job loudly")
	}
	wantStepContaining(t, job, "REGISTRY DID NOT RESTART")
	if r.count("start") != restartAttempts {
		t.Fatalf("want %d restart attempts before giving up, got %d", restartAttempts, r.count("start"))
	}
}

// One transient PodStart failure must not leave the registry down until the
// scheduler's next run: failureBackoff is an hour, and the retry is the only
// thing between a flaky start and an hour-long registry outage.
func TestBlobGC_RestartIsRetriedAfterATransientFailure(t *testing.T) {
	noRetryDelay(t)
	r := &fakeRunner{startErrs: []error{errors.New("transient"), nil}}
	job, err := runBlobGC(t, testBlobGC(r), Payload{})
	if err != nil {
		t.Fatalf("a restart that succeeded on retry must not fail the job: %v", err)
	}
	if r.count("start") != 2 {
		t.Fatalf("want the restart retried once, got %d attempt(s)", r.count("start"))
	}
	wantStepContaining(t, job, "registry restarted")
}

// The job context being cancelled (job cancelled, deadline blown) must not take
// the registry down with it: the restart runs on a context derived with
// WithoutCancel.
func TestBlobGC_RestartsRegistryWhenJobContextIsCancelled(t *testing.T) {
	r := &fakeRunner{waitErr: context.Canceled}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, _, err := runBlobGCCtx(t, ctx, testBlobGC(r), Payload{})
	if err == nil {
		t.Fatal("a cancelled run must still fail the job")
	}
	if r.count("start") != 1 {
		t.Fatalf("registry was NOT restarted under a cancelled context; calls: %s", r.callsJoined())
	}
	for i, done := range r.startCtxDone {
		if done {
			t.Fatalf("PodStart attempt %d was handed an already-cancelled context; "+
				"the restart would fail exactly when it matters most", i)
		}
	}
}

// --- the GC pod name must never be the registry's own ----------------------

// Playing the GC pod over the registry's own pod name would replace it and then
// force-remove it in the teardown — the fleet's only registry DESTROYED, not
// merely down, with no reconciler in the core to rebuild it. validate() runs
// before anything is stopped, so this is caught with nothing touched.
func TestBlobGC_RejectsGCPodNameEqualToRegistryPod(t *testing.T) {
	r := &fakeRunner{}
	g := testBlobGC(r)
	g.PodName = g.RegistryPod
	_, err := runBlobGC(t, g, Payload{})
	if err == nil {
		t.Fatal("a GC pod named after the registry pod must be refused")
	}
	if got := r.callsJoined(); got != "" {
		t.Fatalf("nothing may be touched when the names collide, got calls: %s", got)
	}
}

// The same collision reached through the DEFAULT pod name rather than an
// explicit one — a registry instance that happens to be named registry-blob-gc.
func TestBlobGC_RejectsRegistryPodNamedLikeTheDefaultGCPod(t *testing.T) {
	r := &fakeRunner{}
	g := testBlobGC(r)
	g.RegistryPod = defaultGCPodName
	_, err := runBlobGC(t, g, Payload{})
	if err == nil {
		t.Fatal("the default GC pod name colliding with the registry pod must be refused")
	}
	if got := r.callsJoined(); got != "" {
		t.Fatalf("nothing may be touched when the names collide, got calls: %s", got)
	}
}

// A PlayKube that fails can still have created the pod, so the teardown must
// cover the failure path too — otherwise the "best-effort teardown" comment is
// claiming coverage it does not have.
func TestBlobGC_TearsDownTheGCPodEvenWhenPlayFails(t *testing.T) {
	r := &fakeRunner{playErr: errors.New("play refused")}
	_, err := runBlobGC(t, testBlobGC(r), Payload{})
	if err == nil {
		t.Fatal("a failed play must fail the job")
	}
	if r.count("remove") != 1 {
		t.Fatalf("GC pod was not torn down after a failed play; calls: %s", r.callsJoined())
	}
	if r.count("start") != 1 {
		t.Fatalf("registry was NOT restarted after a failed play; calls: %s", r.callsJoined())
	}
}

// --- gating ----------------------------------------------------------------

func TestBlobGC_SkipBlobGCPlaysNoPod(t *testing.T) {
	r := &fakeRunner{}
	job, err := runBlobGC(t, testBlobGC(r), Payload{SkipBlobGC: true})
	if err != nil {
		t.Fatalf("skip must not fail the job: %v", err)
	}
	if got := r.callsJoined(); got != "" {
		t.Fatalf("SkipBlobGC must touch nothing, got calls: %s", got)
	}
	wantStepContaining(t, job, "skipped")
}

func TestBlobGC_DryRunPlaysNoPod(t *testing.T) {
	r := &fakeRunner{}
	job, err := runBlobGC(t, testBlobGC(r), Payload{DryRun: true})
	if err != nil {
		t.Fatalf("dry run must not fail the job: %v", err)
	}
	if got := r.callsJoined(); got != "" {
		t.Fatalf("dry run must touch nothing, got calls: %s", got)
	}
	wantStepContaining(t, job, "dry run")
}

// The handler must not reach Stage B on a dry run either — belt and braces, so
// the property survives a caller that forgets to pass DryRun through.
func TestHandlerDryRunPlaysNoGCPod(t *testing.T) {
	r := &fakeRunner{}
	h := newHandler(t, &fakeClient{catalogs: [][]string{{"engine"}}})
	h.BlobGC = testBlobGC(r)
	_, _, err := runJob(t, h, Payload{Policy: testPolicy(), DryRun: true})
	if err != nil {
		t.Fatalf("dry run: %v", err)
	}
	if got := r.callsJoined(); got != "" {
		t.Fatalf("dry run played a GC pod: %s", got)
	}
}

// --- the manifest ----------------------------------------------------------

func TestBlobGC_ManifestShape(t *testing.T) {
	r := &fakeRunner{}
	if _, err := runBlobGC(t, testBlobGC(r), Payload{}); err != nil {
		t.Fatalf("unexpected failure: %v", err)
	}
	if len(r.yamls) != 1 {
		t.Fatalf("expected exactly one PlayKube, got %d", len(r.yamls))
	}
	y := r.yamls[0]
	// Assert on the YAML that actually reaches podman, not on a struct built
	// in the test: a manifest that marshals restartPolicy under the wrong key,
	// or drops it, would leave the GC pod restarting forever.
	for _, want := range []string{
		"restartPolicy: Never",
		"image: registry:2",
		"mountPath: /var/lib/registry",
		"path: /srv/registry",
		"hostPath:",
		"garbage-collect",
		"--delete-untagged",
		"/etc/docker/registry/config.yml",
	} {
		if !strings.Contains(y, want) {
			t.Fatalf("generated manifest is missing %q:\n%s", want, y)
		}
	}
	// The GC command must be args (image CMD), not command (ENTRYPOINT
	// override) — registry:2's entrypoint is what turns these words into
	// `registry garbage-collect …`.
	if strings.Contains(y, "command:") {
		t.Fatalf("manifest overrides the entrypoint; GC args would never reach `registry`:\n%s", y)
	}
	// And it must never be pointed at the storage path by a caller-supplied
	// string that YAML would reinterpret.
	if strings.Contains(y, "restartPolicy: Always") {
		t.Fatalf("restartPolicy is wrong:\n%s", y)
	}
	// hostPath type MUST be Directory, never DirectoryOrCreate: a typo'd or
	// unmounted storage path has to fail loudly. DirectoryOrCreate would
	// create an empty directory and garbage-collect it perfectly, reporting a
	// clean run over a registry it never touched.
	if !strings.Contains(y, "type: Directory\n") {
		t.Fatalf("hostPath type must be exactly Directory:\n%s", y)
	}
	if strings.Contains(y, "DirectoryOrCreate") {
		t.Fatalf("hostPath type must not create the storage path:\n%s", y)
	}
}

func TestBlobGC_ManifestUsesConfiguredPaths(t *testing.T) {
	r := &fakeRunner{}
	g := testBlobGC(r)
	g.StoragePath = "/data/reg"
	g.MountPath = "/var/lib/registry-alt"
	g.ConfigPath = "/etc/registry.yml"
	g.Image = "registry:2.8.3"
	if _, err := runBlobGC(t, g, Payload{}); err != nil {
		t.Fatalf("unexpected failure: %v", err)
	}
	y := r.yamls[0]
	for _, want := range []string{"path: /data/reg", "mountPath: /var/lib/registry-alt", "/etc/registry.yml", "image: registry:2.8.3"} {
		if !strings.Contains(y, want) {
			t.Fatalf("manifest missing %q:\n%s", want, y)
		}
	}
}

func TestBlobGC_ValidatesConfig(t *testing.T) {
	cases := map[string]func(*BlobGC){
		"no host":         func(g *BlobGC) { g.HostID = "" },
		"no registry pod": func(g *BlobGC) { g.RegistryPod = "" },
		"no storage path": func(g *BlobGC) { g.StoragePath = "" },
		"relative path":   func(g *BlobGC) { g.StoragePath = "srv/registry" },
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			r := &fakeRunner{}
			g := testBlobGC(r)
			mutate(g)
			if _, err := runBlobGC(t, g, Payload{}); err == nil {
				t.Fatal("expected a config error, got nil")
			}
			if got := r.callsJoined(); got != "" {
				t.Fatalf("a misconfigured stage must stop the registry for nothing: %s", got)
			}
		})
	}
}

// --- sizing ----------------------------------------------------------------

func TestBlobGC_RecordsBeforeAndAfterSize(t *testing.T) {
	r := &fakeRunner{execOut: []podman.ExecResult{
		{ExitCode: 0, Output: "2048\t/var/lib/registry\n"},
		{ExitCode: 0, Output: "1024\t/var/lib/registry\n"},
	}}
	job, err := runBlobGC(t, testBlobGC(r), Payload{})
	if err != nil {
		t.Fatalf("unexpected failure: %v", err)
	}
	txt := stepText(job)
	if !strings.Contains(txt, "2097152") || !strings.Contains(txt, "1048576") {
		t.Fatalf("before/after sizes not recorded in bytes:\n%s", txt)
	}
	if !strings.Contains(txt, "1048576 byte(s) reclaimed") {
		t.Fatalf("reclaimed bytes not recorded:\n%s", txt)
	}
}

// The byte counts must leave this stage as DATA. Task 7 needs a
// bytes-reclaimed metric, and re-parsing a formatted job step is not a data
// path.
func TestBlobGC_ReturnsMeasuredBytesAsData(t *testing.T) {
	r := &fakeRunner{execOut: []podman.ExecResult{
		{ExitCode: 0, Output: "2048\t/var/lib/registry\n"},
		{ExitCode: 0, Output: "1024\t/var/lib/registry\n"},
	}}
	_, res, err := runBlobGCCtx(t, context.Background(), testBlobGC(r), Payload{})
	if err != nil {
		t.Fatalf("unexpected failure: %v", err)
	}
	if !res.Measured {
		t.Fatal("Measured must be true when both sizes were read")
	}
	if res.Before != 2097152 || res.After != 1048576 || res.Reclaimed != 1048576 {
		t.Fatalf("got %+v, want Before=2097152 After=1048576 Reclaimed=1048576", res)
	}
}

func TestBlobGC_ResultIsNotMeasuredWhenSizingFails(t *testing.T) {
	r := &fakeRunner{execErr: errors.New("no such container")}
	_, res, err := runBlobGCCtx(t, context.Background(), testBlobGC(r), Payload{})
	if err != nil {
		t.Fatalf("unexpected failure: %v", err)
	}
	if res.Measured || res.Reclaimed != 0 {
		t.Fatalf("an unmeasured run must not report bytes reclaimed: %+v", res)
	}
}

// SizePath is the path inside the REGISTRY container; MountPath is the GC
// pod's. They coincide only by default. If sizing followed MountPath, the first
// deployment that changed it would measure a path that does not exist in the
// registry container and go permanently "unavailable" with no error.
func TestBlobGC_SizingUsesSizePathNotTheGCPodMountPath(t *testing.T) {
	r := &fakeRunner{execOut: []podman.ExecResult{{ExitCode: 0, Output: "1\t/x\n"}}}
	g := testBlobGC(r)
	g.MountPath = "/var/lib/registry-gcside"
	g.SizePath = "/registry-data"
	if _, err := runBlobGC(t, g, Payload{}); err != nil {
		t.Fatalf("unexpected failure: %v", err)
	}
	if len(r.execCmds) == 0 {
		t.Fatal("no exec was issued")
	}
	got := strings.Join(r.execCmds[0], " ")
	if !strings.Contains(got, "/registry-data") {
		t.Fatalf("sizing did not use SizePath, ran: %s", got)
	}
	if strings.Contains(got, "/var/lib/registry-gcside") {
		t.Fatalf("sizing followed the GC pod's mount path, ran: %s", got)
	}
}

func TestBlobGC_SizingFailureIsNotAJobFailure(t *testing.T) {
	r := &fakeRunner{execErr: errors.New("no such container")}
	job, err := runBlobGC(t, testBlobGC(r), Payload{})
	if err != nil {
		t.Fatalf("a sizing failure must not fail a successful GC: %v", err)
	}
	if r.count("play") != 1 {
		t.Fatalf("GC did not run: %s", r.callsJoined())
	}
	wantStepContaining(t, job, "size unavailable")
}

package podman

import (
	"archive/tar"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"math"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/containers/podman/v5/libpod/define"
	handlers "github.com/containers/podman/v5/pkg/api/handlers"
	"github.com/containers/podman/v5/pkg/bindings"
	"github.com/containers/podman/v5/pkg/bindings/containers"
	"github.com/containers/podman/v5/pkg/bindings/images"
	network "github.com/containers/podman/v5/pkg/bindings/network"
	"github.com/containers/podman/v5/pkg/bindings/play"
	"github.com/containers/podman/v5/pkg/bindings/pods"
	"github.com/containers/podman/v5/pkg/bindings/secrets"
	"github.com/containers/podman/v5/pkg/bindings/system"
	"github.com/containers/podman/v5/pkg/bindings/volumes"
	"github.com/containers/podman/v5/pkg/domain/entities"
	"github.com/containers/podman/v5/pkg/domain/entities/reports"
	dockerContainer "github.com/docker/docker/api/types/container"
	nettypes "go.podman.io/common/libnetwork/types"

	"github.com/iotready/podman-api/internal/config"
)

// Real is the production podman.Client implementation backed by libpod
// over SSH (production) or a local unix socket (dev).
//
// Per-host context.Contexts are created lazily and cached. The bindings
// library stores the underlying connection on the context, so callers must
// pass the cached context (returned by ctxFor) to every libpod call.
//
// Uses github.com/containers/podman/v5 v5.8.2.
// bindings.NewConnection signature: func(ctx context.Context, uri string) (context.Context, error)
type Real struct {
	hosts map[string]config.Host

	mu       sync.Mutex
	ctx      map[string]context.Context // hostID -> connection-bearing ctx
	verified map[string]bool            // hostID -> passed the MinPodmanVersion check
	// sshPool caches one live SSH client per host for the /proc side channels
	// (loadavg, uptime, net ports), which libpod does not serve. See
	// ssh_pool.go: the handshake it removes was ~2.9s per read (#258).
	sshPool map[string]*sshPoolEntry
	// loadavg caches each host's last successful /proc/loadavg read, so the
	// metric survives a request budget that the libpod prelude has already
	// mostly spent. Kept warm by the inventory poller via SampleLoadAvg.
	loadavg map[string]loadSample
	// loadGate single-flights the request-path read-through, one gate per host,
	// so a burst arriving after the TTL lapses costs one round trip and not one
	// each.
	loadGate map[string]chan struct{}

	// hooks is nil in production. It exists because the reload races this file
	// guards against cannot be interleaved from outside — they live between a
	// lock being released and the I/O that follows — and a guard nobody can
	// test is a guard nobody can trust. Kept as one field rather than four so
	// the seam is visible as a seam.
	hooks *testHooks

	// versionProbe overrides the version lookup in tests; nil means
	// system.Info over the supplied connection ctx.
	versionProbe func(context.Context) (string, error)
}

// testHooks is the test-only seam described on Real.hooks.
type testHooks struct {
	// now overrides the clock; nil means time.Now.
	now func() time.Time
	// wedgeGrace overrides sshWedgeGrace for pool entries created after it is
	// set; zero means the default.
	wedgeGrace time.Duration
	// afterLoadRead runs between a loadavg read completing and the sample being
	// committed to the cache.
	afterLoadRead func()
	// beforeLoadResolve runs after the loadavg path has snapshotted the host but
	// before the read resolves it for itself — the other window a reload can
	// land in.
	beforeLoadResolve func()
	// readCap overrides sshReadCap; zero means the default.
	readCap time.Duration
	// beforeConnect runs after a read has resolved its host and pool entry but
	// before it connects — the window a reload has to retire that entry.
	beforeConnect func()
}

// NewReal validates host configs and registers them. Connections are not
// opened here; first use opens them.
func NewReal(hosts []config.Host) (*Real, error) {
	r := &Real{
		hosts:    map[string]config.Host{},
		ctx:      map[string]context.Context{},
		verified: map[string]bool{},
		sshPool:  map[string]*sshPoolEntry{},
		loadavg:  map[string]loadSample{},
		loadGate: map[string]chan struct{}{},
	}
	for _, h := range hosts {
		if h.ID == "" {
			return nil, fmt.Errorf("host with empty id")
		}
		if _, dup := r.hosts[h.ID]; dup {
			return nil, fmt.Errorf("duplicate host id %q", h.ID)
		}
		r.hosts[h.ID] = h
	}
	return r, nil
}

// Knows reports whether the host is registered.
func (r *Real) Knows(id string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	_, ok := r.hosts[id]
	return ok
}

// SetHosts replaces the client's host map at runtime, diff-invalidating cached
// connections so that:
//   - newly added hosts become connectable on first use (lazy ctxFor);
//   - removed hosts are dropped (cached context garbage-collected);
//   - hosts whose addr/socket/ssh_key changed get their cached connection and
//     verified flag cleared so the next call reopens with the new params;
//   - unchanged hosts keep their live cached connection.
func (r *Real) SetHosts(hosts []config.Host) {
	newMap := make(map[string]config.Host, len(hosts))
	for _, h := range hosts {
		if h.ID != "" {
			newMap[h.ID] = h
		}
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	// Remove hosts that were deleted.
	for id := range r.hosts {
		if _, keep := newMap[id]; !keep {
			delete(r.hosts, id)
			delete(r.ctx, id)
			delete(r.verified, id)
			delete(r.loadavg, id)
			delete(r.loadGate, id)
			r.dropSSHLocked(id)
		}
	}

	// Add or update hosts.
	for id, h := range newMap {
		if old, exists := r.hosts[id]; exists && !hostConnEq(old, h) {
			// Connection params changed: invalidate cached state. The pooled
			// SSH client and the cached loadavg go too — they describe the old
			// endpoint, and a sample from it must not be served as the new
			// host's.
			delete(r.ctx, id)
			delete(r.verified, id)
			delete(r.loadavg, id)
			r.dropSSHLocked(id)
		}
		r.hosts[id] = h
	}
}

// hostConnEq reports whether two host configs share the same connection
// parameters (address, socket path, SSH key). Other fields (Drain, Labels,
// Prune, etc.) do NOT affect the cached podman connection.
func hostConnEq(a, b config.Host) bool {
	return a.Addr == b.Addr && a.Socket == b.Socket && a.SSHKey == b.SSHKey
}

// URIFor returns the libpod URI for hostID. unix-only when addr=="unix".
func (r *Real) URIFor(id string) (string, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.uriForLocked(id)
}

// uriForLocked is the unsynchronized body of URIFor; callers must hold r.mu.
func (r *Real) uriForLocked(id string) (string, error) {
	h, ok := r.hosts[id]
	if !ok {
		return "", fmt.Errorf("unknown host %q", id)
	}
	if h.Addr == "unix" {
		return "unix://" + h.Socket, nil
	}
	return "ssh://" + h.Addr + h.Socket, nil
}

// ctxFor returns a libpod-ready context for hostID, opening the connection
// on first use. The connection context is rooted at context.Background() so
// it outlives any individual request context — per-request cancellation must
// not kill the cached long-lived connection.
func (r *Real) ctxFor(parent context.Context, id string) (context.Context, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if c, ok := r.ctx[id]; ok {
		return c, nil
	}
	h, ok := r.hosts[id]
	if !ok {
		return nil, fmt.Errorf("unknown host %q", id)
	}
	uri, err := r.uriForLocked(id)
	if err != nil {
		return nil, err
	}
	// Use context.Background() — not the caller's per-request context — so
	// the cached connection context is never cancelled by a request ending.
	// Individual operations still honour per-call cancellation because
	// DoRequest creates http.NewRequestWithContext(ctx, ...) from the
	// per-call context passed into each method (PodInspect, PodList, etc.).
	connBase := context.Background()
	var c context.Context
	if h.Addr != "unix" && h.SSHKey != "" {
		// SSH host with explicit key file. The fourth arg (`machine`) is false
		// for non-machine connections per the bindings API.
		c, err = bindings.NewConnectionWithIdentity(connBase, uri, h.SSHKey, false)
	} else {
		c, err = bindings.NewConnection(connBase, uri)
	}
	if err != nil {
		return nil, fmt.Errorf("connect to host %q: %w", id, err)
	}
	r.wireInvalidation(c, id)
	r.ctx[id] = c
	return c, nil
}

// wireInvalidation wraps c's underlying HTTP transport so a transport-level
// failure evicts the cached connection instead of being replayed on every
// later call to id forever (#252).
//
// This is the only hook point that works for every operation method without
// touching each of their ~30 call sites individually: the bindings library
// stores one *http.Client on the connection context, and every PodList,
// ContainerExec, HostInfo, etc. call funnels through it. A RoundTrip error is
// always a transport failure — dial refused, connection reset, read timeout
// on a wedged host — never an application error (a 404 or 409 comes back as
// a normal *http.Response with a non-2xx status and a nil error), so
// connBroken's classification applies unchanged.
//
// Best-effort: if the bindings package ever changes shape so GetClient or
// Client is unavailable here, this silently does nothing rather than failing
// the connection it was meant to protect.
func (r *Real) wireInvalidation(c context.Context, id string) {
	conn, err := bindings.GetClient(c)
	if err != nil || conn == nil || conn.Client == nil {
		return
	}
	next := conn.Client.Transport
	if next == nil {
		next = http.DefaultTransport
	}
	conn.Client.Transport = &invalidatingTransport{
		next: next,
		onBroken: func() {
			r.invalidateConn(id, c)
		},
	}
}

// invalidatingTransport wraps an http.RoundTripper and reports every
// transport-level failure to onBroken. See wireInvalidation.
type invalidatingTransport struct {
	next     http.RoundTripper
	onBroken func()
}

// RoundTrip evicts on two distinct signals, not one:
//
//   - connBroken(err): a raw transport error (dial refused, reset, an EOF) —
//     the connection visibly died.
//   - req.Context() itself having timed out: the ~30 operation methods that
//     route through opCtxFor bound every call with a fixed callTimeout, and a
//     wedged connection (the TCP socket accepts writes but the OS never
//     surfaces a read error — the #252 incident) manifests as that deadline
//     firing, not as a transport error. connBroken deliberately does not
//     treat context.DeadlineExceeded as broken (ssh_pool.go's sshSession
//     needs that: there ctx is the *caller's* short budget, and hitting it
//     says nothing about the connection's health). Here it is different:
//     req.Context() is opCtxFor's own callTimeout, already generous (10
//     minutes) specifically so that reaching it means the call hung, not
//     that it was merely slow. So the deadline firing here is itself the
//     wedge signal, the same role sshWedgeGrace's watchdog plays for the SSH
//     pool — no extra grace window is needed on top of a timeout that is
//     already the grace window.
//
// Distinguishing "this call's own deadline fired" from "the caller gave up
// early" matters: opCtxFor bridges the caller's cancellation into the derived
// context via context.AfterFunc(parent, cancel), so a client disconnecting
// mid-request cancels req.Context() before its deadline — that reports
// context.Canceled, not context.DeadlineExceeded, and must not evict a
// perfectly healthy connection just because its caller walked away.
func (t *invalidatingTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	resp, err := t.next.RoundTrip(req)
	if err != nil && (connBroken(err) || errors.Is(req.Context().Err(), context.DeadlineExceeded)) {
		t.onBroken()
	}
	return resp, err
}

// invalidateConn drops the cached libpod connection for id if it is still the
// one that just failed. The identity check (dead is the exact context ctxFor
// stored) matters the same way sshPoolEntry.invalidate's does: between this
// failure and the invalidation running, a reload (SetHosts) or a concurrent
// redial may already have replaced the cached connection with a good one, and
// dropping that would turn one transport fault into a reconnect for every
// concurrent caller instead of just the one that actually broke.
func (r *Real) invalidateConn(id string, dead context.Context) {
	r.mu.Lock()
	if r.ctx[id] == dead {
		delete(r.ctx, id)
		// The version check is a property of the connection that just died,
		// not of the host — a fresh connection must re-verify rather than
		// inherit a pass recorded against the socket we just evicted.
		delete(r.verified, id)
	}
	r.mu.Unlock()
}

// probeVersion fetches the podman version over an established connection ctx.
func (r *Real) probeVersion(c context.Context) (string, error) {
	if r.versionProbe != nil {
		return r.versionProbe(c)
	}
	info, err := system.Info(c, &system.InfoOptions{})
	if err != nil {
		return "", err
	}
	return info.Version.Version, nil
}

// ensureVerified enforces MinPodmanVersion once per host per process. On
// failure the host stays unverified (and the connection stays cached), so a
// host whose podman is upgraded in place starts passing without a restart.
// The probe runs outside the mutex; a concurrent duplicate probe is harmless.
func (r *Real) ensureVerified(c context.Context, id string) error {
	r.mu.Lock()
	ok := r.verified[id]
	r.mu.Unlock()
	if ok {
		return nil
	}
	v, err := r.probeVersion(c)
	if err != nil {
		return fmt.Errorf("verify host %q podman version: %w", id, err)
	}
	if err := checkVersion(id, v); err != nil {
		return err
	}
	r.mu.Lock()
	r.verified[id] = true
	r.mu.Unlock()
	return nil
}

// opCtxFor is ctxFor plus the MinPodmanVersion gate. Operation methods must
// call this instead of ctxFor; diagnostics (Ping, Version, HostInfo) use raw
// ctxFor so GET /hosts can still display an unsupported host's version (#85).
//
// The returned context is derived from the cached long-lived connection context
// (rooted at context.Background) with a per-call deadline (callTimeout) so a
// hung libpod/SSH call fails rather than blocking forever. Parent cancellation
// is bridged into the derived context via context.AfterFunc, so job cancellations
// and daemon shutdown tear down in-flight operations promptly.
//
// Callers MUST call the returned CancelFunc (typically via defer) to release
// the WithTimeout timer and the AfterFunc registration when the operation
// completes, regardless of success or failure.
func (r *Real) opCtxFor(parent context.Context, id string) (context.Context, context.CancelFunc, error) {
	return r.opCtxForTimeout(parent, id, callTimeout)
}

// opCtxForTimeout is opCtxFor with an explicit deadline instead of the fixed
// callTimeout, for operations (VolumeExport/VolumeImport) whose data volume,
// not call count, drives how long they legitimately run. See
// SetVolumeTransferTimeout.
func (r *Real) opCtxForTimeout(parent context.Context, id string, timeout time.Duration) (context.Context, context.CancelFunc, error) {
	c, err := r.ctxFor(parent, id)
	if err != nil {
		return nil, nil, err
	}
	if err := r.ensureVerified(c, id); err != nil {
		return nil, nil, err
	}
	ctx, cancel := context.WithTimeout(c, timeout)
	stop := context.AfterFunc(parent, cancel)
	return ctx, func() { stop(); cancel() }, nil
}

// preflightTimeout bounds each host's boot-time connect+version probe.
// var (not const) so tests can shrink it.
var preflightTimeout = 10 * time.Second

// callTimeout bounds each individual libpod operation so a hung SSH or
// libpod call fails rather than blocking a job (e.g. a migrate between
// copy-volume-done and apply-dest) forever. Large image pulls instead get
// their own deadline via imagePullTimeout(); tests that need faster failure
// can override this package var.
var callTimeout = 10 * time.Minute

// volumeTransferTimeoutOverride bounds VolumeExport/VolumeImport when set
// (SetVolumeTransferTimeout); zero (the default) falls back to callTimeout.
// A package var, not a Real field, matching callTimeout/preflightTimeout
// above and instance.SetDeployVerifyTimeout's shape: a startup-configured
// knob, set once via the flag-wired setter before Preflight and read only
// after — same "not mutated concurrently with live use" convention those
// rely on, no additional locking needed or used elsewhere in this family.
var volumeTransferTimeoutOverride time.Duration

// SetVolumeTransferTimeout overrides the deadline VolumeExport/VolumeImport
// derive their context from. No-op for d <= 0, matching
// instance.SetDeployVerifyTimeout's convention — the caller (server.go)
// separately rejects a non-positive flag value at startup so that is never a
// silent fallback in practice.
//
// #223: those two stream GB-scale volume contents, not a single quick libpod
// call, and a Frappe `sites` volume in the hundreds-of-MB-to-low-GB range
// over a tailnet link routinely crossed callTimeout's 10-minute budget mid-
// transfer — the export itself was healthy, it just outlived the same cap
// meant for a single short RPC. This deadline covers the streamed transfer
// itself; it does NOT extend ensureVerified's host-verification probe, which
// (as before this change) runs on an undeadlined context — a first call to a
// newly-added, slow-to-answer host can stall there before this timeout ever
// engages.
//
// Call this once at startup from a configured flag; it is not safe to call
// concurrently with an in-flight transfer.
func SetVolumeTransferTimeout(d time.Duration) {
	if d > 0 {
		volumeTransferTimeoutOverride = d
	}
}

// volumeTransferTimeout resolves the effective deadline for VolumeExport/
// VolumeImport: the configured override if set, else callTimeout.
func volumeTransferTimeout() time.Duration {
	if volumeTransferTimeoutOverride > 0 {
		return volumeTransferTimeoutOverride
	}
	return callTimeout
}

// imagePullTimeoutOverride bounds ImagePull when set (SetImagePullTimeout);
// zero (the default) falls back to callTimeout. Same package-var,
// no-op-below-threshold shape as volumeTransferTimeoutOverride above (#238):
// ImagePull is the same class of large, network-bound transfer that
// motivated volumeTransferTimeout for VolumeExport/VolumeImport (#223), and
// was left on the unmodified callTimeout when that fix landed because #223's
// report was specifically about volume export/import.
var imagePullTimeoutOverride time.Duration

// SetImagePullTimeout overrides the deadline ImagePull derives its context
// from. No-op for d <= 0, matching SetVolumeTransferTimeout's convention —
// the caller (server.go) separately rejects a non-positive flag value at
// startup so that is never a silent fallback in practice.
//
// Call this once at startup from a configured flag; it is not safe to call
// concurrently with an in-flight pull.
func SetImagePullTimeout(d time.Duration) {
	if d > 0 {
		imagePullTimeoutOverride = d
	}
}

// imagePullTimeout resolves the effective deadline for ImagePull: the
// configured override if set, else callTimeout.
func imagePullTimeout() time.Duration {
	if imagePullTimeoutOverride > 0 {
		return imagePullTimeoutOverride
	}
	return callTimeout
}

// Preflight enforces MinPodmanVersion at boot. ALL reachable hosts are checked
// and any below the floor are collected; the returned error aggregates every
// offender via errors.Join so operators see every problem in a single boot
// attempt (main treats it as fatal — the daemon refuses to start). An
// unreachable or slow host is logged and left unverified; the check re-runs on
// its first successful connect (opCtxFor), so a down-at-boot old host still
// cannot sneak in. See #85.
func (r *Real) Preflight(ctx context.Context) error {
	var errs []error
	for id := range r.hosts {
		if err := r.preflightHost(ctx, id); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

func (r *Real) preflightHost(ctx context.Context, id string) error {
	type result struct {
		version string
		err     error
	}
	ch := make(chan result, 1)
	// The dial cannot take a deadline: ctxFor deliberately roots cached
	// connections at context.Background() (per-request cancellation must not
	// kill them), so the attempt is bounded externally. On timeout the
	// goroutine is abandoned; if it completes later it merely caches a usable
	// connection for first use — caching grants no unverified access, it only
	// avoids a second dial (opCtxFor still runs the version gate).
	go func() {
		c, err := r.ctxFor(ctx, id)
		if err != nil {
			ch <- result{err: err}
			return
		}
		v, err := r.probeVersion(c)
		ch <- result{version: v, err: err}
	}()
	select {
	case res := <-ch:
		if res.err != nil {
			log.Printf("preflight: host %q unreachable, podman version check deferred to first use: %v", id, res.err)
			return nil
		}
		if err := checkVersion(id, res.version); err != nil {
			return err
		}
		r.mu.Lock()
		r.verified[id] = true
		r.mu.Unlock()
		log.Printf("preflight: host %q podman %s ok (>= %s)", id, res.version, MinPodmanVersion)
		return nil
	case <-time.After(preflightTimeout):
		log.Printf("preflight: host %q did not answer within %s, podman version check deferred to first use", id, preflightTimeout)
		return nil
	case <-ctx.Done():
		log.Printf("preflight: host %q check cancelled, podman version check deferred to first use: %v", id, ctx.Err())
		return nil
	}
}

func (r *Real) Ping(ctx context.Context, id string) error {
	c, err := r.ctxFor(ctx, id)
	if err != nil {
		return err
	}
	_, err = system.Info(c, &system.InfoOptions{})
	return err
}

func (r *Real) Version(ctx context.Context, id string) (string, error) {
	c, err := r.ctxFor(ctx, id)
	if err != nil {
		return "", err
	}
	return r.probeVersion(c)
}

func (r *Real) PlayKube(ctx context.Context, id, raw string, replace bool, networks ...string) error {
	c, cancel, err := r.opCtxFor(ctx, id)
	if err != nil {
		return err
	}
	defer cancel()
	tmp, err := os.CreateTemp("", "play-kube-*.yaml")
	if err != nil {
		return err
	}
	defer os.Remove(tmp.Name())
	if _, err := tmp.WriteString(raw); err != nil {
		return err
	}
	tmp.Close()

	opts := &play.KubeOptions{}
	if replace {
		t := true
		opts.Replace = &t
	}
	if len(networks) > 0 {
		opts.Network = &networks
	}
	_, err = play.Kube(c, tmp.Name(), opts)
	return err
}

// waitPollInterval bounds how often WaitForPodCompletion re-inspects the pod
// while waiting for its containers to exit.
var waitPollInterval = 2 * time.Second

// WaitForPodCompletion polls the named pod's containers until every one has
// exited or timeout elapses. It returns the first non-zero exit code found
// (in container order), else 0. Timing out returns ErrWaitTimeout, never a
// zero exit code — a caller must not be able to mistake "gave up waiting"
// for "ran and exited 0".
//
// The caller's timeout, not the fixed opCtxFor callTimeout, bounds the wait:
// opCtxFor's 10-minute cap is meant for a single libpod call, but a blob GC
// over a large registry can legitimately run far longer. This method builds
// its own context off the verified connection instead of going through
// opCtxFor, so a 30-minute caller timeout is honoured rather than silently
// truncated to 10 minutes.
func (r *Real) WaitForPodCompletion(ctx context.Context, id, podName string, timeout time.Duration) (int, error) {
	base, err := r.ctxFor(ctx, id)
	if err != nil {
		return 0, err
	}
	if err := r.ensureVerified(base, id); err != nil {
		return 0, err
	}
	c, cancel := context.WithTimeout(base, timeout)
	stop := context.AfterFunc(ctx, cancel)
	defer func() { stop(); cancel() }()

	return waitForCompletion(c, podName,
		func(ctx context.Context, name string) ([]string, error) {
			rep, err := pods.Inspect(ctx, name, &pods.InspectOptions{})
			if err != nil {
				return nil, err
			}
			ids := make([]string, len(rep.Containers))
			for i, ci := range rep.Containers {
				ids[i] = ci.ID
			}
			return ids, nil
		},
		func(ctx context.Context, cid string) (running bool, exitCode int, err error) {
			full, err := containers.Inspect(ctx, cid, &containers.InspectOptions{})
			if err != nil {
				return false, 0, err
			}
			if full.State == nil {
				return false, 0, nil
			}
			return full.State.Running, int(full.State.ExitCode), nil
		},
	)
}

// waitForCompletion is the polling core of WaitForPodCompletion, decoupled
// from libpod bindings behind two small closures so its timeout/error
// handling can be unit-tested without a live podman connection. ctx's own
// deadline is authoritative for the whole wait (the caller is expected to
// have derived it from their own timeout, not a fixed per-call cap).
func waitForCompletion(
	ctx context.Context,
	podName string,
	inspectPod func(ctx context.Context, name string) ([]string, error),
	inspectContainer func(ctx context.Context, id string) (running bool, exitCode int, err error),
) (int, error) {
	for {
		ids, err := inspectPod(ctx, podName)
		if err != nil {
			if isNotFound(err) {
				return 0, ErrNotFound
			}
			if errors.Is(ctx.Err(), context.DeadlineExceeded) {
				return 0, ErrWaitTimeout
			}
			return 0, err
		}

		allExited := true
		exitCode := 0
		for _, cid := range ids {
			running, code, err := inspectContainer(ctx, cid)
			if err != nil {
				if errors.Is(ctx.Err(), context.DeadlineExceeded) {
					return 0, ErrWaitTimeout
				}
				// A container libpod just listed in the pod but can no
				// longer inspect is ambiguous, never success: it may have
				// crash-exited and been reaped, been removed by an external
				// `podman rm`, or raced kube-play teardown. None of those
				// mean the workload finished cleanly, so fail closed rather
				// than silently reporting exit 0 for a container we never
				// actually observed exiting (isNotFound included).
				return 0, fmt.Errorf("podman: inspecting container %s while waiting for pod %s completion: %w", cid, podName, err)
			}
			if running {
				allExited = false
				continue
			}
			if code != 0 && exitCode == 0 {
				exitCode = code
			}
		}
		if allExited {
			return exitCode, nil
		}

		select {
		case <-ctx.Done():
			if errors.Is(ctx.Err(), context.DeadlineExceeded) {
				return 0, ErrWaitTimeout
			}
			return 0, ctx.Err()
		case <-time.After(waitPollInterval):
		}
	}
}

func (r *Real) PodInspect(ctx context.Context, id, name string) (Pod, error) {
	c, cancel, err := r.opCtxFor(ctx, id)
	if err != nil {
		return Pod{}, err
	}
	defer cancel()
	rep, err := pods.Inspect(c, name, &pods.InspectOptions{})
	if err != nil {
		if isNotFound(err) {
			return Pod{}, ErrNotFound
		}
		return Pod{}, err
	}
	out := podFromInspect(rep)
	// Enrich each container with full inspect data (image, ports, env, etc.).
	for i := range out.Containers {
		full, err := containers.Inspect(c, out.Containers[i].ID, &containers.InspectOptions{})
		if err == nil {
			enrichContainer(&out.Containers[i], full)
		}
	}
	return out, nil
}

// enrichContainer fills in Container fields that are not available from the
// pod inspect report alone.
func enrichContainer(c *Container, ins *define.InspectContainerData) {
	c.Image = ins.ImageDigest
	if c.Image == "" {
		c.Image = ins.Image
	}
	c.ImageTag = ins.ImageName
	if ins.State != nil && !ins.State.StartedAt.IsZero() {
		c.StartedAt = ins.State.StartedAt
	}
	if ins.State != nil && ins.State.Health != nil {
		c.Health = ins.State.Health.Status
	}
	if ins.State != nil {
		c.Exited = !ins.State.Running
		c.ExitCode = int(ins.State.ExitCode)
	}
	if ins.Config != nil && ins.Config.Healthcheck != nil {
		c.HealthStartPeriod = ins.Config.Healthcheck.StartPeriod
		c.HealthInterval = ins.Config.Healthcheck.Interval
	}
	c.RestartCount = int(ins.RestartCount)
	if ins.HostConfig != nil {
		// PortBindings maps "<containerPort>/<protocol>" -> []HostPort, so
		// the container port and protocol live in the map key.
		for key, ports := range ins.HostConfig.PortBindings {
			cp, proto := splitPortKey(key)
			for _, b := range ports {
				hp, _ := strconv.Atoi(b.HostPort)
				c.Ports = append(c.Ports, PortMapping{
					HostIP:        b.HostIP,
					HostPort:      hp,
					ContainerPort: cp,
					Protocol:      proto,
				})
			}
		}
	}
	if ins.Config != nil && ins.Config.Env != nil {
		c.Env = make(map[string]string, len(ins.Config.Env))
		for _, e := range ins.Config.Env {
			eq := strings.IndexByte(e, '=')
			if eq > 0 {
				c.Env[e[:eq]] = e[eq+1:]
			}
		}
	}
}

func (r *Real) PodList(ctx context.Context, id string, filters map[string]string) ([]Pod, error) {
	c, cancel, err := r.opCtxFor(ctx, id)
	if err != nil {
		return nil, err
	}
	defer cancel()
	opts := &pods.ListOptions{}
	if len(filters) > 0 {
		f := map[string][]string{}
		for k, v := range filters {
			f["label"] = append(f["label"], k+"="+v)
		}
		opts.Filters = f
	}
	reps, err := pods.List(c, opts)
	if err != nil {
		return nil, err
	}
	out := make([]Pod, 0, len(reps))
	for _, rep := range reps {
		p := podFromList(rep)
		// Enrich each container with full inspect data so list and get return
		// the same shape (image_tag, started_at, ports, env, etc.). Skip on
		// per-container error — partial data beats failing the whole list.
		for i := range p.Containers {
			full, err := containers.Inspect(c, p.Containers[i].ID, &containers.InspectOptions{})
			if err == nil {
				enrichContainer(&p.Containers[i], full)
			}
		}
		out = append(out, p)
	}
	return out, nil
}

func (r *Real) PodStart(ctx context.Context, id, name string) error {
	c, cancel, err := r.opCtxFor(ctx, id)
	if err != nil {
		return err
	}
	defer cancel()
	_, err = pods.Start(c, name, &pods.StartOptions{})
	return mapNotFound(err)
}

func (r *Real) PodStop(ctx context.Context, id, name string) error {
	c, cancel, err := r.opCtxFor(ctx, id)
	if err != nil {
		return err
	}
	defer cancel()
	_, err = pods.Stop(c, name, &pods.StopOptions{})
	return mapNotFound(err)
}

func (r *Real) PodRestart(ctx context.Context, id, name string) error {
	c, cancel, err := r.opCtxFor(ctx, id)
	if err != nil {
		return err
	}
	defer cancel()
	_, err = pods.Restart(c, name, &pods.RestartOptions{})
	return mapNotFound(err)
}

func (r *Real) PodRemove(ctx context.Context, id, name string, force bool) error {
	c, cancel, err := r.opCtxFor(ctx, id)
	if err != nil {
		return err
	}
	defer cancel()
	opts := &pods.RemoveOptions{}
	if force {
		t := true
		opts.Force = &t
	}
	_, err = pods.Remove(c, name, opts)
	return mapNotFound(err)
}

func (r *Real) SecretCreate(ctx context.Context, id, name string, value []byte) error {
	c, cancel, err := r.opCtxFor(ctx, id)
	if err != nil {
		return err
	}
	defer cancel()
	opts := &secrets.CreateOptions{Name: &name}
	_, err = secrets.Create(c, bytes.NewReader(value), opts)
	return err
}

func (r *Real) SecretList(ctx context.Context, id string) ([]Secret, error) {
	c, cancel, err := r.opCtxFor(ctx, id)
	if err != nil {
		return nil, err
	}
	defer cancel()
	reps, err := secrets.List(c, &secrets.ListOptions{})
	if err != nil {
		return nil, err
	}
	out := make([]Secret, 0, len(reps))
	for _, s := range reps {
		out = append(out, Secret{Name: s.Spec.Name, CreatedAt: s.CreatedAt})
	}
	return out, nil
}

func (r *Real) SecretInspect(ctx context.Context, id, name string) (Secret, error) {
	c, cancel, err := r.opCtxFor(ctx, id)
	if err != nil {
		return Secret{}, err
	}
	defer cancel()
	rep, err := secrets.Inspect(c, name, &secrets.InspectOptions{})
	if err != nil {
		return Secret{}, mapNotFound(err)
	}
	return Secret{Name: rep.Spec.Name, CreatedAt: rep.CreatedAt}, nil
}

func (r *Real) SecretRemove(ctx context.Context, id, name string) error {
	c, cancel, err := r.opCtxFor(ctx, id)
	if err != nil {
		return err
	}
	defer cancel()
	return mapNotFound(secrets.Remove(c, name))
}

func (r *Real) VolumeInspect(ctx context.Context, id, name string) (Volume, error) {
	c, cancel, err := r.opCtxFor(ctx, id)
	if err != nil {
		return Volume{}, err
	}
	defer cancel()
	rep, err := volumes.Inspect(c, name, &volumes.InspectOptions{})
	if err != nil {
		return Volume{}, mapNotFound(err)
	}
	v := Volume{Name: rep.Name}
	// Size is not always populated; leave at 0 if missing.
	return v, nil
}

func (r *Real) VolumeRemove(ctx context.Context, id, name string, force bool) error {
	c, cancel, err := r.opCtxFor(ctx, id)
	if err != nil {
		return err
	}
	defer cancel()
	opts := &volumes.RemoveOptions{}
	if force {
		t := true
		opts.Force = &t
	}
	return mapNotFound(volumes.Remove(c, name, opts))
}

// VolumeExport streams a volume's contents as an uncompressed tar. The returned
// reader is the live HTTP response body; the caller must Close it. We issue the
// REST request directly rather than using the high-level volumes.Export binding
// because that binding copies into an io.Writer, whereas our contract must hand
// back a live io.ReadCloser for pipe streaming.
//
// The connection's context deadline is volumeTransferTimeout, not the shorter
// callTimeout every other operation uses (#223): the caller streams the
// response body to completion well after this call returns, so the deadline
// must cover the whole transfer, not just issuing the request.
func (r *Real) VolumeExport(ctx context.Context, id, name string) (io.ReadCloser, error) {
	c, cancel, err := r.opCtxForTimeout(ctx, id, volumeTransferTimeout())
	if err != nil {
		return nil, err
	}
	conn, err := bindings.GetClient(c)
	if err != nil {
		cancel()
		return nil, err
	}
	resp, err := conn.DoRequest(c, nil, http.MethodGet, "/volumes/%s/export", nil, nil, name)
	if err != nil {
		cancel()
		return nil, err
	}
	if !resp.IsSuccess() {
		defer resp.Body.Close()
		cancel()
		// Process(nil) drains the body and returns podman's error for non-2xx.
		return nil, mapNotFound(resp.Process(nil))
	}
	return &cancelReadCloser{ReadCloser: resp.Body, cancel: cancel}, nil
}

// VolumeImport unpacks an uncompressed tar into an existing volume on the host.
func (r *Real) VolumeImport(ctx context.Context, id, name string, src io.Reader) error {
	c, cancel, err := r.opCtxForTimeout(ctx, id, volumeTransferTimeout())
	if err != nil {
		return err
	}
	defer cancel()
	return mapNotFound(volumes.Import(c, name, src))
}

// VolumeCreate creates an empty named volume. An already-existing name is
// treated as success so migrate's create-then-copy step is idempotent on retry.
func (r *Real) VolumeCreate(ctx context.Context, id, name string) error {
	c, cancel, err := r.opCtxFor(ctx, id)
	if err != nil {
		return err
	}
	defer cancel()
	if _, err := volumes.Create(c, entities.VolumeCreateOptions{Name: name}, nil); err != nil {
		if strings.Contains(strings.ToLower(err.Error()), "already exists") {
			return nil
		}
		return err
	}
	return nil
}

// NetworkEnsure creates the named network if absent, with aardvark DNS enabled.
//
// DNS must be on: the `podman network create` CLI defaults it true, but the REST
// API does not, and ingress backend routing resolves pods by name on this
// network — without DNS the proxy can't reach the backend (502).
//
// An existing network is only accepted if its DNS is already on. A network left
// by a pre-DNS build has it off, and DNS can't be flipped on an existing network
// via the API, so we fail with the one-time fix instead of silently keeping it
// disabled (which IgnoreIfExists would do).
func (r *Real) NetworkEnsure(ctx context.Context, id, name string) error {
	c, cancel, err := r.opCtxFor(ctx, id)
	if err != nil {
		return err
	}
	defer cancel()
	if exists, err := network.Exists(c, name, nil); err != nil {
		return err
	} else if exists {
		rep, err := network.Inspect(c, name, nil)
		if err != nil {
			return err
		}
		if !rep.DNSEnabled {
			return fmt.Errorf("network %q exists with DNS disabled (from an older build); remove it and retry: podman network rm %s", name, name)
		}
		return nil
	}
	// IgnoreIfExists guards the create against a concurrent ensure racing in
	// after the Exists check above.
	ignore := true
	_, err = network.CreateWithOptions(c, &nettypes.Network{Name: name, DNSEnabled: true},
		&network.ExtraCreateOptions{IgnoreIfExists: &ignore})
	return err
}

func (r *Real) ContainerExec(ctx context.Context, id, container string, cmd []string) (ExecResult, error) {
	c, cancel, err := r.opCtxFor(ctx, id)
	if err != nil {
		return ExecResult{}, err
	}
	defer cancel()
	sessionID, err := containers.ExecCreate(c, container, &handlers.ExecCreateConfig{
		ExecOptions: dockerContainer.ExecOptions{
			Cmd:          cmd,
			AttachStdout: true,
			AttachStderr: true,
		},
	})
	if err != nil {
		return ExecResult{}, mapNotFound(err)
	}
	var buf bytes.Buffer
	var w io.Writer = &buf
	attach := true
	if err := containers.ExecStartAndAttach(c, sessionID, &containers.ExecStartAndAttachOptions{
		OutputStream: &w,
		ErrorStream:  &w,
		AttachOutput: &attach,
		AttachError:  &attach,
	}); err != nil {
		return ExecResult{}, err
	}
	ins, err := containers.ExecInspect(c, sessionID, &containers.ExecInspectOptions{})
	if err != nil {
		return ExecResult{}, err
	}
	return ExecResult{ExitCode: ins.ExitCode, Output: buf.String()}, nil
}

func (r *Real) CopyToContainer(ctx context.Context, id, container, destDir, name string, content []byte) error {
	c, cancel, err := r.opCtxFor(ctx, id)
	if err != nil {
		return err
	}
	defer cancel()
	var tarBuf bytes.Buffer
	tw := tar.NewWriter(&tarBuf)
	if err := tw.WriteHeader(&tar.Header{
		Name: name,
		Mode: 0o644,
		Size: int64(len(content)),
	}); err != nil {
		return err
	}
	if _, err := tw.Write(content); err != nil {
		return err
	}
	if err := tw.Close(); err != nil {
		return err
	}
	// CopyFromArchive copies INTO the container (PUT /containers/{id}/archive).
	copyFn, err := containers.CopyFromArchive(c, container, destDir, &tarBuf)
	if err != nil {
		return mapNotFound(err)
	}
	return copyFn()
}

// --- helpers ---

func isNotFound(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "no such pod") ||
		strings.Contains(msg, "no such container") ||
		strings.Contains(msg, "no such secret") ||
		strings.Contains(msg, "no such volume") ||
		strings.Contains(msg, "not found")
}

func mapNotFound(err error) error {
	if isNotFound(err) {
		return ErrNotFound
	}
	return err
}

func podFromInspect(p *entities.PodInspectReport) Pod {
	out := Pod{
		ID:      p.ID,
		Name:    p.Name,
		Status:  p.State,
		Labels:  p.Labels,
		InfraID: p.InfraContainerID,
	}
	if !p.Created.IsZero() {
		out.Created = p.Created
	}
	for _, c := range p.Containers {
		out.Containers = append(out.Containers, Container{
			ID:     c.ID,
			Name:   c.Name,
			Status: c.State,
		})
	}
	return out
}

func podFromList(p *entities.ListPodsReport) Pod {
	out := Pod{
		ID:      p.Id,
		Name:    p.Name,
		Status:  p.Status,
		Labels:  p.Labels,
		InfraID: p.InfraId,
	}
	if !p.Created.IsZero() {
		out.Created = p.Created
	}
	for _, c := range p.Containers {
		out.Containers = append(out.Containers, Container{
			ID:     c.Id,
			Name:   c.Names,
			Status: c.Status,
		})
	}
	return out
}

func mapContainerStats(s define.ContainerStats) ContainerStats {
	out := ContainerStats{
		Name:            s.Name,
		CPUNano:         s.CPUNano,
		MemUsageBytes:   s.MemUsage,
		MemLimitBytes:   s.MemLimit,
		BlockReadBytes:  s.BlockInput,
		BlockWriteBytes: s.BlockOutput,
		PIDs:            s.PIDs,
	}
	for _, n := range s.Network {
		out.NetRxBytes += n.RxBytes
		out.NetTxBytes += n.TxBytes
	}
	return out
}

// ContainerStats issues one non-streaming stats call and returns the first
// (and only) report. Passing nil containers asks podman for every container on
// the host, so a fleet sample costs one round trip per host.
//
// All is deliberately left unset. With an empty name list podman's abi selects
// GetRunningContainers when All is false and GetAllContainers when it is true —
// but computeStats then skips every non-running container regardless, swallowing
// ErrCtrStopped/ErrCtrStateInvalid/ErrNoCgroups because the query was for all.
// So All:true yields no extra samples; it only enumerates and lock-touches every
// exited container on the host, once per tick.
func (r *Real) ContainerStats(ctx context.Context, id string) ([]ContainerStats, error) {
	c, cancel, err := r.opCtxFor(ctx, id)
	if err != nil {
		return nil, err
	}
	defer cancel()
	stream := false
	ch, err := containers.Stats(c, nil, &containers.StatsOptions{Stream: &stream})
	if err != nil {
		return nil, err
	}
	select {
	case <-c.Done():
		// The binding's own goroutine (containers.Stats) does an unbuffered,
		// non-select send on ch and only closes it (and the response body)
		// after that send completes. Abandoning ch here without a further
		// receive would leave that goroutine blocked forever on the send,
		// leaking it and the underlying HTTP connection. Drain it in the
		// background: for a non-streaming call (Stream=false, as set above)
		// the binding goroutine sends at most one report and then returns,
		// so a single receive is always enough to unblock and let it clean
		// up, whether that receive yields the report or the zero value from
		// a channel the goroutine closed without sending (e.g. because it
		// observed the same ctx cancellation via response.Request.Context()).
		go func() { <-ch }()
		return nil, c.Err()
	case rep, ok := <-ch:
		if !ok {
			return nil, nil
		}
		if rep.Error != nil {
			return nil, rep.Error
		}
		out := make([]ContainerStats, 0, len(rep.Stats))
		for _, s := range rep.Stats {
			out = append(out, mapContainerStats(s))
		}
		return out, nil
	}
}

// VolumeUsage returns each volume's on-disk size in bytes, keyed by volume
// name, via one `system df` call.
func (r *Real) VolumeUsage(ctx context.Context, id string) (map[string]int64, error) {
	c, cancel, err := r.opCtxFor(ctx, id)
	if err != nil {
		return nil, err
	}
	defer cancel()
	df, err := system.DiskUsage(c, &system.DiskOptions{})
	if err != nil {
		return nil, err
	}
	if df == nil {
		// Unreachable via system.DiskUsage, which never returns (nil, nil) — but
		// returning (nil, nil) here would look like a successful walk that found
		// no volumes, and RefreshHostVolumeUsage would then cache an empty map
		// with a fresh timestamp: every size wiped and volume_usage_age_seconds
		// reset to 0. The cache's contract is to keep the last sizing through a
		// failure, so this must be a failure.
		return nil, errors.New("system df returned no report")
	}
	out := make(map[string]int64, len(df.Volumes))
	for _, v := range df.Volumes {
		out[v.VolumeName] = v.Size
	}
	return out, nil
}

// ContainerLogs streams log lines from a container. Cancellation propagates
// bidirectionally: if the caller's ctx is cancelled the underlying
// containers.Logs call is cancelled (via mergedCtx), and if the producer
// finishes naturally the bridge goroutine exits cleanly.
//
// The connection context (c) is long-lived and must not be cancelled per
// request; mergedCtx derives from c but can be independently cancelled so
// the streaming call is torn down without killing the cached connection.
func (r *Real) ContainerLogs(ctx context.Context, id, container string, opts LogOptions) (<-chan LogLine, error) {
	c, cleanupOpCtx, err := r.opCtxFor(ctx, id)
	if err != nil {
		return nil, err
	}
	// No defer cancel() here — the goroutines below own the lifecycle of c.
	// cleanupOpCtx runs inside the fan-in goroutine when streaming ends.

	// mergedCtx is derived from the connection context but independently
	// cancellable. Cancelling it tears down the containers.Logs HTTP stream
	// without affecting the cached connection context c.
	mergedCtx, cancel := context.WithCancel(c)

	// Bridge goroutine: propagate caller cancellation to mergedCtx, and also
	// exit when mergedCtx itself finishes (natural completion or cancel()).
	go func() {
		select {
		case <-ctx.Done():
		case <-mergedCtx.Done():
		}
		cancel()
	}()

	stdoutCh := make(chan string, 64)
	stderrCh := make(chan string, 64)
	out := make(chan LogLine, 64)

	go func() {
		defer close(out)
		defer cancel()       // signal the bridge and producer to stop when fan-in exits
		defer cleanupOpCtx() // release the WithTimeout timer and AfterFunc registration
		for {
			select {
			case <-ctx.Done():
				return
			case line, ok := <-stdoutCh:
				if !ok {
					stdoutCh = nil
				} else {
					select {
					case out <- LogLine{Container: container, Stream: "stdout", Line: line, Time: time.Now()}:
					case <-ctx.Done():
						return
					}
				}
			case line, ok := <-stderrCh:
				if !ok {
					stderrCh = nil
				} else {
					select {
					case out <- LogLine{Container: container, Stream: "stderr", Line: line, Time: time.Now()}:
					case <-ctx.Done():
						return
					}
				}
			}
			if stdoutCh == nil && stderrCh == nil {
				return
			}
		}
	}()

	tail := ""
	if opts.Tail > 0 {
		tail = strconv.Itoa(opts.Tail)
	}
	follow := opts.Follow
	logsOpts := &containers.LogOptions{
		Stdout: boolPtr(true), Stderr: boolPtr(true),
		Follow: &follow, Tail: &tail,
	}
	// Only set Since if non-empty: libpod rejects an empty value with
	// "unable to interpret time value".
	if opts.Since != "" {
		logsOpts.Since = &opts.Since
	}
	go func() {
		_ = containers.Logs(mergedCtx, container, logsOpts, stdoutCh, stderrCh)
		close(stdoutCh)
		close(stderrCh)
	}()
	return out, nil
}

func (r *Real) ImagePull(ctx context.Context, id, ref string) error {
	c, cancel, err := r.opCtxForTimeout(ctx, id, imagePullTimeout())
	if err != nil {
		return err
	}
	defer cancel()
	_, err = images.Pull(c, ref, &images.PullOptions{})
	return err
}

func (r *Real) UsedHostPorts(ctx context.Context, id string) ([]PortMapping, error) {
	c, cancel, err := r.opCtxFor(ctx, id)
	if err != nil {
		return nil, err
	}
	defer cancel()
	all := true
	conts, err := containers.List(c, &containers.ListOptions{All: &all})
	if err != nil {
		return nil, err
	}
	var out []PortMapping
	for _, ct := range conts {
		containerName := ""
		if len(ct.Names) > 0 {
			containerName = ct.Names[0]
		}
		for _, p := range ct.Ports {
			out = append(out, PortMapping{
				HostIP:        p.HostIP,
				HostPort:      int(p.HostPort),
				ContainerPort: int(p.ContainerPort),
				Protocol:      p.Protocol,
				Pod:           ct.PodName,
				Container:     containerName,
			})
		}
	}
	return out, nil
}

// HostInfo returns a point-in-time resource snapshot for a host. CPU/mem/disk
// come from libpod `info`; reclaimable from `system df`; loadavg is a
// best-effort read of /proc/loadavg (the one metric libpod does not expose).
// Any sub-metric that cannot be obtained is left at its zero/nil value rather
// than failing the whole call; only a failed `info` call (host unreachable)
// returns an error.
func (r *Real) HostInfo(ctx context.Context, id string) (HostInfo, error) {
	c, err := r.ctxFor(ctx, id)
	if err != nil {
		return HostInfo{}, err
	}
	info, err := system.Info(c, &system.InfoOptions{})
	if err != nil {
		return HostInfo{}, err
	}
	out := HostInfo{}
	if info.Host != nil {
		out.CPUs = info.Host.CPUs
		out.MemTotal = info.Host.MemTotal
		out.MemFree = info.Host.MemFree
		if info.Host.MemTotal > 0 {
			out.MemUsedPct = float64(info.Host.MemTotal-info.Host.MemFree) / float64(info.Host.MemTotal) * 100
		}
		if u := info.Host.CPUUtilization; u != nil {
			// libpod reports utilization cumulative since boot, not instantaneous.
			sinceBoot := u.UserPercent + u.SystemPercent
			out.CPUPct = &sinceBoot
		}
	}
	if info.Store != nil {
		out.Disk.Total = int64(info.Store.GraphRootAllocated)
		out.Disk.Used = int64(info.Store.GraphRootUsed)
		out.Disk.Free = out.Disk.Total - out.Disk.Used
		if out.Disk.Free < 0 {
			out.Disk.Free = 0
		}
	}
	if df, err := system.DiskUsage(c, &system.DiskOptions{}); err == nil && df != nil {
		var reclaimable int64
		for _, v := range df.Volumes {
			reclaimable += v.ReclaimableSize
		}
		out.Disk.Reclaimable = reclaimable
	}
	if la := r.hostLoadAvg(ctx, id); la != nil {
		out.LoadAvg = la
	}
	return out, nil
}

// parseProcUptime parses the first field of a /proc/uptime line ("12345.67
// 98765.43" — uptime seconds, then idle seconds summed across CPUs) into a
// Duration. ok is false when the line doesn't start with a valid float
// (empty, or garbage from a failed read) — callers must not mistake that for
// a genuine zero uptime.
func parseProcUptime(s string) (time.Duration, bool) {
	fields := strings.Fields(s)
	if len(fields) == 0 {
		return 0, false
	}
	secs, err := strconv.ParseFloat(fields[0], 64)
	if err != nil || secs < 0 {
		return 0, false
	}
	return time.Duration(secs * float64(time.Second)), true
}

// HostUptime returns hostID's current kernel uptime, read directly from
// /proc/uptime — for a unix (local) host, straight off disk; for an SSH host,
// via a short-lived `cat /proc/uptime` exec bounded by ctx — mirroring
// hostLoadAvg's dual-path pattern below rather than going through libpod's
// `info` endpoint. The inventory poller calls this on every host on every
// tick to detect a reboot, and `info` (system.Info, the same call HostInfo
// above makes) pays for a full host-info aggregation — distro, OCI runtime,
// network backend, CPU utilization, lock manager, etc. — just to read one
// uptime string; /proc/uptime is the one-line read that's actually needed.
func (r *Real) HostUptime(ctx context.Context, id string) (time.Duration, bool, error) {
	raw, err := r.readHostProc(id,
		func() (string, error) {
			b, err := os.ReadFile("/proc/uptime")
			return string(b), err
		},
		func() (string, error) { return r.sshReadUptime(ctx, id) },
	)
	if err != nil {
		return 0, false, err
	}
	d, ok := parseProcUptime(raw)
	return d, ok, nil
}

// HostBoundPorts returns every port of protocol ("tcp" or "udp") currently
// bound on id, read from /proc/net/<protocol> and (best-effort)
// /proc/net/<protocol>6 — mirroring HostUptime's dual local/SSH path rather
// than going through libpod, because libpod has no visibility at all into a
// plain host-level process (see UsedHostPorts, which only sees ports podman
// itself published). /proc parsing is chosen over shelling out to `ss` (or
// `netstat`) so this doesn't add an iproute2/net-tools dependency to managed
// hosts beyond what the rest of this file already assumes (`cat`, used
// identically by HostUptime/hostLoadAvg above) — /proc/net/{tcp,udp}[6] is
// present on every Linux kernel with CONFIG_PROC_FS, which podman itself
// already requires.
func (r *Real) HostBoundPorts(ctx context.Context, id, protocol string) ([]int, error) {
	raw, err := r.readHostProc(id,
		func() (string, error) { return readProcNetPortsLocal(protocol) },
		func() (string, error) { return r.sshReadProcNetPorts(ctx, id, protocol) },
	)
	if err != nil {
		return nil, err
	}
	return parseProcNetPorts(raw), nil
}

// readHostProc reads one /proc file from id by whichever path its current
// address calls for — locally for a unix host, over SSH for a remote one —
// and re-dispatches once if a reload moved the host under the read.
//
// The re-dispatch is the point. Choosing the branch means reading r.hosts, and
// the read that follows resolves it again for itself (see sshRun on why it must),
// so a SIGHUP landing between the two makes the branch and the read disagree.
// sshRun refuses that rather than dialing "unix:22", and for the loadavg path
// refusing is enough — it skips a cycle and the cache still holds. It is not
// enough here: HostBoundPorts feeds a deploy's port-conflict precheck where
// every error aborts the deploy, so a host edited to local at the wrong instant
// would fail a deploy that had nothing wrong with it. Looking again yields the
// local branch, which is where the answer now lives.
//
// Exactly one re-dispatch, matching connectFresh: a second one means reloads are
// arriving faster than reads complete, and looping there is indistinguishable
// from a hang.
func (r *Real) readHostProc(id string, local, remote func() (string, error)) (string, error) {
	for attempt := 0; attempt < 2; attempt++ {
		r.mu.Lock()
		h, ok := r.hosts[id]
		r.mu.Unlock()
		if !ok {
			return "", fmt.Errorf("unknown host %q", id)
		}
		if h.Addr == "unix" {
			return local()
		}
		raw, err := remote()
		if errors.Is(err, errHostReconfigured) && attempt == 0 {
			// The only way sshRun reports this: the host became local while we
			// were connecting to it.
			continue
		}
		return raw, err
	}
	return "", fmt.Errorf("host %q: %w", id, errHostReconfigured)
}

// readProcNetPortsLocal reads /proc/net/<protocol> (required — its absence is
// a real error) and appends /proc/net/<protocol>6 (best-effort — a host with
// IPv6 disabled simply lacks this file, which is not an error condition; see
// the IPv4-only precedent for a similar host/network gap already documented
// on vpn.Config.validate's remote_subnet check in podman-api-pro).
func readProcNetPortsLocal(protocol string) (string, error) {
	b, err := os.ReadFile("/proc/net/" + protocol)
	if err != nil {
		return "", err
	}
	out := string(b)
	if b6, err := os.ReadFile("/proc/net/" + protocol + "6"); err == nil {
		out += string(b6)
	}
	return out, nil
}

// parseProcNetPorts extracts every locally-bound port number from the
// concatenated contents of /proc/net/<protocol>[6]. Each data line's second
// field is "local_address" as hex "ADDR:PORT" (e.g. "00000000:01F4" for
// 0.0.0.0:500, or 32 hex chars before the colon for an IPv6 "::" address);
// the port is the last ':'-separated hex token, always 4 hex digits
// regardless of address family, and needs no endianness handling — the
// kernel already prints it big-endian (network byte order), unlike the
// address bytes. The port is returned for EVERY bound entry regardless of
// state: UDP has no listen/established distinction the way TCP does — an
// unconnected (recvfrom-style) server socket shows state "07", so filtering
// by state would silently stop detecting exactly the sockets this exists to
// catch. A malformed line (short header, non-hex field) is skipped, not fatal
// — matching parseProcUptime/parseLoadAvg's best-effort posture for lines
// this parser doesn't expect.
func parseProcNetPorts(raw string) []int {
	var out []int
	for _, line := range strings.Split(raw, "\n") {
		fields := strings.Fields(line)
		if len(fields) < 2 {
			continue
		}
		local := fields[1]
		colon := strings.LastIndexByte(local, ':')
		if colon < 0 || colon == len(local)-1 {
			continue
		}
		port, err := strconv.ParseUint(local[colon+1:], 16, 16)
		if err != nil {
			continue
		}
		out = append(out, int(port))
	}
	return out
}

// loadSample is one host's /proc/loadavg reading and when it was taken. A
// sample with ok=false records a *failed* read: the failure is cached too, so
// a host whose side channel is broken does not cost every single request a
// fresh dial to rediscover that (the trust-divergence case in
// sshReadLoadAvg's doc comment fails exactly this way, on every read, forever).
type loadSample struct {
	val [3]float64
	at  time.Time
	ok  bool
	// seq increments on every commit for this host, so a caller that waited on
	// the read-through gate can tell whether someone else refreshed the entry
	// while it queued. A timestamp cannot answer that: two commits can share
	// one clock reading.
	seq uint64
}

// loadAvgTTL is how long a sample is served before a reader takes a fresh one.
// It is deliberately longer than the default inventory interval (30s), so on a
// poller-enabled daemon the poller is the only thing that ever pays for the
// read and the request path is pure cache. A daemon running with polling
// disabled still gets the fix — it just refreshes lazily, one read per host
// per TTL instead of one per request.
//
// The staleness this admits is bounded by TTL and harmless for what the metric
// is: loadavg's shortest component is already a 1-minute average, so a sample
// up to a minute old carries essentially the same information as a live one.
// Anything wanting an instantaneous reading wants cpu_pct, not this.
const loadAvgTTL = 60 * time.Second

// loadAvgFailTTL is how long a failed read suppresses the next request-path
// attempt. Shorter than loadAvgTTL so a host that recovers starts reporting
// again promptly on a daemon with no poller; the poller itself ignores both
// TTLs, so a polled daemon retries every tick regardless.
const loadAvgFailTTL = 30 * time.Second

// errHostReconfigured means a read completed but described a host that had
// been reconfigured or removed while it was in flight, so the result was
// discarded. It is not a host fault: nothing is wrong with the host, the
// sample simply belongs to an endpoint that is no longer this host's.
// SampleLoadAvg swallows it for exactly that reason — surfaced to the poller
// it would be logged as "loadavg unavailable", which is the false-positive
// this change exists to remove.
// Not "…while reading loadavg": readHostProc reports it for uptime and port
// reads too, where it is recoverable rather than merely benign.
var errHostReconfigured = errors.New("host was reconfigured while reading")

// errUnknownHost marks a read for a host that is no longer configured. On the
// poller's path that always means "removed since the tick started", never a
// bad id: the poller only ever passes ids it took off the host list.
var errUnknownHost = errors.New("unknown host")

func (r *Real) now() time.Time {
	if r.hooks != nil && r.hooks.now != nil {
		return r.hooks.now()
	}
	return time.Now()
}

// hostLoadAvg returns a host's 1/5/15-minute load averages, or nil if they
// cannot be obtained. A cached sample younger than loadAvgTTL is served as-is;
// otherwise it reads through and caches the result.
//
// Reading through is what used to happen on every single request, at the tail
// of a 5s per-host budget that libpod had usually already eaten — so the
// metric silently vanished for exactly the busiest hosts (#258).
func (r *Real) hostLoadAvg(ctx context.Context, id string) *[3]float64 {
	if la, done := r.servableLoadAvg(id); done {
		return la
	}
	// Single-flight the read-through. Without this, N requests arriving on the
	// same host after the TTL lapses each pay their own SSH round trip — which
	// is the cost this whole change exists to remove, just moved from "every
	// request" to "every request in the first burst after each expiry". Only
	// one of them needs to go and ask.
	//
	// ctx-aware, for the same reason the pool's dial gate is: a caller that
	// waited out someone else's read and only then started its own would have
	// spent its budget to arrive exactly where it began.
	gate, known := r.loadGateFor(id)
	if !known {
		return nil
	}
	release, acquired := acquireGate(ctx, gate)
	if !acquired {
		return nil
	}
	defer release()
	// The winner of that race has filled the cache (or recorded a failure) by
	// the time we get here, so re-check before reading through.
	if la, done := r.servableLoadAvg(id); done {
		return la
	}

	la, logWorthy, err := r.sampleLoadAvg(ctx, id)
	if err != nil {
		// Logged, not swallowed: this failure mode was previously diagnosed by
		// bisecting a table of hosts by hand, which it should never have taken.
		// Gated on the transition, like the poller gates its own: a host that
		// is permanently broken would otherwise emit a line on every scrape.
		if logWorthy {
			log.Printf("podman: loadavg unavailable for host %q: %v", id, err)
		}
		return nil
	}
	return la
}

// servableLoadAvg answers from cache where it can. done reports whether the
// cache settled the question: either a fresh sample to serve, or a recent
// failure that means "absent, and not worth another round trip yet" — the
// poller, or the next request after loadAvgFailTTL, is what retries.
func (r *Real) servableLoadAvg(id string) (la *[3]float64, done bool) {
	if s, ok := r.cachedLoadAvg(id); ok {
		v := s
		return &v, true
	}
	if r.recentLoadAvgFailure(id) {
		return nil, true
	}
	return nil, false
}

// loadGateFor returns hostID's capacity-1 read-through gate, creating it on
// first use. ok is false for a host that is not configured.
//
// That check is what actually keeps the map bounded, and it is the same guard
// sshEntryFor carries: SetHosts's removal loop has already run by the time a
// racing request gets here, so creating on demand without it would leave a
// gate behind for a host nothing will ever remove again.
func (r *Real) loadGateFor(id string) (chan struct{}, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, known := r.hosts[id]; !known {
		return nil, false
	}
	if r.loadGate == nil {
		r.loadGate = map[string]chan struct{}{}
	}
	g, ok := r.loadGate[id]
	if !ok {
		g = make(chan struct{}, 1)
		r.loadGate[id] = g
	}
	return g, true
}

// cachedLoadAvg returns the host's cached sample if it is a successful read
// younger than loadAvgTTL.
func (r *Real) cachedLoadAvg(id string) ([3]float64, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	s, ok := r.loadavg[id]
	if !ok || !s.ok || r.now().Sub(s.at) >= loadAvgTTL {
		return [3]float64{}, false
	}
	return s.val, true
}

// recentLoadAvgFailure reports whether the host's last read failed, recently
// enough that retrying on the request path is not worth the round trip.
func (r *Real) recentLoadAvgFailure(id string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	s, ok := r.loadavg[id]
	return ok && !s.ok && r.now().Sub(s.at) < loadAvgFailTTL
}

// markLoadAvgFailure records a failed read against the host as it looked when
// the read started, and reports whether this is a transition — i.e. whether
// the previous state was anything other than already-failing. Callers use the
// return value to decide whether to log.
//
// A read that raced a SetHosts reload records nothing: the failure describes
// an endpoint that is no longer this host's, and writing it would both
// resurrect a cache entry for a host that may have been removed and suppress
// reads of the new endpoint for a full loadAvgFailTTL.
func (r *Real) markLoadAvgFailure(id string, h config.Host) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	if cur, ok := r.hosts[id]; !ok || !hostConnEq(cur, h) {
		return false
	}
	prev, seen := r.loadavg[id]
	r.loadavg[id] = loadSample{at: r.now(), seq: prev.seq + 1}
	return !seen || prev.ok
}

// SampleLoadAvg reads the host's load averages and caches them, ignoring the
// TTL. This is the inventory poller's entry point: it moves the cost of the
// read off the request path entirely, onto a loop that already visits every
// host on a schedule and whose budget is its own.
//
// It does not log: the poller logs its own per-host tick outcomes, with the
// same transition gating, and one event logged twice at two layers is worse
// than the silence this whole change is about removing. The request path,
// which has no such caller, logs for itself in hostLoadAvg.
func (r *Real) SampleLoadAvg(ctx context.Context, id string) (sampled bool, err error) {
	gate, ok := r.loadGateFor(id)
	if !ok {
		// The host went away between the poller picking it off the host list
		// and this call. That is a clean removal mid-tick, not an outage —
		// and it is reachable on every reload, because applyHosts updates the
		// client before the service, so the service's existence check can pass
		// against a map the client has already swept. Reordering those two
		// calls only moves the window: client-first is what an *added* host
		// needs, service-first is what a removed one needs, and there is no
		// single order that satisfies both. So the tolerance belongs here.
		return false, nil
	}
	// The sampler ignores the TTL, but it must not ignore a read that is
	// happening right now: a tick landing on top of a request's read-through
	// would otherwise put two SSH reads on one host at one moment, which is
	// precisely what the gate exists to prevent. Sequence, not timestamp,
	// because two commits can share a clock reading.
	before := r.loadSeq(id)
	release, acquired := acquireGate(ctx, gate)
	if !acquired {
		// Someone else is mid-read on this host and we ran out of budget
		// waiting. That is not an outage — a read is in flight and the cache
		// is about to be updated — and reporting it would log one, since a
		// request-path holder can legitimately outlast this tick's budget
		// (getHost passes a context with no deadline at all).
		//
		// Reported as success it would be worse than reported as failure: the
		// caller derives its state from err == nil, so a host sitting in
		// "failing" would be flipped to "recovered" and announced as such on a
		// tick where nothing was read at all. sampled=false says the only true
		// thing — no outcome — and leaves the last real one standing.
		return false, nil
	}
	defer release()
	if _, fresh := r.cachedLoadAvg(id); fresh && r.loadSeq(id) != before {
		//nolint:nestif // the comment below is the point
		// Someone finished a real, *successful* read while we queued. That is
		// this tick's sample — at most one gate-wait old and genuinely read
		// from the host — so going out again would buy nothing.
		//
		// The freshness check is not redundant with the sequence check: a
		// failed read bumps the sequence too, and adopting that would report
		// success for a host whose read just failed, silently corrupting the
		// poller's transition state and swallowing the outage it exists to
		// report. A failure is not something to adopt; we go and read for
		// ourselves.
		return true, nil
	}

	_, _, err = r.sampleLoadAvg(ctx, id)
	if isReloadRace(err) {
		// A benign race with a config reload, not a host fault, and not an
		// outcome either: nothing was read against a host that still exists as
		// it was. The next tick resamples against the new endpoint.
		return false, nil
	}
	return true, err
}

// isReloadRace reports whether err is one of the ways a read can lose a race
// with a host-config reload rather than actually fail.
//
// There are three, at three different depths, and they have to be classified
// together: the sample is discarded at commit time (errHostReconfigured), the
// pool entry is retired mid-dial (errRetiredHost), or the host is gone by the
// time the read resolves it (errUnknownHost). Only the first is caught by the
// commit-time check, so treating that one alone as benign left the other two
// logging outages for hosts that had simply been reconfigured.
func isReloadRace(err error) bool {
	return errors.Is(err, errHostReconfigured) ||
		errors.Is(err, errRetiredHost) ||
		errors.Is(err, errUnknownHost)
}

// loadSeq returns the host's current cache generation; 0 if it has none.
func (r *Real) loadSeq(id string) uint64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.loadavg[id].seq
}

// sampleLoadAvg does the live read and caches the outcome, success or failure.
// For a unix (local) host it reads the daemon's own /proc/loadavg; for an SSH
// host it execs `cat /proc/loadavg` over the host's pooled connection, bounded
// by ctx.
//
// logWorthy reports whether a failure is a transition worth logging; it is
// meaningless when err is nil. Only the request path uses it — the poller logs
// its own tick outcomes.
func (r *Real) sampleLoadAvg(ctx context.Context, id string) (la *[3]float64, logWorthy bool, err error) {
	r.mu.Lock()
	h, ok := r.hosts[id]
	r.mu.Unlock()
	if !ok {
		return nil, false, fmt.Errorf("%q: %w", id, errUnknownHost)
	}
	// Every failure exit below records the failure against h, so a broken host
	// costs one round trip per TTL rather than one per request.
	//
	// Except a reload race, which is not a failure of anything. It has to be
	// excluded here rather than by markLoadAvgFailure's own guard, because
	// that guard asks "is h still current" — and after a race the answer can
	// be yes: sshRun reports the *new* config it resolved, so a host switched
	// to a unix socket mid-read comes back with h equal to r.hosts[id], sails
	// past the guard, and is recorded as failing on a read that was never
	// attempted. That poisons the cache for a host whose local /proc read
	// would have succeeded instantly, and logs an outage for it.
	fail := func(err error) (*[3]float64, bool, error) {
		if isReloadRace(err) {
			return nil, false, err
		}
		return nil, r.markLoadAvgFailure(id, h), err
	}
	var raw string
	if h.Addr == "unix" {
		b, rerr := os.ReadFile("/proc/loadavg")
		if rerr != nil {
			return fail(rerr)
		}
		raw = string(b)
	} else {
		if r.hooks != nil && r.hooks.beforeLoadResolve != nil {
			r.hooks.beforeLoadResolve()
		}
		out, used, rerr := r.sshReadLoadAvg(ctx, id)
		// The read resolves the host itself, so it may have run against a
		// newer config than the snapshot above. Judge the outcome by what
		// produced it, and do so before the error check: comparing a *failure*
		// against the stale snapshot finds a mismatch, concludes the reload
		// invalidated it, and drops a real ongoing outage — neither cached nor
		// logged, which is the silence this whole change is about.
		if used.ID != "" {
			h = used
		}
		if rerr != nil {
			return fail(rerr)
		}
		raw = out
	}
	sampled := parseLoadAvg(raw)
	if sampled == nil {
		return fail(fmt.Errorf("unparseable /proc/loadavg %q", strings.TrimSpace(raw)))
	}
	if r.hooks != nil && r.hooks.afterLoadRead != nil {
		r.hooks.afterLoadRead()
	}
	// Commit only if this is still the same host at the same endpoint. The read
	// happened with r.mu released, so a SIGHUP reload may have reconfigured or
	// removed the host meanwhile — and storing this would resurrect a sample
	// taken against an endpoint the operator has already moved away from,
	// which is the exact invariant SetHosts's own comment promises.
	r.mu.Lock()
	if cur, ok := r.hosts[id]; !ok || !hostConnEq(cur, h) {
		r.mu.Unlock()
		return nil, false, fmt.Errorf("%q: %w", id, errHostReconfigured)
	}
	r.loadavg[id] = loadSample{val: *sampled, at: r.now(), ok: true, seq: r.loadavg[id].seq + 1}
	r.mu.Unlock()
	return sampled, false, nil
}

// parseLoadAvg extracts the first three space-separated floats from a
// /proc/loadavg line ("0.42 0.37 0.31 1/512 12345").
func parseLoadAvg(raw string) *[3]float64 {
	fields := strings.Fields(raw)
	if len(fields) < 3 {
		return nil
	}
	var la [3]float64
	for i := 0; i < 3; i++ {
		v, err := strconv.ParseFloat(fields[i], 64)
		if err != nil {
			return nil
		}
		la[i] = v
	}
	return &la
}

func boolPtr(b bool) *bool { return &b }

// splitPortKey parses a libpod PortBindings key like "5432/tcp" into
// (containerPort, protocol). Missing protocol defaults to "tcp".
func splitPortKey(k string) (int, string) {
	slash := strings.IndexByte(k, '/')
	if slash < 0 {
		port, _ := strconv.Atoi(k)
		return port, "tcp"
	}
	port, _ := strconv.Atoi(k[:slash])
	return port, k[slash+1:]
}

// sumPrune folds libpod's per-item prune reports into our PruneReport. Items with
// a non-nil Err are skipped from the reclaimed total but still surfaced as ids.
func sumPrune(reps []*reports.PruneReport) PruneReport {
	var out PruneReport
	for _, r := range reps {
		if r == nil {
			continue
		}
		out.Items = append(out.Items, r.Id)
		// r.Size is uint64; guard the int64 conversion so an implausibly huge
		// item can't wrap Reclaimed negative.
		if r.Err == nil && r.Size <= math.MaxInt64 {
			out.Reclaimed += int64(r.Size)
		}
	}
	return out
}

func (r *Real) ImagePrune(ctx context.Context, id string, all bool) (PruneReport, error) {
	c, cancel, err := r.opCtxFor(ctx, id)
	if err != nil {
		return PruneReport{}, err
	}
	defer cancel()
	reps, err := images.Prune(c, new(images.PruneOptions).WithAll(all))
	if err != nil {
		return PruneReport{}, err
	}
	return sumPrune(reps), nil
}

func (r *Real) ContainerPrune(ctx context.Context, id string) (PruneReport, error) {
	c, cancel, err := r.opCtxFor(ctx, id)
	if err != nil {
		return PruneReport{}, err
	}
	defer cancel()
	reps, err := containers.Prune(c, new(containers.PruneOptions))
	if err != nil {
		return PruneReport{}, err
	}
	return sumPrune(reps), nil
}

func (r *Real) BuildCachePrune(ctx context.Context, id string) (PruneReport, error) {
	c, cancel, err := r.opCtxFor(ctx, id)
	if err != nil {
		return PruneReport{}, err
	}
	defer cancel()
	// Build cache is pruned through the images prune endpoint with the
	// build-cache flag set (libpod has no standalone build-cache binding in v5).
	reps, err := images.Prune(c, new(images.PruneOptions).WithBuildCache(true))
	if err != nil {
		return PruneReport{}, err
	}
	return sumPrune(reps), nil
}

func (r *Real) VolumePrune(ctx context.Context, id string, filters map[string][]string) (PruneReport, error) {
	c, cancel, err := r.opCtxFor(ctx, id)
	if err != nil {
		return PruneReport{}, err
	}
	defer cancel()
	opts := new(volumes.PruneOptions)
	if len(filters) > 0 {
		opts = opts.WithFilters(filters)
	}
	reps, err := volumes.Prune(c, opts)
	if err != nil {
		return PruneReport{}, err
	}
	return sumPrune(reps), nil
}

// cancelReadCloser wraps an io.ReadCloser with a cancel function that is
// called when the reader is closed. Used for VolumeExport: the caller reads
// the live HTTP response body after the function returns, so context
// cancellation must be deferred until the body is consumed.
type cancelReadCloser struct {
	io.ReadCloser
	cancel func()
}

func (c *cancelReadCloser) Close() error {
	c.cancel()
	return c.ReadCloser.Close()
}

// Compile-time guarantee that Real satisfies the Client interface.
var _ Client = (*Real)(nil)

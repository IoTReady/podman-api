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
	"net"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
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
	ctx      map[string]*connEntry // hostID -> cached connection
	verified map[string]bool       // hostID -> passed the MinPodmanVersion check
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
	// dialing single-flights the dial for r.ctx: one in-flight dial per host,
	// which every other caller for that host waits on instead of starting its
	// own. It exists because the dial no longer runs under r.mu (#277) — see
	// connFor — so nothing else serialises a burst of first-use callers
	// against a cold host. An entry lives only for the duration of one dial:
	// the dial's own goroutine removes it, whether it committed a connection
	// or failed.
	//
	// Exec has no equivalent, deliberately: it dials a connection of its own
	// per call and closes it on the way out (execConnFor, #278), so there is
	// no cache for a second caller to join a dial into.
	dialing map[string]*dialCall

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

// connEntry is one cached libpod connection: the connection-bearing context
// bindings hands back, plus the transport wireInvalidation installed on it.
//
// The transport is recorded here rather than read back off the connection when
// it is needed, because it is not always there to read: podman's exec replaces
// conn.Client.Transport for the whole duration of an exec (attach.go:611), so a
// close that reads the field mid-exec closes podman's throwaway and leaves the
// hooked transport's pooled sockets — and net/http's read and write goroutine
// per socket — alive with nothing left holding a reference to them. Reading it
// there would also race newUpgradeRequest's unsynchronised write. A nil hooked
// means wireInvalidation found no *http.Transport to hook, in which case there
// is nothing of ours to close either.
type connEntry struct {
	ctx    context.Context
	hooked *http.Transport
	// probeTimeouts counts CONSECUTIVE diagnostic timeouts on this connection
	// (probeCtx/verifyCtx); any diagnostic that answers inside its budget
	// resets it to zero. It is the escape hatch described at
	// probeEvictThreshold — the only way a host that receives nothing but
	// diagnostics ever drops a wedged connection.
	probeTimeouts atomic.Int64
}

// connCache is one connection cache and everything that belongs to it: the
// entries themselves, the single-flight map for dials into it, and the
// invalidator that drops a dead entry from it. Bundled so connFor takes one
// argument that cannot be assembled wrongly, rather than three that can.
type connCache struct {
	entries    map[string]*connEntry
	inflight   map[string]*dialCall
	invalidate invalidator
}

// dialCall is one in-flight dial: the placeholder connFor reserves under r.mu
// before dialing off it. done is closed exactly once, by the dialing
// goroutine, after entry/err are set and the reservation has been removed —
// so a waiter that has seen done closed may read both without holding r.mu.
//
// h is the host config this dial was started against, and it is what makes a
// joiner's decision possible: a caller must not queue behind a dial to an
// endpoint the operator has since reconfigured away from. It is written once,
// before the reservation is published, and never again.
type dialCall struct {
	h     config.Host
	done  chan struct{}
	entry *connEntry
	err   error
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
	// execDialed runs with each per-exec connection, just after it is dialed
	// and before the exec runs on it. It exists because that connection is by
	// design published nowhere a test could read it from — no cache, no map —
	// and "each exec gets its own" is otherwise unobservable.
	execDialed func(*connEntry)
}

// NewReal validates host configs and registers them. Connections are not
// opened here; first use opens them.
func NewReal(hosts []config.Host) (*Real, error) {
	r := &Real{
		hosts:    map[string]config.Host{},
		ctx:      map[string]*connEntry{},
		verified: map[string]bool{},
		sshPool:  map[string]*sshPoolEntry{},
		loadavg:  map[string]loadSample{},
		loadGate: map[string]chan struct{}{},
		dialing:  map[string]*dialCall{},
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

	// Connections dropped below are collected rather than closed inline: the
	// same invariant invalidateConn states applies here — nothing else holds a
	// dropped connection, so its pooled sockets and their per-socket goroutines
	// would live for the process lifetime — but closing them is I/O and must
	// not happen under r.mu. The locked section is a closure so the unlock stays
	// a defer: a panic inside it (dropSSHLocked is the only non-trivial call)
	// would otherwise leave r.mu held for the process lifetime, wedging every
	// host operation rather than failing one reload.
	dropped := func() []*connEntry {
		var dropped []*connEntry
		drop := func(id string) {
			if e, ok := r.ctx[id]; ok {
				dropped = append(dropped, e)
				delete(r.ctx, id)
			}
		}

		r.mu.Lock()
		defer r.mu.Unlock()

		// Remove hosts that were deleted.
		for id := range r.hosts {
			if _, keep := newMap[id]; !keep {
				delete(r.hosts, id)
				drop(id)
				delete(r.verified, id)
				delete(r.loadavg, id)
				delete(r.loadGate, id)
				r.dropSSHLocked(id)
			}
		}

		// Add or update hosts.
		for id, h := range newMap {
			if old, exists := r.hosts[id]; exists && !hostConnEq(old, h) {
				// Connection params changed: invalidate cached state. The
				// pooled SSH client and the cached loadavg go too — they
				// describe the old endpoint, and a sample from it must not be
				// served as the new host's.
				drop(id)
				delete(r.verified, id)
				delete(r.loadavg, id)
				r.dropSSHLocked(id)
			}
			r.hosts[id] = h
		}
		return dropped
	}()

	for _, e := range dropped {
		e.closeIdleConns()
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
//
// The caller's context is honoured for the *wait*, and only for the wait: it
// bounds how long this call sits behind an in-flight dial, not the dial
// itself, which bindings gives no way to cancel. See connFor. It used to take
// one and ignore it, which read as if the caller's deadline bounded the dial;
// #274 removed the parameter to make that visible, and #277 made it true
// enough to reintroduce.
func (r *Real) ctxFor(parent context.Context, id string) (context.Context, error) {
	e, err := r.entryFor(parent, id)
	if err != nil {
		return nil, err
	}
	return e.ctx, nil
}

// entryFor is ctxFor for callers that also need the connection itself — the
// ones that can evict it (callCtx) and so must be able to close exactly what
// they dropped.
func (r *Real) entryFor(parent context.Context, id string) (*connEntry, error) {
	return r.connFor(parent, id, connCache{
		entries:    r.ctx,
		inflight:   r.dialing,
		invalidate: r.invalidateConn,
	})
}

// execConnFor dials a connection for the exclusive use of one exec, and hands
// it to a caller that must close it (ContainerExec does, unconditionally).
// Nothing caches it: no map, no single-flight, no invalidator.
//
// A connection per exec, rather than one cached per host, because podman's
// attaching exec is not a guest on the connection it runs over, it takes it
// over. newUpgradeRequest reads and writes conn.Client.Transport
// unsynchronised, replaces it with one of its own for the whole duration of
// the exec (attach.go:611) and never puts it back, stashing the dialed socket
// in a closure variable on the way — podman's own comment there reads "FIXME:
// This is one giant race condition". Two execs sharing a connection can
// therefore cross-wire, one's closeWrite half-closing the other's socket.
//
// #273 answered that with one cached exec connection per host plus a
// turnstile serialising exec against exec on it. That was correct but it
// coupled unrelated instances (#278): the turnstile was per *host*, so one
// instance's pre_backup database dump legitimately holding it for the whole
// callTimeout (10 minutes) blocked the registry blob GC and every other
// instance's pre_backup on that host, and the wait for it carried no deadline
// of its own — with k execs queued the tail waited up to k × callTimeout. An
// exec that owns the transport podman scribbles on needs no turnstile at all,
// so there is no queue and no ceiling to bound.
//
// The cost is one connection setup — for an ssh:// host, a full SSH handshake
// — per exec. That is hook and GC frequency (a pre_backup, a blob GC), not
// request frequency, and it is paid by the exec that incurs it rather than by
// every other exec on the host. Its other half is what closeIdleConns already
// documents: the SSH client underneath an ssh:// connection is not reachable
// through the bindings API, so releasing the connection closes its HTTP
// sockets and leaves the client itself to the garbage collector. Per exec
// rather than per host, that is now a repeated cost — bounded, since Go's
// net.netFD carries a finalizer that closes the socket, but not a deterministic
// one.
//
// The dial runs on its own goroutine for the reason connFor gives: it cannot
// be cancelled, so running it on the caller's stack would make the caller
// uncancellable too. A caller that gives up leaves the dial running, and the
// connection it eventually produces is closed by the dial itself rather than
// stranded — see execDial.
func (r *Real) execConnFor(parent context.Context, id string) (*connEntry, error) {
	r.mu.Lock()
	h, ok := r.hosts[id]
	if !ok {
		r.mu.Unlock()
		return nil, fmt.Errorf("unknown host %q", id)
	}
	uri, err := r.uriForLocked(id)
	r.mu.Unlock()
	if err != nil {
		return nil, err
	}
	d := &execDial{done: make(chan struct{})}
	go r.runExecDial(id, h, uri, d)
	return awaitExecDial(parent, id, d)
}

// execDial is one in-flight per-exec dial, and the handoff of the connection
// it produces. Exactly one of the two sides ends up owning that connection:
// the waiter, which then closes it when its exec finishes, or the dial itself,
// when the waiter gave up first. Neither can be the answer alone — the dial
// cannot be cancelled, so a waiter that times out or is cancelled returns
// before the connection exists, and a connection nobody owns is an SSH client
// and its sockets left alive with nothing holding a reference to them.
//
// settled/abandoned are read and written under mu rather than inferred from
// done, because the two can race: select may pick this caller's timer even
// though done is already closed, so a waiter can reach abandon() after the
// dial has settled and must take the connection over rather than orphan it.
type execDial struct {
	done chan struct{}

	mu        sync.Mutex
	settled   bool
	abandoned bool
	entry     *connEntry
	err       error
}

// settle records the dial's outcome and wakes the waiter. It returns a
// connection the caller must close: non-nil exactly when the waiter has
// already given up, in which case nobody else will ever hold it.
func (d *execDial) settle(e *connEntry, err error) (surplus *connEntry) {
	d.mu.Lock()
	if d.abandoned {
		surplus = e
	} else {
		d.entry, d.err, d.settled = e, err, true
	}
	d.mu.Unlock()
	close(d.done)
	return surplus
}

// abandon marks the waiter as gone. It returns a connection the caller must
// close: non-nil exactly when the dial settled before this ran, so the entry
// it published has no other owner.
func (d *execDial) abandon() (surplus *connEntry) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.settled {
		surplus, d.entry = d.entry, nil
		return surplus
	}
	d.abandoned = true
	return nil
}

// runExecDial performs one per-exec dial and resolves its execDial. Always
// called on its own goroutine.
func (r *Real) runExecDial(id string, h config.Host, uri string, d *execDial) {
	cc, err := dialConn(h, uri)
	var e *connEntry
	if err != nil {
		err = fmt.Errorf("connect to host %q: %w", id, err)
	} else {
		e = &connEntry{ctx: cc}
		// Wired for what it leaves on the entry, not for eviction: there is no
		// cache to evict from, and noExecInvalidation says so. wireInvalidation
		// is still what records e.hooked (which restoreTransport puts back and
		// closeIdleConns closes) and what keeps the dialed socket wrapped so
		// podman's CloseWriter probe still finds a half-closable connection.
		r.wireInvalidation(e, id, noExecInvalidation)
	}
	if surplus := d.settle(e, err); surplus != nil {
		surplus.closeIdleConns()
	}
}

// awaitExecDial waits for a per-exec dial, bounded by connDialTimeout and by
// the caller's own context, whichever comes first — the same budget and the
// same reasoning as awaitDial. Giving up abandons the wait, never the dial;
// what it does do, which awaitDial cannot, is make sure the connection that
// dial produces is closed rather than stranded, since no cache will hold it.
func awaitExecDial(parent context.Context, id string, d *execDial) (*connEntry, error) {
	timer := time.NewTimer(connDialTimeout)
	defer timer.Stop()
	var gaveUp error
	select {
	case <-d.done:
		// Written before done was closed, so both are safe to read here.
		if d.err != nil {
			return nil, d.err
		}
		return d.entry, nil
	case <-timer.C:
		gaveUp = fmt.Errorf("connect to host %q: dial did not complete within %s: %w", id, connDialTimeout, context.DeadlineExceeded)
	case <-parent.Done():
		gaveUp = fmt.Errorf("connect to host %q: %w", id, parent.Err())
	}
	if surplus := d.abandon(); surplus != nil {
		surplus.closeIdleConns()
	}
	return nil, gaveUp
}

// noExecInvalidation is the invalidator a per-exec connection gets: nothing to
// do. Invalidation exists to drop a *cached* connection so later calls stop
// being served over a dead one; a per-exec connection is in no cache and gets
// no later calls, and ContainerExec closes it on every path regardless of how
// it ended.
func noExecInvalidation(string, *connEntry) {}

// connFor is the body of entryFor: return the cached connection from
// c.entries, or dial, wire invalidation, and cache one.
//
// The dial does not run under r.mu (#277). It used to, and the dial has no
// deadline it can be given: bindings.NewConnection's SSH path
// (bindings/connection.go sshClient) calls ssh.Dial with no context, and the
// context NewConnection does take becomes the *connection's* lifetime, which
// must outlive any one request. So a blackholed host — SYN dropped rather than
// refused — parked every operation on *every* host behind r.mu until the
// kernel's TCP timeout. WaitForPodCompletion resolving its connection per poll
// made that a routine exposure rather than a first-use-only one.
//
// The shape is reserve / dial / commit-or-roll-back:
//
//   - Under r.mu: return a cached entry if there is one; join the in-flight
//     dial if there is one; otherwise reserve a placeholder in c.inflight and
//     release the lock. r.mu is held for map operations only, so a caller for
//     a *different* host never waits on this host's dial at all — the point of
//     the whole exercise.
//   - Off the lock, on its own goroutine: the dial. It runs on a goroutine
//     rather than inline precisely because it cannot be cancelled — putting it
//     on the caller's own stack would make the caller uncancellable too.
//   - Back under r.mu: remove the reservation and either publish the entry or
//     record the error, then close done. A failure leaves nothing behind, so
//     the next caller dials again rather than inheriting a poisoned entry.
//
// Every caller — the one that reserved the dial included — then waits in
// awaitDial, bounded by connDialTimeout and by its own context.
//
// A caller only ever joins a dial that matches the host config as it stands
// *now*. The host is therefore resolved before the in-flight check, not after:
// resolving it after meant an unknown host waited out a full connDialTimeout
// on a dial nobody would publish instead of failing immediately, and — the
// case that matters — a host repointed at a working address by a config reload
// stayed unreachable for as long as the dial to the old address lived, which
// on a genuinely blackholed host is the kernel's TCP timeout. That reload is
// the operator's way out of exactly the failure this function exists to bound,
// so it cannot be gated on the failure clearing itself first. A caller finding
// a mismatched dial reserves a fresh one over the top of it; see runDial for
// what happens to the one it displaced.
func (r *Real) connFor(parent context.Context, id string, c connCache) (*connEntry, error) {
	r.mu.Lock()
	if e, ok := c.entries[id]; ok {
		r.mu.Unlock()
		return e, nil
	}
	h, ok := r.hosts[id]
	if !ok {
		r.mu.Unlock()
		return nil, fmt.Errorf("unknown host %q", id)
	}
	if d, ok := c.inflight[id]; ok && hostConnEq(d.h, h) {
		r.mu.Unlock()
		return awaitDial(parent, id, d)
	}
	uri, err := r.uriForLocked(id)
	if err != nil {
		r.mu.Unlock()
		return nil, err
	}
	d := &dialCall{h: h, done: make(chan struct{})}
	// Replaces any reservation for a since-reconfigured endpoint. The dial it
	// displaces keeps running (it cannot be cancelled) and still resolves its
	// own waiters; it simply no longer speaks for this host. runDial's
	// compare-and-delete is what keeps it from clearing this reservation on
	// its way out.
	c.inflight[id] = d
	r.mu.Unlock()

	go r.runDial(id, h, uri, d, c)

	return awaitDial(parent, id, d)
}

// awaitDial waits for an in-flight dial to resolve, bounded by
// connDialTimeout and by the caller's own context, whichever comes first.
//
// The budget is per waiter, armed here, not shared and anchored at the dial's
// start: a caller joining a 29-second-old dial waits a fresh 30 seconds, so
// two callers can span nearly twice the budget between them. That is the
// intended reading — connDialTimeout is how long *this* caller is willing to
// wait for a connection, and a deadline anchored at dial start would instead
// give the last joiner an arbitrarily short one (a few milliseconds, if it
// arrived late enough) and fail it for someone else's slowness.
//
// Giving up here abandons the wait, never the dial: the dial cannot be
// cancelled (see connFor), so it keeps running and will still publish a usable
// connection for whoever asks next if it eventually succeeds. That is also why
// a timed-out waiter does not tear the reservation down — doing so would let
// the next caller start a *second* dial against a host that is already not
// answering, which is how a blackholed host turns one stuck handshake into a
// pile of them.
func awaitDial(parent context.Context, id string, d *dialCall) (*connEntry, error) {
	timer := time.NewTimer(connDialTimeout)
	defer timer.Stop()
	select {
	case <-d.done:
		if d.err != nil {
			return nil, d.err
		}
		return d.entry, nil
	case <-timer.C:
		return nil, fmt.Errorf("connect to host %q: dial did not complete within %s: %w", id, connDialTimeout, context.DeadlineExceeded)
	case <-parent.Done():
		return nil, fmt.Errorf("connect to host %q: %w", id, parent.Err())
	}
}

// dialConn opens one libpod connection to h. Shared by the cached dial
// (runDial) and the per-exec one (runExecDial), which differ in what they do
// with the result, not in how they get it.
//
// The connection is rooted at context.Background() — not any caller's
// per-request context — so it is never cancelled by a request ending.
// Individual operations still honour per-call cancellation because DoRequest
// creates http.NewRequestWithContext(ctx, ...) from the per-call context
// passed into each method (PodInspect, PodList, etc.).
func dialConn(h config.Host, uri string) (context.Context, error) {
	base := context.Background()
	if h.Addr != "unix" && h.SSHKey != "" {
		// SSH host with explicit key file. The fourth arg (`machine`) is false
		// for non-machine connections per the bindings API.
		return bindings.NewConnectionWithIdentity(base, uri, h.SSHKey, false)
	}
	return bindings.NewConnection(base, uri)
}

// runDial performs one reserved dial and resolves its dialCall. Always called
// on its own goroutine, and always the only thing that removes the
// reservation it resolves.
func (r *Real) runDial(id string, h config.Host, uri string, d *dialCall, c connCache) {
	cc, err := dialConn(h, uri)

	var e *connEntry
	if err == nil {
		e = &connEntry{ctx: cc}
		// Wired before publishing, and off the lock: wireInvalidation touches
		// only this connection and this entry, and it closes the transport
		// bindings built, which is I/O.
		r.wireInvalidation(e, id, c.invalidate)
	}

	// surplus is a connection this dial opened but is not going to publish.
	// Nothing else holds it, so any pooled socket of its own — and net/http's
	// read and write goroutine per socket — would live for the process
	// lifetime. In practice its pool is empty by now: bindings pings over the
	// transport it built, and wireInvalidation hooks a clone and closes that
	// original, so this entry's hooked transport has never served a request.
	// Closed anyway, on the same belt-and-braces reasoning as every other
	// close site for an entry nobody will use again. Closing is I/O, so it
	// happens after the unlock — the same rule SetHosts follows.
	var surplus *connEntry

	r.mu.Lock()
	// Compare-and-delete, the same identity check the invalidators make: a
	// dial displaced by connFor (the host was reconfigured mid-dial) must not
	// clear the reservation that replaced it, or the next caller starts a
	// second dial against a host that already has one in flight — precisely
	// the pile-up single-flight exists to prevent.
	if c.inflight[id] == d {
		delete(c.inflight, id)
	}
	switch {
	case err != nil:
		d.err = fmt.Errorf("connect to host %q: %w", id, err)
	case !hostStillMatches(r.hosts, id, h):
		// SetHosts landed while this dial was in flight — the host was removed
		// or re-addressed. Under the old lock-held dial this was impossible;
		// off the lock it is a real interleaving, and publishing here would
		// cache a connection to the endpoint the operator just reconfigured
		// away from and serve every later call over it. Fail this call instead;
		// the next one dials the new endpoint.
		surplus = e
		d.err = fmt.Errorf("connect to host %q: host reconfigured while connecting", id)
	case c.entries[id] != nil:
		// Reachable: a host repointed away and then back again can have two
		// dials outstanding whose configs both match the current one, and
		// either may finish first. The published entry is the one every other
		// caller already has, so it wins and ours is the surplus. Waiters on
		// this call get the live cached connection rather than an error.
		surplus = e
		d.entry = c.entries[id]
	default:
		c.entries[id] = e
		d.entry = e
	}
	r.mu.Unlock()
	close(d.done)

	if surplus != nil {
		surplus.closeIdleConns()
	}
}

// hostStillMatches reports whether id is still registered with the same
// connection parameters it had when a dial for it started.
func hostStillMatches(hosts map[string]config.Host, id string, dialed config.Host) bool {
	cur, ok := hosts[id]
	return ok && hostConnEq(cur, dialed)
}

// wireInvalidation hooks c's underlying HTTP transport so a transport-level
// failure evicts the cached connection instead of being replayed on every
// later call to id forever (#252).
//
// The hook goes *below* the RoundTripper, on the transport's dialer, rather
// than around it. Wrapping conn.Client.Transport in a custom RoundTripper is
// the obvious shape and was the original one, but it panics the daemon (#273):
// podman's newUpgradeRequest — reached by every ExecStartAndAttach, i.e. every
// attaching exec — does
//
//	conn.Client.Transport.(*http.Transport).DialContext
//
// with no comma-ok, so anything but a real *http.Transport there is a panic,
// not an error. Wrapping DialContext keeps the concrete type podman asserts on
// while still seeing every byte of every request: the ~30 operation methods
// route through this one *http.Client, and exec's hijacked connection is
// dialed by this very DialContext, so it is covered too.
//
// A read/write error on the connection is always a transport failure — dial
// refused, connection reset, read timeout on a wedged host — never an
// application error (a 404 or 409 arrives as a normal HTTP response over a
// perfectly healthy socket), so connBroken's classification applies unchanged.
// The complementary wedge signal — a call that hangs until its own callTimeout
// fires, with no error ever surfacing at the socket — is caught by
// opCtxForTimeout instead; see the comment there.
//
// The hook is installed on a *clone*, not on the transport bindings built:
// bindings.NewConnection pings the host as part of connecting, so by the time
// this runs the original transport has already served a request and may hold
// an idle connection with live read/write goroutines — and http.Transport
// documents that its fields must not be modified after first use. The clone
// starts empty, so the original's now-orphaned idle connection is closed
// rather than stranded.
//
// Best-effort: if the bindings package ever changes shape so GetClient, Client
// or the *http.Transport is unavailable here, this silently does nothing
// rather than failing (or panicking) the connection it was meant to protect.
func (r *Real) wireInvalidation(e *connEntry, id string, invalidate invalidator) {
	conn, err := bindings.GetClient(e.ctx)
	if err != nil || conn == nil || conn.Client == nil {
		return
	}
	tr, ok := conn.Client.Transport.(*http.Transport)
	if !ok || tr == nil {
		return
	}
	dial := tr.DialContext
	if dial == nil {
		var d net.Dialer
		dial = d.DialContext
	}
	hooked := tr.Clone()
	// Invalidation runs on its own goroutine, never inline. report is called
	// from net/http's per-connection readLoop/writeLoop, and eviction both
	// takes r.mu and closes the dead connection's idle sockets — I/O on the
	// path of the read loop that is reporting the failure. Since #277 r.mu is
	// no longer held across a dial, so the wait for it is short; the eviction
	// itself is still not the read loop's work to do, and blocking that loop
	// stalls every other request multiplexed onto the same connection. The
	// identity check inside the invalidators already makes a late eviction
	// safe.
	onBroken := func() { go invalidate(id, e) }
	hooked.DialContext = func(ctx context.Context, network, addr string) (net.Conn, error) {
		nc, err := dial(ctx, network, addr)
		if err != nil {
			// A dial that fails is the clearest possible statement that the
			// cached connection is no longer usable: for the SSH transport the
			// dialer rides the multiplexed SSH client, so a refused dial means
			// that client is gone, not merely that one socket was unlucky.
			if connBroken(err) {
				onBroken()
			}
			return nil, err
		}
		return newInvalidatingConn(nc, onBroken), nil
	}
	conn.Client.Transport = hooked
	// Recorded on the entry so every close site has it without reading the
	// field back off the connection; see connEntry.
	e.hooked = hooked
	tr.CloseIdleConnections()
}

// closeWriter is podman's CloseWriter (bindings/containers/attach.go): the
// interface it probes the dialed connection for to half-close STDIN on an
// attached exec. Redeclared rather than imported because it is unexported
// there.
type closeWriter interface {
	CloseWrite() error
}

// newInvalidatingConn wraps nc so its failures reach onBroken, preserving
// whether the connection can half-close. Two shapes, not one interface check
// inside a single type: podman decides with a `conn.(CloseWriter)` assertion,
// so a wrapper that always declares CloseWrite would claim the ability on
// transports that lack it, and one that never declares it would hide the
// ability on transports (the local unix socket, TCP) that have it — silently
// skipping the STDIN half-close that tells an exec's command its input ended.
func newInvalidatingConn(nc net.Conn, onBroken func()) net.Conn {
	c := &invalidatingConn{Conn: nc, onBroken: onBroken}
	if cw, ok := nc.(closeWriter); ok {
		return &invalidatingCloseWriteConn{invalidatingConn: c, cw: cw}
	}
	return c
}

// invalidatingCloseWriteConn is invalidatingConn plus the pass-through
// CloseWrite of an underlying connection that supports one.
type invalidatingCloseWriteConn struct {
	*invalidatingConn
	cw closeWriter
}

func (c *invalidatingCloseWriteConn) CloseWrite() error {
	err := c.cw.CloseWrite()
	c.report(err)
	return err
}

// restoreTransport returns a func that puts e's hooked transport back on the
// connection. podman's newUpgradeRequest does not borrow the
// transport, it replaces it: it builds its own and assigns
// conn.Client.Transport = t permanently (attach.go:611). Without this, the
// exec ends with podman's replacement installed and the transport
// wireInvalidation put there stranded — and it is the hooked one that the
// connection's close (closeIdleConns) knows how to close, so podman's
// replacement keeps whatever keep-alive socket the post-attach ExecInspect
// parked in it, with IdleConnTimeout: 0 so nothing reaps it. This daemon
// execs on every pre_backup hook and every blob GC, so it accumulates for the
// process lifetime. The replacement also silently drops what bindings had
// configured (DisableCompression, among others).
//
// What goes back is e.hooked, recorded when it was installed — not the field
// read back off the connection, for the reason connEntry gives: the field is
// not reliably ours to read (podman writes it unsynchronised, and mid-exec it
// holds podman's throwaway), and restoring a value read from it would cement
// that throwaway — IdleConnTimeout: 0, DisableCompression dropped — as the
// connection's permanent transport while orphaning the hooked one.
//
// Best-effort, like wireInvalidation: an unavailable client, or a nil hooked
// (wireInvalidation found no *http.Transport to install on), means nothing of
// ours to restore, not a failed exec. Nil cannot occur where it would matter —
// a connection whose transport is not an *http.Transport panics in podman's own
// newUpgradeRequest long before this defer runs (#273).
func (r *Real) restoreTransport(e *connEntry) func() {
	if e.hooked == nil {
		return func() {}
	}
	conn, err := bindings.GetClient(e.ctx)
	if err != nil || conn == nil || conn.Client == nil {
		return func() {}
	}
	return func() {
		theirs := conn.Client.Transport
		if theirs == e.hooked {
			return
		}
		if tr, ok := theirs.(*http.Transport); ok {
			tr.CloseIdleConnections()
		}
		conn.Client.Transport = e.hooked
	}
}

// invalidatingConn reports read/write failures on an established connection to
// onBroken. See wireInvalidation.
type invalidatingConn struct {
	net.Conn
	onBroken func()
	closed   atomic.Bool

	// served and writePending are what let report tell an idle-pool close
	// apart from a host that EOFs every response (#276); see reportRead.
	// Plain atomics, not a mutex: net/http drives one connection from two
	// goroutines (readLoop and writeLoop), so Write can run concurrently with
	// a blocked Read, and these are two independent flags rather than one
	// invariant needing a consistent pair of values.
	//
	// The one pairing that has to hold is the suppressing one, and reportRead
	// establishes it by load order rather than by locking: Read stores served
	// before it clears writePending, and reportRead loads writePending before
	// served, so seeing writePending false guarantees the served store is
	// already visible. Every other interleaving falls out on the evicting
	// side, which is the cheap mistake by design (see report).
	served       atomic.Bool // a read has returned response bytes on this conn
	writePending atomic.Bool // a write happened since the last such read
}

func (c *invalidatingConn) Read(b []byte) (int, error) {
	n, err := c.Conn.Read(b)
	if n > 0 {
		c.served.Store(true)
		c.writePending.Store(false)
	}
	c.reportRead(n, err)
	return n, err
}

func (c *invalidatingConn) Write(b []byte) (int, error) {
	// Before the write, not after: writeLoop and readLoop are different
	// goroutines, so a server that answers between Conn.Write returning and
	// the store landing would have readLoop clear writePending first and this
	// store then set it back — leaving a stale "pending" write on a
	// connection that is now idle, whose next idle-pool close would evict
	// spuriously. Setting it first also matches the name: a write is pending
	// from the moment it is attempted. A write that fails leaves it set,
	// which is what we want — that connection carried an unanswered request.
	c.writePending.Store(true)
	n, err := c.Conn.Write(b)
	c.report(err)
	return n, err
}

// reportRead is report plus the one thing report cannot see: whether this EOF
// is the idle-pool close the io.EOF exclusion exists for. A connection taken
// from the idle pool has, by definition, already carried a response and has
// nothing outstanding — so a zero-byte EOF on a conn that has served bytes and
// has no write pending since is that case, and only that case, is suppressed.
//
// The two shapes #276 names fail both halves and now evict: a podman service
// restarting in a loop, or an SSH channel that opens on a half-dead client,
// EOFs a request that is still outstanding (writePending) on a connection that
// never carried a response (not served). The second half is load-bearing on
// its own, because net/http's readLoop Peeks a freshly dialed connection
// before writeLoop has written anything to it — a server that hangs up
// instantly can therefore surface its EOF before the write ever sets
// writePending.
func (c *invalidatingConn) reportRead(n int, err error) {
	if n == 0 && errors.Is(err, io.EOF) && !c.writePending.Load() && c.served.Load() {
		return
	}
	c.report(err)
}

// Close marks the connection as deliberately torn down, so the read error that
// tearing it down provokes is not mistaken for the host breaking. net/http
// closes the underlying connection to cancel an in-flight request — including
// when the *caller* walked away mid-request — and the blocked read then returns
// net.ErrClosed. Evicting on that would punish every abandoned request with a
// redial for everyone else.
func (c *invalidatingConn) Close() error {
	c.closed.Store(true)
	return c.Conn.Close()
}

// report evicts only on errors that say the far end is gone. Where exactly
// that line falls is a judgement, and this is the one place it is drawn:
//
// Evicting when the connection was in fact fine costs one redial and one
// version re-probe — bounded, self-healing, and only that cheap because
// invalidateConn closes the sockets of what it drops. NOT evicting a
// connection that is in fact dead is the #252 incident: every call to that
// host replaying the same failure until someone restarts the daemon. The
// classification is therefore deliberately broad — an ECONNRESET or EPIPE
// from an idle keep-alive close net/http itself recovered from does evict,
// and that is accepted rather than overlooked.
//
// The exclusions are where the cheap mistake stops being cheap. There are two,
// and only the first is applied here:
//
//   - net.ErrClosed after our own Close: net/http closes the connection to
//     cancel an in-flight request, including when the caller walked away.
//     Evicting there would punish every abandoned request.
//   - io.EOF, but only the idle-pool kind: the routine end of a keep-alive
//     connection the server closed while idle, which http.Transport handles
//     by dialing a fresh one. Evicting there would drop a healthy connection
//     on a routine event. report itself cannot tell that apart from an EOF
//     that means the host is gone — it is handed only the error — so the
//     distinction is drawn one layer up, in reportRead, and every EOF that
//     reaches report is already known not to be an idle close. #276 is why:
//     a host that keeps dialing fine but EOFs every response read used to be
//     excluded here too, and #252's replay-forever shape came back for it.
//
// A write failure has no such exception: it is reported as-is, since there is
// no idle-pool equivalent of a write that fails.
//
// The accepted cost of drawing the line at "nothing outstanding" is that a
// response already in progress looks identical to a finished one, because
// every chunk of it is an n > 0 read that clears writePending. That is not
// limited to a rare truncated body: it covers every LONG-LIVED STREAM for its
// whole duration — ContainerLogs in follow mode (opCtxForStream; the UI opens
// one per logs tab) and the hijacked exec connection. A host that dies
// mid-stream therefore suppresses, for as long as that stream was open. For
// exec that is the wanted behaviour, since a normal command ending looks
// exactly the same from here. For follow-logs against a dying podman it is
// #252's shape again, on the call most likely to be open when a host goes
// down — accepted rather than overlooked, because separating the two needs
// response framing this wrapper cannot see. The dial-error path in
// wireInvalidation remains the backstop for the next call.
func (c *invalidatingConn) report(err error) {
	if err == nil || c.closed.Load() {
		return
	}
	if errors.Is(err, net.ErrClosed) {
		return
	}
	if connBroken(err) {
		c.onBroken()
	}
}

// invalidator drops a dead connection from the cache it belongs to.
type invalidator func(id string, dead *connEntry)

// invalidateConn drops the cached libpod connection for id if it is still the
// one that just failed. The identity check (dead is the exact entry connFor
// stored) matters the same way sshPoolEntry.invalidate's does: between this
// failure and the invalidation running, a reload (SetHosts) or a concurrent
// redial may already have replaced the cached connection with a good one, and
// dropping that would turn one transport fault into a reconnect for every
// concurrent caller instead of just the one that actually broke.
// Dropping the map entry is not enough on its own: nothing else holds the
// evicted connection, so its pooled sockets — and net/http's read and write
// goroutine per socket — would stay alive for the process lifetime with no way
// to reach them. Eviction is deliberately trigger-happy (a transport error can
// also come from an idle keep-alive close net/http itself recovered from, and
// dropping a healthy connection is the cheap mistake to make), which only
// holds while a wrong guess costs exactly one redial.
func (r *Real) invalidateConn(id string, dead *connEntry) {
	r.mu.Lock()
	evicted := r.ctx[id] == dead
	if evicted {
		delete(r.ctx, id)
		// The version check is a property of the connection that just died,
		// not of the host — a fresh connection must re-verify rather than
		// inherit a pass recorded against the socket we just evicted.
		delete(r.verified, id)
	}
	r.mu.Unlock()
	if evicted {
		dead.closeIdleConns()
	}
}

// closeIdleConns releases the pooled sockets of a connection nobody will use
// again. The SSH client underneath an ssh:// connection is not reachable
// through the bindings API and is left to the garbage collector.
func (e *connEntry) closeIdleConns() {
	if e.hooked != nil {
		e.hooked.CloseIdleConnections()
	}
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
//
// The probe gets its own verifyTimeout budget rather than the caller's (#275).
// It used to run on e.ctx directly — the raw cached connection context, rooted
// at context.Background() — so against a wedged host a host's *first* use
// after a (re)connect hung here forever, before the caller's own deadline had
// been built and regardless of how tight it was. Its own budget, rather than
// the caller's remaining one, because the caller's
// budget is sometimes far larger than any answer to it is worth waiting for: a
// volume transfer's hours (SetVolumeTransferTimeout) or a blob GC's wait
// (WaitForPodCompletion) would otherwise be spent entirely inside the version
// check. parent is still bridged in, so a caller that gives up first is not
// held here either. Its budget is deliberately NOT probeTimeout's, because
// this failure is hard where a diagnostic's is cheap — see verifyTimeout.
func (r *Real) ensureVerified(parent context.Context, e *connEntry, id string) error {
	r.mu.Lock()
	ok := r.verified[id]
	r.mu.Unlock()
	if ok {
		return nil
	}
	c, cancel := r.verifyCtx(parent, e, id)
	defer cancel()
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

// opCtxFor is entryFor plus the MinPodmanVersion gate. Operation methods must
// call this instead of resolving the connection themselves; diagnostics (Ping,
// Version, HostInfo) skip the gate — via probeCtx, which is bounded the same
// way but ungated — so GET /hosts can still display an unsupported host's
// version (#85).
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
	return r.opCtx(parent, id, callTimeout, r.invalidateConn)
}

// opCtxForTimeout is opCtxFor with an explicit deadline instead of the fixed
// callTimeout, for operations (VolumeExport/VolumeImport, ImagePull) whose data
// volume, not call count, drives how long they legitimately run. See
// SetVolumeTransferTimeout and imagePullTimeout.
//
// Those deadlines carry no invalidation, for the same reason opCtxForStream's
// does not: callCtx evicts on a deadline because callTimeout is sized so that
// reaching it means the call hung. A transfer budget is sized to bytes instead,
// so a large-but-healthy pull or volume copy overrunning it says nothing about
// the connection — and evicting would drop the host's cached connection and its
// verified flag on what is merely a slow link.
func (r *Real) opCtxForTimeout(parent context.Context, id string, timeout time.Duration) (context.Context, context.CancelFunc, error) {
	return r.opCtx(parent, id, timeout, nil)
}

// opCtxForStream is opCtxFor for operations that hold the context open for a
// stream rather than a single request/response: ContainerLogs in follow mode,
// which the UI opens for every logs tab and which stays open for as long as
// someone is watching. There, reaching callTimeout is the routine end of the
// stream, not evidence of a wedge — evicting on it would drop the host's
// cached connection (and its version verification) every callTimeout for as
// long as one logs tab stays open. Everything else about the context is
// identical to opCtxFor's.
func (r *Real) opCtxForStream(parent context.Context, id string) (context.Context, context.CancelFunc, error) {
	return r.opCtx(parent, id, callTimeout, nil)
}

// opCtx is the shared body of the three above: resolve the host's cached
// connection, verify its version once, and derive the per-call context.
func (r *Real) opCtx(parent context.Context, id string, timeout time.Duration, invalidate invalidator) (context.Context, context.CancelFunc, error) {
	c, err := r.entryFor(parent, id)
	if err != nil {
		return nil, nil, err
	}
	if err := r.ensureVerified(parent, c, id); err != nil {
		return nil, nil, err
	}
	ctx, cancel := r.callCtx(parent, c, id, timeout, invalidate)
	return ctx, cancel, nil
}

// callCtx derives a per-call context from base (a cached, background-rooted
// connection context, or a longer-lived budget derived from one), bridging
// parent's cancellation into it. Callers MUST call the returned CancelFunc —
// typically via defer — to release the timer and the AfterFunc registration.
//
// When invalidate is non-nil, the returned CancelFunc carries the second half
// of the #252 invalidation, and the half no socket-level hook can see: a
// wedged connection (the TCP socket accepts writes but the OS never surfaces a
// read error) produces no error at all — the call just hangs until this
// deadline fires. connBroken deliberately does not treat
// context.DeadlineExceeded as broken, because ssh_pool.go's sshSession needs
// the opposite reading: there the ctx is the *caller's* short budget, and
// hitting it says nothing about the connection's health. Here it is different.
// This deadline is a single libpod call's own budget, already generous
// (callTimeout is 10 minutes) precisely so that reaching it means the call
// hung rather than that it was merely slow — the same role sshWedgeGrace's
// watchdog plays for the SSH pool.
//
// A DEADLINE counts as that evidence, whether it was this call's own or the
// parent's; genuine CANCELLATION never does (#282):
//
//   - The caller giving up early — a client disconnecting mid-request, a
//     shutdown — cancels ctx through the AfterFunc bridge, which records
//     context.Canceled. Nothing about the connection follows from someone
//     walking away, so that evicts nothing. A cancelled ancestor reaches this
//     check as Canceled through any number of intervening deadlines, because
//     Go propagates the ancestor's error, not the wrapper's kind.
//   - A parent whose own deadline expired is a different statement: nobody
//     walked away, the host did not answer inside the time the caller had
//     budgeted. That reading is what #282 fixes. The exclusion used to be
//     `parent.Err() == nil`, which collapsed both cases into "caller walked
//     away" — and so made the one caller that structurally touches every
//     registered host, the inventory poller, permanently unable to evict
//     anything: its per-host budget (-inventory-refresh-timeout, 20s) is
//     necessarily far below callTimeout, so against a wedged host the parent
//     always expired first. Every tick, forever. The same was true of every
//     other short-deadlined caller (GET /hosts' 5s perHostTimeout, the UI's
//     host fetch), which is to say of nearly every caller that runs without an
//     operator waiting on it.
//
// Note ctx derives from base, not from parent, so a parent deadline never
// propagates into ctx as anything but the bridge's Canceled — parent is
// therefore examined directly, and anyone re-parenting ctx to parent must
// revisit this.
//
// callCtxOwnDeadlineOnly is the exception, for a parent that is not a caller's
// budget at all; see there.
func (r *Real) callCtx(parent context.Context, base *connEntry, id string, timeout time.Duration, invalidate invalidator) (context.Context, context.CancelFunc) {
	return r.callCtxWith(parent, true, base, id, timeout, invalidate)
}

// callCtxOwnDeadlineOnly is callCtx for a parent whose deadline says nothing
// about the connection, so that only the call's OWN deadline is evidence —
// the pre-#282 reading, kept where it is the correct one.
//
// The single such parent is WaitForPodCompletion's wait budget, which is not a
// caller's patience but a budget sized to how long the POD may legitimately
// run (30 minutes for a blob GC). It expiring means the pod is still running,
// which is compatible with every poll along the way having answered promptly:
// evicting on it would drop a healthy host's connection at the end of any
// wait that timed out. It is the same reasoning that has opCtxForTimeout and
// opCtxForStream pass no invalidator at all — a budget sized to something
// other than the connection's responsiveness cannot testify about it.
func (r *Real) callCtxOwnDeadlineOnly(parent context.Context, base *connEntry, id string, timeout time.Duration, invalidate invalidator) (context.Context, context.CancelFunc) {
	return r.callCtxWith(parent, false, base, id, timeout, invalidate)
}

// callCtxWith is the shared body of the two above. parentDeadlineIsEvidence
// says whether parent's own deadline expiring counts as evidence about the
// connection, alongside this call's deadline, which always does.
func (r *Real) callCtxWith(parent context.Context, parentDeadlineIsEvidence bool, base *connEntry, id string, timeout time.Duration, invalidate invalidator) (context.Context, context.CancelFunc) {
	ctx, cancel := context.WithTimeout(base.ctx, timeout)
	stop := context.AfterFunc(parent, cancel)
	return ctx, func() {
		stop()
		// cancel() cannot mask a deadline that already fired — a context keeps
		// the first error it records — so the check below is safe after it.
		cancel()
		if invalidate == nil {
			return
		}
		if timedOut(ctx, parent, parentDeadlineIsEvidence) {
			invalidate(id, base)
		}
	}
}

// timedOut reports whether the call ended on a deadline rather than on
// cancellation — i.e. whether its ending is evidence that the host did not
// answer. See callCtx for what each case means.
func timedOut(ctx, parent context.Context, parentDeadlineIsEvidence bool) bool {
	// Checked first, and against parent rather than ctx: when a cancelled
	// caller and this call's own deadline race, ctx can record
	// DeadlineExceeded from its own timer before the bridge's cancel lands,
	// and a cancelled caller must not evict either way.
	if errors.Is(parent.Err(), context.Canceled) {
		return false
	}
	if errors.Is(ctx.Err(), context.DeadlineExceeded) {
		return true
	}
	return parentDeadlineIsEvidence && errors.Is(parent.Err(), context.DeadlineExceeded)
}

// connDialTimeout bounds one attempt to open a libpod connection: TCP connect,
// the SSH handshake for an ssh:// host, and the /_ping bindings.NewConnection
// issues as part of connecting.
//
// It has to be a budget of its own because none of the existing ones can serve:
// the context NewConnection takes becomes the *connection's* lifetime (it must
// outlive any one request, so it cannot carry a deadline), and its SSH path
// calls ssh.Dial with no context at all. Left unbounded, a blackholed host
// (SYN dropped rather than refused) rides the kernel's TCP timeout — over two
// minutes on Linux's default retry schedule.
//
// 30s is chosen against the two neighbours it sits between. It is deliberately
// well above ssh_pool.go's sshDialTimeout (5s), which bounds a bare TCP+SSH
// handshake for a `cat` of a /proc file: this dial does that handshake plus an
// HTTP round trip, on a path taken once per host rather than per read, and
// failing it evicts nothing but costs a real operation. It is equally
// deliberately nowhere near callTimeout (10 minutes), which is a whole libpod
// operation's budget — getting a socket is not an operation, and a host that
// has not answered a ping in 30s is not slow, it is gone. It is also longer
// than preflightTimeout (10s) so that boot-time preflight keeps giving up
// first and deferring the version check to first use, exactly as it did
// before, rather than having this fail the dial underneath it.
//
// A caller's own deadline still applies on top: whichever expires first wins.
// var (not const) so tests can shrink it.
var connDialTimeout = 30 * time.Second

// probeCtx is callCtx with the diagnostic budget: bounded by probeTimeout,
// bridged to the caller's cancellation, and evicting only on a sustained
// streak of timeouts rather than on the first one. It is what the calls that
// deliberately bypass opCtxFor use instead.
//
// Bypassing opCtxFor is about the *version gate*, not about deadlines: GET
// /hosts must still be able to display an unsupported host's version (#85), so
// Ping/Version/HostInfo cannot go through ensureVerified. That was never a
// reason for them to run undeadlined on the raw connection context, which is
// what they did until #275.
func (r *Real) probeCtx(parent context.Context, base *connEntry, id string) (context.Context, context.CancelFunc) {
	return r.diagCtx(parent, base, id, probeTimeout)
}

// verifyCtx is probeCtx with the version gate's own, much larger budget. See
// verifyTimeout for why the two are not one constant.
func (r *Real) verifyCtx(parent context.Context, base *connEntry, id string) (context.Context, context.CancelFunc) {
	return r.diagCtx(parent, base, id, verifyTimeout)
}

// diagCtx is the shared body of the two above: a deadlined, caller-cancellable
// context whose timeout feeds the connection's consecutive-timeout streak
// instead of evicting outright, and whose completion inside budget clears it.
func (r *Real) diagCtx(parent context.Context, base *connEntry, id string, timeout time.Duration) (context.Context, context.CancelFunc) {
	ctx, cancel := r.callCtx(parent, base, id, timeout, r.countDiagTimeout)
	return ctx, func() {
		// Checked BEFORE cancel(), which would otherwise be the error we read.
		// A diagnostic that answered inside its budget is direct evidence the
		// connection is alive, which is exactly what retires the streak.
		if ctx.Err() == nil {
			base.probeTimeouts.Store(0)
		}
		cancel()
	}
}

// countDiagTimeout is the invalidator diagCtx hands callCtx: it records one
// more consecutive diagnostic timeout on the connection and evicts only once
// the streak reaches probeEvictThreshold. callCtx has already established the
// two things that make the count meaningful — the deadline that fired was this
// call's own, and the caller had not given up — so a cancelled request never
// contributes.
func (r *Real) countDiagTimeout(id string, e *connEntry) {
	if e.probeTimeouts.Add(1) < probeEvictThreshold {
		return
	}
	r.invalidateConn(id, e)
}

// preflightTimeout bounds each host's boot-time connect+version probe.
// var (not const) so tests can shrink it.
var preflightTimeout = 10 * time.Second

// probeTimeout bounds a *diagnostic* libpod call — Ping, Version and HostInfo
// — as opposed to an operation, which gets callTimeout, or the version gate,
// which gets verifyTimeout (#275).
//
// Where it actually bites is worth being precise about, because the obvious
// answer is wrong. GET /hosts does NOT run on this budget: listHosts already
// wraps each host in its own goroutine under a 5s perHostTimeout
// (internal/api/hosts.go) covering Ping + Version + HostCounts + HostLoad
// together, so that cap always fires first. What #275 fixed for that path is
// not this number at all — it is the context.AfterFunc bridge in callCtx,
// which is what finally makes the existing 5s cap reach libpod instead of
// being ignored by a request pinned to the connection context.
//
// This budget is the backstop for the diagnostic callers that have no cap of
// their own: GET /hosts/{id} (a bare r.Context()), instance.Service's Ping and
// HostInfo callers, and the prune scheduler and handler (internal/prune).
// 10s is preflightTimeout's number because it is the same `info` call asked
// the same reachability question, and because failing a diagnostic is cheap —
// the host reads "unreachable" for one render, or a prune tick skips it, and
// the next call tries again. That cheapness is what separates it from
// verifyTimeout below.
//
// Reaching it once deliberately does NOT evict the cached connection, unlike
// callTimeout (#252/#273): 10s is the caller's short budget — the reading
// ssh_pool.go's sshSession takes of its own ctx — and a merely slow host must
// not have its connection dropped by the cheapest call in the API. Reaching it
// probeEvictThreshold times in a row does, which is a different claim; see
// there.
//
// var (not const) so tests can shrink it.
var probeTimeout = 10 * time.Second

// verifyTimeout bounds ensureVerified's version probe. It is deliberately much
// larger than probeTimeout even though it is literally the same libpod `info`
// call, because the two failures are not the same failure (#281 review).
//
// preflightHost hitting its 10s is SOFT: it logs, defers verification to first
// use, and the host keeps working. ensureVerified hitting its budget is HARD —
// opCtx returns the error, the operation fails, and nothing is cached on
// failure, so the next operation retries and can fail identically. A host
// whose `info` genuinely takes longer than the budget therefore has EVERY
// operation fail until a probe happens to land under it. Before #275 those
// operations succeeded, slowly; a 10s hard cap would have turned "slow" into
// "down" on exactly the hosts most likely to be slow — vedanta carries 14
// Frappe tenants on 12 cores and has been observed at load average 75-116.
//
// 60s buys that host real headroom while still bounding a wedge to a minute
// instead of forever, and it is affordable precisely because it is paid ONCE
// per connection (r.verified is per host, cleared only when the connection is
// evicted) rather than once per GET /hosts render. It sits coherently with its
// neighbours: above connDialTimeout's 30s (#277), since dialing and verifying
// are the two setup costs of a connection and verification is the heavier of
// the two, and an order of magnitude below callTimeout's 10 minutes, since an
// operation may legitimately run for minutes and a version check never may.
//
// Keeping the failure hard rather than making it soft like preflight's is
// deliberate: the gate is a safety check (MinPodmanVersion), and a
// timed-out probe is not evidence that the host passes it.
//
// var (not const) so tests can shrink it.
var verifyTimeout = 60 * time.Second

// probeEvictThreshold is how many CONSECUTIVE diagnostic timeouts on one
// connection evict it (#281 review). It exists because "a genuinely wedged
// connection is still evicted by the first real operation through opCtxFor"
// silently assumes such an operation arrives, and on some hosts none does.
//
// A registered host carrying no instances — drained, newly added, a spare —
// receives only Ping, Version and HostInfo, all of which are non-evicting by
// the decision above, and wireInvalidation's socket hook sees nothing because
// a wedged socket produces no error by definition. Such a host would report
// unreachable forever, including after podman on it recovered, until a reload
// or a restart.
//
// Three, not one, so the property that motivated the no-evict decision
// survives intact: one slow answer still costs nothing, and any diagnostic
// that completes inside its budget resets the streak to zero. That reset is
// the discriminator between "slow" and "wedged" — a host that sometimes
// answers is never evicted no matter how long it stays slow.
//
// What this does NOT amount to is "wedged for 3 x probeTimeout and it goes":
// what fires is whichever budget is shorter, this one or the caller's own.
// Since #282 a caller's deadline expiring counts the same as this one's, so
// the short-budgeted diagnostics do contribute — GET /hosts, whose listHosts
// caps each host at perHostTimeout (5s, internal/api/hosts.go), advances the
// streak once per render and evicts on the third, and its successes still
// reset it. Only a caller that was CANCELLED (a client disconnecting) is a
// no-op in both directions: it neither advances the streak nor resets it.
const probeEvictThreshold = 3

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
// carries verifyTimeout's own much shorter budget (#275) — a first call to a
// newly-added, slow-to-answer host fails there rather than spending this
// transfer-sized budget inside a version check.
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

// Ping reports whether a host answers libpod at all. It bypasses opCtxFor's
// version gate (see probeCtx) but is bounded by probeTimeout and unblocked by
// the caller's cancellation.
func (r *Real) Ping(ctx context.Context, id string) error {
	e, err := r.entryFor(ctx, id)
	if err != nil {
		return err
	}
	c, cancel := r.probeCtx(ctx, e, id)
	defer cancel()
	_, err = system.Info(c, &system.InfoOptions{})
	return err
}

// Version reports a host's podman version. Deliberately ungated so an
// unsupported host's version is still displayable (#85); bounded like Ping.
func (r *Real) Version(ctx context.Context, id string) (string, error) {
	e, err := r.entryFor(ctx, id)
	if err != nil {
		return "", err
	}
	c, cancel := r.probeCtx(ctx, e, id)
	defer cancel()
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
	base, err := r.entryFor(ctx, id)
	if err != nil {
		return 0, err
	}
	if err := r.ensureVerified(ctx, base, id); err != nil {
		return 0, err
	}
	// The wait budget carries no connection of its own — it is rooted at
	// Background, not at base.ctx, so that resolving the connection is entirely
	// the polls' business (below) and this context cannot pin the one the wait
	// happened to start on.
	c, cancel := context.WithTimeout(context.Background(), timeout)
	stop := context.AfterFunc(ctx, cancel)
	defer func() { stop(); cancel() }()

	// Each poll is a single libpod call and gets a single call's budget, with
	// the wedge eviction that comes with it (#252). The wait budget above
	// cannot carry that signal: it expiring means the pod is still running,
	// which says nothing about the connection — a legitimate 30-minute blob-GC
	// wait would otherwise evict a perfectly healthy host on every timeout.
	// The poll context is derived from the connection (so eviction identifies
	// the right connection) and bridged to the wait budget (so a wait that ends
	// tears the poll down at once) — via callCtxOwnDeadlineOnly, so that the
	// wait budget expiring does not read as the host failing to answer the way
	// a caller's deadline does since #282. See callCtxOwnDeadlineOnly.
	//
	// The connection is resolved per poll rather than once up front, for the
	// reason ContainerExec resolves its own only after winning the turnstile: a
	// wait budget is long by design (30 minutes for a blob GC), and an entry
	// resolved before it can be retired during it — by SetHosts when the
	// operator re-addresses the host, or by this wait's own eviction. Holding
	// the first entry would spend the rest of the budget polling the endpoint
	// the operator just reconfigured away from.
	poll := func(fn func(context.Context) error) error {
		base, err := r.entryFor(c, id)
		if err != nil {
			return err
		}
		pctx, done := r.callCtxOwnDeadlineOnly(c, base, id, callTimeout, r.invalidateConn)
		defer done()
		return fn(pctx)
	}

	return waitForCompletion(c, podName,
		func(_ context.Context, name string) ([]string, error) {
			var rep *entities.PodInspectReport
			err := poll(func(ctx context.Context) error {
				var err error
				rep, err = pods.Inspect(ctx, name, &pods.InspectOptions{})
				return err
			})
			if err != nil {
				return nil, err
			}
			ids := make([]string, len(rep.Containers))
			for i, ci := range rep.Containers {
				ids[i] = ci.ID
			}
			return ids, nil
		},
		func(_ context.Context, cid string) (running bool, exitCode int, err error) {
			var full *define.InspectContainerData
			err = poll(func(ctx context.Context) error {
				var err error
				full, err = containers.Inspect(ctx, cid, &containers.InspectOptions{})
				return err
			})
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

// ContainerExec runs cmd in an existing container and returns its exit code
// and combined output.
//
// Two things guard the one property podman's attaching exec does not give us —
// that a call cannot disturb anything but itself. The exec runs on a
// connection dialed for it alone (execConnFor), so it cannot disturb any other
// operation or any other exec; and the transport podman leaves behind is
// closed and replaced afterwards (restoreTransport) before that connection is
// released, so nothing of podman's outlives the call that created it.
//
// There used to be a third: a per-host turnstile serialising exec against exec
// on a single cached exec connection. A connection per exec removes the need
// for it along with the cross-instance coupling it caused — one instance's
// pre_backup dump holding the turnstile for the whole callTimeout blocked
// every other exec on the host (#278).
func (r *Real) ContainerExec(ctx context.Context, id, container string, cmd []string) (ExecResult, error) {
	// Verified over the *primary* connection, not the exec one, and before the
	// exec connection is dialed. The version is a host property that the
	// primary connection establishes anyway, and it is the connection that
	// caches the answer — probing over the exec connection would pay for a
	// second dial before the call has been admitted, on a host that may be too
	// old to run anything at all.
	primary, err := r.entryFor(ctx, id)
	if err != nil {
		return ExecResult{}, err
	}
	if err := r.ensureVerified(ctx, primary, id); err != nil {
		return ExecResult{}, err
	}

	// One connection, this exec's own, closed on the way out whatever happens:
	// success, a failure at any step, a caller that cancelled. Nothing else
	// will ever close it — it is in no cache, so no eviction and no SetHosts
	// reload can reach it — which is why the close is a defer registered
	// immediately after the dial and ahead of everything else.
	conn, err := r.execConnFor(ctx, id)
	if err != nil {
		return ExecResult{}, err
	}
	defer conn.closeIdleConns()
	if r.hooks != nil && r.hooks.execDialed != nil {
		r.hooks.execDialed(conn)
	}

	// No invalidator: there is no cached entry for a deadline to evict, and
	// the connection is released below regardless.
	c, cancel := r.callCtx(ctx, conn, id, callTimeout, nil)
	defer cancel()

	// Registered last, so it runs first: podman's throwaway transport is closed
	// and ours put back before the deferred closeIdleConns above releases the
	// connection, which closes e.hooked and would otherwise leave podman's
	// replacement — and the keep-alive socket ExecInspect parked in it — with
	// nothing holding a reference to them.
	defer r.restoreTransport(conn)()
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
	// Only follow mode opts out of the wedge eviction: it holds the context
	// open for as long as someone is watching, so reaching callTimeout is the
	// routine end of the stream. A tail-N fetch is an ordinary bounded call and
	// keeps the #252 signal — a hang past callTimeout there means the
	// connection is wedged, exactly what opCtxFor's eviction is for.
	opCtx := r.opCtxFor
	if opts.Follow {
		opCtx = r.opCtxForStream
	}
	c, cleanupOpCtx, err := opCtx(ctx, id)
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
	e, err := r.entryFor(ctx, id)
	if err != nil {
		return HostInfo{}, err
	}
	// One probe budget for both libpod calls below, not one each: `info` and
	// `system df` are the same reachability question asked twice, and a host
	// that answered the first is not wedged for the second. The loadavg read
	// is not covered by it — that is the SSH pool's own, separately bounded
	// path, and it takes the caller's ctx directly.
	c, cancel := r.probeCtx(ctx, e, id)
	defer cancel()
	info, err := system.Info(c, &system.InfoOptions{})
	if err != nil {
		return HostInfo{}, err
	}
	out := HostInfo{PodmanVersion: info.Version.Version}
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
// The re-dispatch covers every way a read can lose that race, not just the one
// this branch chose wrongly for. isReloadRace names all three (#266): the host
// became local under us (errHostReconfigured), the pool entry was retired
// mid-dial past what connectFresh's own single re-resolution absorbs
// (errRetiredHost), or the host was gone when the read resolved it
// (errUnknownHost). All three say the same thing — a reload moved the host, not
// that anything is wrong with it — so all three deserve the same second look.
// Recognizing only the first left a double reload aborting a deploy over a
// config edit.
//
// Exactly one re-dispatch, matching connectFresh: a second one means reloads are
// arriving faster than reads complete, and looping there is indistinguishable
// from a hang. A host that is genuinely gone resolves to the same answer on the
// second look and fails there, so self-healing never hides a removal.
func (r *Real) readHostProc(id string, local, remote func() (string, error)) (string, error) {
	for attempt := 0; attempt < 2; attempt++ {
		r.mu.Lock()
		h, ok := r.hosts[id]
		r.mu.Unlock()
		if !ok {
			// Wrapped, not spelled out: this is the same condition sshRun reports
			// from the pool, and callers classify it with isReloadRace.
			return "", fmt.Errorf("%q: %w", id, errUnknownHost)
		}
		if h.Addr == "unix" {
			return local()
		}
		raw, err := remote()
		if isReloadRace(err) && attempt == 0 {
			continue
		}
		return raw, err
	}
	panic("unreachable: every loop body path above returns")
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
	// Three give-up paths below return a bare nil without logging — this one,
	// acquireGate's, and sampleLoadAvg's reload-race branch — deliberately:
	// each is either a benign race or a contended moment, and logging them on
	// a scrape target would be the noise this gating exists to avoid. What
	// they cannot do is add up to a host reporting nothing FOREVER with no
	// signal at all, which is how #258's residue survived; the render layer
	// now reports an absent load.loadavg on the transition instead
	// (api.handlers.logHostViewTransition), so the silence here is bounded by
	// an observation nobody has to remember to add a case to.
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

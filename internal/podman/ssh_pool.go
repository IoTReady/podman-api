package podman

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"sync"
	"time"

	"golang.org/x/crypto/ssh"
	"golang.org/x/crypto/ssh/knownhosts"

	"github.com/iotready/podman-api/internal/config"
)

// sshDialTimeout bounds one TCP+SSH handshake. It is a ceiling on the dial
// alone, not on the whole call: the caller's ctx still bounds everything, and
// whichever expires first wins.
const sshDialTimeout = 5 * time.Second

// sshDialBudgetShare is the most of a caller's remaining time the connect may
// spend, so a cold reconnect always leaves room for the command itself. Without
// it a dial that ran to its own ceiling produced a deadline error from the
// session that followed, reported as "loadavg unavailable" — a self-inflicted
// outage on exactly the hosts slow enough to need a reconnect.
const sshDialBudgetShare = 0.6

// sshWedgeGrace is how long a session that outlived its caller's context is
// given to finish before the connection carrying it is presumed wedged and
// torn down. See sshSession: it is the difference between "our deadline was
// short" and "this transport is never going to answer", which is otherwise
// unobservable — both look like ctx.Err() to the caller.
//
// It is generous relative to the commands this pool runs (single `cat`s of
// /proc files) precisely because being wrong costs one handshake, while not
// checking at all costs a permanently dead host.
//
// Per-entry rather than global (see Real.wedgeGrace) so a test can shorten it
// without mutating shared state under a running watchdog.
const sshWedgeGrace = 10 * time.Second

// acquireGate takes a capacity-1 channel used as a ctx-aware lock, returning a
// release func and true, or false if ctx expired while queued.
//
// Two things in this package single-flight on that idiom — the pool's dial and
// the loadavg read-through — and both need the same property for the same
// reason: a caller that waited out someone else's slow operation and only then
// started its own would have spent its whole budget to arrive where it began.
// One implementation, so a fix to the acquire protocol does not have to be
// reasoned through twice.
func acquireGate(ctx context.Context, gate chan struct{}) (release func(), ok bool) {
	select {
	case gate <- struct{}{}:
		return func() { <-gate }, true
	case <-ctx.Done():
		return nil, false
	}
}

// sshParams are the connection parameters an entry's live client was dialed
// with. A change to any of them (an edited hosts file, a SIGHUP reload) must
// invalidate the pooled client rather than keep talking to the old endpoint —
// the same invariant hostConnEq enforces for the libpod connection cache.
type sshParams struct {
	user string
	addr string
	key  string
}

func sshParamsFor(h config.Host) sshParams {
	user, addr := splitUserHost(h.Addr)
	return sshParams{user: user, addr: addr, key: h.SSHKey}
}

// sshPoolEntry holds one host's live SSH client. An *ssh.Client multiplexes
// sessions over a single connection and is safe for concurrent use, so nothing
// here guards the session itself — only the transitions that replace the
// client.
//
// Two separate guards, deliberately:
//
//   - mu is a plain mutex over the client/params fields. It is never held
//     across I/O, so it can be taken without a context.
//   - gate is a capacity-1 channel used as a ctx-aware lock around the dial.
//     It collapses a burst of concurrent readers on a cold host into one
//     handshake instead of one per caller, while still letting a caller whose
//     own deadline expires while queued give up instead of waiting out an
//     unreachable host's full dial timeout and then starting its own.
type sshPoolEntry struct {
	gate chan struct{}
	// grace is this entry's sshWedgeGrace; fixed at creation so the watchdog
	// never reads a value another goroutine can be writing.
	grace time.Duration

	mu       sync.Mutex
	client   *ssh.Client
	params   sshParams
	dialedAt time.Time
	retired  bool
}

// sshEntryFor returns hostID's pooled entry, creating an empty one on first
// use. Only the map lookup is synchronized on r.mu; the dial happens under the
// entry's own gate, so a slow handshake to one host never blocks calls to
// another.
//
// An unconfigured host is an error rather than a fresh entry: a call that
// raced a SIGHUP removal would otherwise insert an entry for a host SetHosts
// has already swept, and nothing would ever close the connection it then
// dialed — SetHosts only iterates hosts it still knows about.
func (r *Real) sshEntryFor(hostID string) (*sshPoolEntry, config.Host, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	h, ok := r.hosts[hostID]
	if !ok {
		return nil, config.Host{}, fmt.Errorf("%q: %w", hostID, errUnknownHost)
	}
	if r.sshPool == nil {
		r.sshPool = map[string]*sshPoolEntry{}
	}
	e, ok := r.sshPool[hostID]
	if !ok {
		e = &sshPoolEntry{gate: make(chan struct{}, 1), grace: sshWedgeGrace}
		if r.hooks != nil && r.hooks.wedgeGrace > 0 {
			e.grace = r.hooks.wedgeGrace
		}
		r.sshPool[hostID] = e
	}
	return e, h, nil
}

// sshMaxConnAge bounds how long one pooled connection is reused before it is
// retired and redialed.
//
// Reuse is the whole point of the pool, but indefinite reuse means the
// credentials a connection was established with are never re-examined: a key
// rotated (or revoked) at the same path on disk, or a known_hosts edit, is only
// consulted at dial time, so an already-open connection keeps working on the
// strength of a check made arbitrarily long ago. A change to the *configured*
// path is already handled — SetHosts sees it via hostConnEq and retires the
// entry — so this covers the contents changing underneath a stable config.
//
// The same bound quietly helps the half-open case: a connection through a NAT
// mapping that expired while idle is redialed on age rather than waiting to be
// detected by sshSession's wedge grace.
//
// 15 minutes is a compromise: 30 poller ticks at the default interval, so
// reuse still dominates by a wide margin, while the window in which a revoked
// key keeps working stays short enough to be operationally meaningful.
const sshMaxConnAge = 15 * time.Minute

// sshReadCap is the longest one side-channel read may take when its caller
// supplied no deadline of its own. See sshRun: it exists so a wedged
// connection cannot park a caller — and the per-host read gate it holds —
// indefinitely.
const sshReadCap = 15 * time.Second

// current returns the live client if it was dialed with want and is not past
// sshMaxConnAge, else nil.
func (e *sshPoolEntry) current(want sshParams, now time.Time) *ssh.Client {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.client != nil && e.params == want && now.Sub(e.dialedAt) < sshMaxConnAge {
		return e.client
	}
	return nil
}

// errRetiredHost is returned when the host this entry belongs to was
// reconfigured or removed while the caller was connecting to it.
var errRetiredHost = errors.New("host was reconfigured while connecting")

// connect returns the entry's live client, dialing one if the entry is empty
// or was dialed with different parameters.
//
// The retired checks are the SIGHUP race: sshEntryFor hands out the entry
// under r.mu, but the dial happens after that lock is released, so SetHosts
// can drop this entry from r.sshPool while the dial is still in flight. Left
// unchecked, the fresh client would be stored on an entry nothing references
// any more, and nothing would ever close it — one leaked file descriptor per
// host-config edit that races a request to that host, and a connection to an
// endpoint the operator has already moved away from.
func (e *sshPoolEntry) connect(ctx context.Context, h config.Host, now func() time.Time) (*ssh.Client, error) {
	want := sshParamsFor(h)
	if cl := e.current(want, now()); cl != nil {
		return cl, nil
	}
	// Queue for the right to dial, but never past our own deadline: waiting
	// out someone else's 5s dial to an unreachable host and only then starting
	// our own would make an unreachable host's tail latency scale with the
	// number of waiting callers.
	release, ok := acquireGate(ctx, e.gate)
	if !ok {
		return nil, ctx.Err()
	}
	defer release()
	// Re-check, against the clock as it is *now*: waiting on the gate takes
	// real time, and a connection that was fresh when we queued can have
	// crossed sshMaxConnAge by the time we are let through.
	if cl := e.current(want, now()); cl != nil {
		return cl, nil
	}
	e.mu.Lock()
	retired := e.retired
	stale := e.client // stale params, or too old to keep reusing
	e.client = nil
	e.mu.Unlock()
	if stale != nil {
		// Closed outside the lock *and* off the gate — the same reasoning twice
		// over, for two different guards. mu must never be held across I/O or a
		// half-dead socket's Close can stall retire(), which SetHosts calls while
		// holding r.mu, delaying the reload for every other host. The gate is
		// narrower but the shape is identical: it is this host's dial queue, and
		// a caller waiting in it is waiting to dial, not to watch the previous
		// connection be torn down. Nobody needs the close to complete before the
		// redial — the entry no longer references this client, so it is ours
		// alone and closing it is pure cleanup (dropSSHLocked hands its close off
		// the same way).
		go stale.Close()
	}
	if retired {
		// Already gone before we even dialed; don't pay for a handshake whose
		// result we would only throw away below.
		return nil, errRetiredHost
	}

	cl, err := sshDial(ctx, h)
	if err != nil {
		return nil, err
	}
	e.mu.Lock()
	if e.retired {
		// Retired *during* the dial. Ours to close: the entry is no longer in
		// r.sshPool, so retire() cannot have seen this client and never will.
		e.mu.Unlock()
		cl.Close()
		return nil, errRetiredHost
	}
	e.client, e.params, e.dialedAt = cl, want, now()
	e.mu.Unlock()
	return cl, nil
}

// invalidate drops the entry's client if it is still the one the caller was
// using. The identity check matters: between a caller's failed session and its
// retry, another goroutine may already have replaced the dead client with a
// good one, and closing that would turn one host's transient fault into a
// reconnect loop for every concurrent reader.
func (e *sshPoolEntry) invalidate(dead *ssh.Client) {
	e.mu.Lock()
	stale := e.client == dead
	if stale {
		e.client = nil
	}
	e.mu.Unlock()
	if stale {
		dead.Close()
	}
}

// retire marks the entry permanently dead and hands its client (if any) back
// for the caller to close. Marking is the point: a concurrent connect may be
// mid-dial, and after this returns that dial's result belongs to nobody, so
// connect must discard it rather than store it.
func (e *sshPoolEntry) retire() *ssh.Client {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.retired = true
	cl := e.client
	e.client = nil
	return cl
}

// dropSSHLocked retires and forgets hostID's pooled SSH client. Callers must
// hold r.mu (SetHosts does).
//
// Retirement is synchronous under r.mu — safe because e.mu is never held
// across I/O — so no in-flight dial can slip a client in after the entry
// leaves the pool. Only the actual Close, which touches a socket, is handed
// to a goroutine.
func (r *Real) dropSSHLocked(hostID string) {
	if e, ok := r.sshPool[hostID]; ok {
		if cl := e.retire(); cl != nil {
			go cl.Close()
		}
		delete(r.sshPool, hostID)
	}
}

// sshDial opens one authenticated SSH connection to h, honouring ctx for both
// the TCP connect and the SSH handshake.
//
// ctx-awareness is the point: ssh.Dial's own cfg.Timeout is not a context, so
// a caller whose deadline has already passed used to pay a full handshake for
// a result guaranteed to be discarded — and, because the read is synchronous,
// the caller's "deadline" could be overrun by that whole handshake (#258).
func sshDial(ctx context.Context, h config.Host) (*ssh.Client, error) {
	user, addr := splitUserHost(h.Addr)
	auth := []ssh.AuthMethod{}
	if h.SSHKey != "" {
		key, err := os.ReadFile(h.SSHKey)
		if err != nil {
			return nil, err
		}
		signer, err := ssh.ParsePrivateKey(key)
		if err != nil {
			return nil, err
		}
		auth = append(auth, ssh.PublicKeys(signer))
	}
	hostKeyCb, err := knownhosts.New(filepath.Join(os.Getenv("HOME"), ".ssh", "known_hosts"))
	if err != nil {
		return nil, err
	}
	// No Timeout field: it is only consulted by ssh.Dial, and this dials
	// through net.Dialer + NewClientConn so the bound can be shared between the
	// connect and the handshake. Setting it would be configuration that looks
	// live and does nothing.
	cfg := &ssh.ClientConfig{
		User:            user,
		Auth:            auth,
		HostKeyCallback: hostKeyCb,
	}

	// A connect that is allowed to consume the caller's whole budget leaves
	// nothing for the command it was opened to run — and the caller's budget
	// and sshDialTimeout are both 5s on the poller's path, so "dial took its
	// full allowance" and "the read had no time at all" were the same event.
	// The dial gets a share; the rest is the session's.
	budget := sshDialTimeout
	if dl, ok := ctx.Deadline(); ok {
		if share := time.Duration(float64(time.Until(dl)) * sshDialBudgetShare); share < budget {
			budget = share
		}
	}
	if budget <= 0 {
		return nil, context.DeadlineExceeded
	}

	// One instant, shared by the TCP connect and the handshake that follows.
	// Giving the handshake a fresh copy of the budget lets the two together
	// spend twice the share the split was there to enforce.
	deadline := time.Now().Add(budget)

	d := net.Dialer{Deadline: deadline}
	nc, err := d.DialContext(ctx, "tcp", addr)
	if err != nil {
		return nil, err
	}
	// The handshake happens on a raw conn that DialContext no longer watches,
	// so bound it explicitly, by what is left of the same share.
	_ = nc.SetDeadline(deadline)

	// A deadline is not a context: it does nothing for a ctx that is
	// cancel-only (getHost passes r.Context(), which cancels when the client
	// disconnects and carries no deadline of its own). Without this, a
	// cancelled request still pays the full handshake.
	//
	// context.AfterFunc, not a hand-rolled goroutine and flag, for the
	// handover: stop() reports whether the closer was cancelled *before it
	// started*, which is the question that actually matters here. A
	// mutex-guarded bool can only answer "are we both in the critical section
	// at once", and cannot stop the closer from firing between a successful
	// handshake and the flag being set — which would hand the pool a client
	// wrapping an already-closed connection.
	stopCloser := context.AfterFunc(ctx, func() { nc.Close() })
	c, chans, reqs, err := ssh.NewClientConn(nc, addr, cfg)
	if err != nil {
		stopCloser()
		nc.Close()
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		return nil, err
	}
	if !stopCloser() {
		// Cancellation won the race: the connection is closed, or about to be,
		// by a closer we can no longer call off. Whatever the handshake just
		// produced is unusable.
		c.Close()
		return nil, ctx.Err()
	}
	// Clear it again before the connection is pooled. A deadline left in place
	// would fire on the *pooled* conn once the dialing caller's request budget
	// elapsed, killing it mid-session for some later, unrelated caller.
	_ = nc.SetDeadline(time.Time{})
	return ssh.NewClient(c, chans, reqs), nil
}

// sshRun executes cmd on hostID over the pooled connection and returns its
// stdout together with the host config the read actually ran against.
//
// Returning the host is not a convenience: it is the only way a caller can
// judge whether its result is still current. sshRun resolves the host itself
// (see below), so a caller comparing against its own earlier snapshot would
// discard perfectly good samples whenever a reload landed between the two
// lookups.
//
// A pooled connection can be closed under us at any time — an idle NAT
// mapping, a restarted sshd, a rebooted host — and the failure surfaces only
// when we try to use it. So a session failure that is not the remote command's
// own non-zero exit costs one reconnect and one retry, which is what makes
// reuse safe to prefer over the old dial-per-read.
func (r *Real) sshRun(ctx context.Context, hostID, cmd string) (string, config.Host, error) {
	// The host is resolved here, in the same critical section as the pool
	// entry, rather than taken from the caller. A caller that read its copy
	// before a SIGHUP reload and arrived here after it would otherwise dial
	// the endpoint the operator has just moved away from, and — because
	// SetHosts has already swept the old entry — pool that connection under a
	// freshly created one, so the wrong endpoint answers this call rather than
	// erroring. The loadavg path catches that at commit time; uptime and port
	// reads have no commit to catch it at.
	// A backstop, not a budget. Callers are supposed to bound these reads, but
	// getHost passes the request context straight through with no deadline of
	// its own — so a connection that wedges under such a request leaves this
	// goroutine parked forever holding the per-host read gate, and every later
	// request and poller tick for that host queues behind it and gives up.
	// Loadavg for that host then goes stale permanently, and silently, because
	// a tick that never sampled reports no outcome.
	//
	// Nothing legitimate takes this long: it is a single `cat` of a /proc file
	// over a connection that is usually already open. WithTimeout keeps the
	// earlier of this and any deadline the caller did set, so a caller with a
	// tighter budget is unaffected.
	cap := sshReadCap
	if r.hooks != nil && r.hooks.readCap > 0 {
		cap = r.hooks.readCap
	}
	ctx, cancel := context.WithTimeout(ctx, cap)
	defer cancel()

	e, h, cl, err := r.connectFresh(ctx, hostID)
	if err != nil {
		return "", h, err
	}
	out, err := sshSession(ctx, e, cl, cmd)
	if err == nil || !connBroken(err) {
		return out, h, err
	}
	e.invalidate(cl)
	e, h, cl, rerr := r.connectFresh(ctx, hostID)
	if rerr != nil {
		if isReloadRace(rerr) {
			// Not noise to be discarded in favour of the original: it says the
			// host was reconfigured, which is the whole story, and callers
			// classify on it. Reporting the first session's plain transport
			// error instead would have this logged as an outage for a host
			// that was simply moved.
			return "", h, rerr
		}
		return "", h, err // an ordinary reconnect failure; the original is more informative
	}
	out, err = sshSession(ctx, e, cl, cmd)
	if err != nil && connBroken(err) {
		// The reconnect handshaked, but the session broke the same way: the
		// replacement is no better than what it replaced. Evict it too. The
		// command is not attempted a third time — but leaving the known-bad
		// client pooled would make every call during a flap pay two round trips
		// to rediscover what this one already knows, which is the opposite of
		// what the retry is for.
		//
		// "One retry" counts attempts at the command, not handshakes:
		// connectFresh may re-resolve once per call if a reload retires the
		// entry under it, so a call that races a reload can dial more than
		// twice. That is bounded (2 command attempts × 2 resolutions) and needs
		// a SIGHUP per extra dial, so it is not a path a flapping host can loop
		// on by itself.
		e.invalidate(cl)
	}
	return out, h, err
}

// connectFresh resolves the host and opens (or reuses) its pooled connection,
// re-resolving once if the entry it got was retired under it.
//
// A retired entry does not mean failure; it means a reload happened and this
// caller is holding the previous answer. Looking again yields the new entry
// and the new endpoint, so a read that races a reload completes against the
// host's current config instead of failing. That matters beyond loadavg, which
// can afford to skip a cycle: HostBoundPorts feeds a deploy's port-conflict
// precheck, and every error there aborts the deploy — so without this, a
// SIGHUP landing at the wrong moment would fail a deploy that had nothing
// wrong with it.
//
// Exactly one re-resolution: a second retirement means reloads are arriving
// faster than reads complete, and looping on that would be indistinguishable
// from a hang.
func (r *Real) connectFresh(ctx context.Context, hostID string) (*sshPoolEntry, config.Host, *ssh.Client, error) {
	var h config.Host
	for attempt := 0; attempt < 2; attempt++ {
		var e *sshPoolEntry
		var err error
		e, h, err = r.sshEntryFor(hostID)
		if err != nil {
			return nil, h, nil, err
		}
		if r.hooks != nil && r.hooks.beforeConnect != nil {
			r.hooks.beforeConnect()
		}
		if h.Addr == "unix" {
			// The caller chose the SSH branch from a snapshot that a reload has
			// since replaced with a local host. Say so plainly rather than
			// dialing "unix:22", which is what splitUserHost would produce.
			return nil, h, nil, fmt.Errorf("host %q is local: %w", hostID, errHostReconfigured)
		}
		cl, err := e.connect(ctx, h, r.now)
		if err == nil {
			return e, h, cl, nil
		}
		if errors.Is(err, errRetiredHost) && attempt == 0 {
			continue
		}
		return nil, h, nil, err
	}
	panic("unreachable: every loop body path above returns")
}

// sshSession runs one command on an established client, bounded by ctx.
//
// Everything that talks to the connection — opening the channel as well as
// running the command — happens in the goroutine, because both block. Channel
// open is a round trip too, and ssh.Client.NewSession takes no context: called
// inline, it would hang forever on a wedged connection and ctx would never be
// consulted at all, which is a worse version of the bug this change fixes.
//
// On ctx expiry the client is deliberately left in the pool: it is shared and
// outlives this call. But "the caller gave up" and "this connection is wedged"
// are indistinguishable at that moment — a half-open TCP connection (the idle
// NAT mapping this pool exists to survive) answers nothing at all, so a
// deadline is the *only* symptom it ever produces. Left at that, a wedged
// client would sit in the pool forever, failing every later call with a ctx
// error that connBroken deliberately does not treat as a transport fault,
// and pinning this goroutine on a call that never returns.
//
// So the session gets sshWedgeGrace to finish after we stop waiting. If it
// does, the connection was fine and stays pooled. If it doesn't, the client is
// invalidated and closed — which both frees the next caller to dial a working
// one and unblocks the pinned goroutine.
func sshSession(ctx context.Context, e *sshPoolEntry, cl *ssh.Client, cmd string) (string, error) {
	type result struct {
		out []byte
		err error
	}
	ch := make(chan result, 1) // buffered: a late result must never block
	go func() {
		sess, err := cl.NewSession()
		if err != nil {
			ch <- result{nil, err}
			return
		}
		defer sess.Close()
		out, err := sess.Output(cmd)
		ch <- result{out, err}
	}()
	select {
	case <-ctx.Done():
		go func() {
			// A stoppable timer, not time.After: the common case is the
			// session finishing promptly, and an unstopped timer stays live
			// (and its goroutine's memory reachable) for the whole grace.
			t := time.NewTimer(e.grace)
			defer t.Stop()
			select {
			case res := <-ch:
				// Finished in time, so the transport is not wedged — but it may
				// still have broken. Nobody is left to act on that error, and
				// leaving the client pooled makes the next caller rediscover it
				// at the cost of its own round trip.
				if res.err != nil && connBroken(res.err) {
					e.invalidate(cl)
				}
			case <-t.C:
				e.invalidate(cl)
			}
		}()
		return "", ctx.Err()
	case res := <-ch:
		if res.err != nil {
			return "", res.err
		}
		return string(res.out), nil
	}
}

// connBroken reports whether err means the connection itself is unusable,
// as opposed to the remote command having failed on a perfectly good one.
// Only the former is worth a reconnect: retrying a command that exited 1
// (say, `cat` on a file the host does not have) just fails twice as slowly.
//
// Shared beyond this pool: real.go's libpod connection cache (ctxFor) uses it
// too, via the same http.RoundTripper failure shape — a RoundTrip error is
// always a transport failure (dial/write/read), never a decoded HTTP
// response, so the same "everything but our own cancellation/deadline is the
// transport" rule applies without change (#252).
func connBroken(err error) bool {
	if err == nil {
		return false
	}
	// The command ran and exited non-zero: the connection carried it fine.
	var exitErr *ssh.ExitError
	if errors.As(err, &exitErr) {
		return false
	}
	// Our own deadline, not (necessarily) the connection's health — and
	// reconnecting under an expired ctx would only fail again at the dial.
	// Whether the connection was actually wedged is decided separately, and
	// asynchronously, by sshSession's grace period.
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return false
	}
	// Everything else — EOF, closed-network-connection, a session that never
	// reported an exit status — is the transport, and worth one retry.
	return true
}

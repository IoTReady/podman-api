package podman

import (
	"sync"
	"time"
)

// execConnMaxAge bounds how long one exec connection may be offered to a
// later exec before it is retired instead of reused, mirroring
// sshMaxConnAge's reasoning in ssh_pool.go: reuse trades away a freshness
// guarantee (a rotated or revoked SSH key keeps working on an already-open
// connection until this expires), so the window is kept short relative to
// the bursts it exists to amortize — a live-backup's pre/backup/post-exec
// sequence runs its execs seconds apart, not minutes.
//
// Deliberately much shorter than sshMaxConnAge: that pool is read-only
// traffic to /proc on hosts we manage end to end, while exec is arbitrary
// commands inside a customer's container, so the same credential-rotation
// exposure is worth less time here.
const execConnMaxAge = 2 * time.Minute

// execPoolEntry is one host's single, opportunistically-reused exec
// connection.
//
// It is deliberately not a full pool: one slot per host, taken and cleared
// atomically by takeExecConn so at most one caller ever holds it, and never
// waited on — a caller that finds the slot empty or stale dials its own
// exactly as execConnFor always has. That is what keeps this safe against
// #278: nothing here can make one exec block behind another's callTimeout,
// which is the coupling #278 removed the turnstile to avoid. What this adds
// back is reuse ACROSS TIME for the common case (one job's several execs, a
// few seconds apart, on an otherwise idle host), not reuse AT THE SAME TIME.
type execPoolEntry struct {
	mu       sync.Mutex
	conn     *connEntry
	dialedAt time.Time
}

// execPoolEntryFor returns hostID's slot, creating an empty one on first use.
func (r *Real) execPoolEntryFor(hostID string) *execPoolEntry {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.execPool == nil {
		r.execPool = map[string]*execPoolEntry{}
	}
	e, ok := r.execPool[hostID]
	if !ok {
		e = &execPoolEntry{}
		r.execPool[hostID] = e
	}
	return e
}

// takeExecConn returns hostID's pooled exec connection if one is present and
// still within execConnMaxAge, clearing the slot so no other caller can be
// handed the same connection concurrently. Returns nil — meaning "dial your
// own", execConnFor's original behavior — when the slot is empty, was
// already taken, or has aged out.
//
// A stale entry found here is closed rather than left for a future release
// to notice, since nothing else will look at this slot again until another
// exec dials fresh and offers its own connection.
func (r *Real) takeExecConn(hostID string, now time.Time) *connEntry {
	e := r.execPoolEntryFor(hostID)
	e.mu.Lock()
	conn := e.conn
	stale := conn != nil && now.Sub(e.dialedAt) >= execConnMaxAge
	e.conn = nil // taken (or evicted) either way; the slot is empty now
	e.mu.Unlock()
	if stale {
		conn.closeIdleConns()
		return nil
	}
	return conn
}

// putExecConn returns conn to hostID's reuse slot for a later exec, or closes
// what we can of it. keep must be false for anything but a clean exec —
// ContainerExec passes err == nil, the same posture connBroken already takes
// toward the primary connection cache: a connection that misbehaved once is
// not worth the risk of handing to an unrelated exec next.
//
// restoreTransport has already run by the time ContainerExec's deferred call
// to this reaches here (see the ordering comment there), so a kept
// connection is guaranteed to have wireInvalidation's transport back in
// place — a prerequisite this relies on rather than re-checks.
func (r *Real) putExecConn(hostID string, conn *connEntry, keep bool, now time.Time) {
	if !keep || now.Sub(conn.execDialedAt) >= execConnMaxAge {
		conn.closeIdleConns()
		return
	}
	e := r.execPoolEntryFor(hostID)
	e.mu.Lock()
	displaced := e.conn
	e.conn, e.dialedAt = conn, conn.execDialedAt
	e.mu.Unlock()
	if displaced != nil {
		// Another exec published first while this one was still running —
		// there is one slot, not a queue, so the earlier offer loses.
		displaced.closeIdleConns()
	}
}

// dropExecPoolLocked clears and closes hostID's pooled exec connection, if
// any. Callers must hold r.mu (SetHosts does): a host removed or
// reconfigured must not leave a stale connection offered to the next exec
// against what is now the wrong endpoint, or no endpoint at all.
//
// Closing is deferred to a goroutine, the same reasoning dropSSHLocked
// documents: it is I/O, and r.mu must never be held across it.
func (r *Real) dropExecPoolLocked(hostID string) {
	e, ok := r.execPool[hostID]
	if !ok {
		return
	}
	e.mu.Lock()
	conn := e.conn
	e.conn = nil
	e.mu.Unlock()
	if conn != nil {
		go conn.closeIdleConns()
	}
}

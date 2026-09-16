package podman

import (
	"testing"
	"time"

	"github.com/iotready/podman-api/internal/config"
)

func newExecPoolTestReal(t *testing.T, clk *fakeClock) *Real {
	t.Helper()
	r, err := NewReal([]config.Host{{ID: "h1", Addr: "user@h1", Socket: "/run/podman.sock"}})
	if err != nil {
		t.Fatalf("NewReal: %v", err)
	}
	r.hooks = &testHooks{now: clk.now}
	return r
}

// #307: a connection offered back by one exec must be handed to the very
// next exec on that host instead of every exec paying its own SSH handshake.
func TestExecPool_PutThenTake_Reuses(t *testing.T) {
	clk := newFakeClock()
	r := newExecPoolTestReal(t, clk)
	conn := &connEntry{execDialedAt: clk.now()}

	r.putExecConn("h1", conn, true, clk.now())

	got := r.takeExecConn("h1", clk.now())
	if got != conn {
		t.Fatalf("takeExecConn did not return the offered connection")
	}
}

// The slot holds at most one connection: a second taker must dial its own
// rather than be handed the same connection a concurrent exec is still
// using — this is what keeps #278's cross-instance coupling from recurring.
func TestExecPool_Take_ClearsTheSlot(t *testing.T) {
	clk := newFakeClock()
	r := newExecPoolTestReal(t, clk)
	conn := &connEntry{execDialedAt: clk.now()}
	r.putExecConn("h1", conn, true, clk.now())

	if got := r.takeExecConn("h1", clk.now()); got != conn {
		t.Fatalf("first take: got %v, want the pooled connection", got)
	}
	if got := r.takeExecConn("h1", clk.now()); got != nil {
		t.Fatalf("second take: got %v, want nil (slot already emptied)", got)
	}
}

// keep=false is ContainerExec's signal that the exec did not complete
// cleanly — connBroken's posture toward the primary connection cache,
// applied here: don't hand a connection that just misbehaved to an
// unrelated exec next.
func TestExecPool_Put_DiscardsOnFailure(t *testing.T) {
	clk := newFakeClock()
	r := newExecPoolTestReal(t, clk)
	conn := &connEntry{execDialedAt: clk.now()}

	r.putExecConn("h1", conn, false, clk.now())

	if got := r.takeExecConn("h1", clk.now()); got != nil {
		t.Fatalf("got %v, want nil: a failed exec's connection must not be reused", got)
	}
}

// execConnMaxAge bounds reuse the same way sshMaxConnAge bounds the /proc
// side-channel pool: a connection old enough is evicted rather than handed
// out, so a rotated SSH key does not keep working indefinitely on an
// already-open connection.
func TestExecPool_Take_EvictsPastMaxAge(t *testing.T) {
	clk := newFakeClock()
	r := newExecPoolTestReal(t, clk)
	conn := &connEntry{execDialedAt: clk.now()}
	r.putExecConn("h1", conn, true, clk.now())

	clk.advance(execConnMaxAge)

	if got := r.takeExecConn("h1", clk.now()); got != nil {
		t.Fatalf("got %v, want nil: a connection past execConnMaxAge must not be reused", got)
	}
}

// A connection just under the age limit is still good — the boundary is
// exclusive on the "still usable" side, mirroring sshPoolEntry.current.
func TestExecPool_Take_ReusesJustUnderMaxAge(t *testing.T) {
	clk := newFakeClock()
	r := newExecPoolTestReal(t, clk)
	conn := &connEntry{execDialedAt: clk.now()}
	r.putExecConn("h1", conn, true, clk.now())

	clk.advance(execConnMaxAge - time.Second)

	if got := r.takeExecConn("h1", clk.now()); got != conn {
		t.Fatalf("got %v, want the pooled connection (still within max age)", got)
	}
}

// A connection reused across several put/take cycles ages out from its
// ORIGINAL dial, not from its last release — otherwise a busy host could
// keep one SSH connection alive indefinitely by exec-ing on it faster than
// execConnMaxAge, defeating the bound entirely.
func TestExecPool_MaxAge_MeasuredFromOriginalDial(t *testing.T) {
	clk := newFakeClock()
	r := newExecPoolTestReal(t, clk)
	conn := &connEntry{execDialedAt: clk.now()}

	r.putExecConn("h1", conn, true, clk.now())
	clk.advance(execConnMaxAge / 2)
	got := r.takeExecConn("h1", clk.now())
	if got != conn {
		t.Fatalf("mid-window take: got %v, want the pooled connection", got)
	}
	r.putExecConn("h1", got, true, clk.now())

	clk.advance(execConnMaxAge/2 + time.Second) // past execConnMaxAge from the original dial

	if got := r.takeExecConn("h1", clk.now()); got != nil {
		t.Fatalf("got %v, want nil: reuse must not extend the connection's life past execConnMaxAge", got)
	}
}

// Putting a second connection while the slot is already occupied must not
// leak the one it displaces — there is one slot, not a queue, so the
// earlier offer is the one that loses.
func TestExecPool_Put_DisplacesWithoutLeaking(t *testing.T) {
	clk := newFakeClock()
	r := newExecPoolTestReal(t, clk)
	first := &connEntry{execDialedAt: clk.now()}
	second := &connEntry{execDialedAt: clk.now()}

	r.putExecConn("h1", first, true, clk.now())
	r.putExecConn("h1", second, true, clk.now())

	got := r.takeExecConn("h1", clk.now())
	if got != second {
		t.Fatalf("got %v, want the most recently offered connection", got)
	}
	if got := r.takeExecConn("h1", clk.now()); got != nil {
		t.Fatalf("got %v, want nil: only one connection should have been held", got)
	}
}

// SetHosts removing or reconfiguring a host must not leave a pooled exec
// connection offered against an endpoint that is now wrong, or gone.
func TestExecPool_SetHosts_DropsPooledConnOnRemoval(t *testing.T) {
	clk := newFakeClock()
	r := newExecPoolTestReal(t, clk)
	conn := &connEntry{execDialedAt: clk.now()}
	r.putExecConn("h1", conn, true, clk.now())

	r.SetHosts(nil) // h1 removed

	if got := r.takeExecConn("h1", clk.now()); got != nil {
		t.Fatalf("got %v, want nil: SetHosts must drop the pooled exec connection for a removed host", got)
	}
}

func TestExecPool_SetHosts_DropsPooledConnOnReconfigure(t *testing.T) {
	clk := newFakeClock()
	r := newExecPoolTestReal(t, clk)
	conn := &connEntry{execDialedAt: clk.now()}
	r.putExecConn("h1", conn, true, clk.now())

	r.SetHosts([]config.Host{{ID: "h1", Addr: "user@h1-new", Socket: "/run/podman.sock"}})

	if got := r.takeExecConn("h1", clk.now()); got != nil {
		t.Fatalf("got %v, want nil: SetHosts must drop the pooled exec connection when the host's endpoint changes", got)
	}
}

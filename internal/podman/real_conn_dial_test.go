package podman

import (
	"context"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/iotready/podman-api/internal/config"
)

// hangingLibpodServer listens on a unix socket, accepts every connection and
// then answers nothing at all. It is the in-process stand-in for the failure
// #277 is about: a blackholed host, where the connect itself completes (or the
// SYN is simply dropped) and the dial then sits there until the kernel gives
// up. bindings.NewConnection pings as part of connecting, so a dial against
// this listener blocks in exactly the place the production one does.
func hangingLibpodServer(t *testing.T) string {
	t.Helper()
	sock := filepath.Join(t.TempDir(), "podman.sock")
	ln, err := net.Listen("unix", sock)
	require.NoError(t, err)

	var mu sync.Mutex
	var held []net.Conn
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			mu.Lock()
			held = append(held, c)
			mu.Unlock()
		}
	}()
	t.Cleanup(func() {
		_ = ln.Close()
		mu.Lock()
		for _, c := range held {
			_ = c.Close()
		}
		mu.Unlock()
		_ = os.Remove(sock)
	})
	return sock
}

// libpodServerAt is fakeLibpodServer for a caller that needs the socket at a
// path it chose — a test that has to observe the "no listener yet" failure
// before the server exists.
func libpodServerAt(t *testing.T, sockPath string) {
	t.Helper()
	raw, err := net.Listen("unix", sockPath)
	require.NoError(t, err)
	mux := http.NewServeMux()
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Libpod-API-Version", "5.8.2")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{}`))
	})
	srv := &http.Server{Handler: mux}
	go func() { _ = srv.Serve(raw) }()
	t.Cleanup(func() {
		_ = srv.Close()
		_ = os.Remove(sockPath)
	})
}

// mustNotBlock runs fn on its own goroutine and fails the test if it has not
// returned within d. Every assertion in this file is about something NOT
// blocking, so none of them may be written as a straight call: before the fix
// they would hang the whole package rather than fail. fn must only record its
// result — asserting inside it would panic the process from an abandoned
// goroutine once the test it belongs to has already failed and finished.
//
// Not runWithin: real_prune_integration_test.go already has one of those with
// a different signature. It is //go:build integration, so a plain `go vet`
// never compiles it and the clash only surfaces in the integration-tagged vet.
func mustNotBlock(t *testing.T, d time.Duration, what string, fn func()) {
	t.Helper()
	done := make(chan struct{})
	go func() {
		defer close(done)
		fn()
	}()
	select {
	case <-done:
	case <-time.After(d):
		t.Fatalf("%s did not return within %s", what, d)
	}
}

// TestRealClient_HangingDialDoesNotBlockOtherHosts is #277 itself: connFor
// dialed under r.mu, so one blackholed host parked every operation on every
// other host behind that mutex until the kernel's TCP timeout.
func TestRealClient_HangingDialDoesNotBlockOtherHosts(t *testing.T) {
	hung := hangingLibpodServer(t)
	good, _ := fakeLibpodServer(t)
	c, err := NewReal([]config.Host{
		{ID: "hung", Addr: "unix", Socket: hung},
		{ID: "good", Addr: "unix", Socket: good},
	})
	require.NoError(t, err)

	go func() { _, _ = c.HostInfo(context.Background(), "hung") }()
	// Give the hung dial time to be in flight; without the fix it is holding
	// r.mu by now.
	time.Sleep(100 * time.Millisecond)

	var perr error
	mustNotBlock(t, 2*time.Second, "an operation on a healthy host", func() {
		perr = c.Ping(context.Background(), "good")
	})
	require.NoError(t, perr)
}

// TestRealClient_DialIsBoundedByItsOwnDeadline pins the second half of #277:
// even for the host that is actually blackholed, the dial must fail on a
// budget of its own rather than riding the kernel's TCP timeout. The context
// bindings.NewConnection takes becomes the connection's lifetime, so it cannot
// double as that budget — connDialTimeout is it.
func TestRealClient_DialIsBoundedByItsOwnDeadline(t *testing.T) {
	orig := connDialTimeout
	connDialTimeout = 100 * time.Millisecond
	t.Cleanup(func() { connDialTimeout = orig })

	sock := hangingLibpodServer(t)
	c, err := NewReal([]config.Host{{ID: "hung", Addr: "unix", Socket: sock}})
	require.NoError(t, err)

	start := time.Now()
	var perr error
	mustNotBlock(t, 2*time.Second, "a dial to a blackholed host", func() {
		perr = c.Ping(context.Background(), "hung")
	})
	require.Error(t, perr)
	require.ErrorIs(t, perr, context.DeadlineExceeded)
	require.Less(t, time.Since(start), time.Second)
}

// TestRealClient_CallerCancellationUnblocksADialWait covers the caller side:
// a request that gives up — a disconnected client, a cancelled job — must not
// be pinned to someone else's in-flight dial. The waiter here is the SECOND
// caller, joining a dial another goroutine started, because that is the shape
// the single-flight introduces and the one that would otherwise be
// uncancellable.
func TestRealClient_CallerCancellationUnblocksADialWait(t *testing.T) {
	sock := hangingLibpodServer(t)
	c, err := NewReal([]config.Host{{ID: "hung", Addr: "unix", Socket: sock}})
	require.NoError(t, err)

	go func() { _ = c.Ping(context.Background(), "hung") }()
	time.Sleep(100 * time.Millisecond)

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(50 * time.Millisecond)
		cancel()
	}()
	var perr error
	mustNotBlock(t, 2*time.Second, "a cancelled caller waiting on an in-flight dial", func() {
		perr = c.Ping(ctx, "hung")
	})
	require.ErrorIs(t, perr, context.Canceled)
}

// TestRealClient_FailedDialRollsBackAndRedials pins the placeholder's other
// half: a dial that fails must leave nothing behind — no cached entry, no
// in-flight reservation nobody will ever complete — so the next caller dials
// again rather than inheriting a poisoned one.
func TestRealClient_FailedDialRollsBackAndRedials(t *testing.T) {
	sock := filepath.Join(t.TempDir(), "podman.sock")
	c, err := NewReal([]config.Host{{ID: "h1", Addr: "unix", Socket: sock}})
	require.NoError(t, err)

	var perr error
	mustNotBlock(t, 2*time.Second, "a dial with no listener", func() {
		perr = c.Ping(context.Background(), "h1")
	})
	require.Error(t, perr)

	c.mu.Lock()
	cached, hasEntry := c.ctx["h1"]
	_, pending := c.dialing["h1"]
	c.mu.Unlock()
	require.False(t, hasEntry, "a failed dial cached an entry: %v", cached)
	require.False(t, pending, "a failed dial left its placeholder behind")

	libpodServerAt(t, sock)
	mustNotBlock(t, 2*time.Second, "the redial after a failed dial", func() {
		perr = c.Ping(context.Background(), "h1")
	})
	require.NoError(t, perr)
}

// TestRealClient_ConcurrentCallersShareOneDial pins the placeholder's purpose.
// Dialing off the lock means nothing serialises callers any more, so without a
// reservation a burst against a cold host would start one SSH handshake each
// — and on a slow host, keep doing so for as long as the first one takes.
func TestRealClient_ConcurrentCallersShareOneDial(t *testing.T) {
	var pings atomic.Int64
	sock, _ := fakeLibpodServerWith(t, func(w http.ResponseWriter, r *http.Request) bool {
		if !strings.Contains(r.URL.Path, "_ping") {
			return false
		}
		pings.Add(1)
		time.Sleep(200 * time.Millisecond)
		w.WriteHeader(http.StatusOK)
		return true
	})
	c, err := NewReal([]config.Host{{ID: "h1", Addr: "unix", Socket: sock}})
	require.NoError(t, err)

	var wg sync.WaitGroup
	errs := make([]error, 5)
	for i := range errs {
		wg.Add(1)
		go func() {
			defer wg.Done()
			errs[i] = c.Ping(context.Background(), "h1")
		}()
	}
	mustNotBlock(t, 5*time.Second, "five concurrent first-use callers", wg.Wait)
	for _, e := range errs {
		require.NoError(t, e)
	}
	require.Equal(t, int64(1), pings.Load(), "each caller started its own dial")
}

// gatedLibpodServer is a libpod server whose connect-time ping blocks until
// the returned channel is closed, so a test can hold a dial in flight for
// exactly as long as it needs to and then let it finish successfully. pings
// counts connect-time pings, i.e. dials — the thing single-flight is about.
// closes counts sockets the client dropped, which is how a test observes a
// connection actually being torn down rather than merely forgotten.
func gatedLibpodServer(t *testing.T) (sock string, release chan struct{}, pings *atomic.Int64, ln *countingUnixListener) {
	t.Helper()
	release = make(chan struct{})
	var n atomic.Int64
	sockPath, srvLn := fakeLibpodServerListener(t, func(w http.ResponseWriter, r *http.Request) bool {
		if !strings.Contains(r.URL.Path, "_ping") {
			return false
		}
		n.Add(1)
		select {
		case <-release:
		case <-time.After(10 * time.Second):
		}
		w.WriteHeader(http.StatusOK)
		return true
	})
	t.Cleanup(func() {
		select {
		case <-release:
		default:
			close(release)
		}
	})
	return sockPath, release, &n, srvLn
}

// TestRealClient_RemovedHostFailsFastDuringADial pins the ordering inside
// connFor: the host is resolved BEFORE the in-flight dial is joined.
//
// A dialCall carries the host it was started against precisely so a joiner can
// refuse it. With the check the other way round a caller joined whatever dial
// happened to be outstanding and waited its full connDialTimeout for an answer
// runDial was never going to publish — a host the operator has removed
// answering "deadline exceeded" 30 seconds later instead of "unknown host"
// at once.
func TestRealClient_RemovedHostFailsFastDuringADial(t *testing.T) {
	// connDialTimeout is deliberately NOT shrunk here. The point of the test
	// is that this caller does not wait for the dial at all, so the budget's
	// value is irrelevant to the assertion — and the background caller below
	// outlives the test, so restoring a shrunk package var in t.Cleanup would
	// race its read of it (caught by -race, not by inspection).
	sock := hangingLibpodServer(t)
	c, err := NewReal([]config.Host{{ID: "h1", Addr: "unix", Socket: sock}})
	require.NoError(t, err)

	go func() { _ = c.Ping(context.Background(), "h1") }()
	waitFor(t, "the dial to be in flight", func() bool {
		c.mu.Lock()
		defer c.mu.Unlock()
		return c.dialing["h1"] != nil
	})
	c.SetHosts(nil)

	start := time.Now()
	var perr error
	mustNotBlock(t, 10*time.Second, "a ping to a removed host", func() {
		perr = c.Ping(context.Background(), "h1")
	})
	require.ErrorContains(t, perr, "unknown host")
	require.Less(t, time.Since(start), time.Second,
		"the caller queued behind a dial for a host that no longer exists")
}

// TestRealClient_ReaddressedHostTakesEffectDuringADial is the case that
// actually matters, and the reason a joiner checks the host rather than just
// the presence of a dial.
//
// A host is blackholed; its dial is in flight and will stay that way until the
// kernel gives up, minutes away. The operator does the one thing that fixes
// it: repoints the host at a working address and reloads. If callers keep
// joining the dial to the OLD address — which is what a reservation keyed on
// id alone forces — the reload is inert for the whole life of that dial, and
// the recovery path out of the failure this file exists to bound is gated on
// the failure clearing itself first.
func TestRealClient_ReaddressedHostTakesEffectDuringADial(t *testing.T) {
	// Not shrunk, for the reason the removed-host test above gives.
	blackholed := hangingLibpodServer(t)
	healthy, _ := fakeLibpodServer(t)
	c, err := NewReal([]config.Host{{ID: "h1", Addr: "unix", Socket: blackholed}})
	require.NoError(t, err)

	go func() { _ = c.Ping(context.Background(), "h1") }()
	waitFor(t, "the dial to be in flight", func() bool {
		c.mu.Lock()
		defer c.mu.Unlock()
		return c.dialing["h1"] != nil
	})
	c.SetHosts([]config.Host{{ID: "h1", Addr: "unix", Socket: healthy}})

	start := time.Now()
	var perr error
	mustNotBlock(t, 10*time.Second, "a ping after the host was repointed", func() {
		perr = c.Ping(context.Background(), "h1")
	})
	require.NoError(t, perr)
	require.Less(t, time.Since(start), time.Second,
		"the caller queued behind the dial to the address the operator just replaced")
}

// TestRealClient_DialForAReconfiguredHostIsNotPublished covers the hazard
// dialing off the lock introduces and the commit-time hostStillMatches check
// exists for: a dial that SUCCEEDS after SetHosts has changed the endpoint
// under it. Publishing it would cache a live connection to the address the
// operator just reconfigured away from and serve every later call over it.
//
// The other half is that it leaves no socket behind: a connection nothing
// holds any more keeps its pooled sockets, and net/http's read and write
// goroutine per socket, alive for the process lifetime. Asserted on the server
// side, and as "every socket this dial opened is closed" rather than "at least
// one was" — see the note at the end for which code actually does the closing,
// which is not the obvious one.
func TestRealClient_DialForAReconfiguredHostIsNotPublished(t *testing.T) {
	sock, release, pings, ln := gatedLibpodServer(t)
	other, _ := fakeLibpodServer(t)
	c, err := NewReal([]config.Host{{ID: "h1", Addr: "unix", Socket: sock}})
	require.NoError(t, err)

	dialErr := make(chan error, 1)
	go func() { dialErr <- c.Ping(context.Background(), "h1") }()
	waitFor(t, "the dial to reach the server", func() bool { return pings.Load() == 1 })

	// The reload lands while the dial is mid-flight, then the dial succeeds.
	c.SetHosts([]config.Host{{ID: "h1", Addr: "unix", Socket: other}})
	close(release)

	select {
	case err := <-dialErr:
		require.ErrorContains(t, err, "reconfigured")
	case <-time.After(5 * time.Second):
		t.Fatal("the dial never resolved")
	}

	c.mu.Lock()
	cached := c.ctx["h1"]
	_, pending := c.dialing["h1"]
	c.mu.Unlock()
	require.Nil(t, cached, "a connection to the replaced endpoint was published")
	require.False(t, pending, "the reservation was left behind")

	// Every socket the abandoned dial opened is closed, none left pooled.
	//
	// Worth knowing which code satisfies this, because it is not
	// surplus.closeIdleConns(): bindings issues its connect-time ping over the
	// transport it built, and wireInvalidation installs the hook on a *clone*
	// and closes that original — so by commit time the hooked transport this
	// entry carries has never served a request and its idle pool is empty.
	// Deleting surplus.closeIdleConns() does not fail this test, verified by
	// mutation; it is kept as the belt-and-braces the entry's own close sites
	// all use, not as the thing standing between this path and a leak.
	waitFor(t, "every socket the abandoned dial opened to be closed", func() bool {
		return ln.closes.Load() == ln.accepts.Load() && ln.accepts.Load() > 0
	})
}

// TestRealClient_DisplacedDialDoesNotClearTheNewReservation pins the
// compare-and-delete in runDial. A dial displaced by a config reload keeps
// running — it cannot be cancelled — and resolves whenever its endpoint
// eventually answers, which can be after a newer dial for the same host has
// been reserved. Deleting the reservation unconditionally on the way out would
// clear the NEWER dial's, and the next caller would start a second dial
// against a host that already has one in flight.
func TestRealClient_DisplacedDialDoesNotClearTheNewReservation(t *testing.T) {
	oldSock, releaseOld, oldPings, _ := gatedLibpodServer(t)
	newSock, releaseNew, newPings, _ := gatedLibpodServer(t)
	c, err := NewReal([]config.Host{{ID: "h1", Addr: "unix", Socket: oldSock}})
	require.NoError(t, err)

	first := make(chan error, 1)
	go func() { first <- c.Ping(context.Background(), "h1") }()
	waitFor(t, "the first dial to reach the old endpoint", func() bool { return oldPings.Load() == 1 })

	// Reload, then a caller that displaces the old reservation with one of its
	// own against the new endpoint.
	c.SetHosts([]config.Host{{ID: "h1", Addr: "unix", Socket: newSock}})
	second := make(chan error, 1)
	go func() { second <- c.Ping(context.Background(), "h1") }()
	waitFor(t, "the second dial to reach the new endpoint", func() bool { return newPings.Load() == 1 })

	// The displaced dial now finishes, while the new one is still in flight.
	close(releaseOld)
	select {
	case err := <-first:
		require.ErrorContains(t, err, "reconfigured")
	case <-time.After(5 * time.Second):
		t.Fatal("the displaced dial never resolved")
	}

	// A third caller must JOIN the outstanding dial, not start another.
	third := make(chan error, 1)
	go func() { third <- c.Ping(context.Background(), "h1") }()
	c.mu.Lock()
	pending := c.dialing["h1"]
	c.mu.Unlock()
	require.NotNil(t, pending, "the displaced dial cleared the newer reservation")

	close(releaseNew)
	for _, ch := range []chan error{second, third} {
		select {
		case err := <-ch:
			require.NoError(t, err)
		case <-time.After(5 * time.Second):
			t.Fatal("a caller on the new endpoint never resolved")
		}
	}
	require.Equal(t, int64(1), newPings.Load(), "the new endpoint was dialed more than once")
}

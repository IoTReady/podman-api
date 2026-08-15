package podman

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/pem"
	"errors"
	"io"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"golang.org/x/crypto/ssh"
	"golang.org/x/crypto/ssh/knownhosts"

	"github.com/iotready/podman-api/internal/config"
)

// testSSHServer is a minimal in-process sshd: enough to accept a public-key
// login, answer one `exec` request per session, and — crucially for these
// tests — count handshakes and drop live connections on demand. Counting
// handshakes is the only way to actually prove connection reuse rather than
// assert it; #258 was in production precisely because nobody could see how
// many handshakes a request was paying for.
type testSSHServer struct {
	ln         net.Listener
	hostKey    ssh.Signer
	handshakes atomic.Int64
	execs      atomic.Int64
	// open is the number of accepted connections not yet torn down, so a test
	// can assert a dialed connection was actually closed rather than leaked.
	open atomic.Int64

	mu            sync.Mutex
	live          []net.Conn // raw conns, so a test can kill one mid-pool
	stdout        string
	exitCode      uint32
	execDelay     time.Duration // answer, but slowly
	dropOnSession bool          // handshake fine, then kill the conn on first use
}

func newTestSSHServer(t *testing.T, stdout string) *testSSHServer {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	s := &testSSHServer{ln: ln, hostKey: mustSigner(t), stdout: stdout}
	go s.serve()
	t.Cleanup(func() { ln.Close() })
	return s
}

func (s *testSSHServer) addr() string { return s.ln.Addr().String() }

func (s *testSSHServer) setStdout(out string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.stdout = out
}

func (s *testSSHServer) setExit(code uint32) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.exitCode = code
}

// setExecDelay makes every subsequent exec answer, but slowly — a command
// that outlives a short-deadlined caller on a perfectly healthy connection.
func (s *testSSHServer) setExecDelay(d time.Duration) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.execDelay = d
}

// setDropOnSession makes the server complete handshakes normally but kill the
// connection the moment a session is opened on it — a flapping host, where
// reconnecting works and using the connection does not.
func (s *testSSHServer) setDropOnSession(v bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.dropOnSession = v
}

// dropLive closes every accepted connection, simulating a restarted sshd or an
// expired NAT mapping: the pooled client stays in the pool and only discovers
// it is dead on next use.
func (s *testSSHServer) dropLive() {
	s.mu.Lock()
	conns := s.live
	s.live = nil
	s.mu.Unlock()
	for _, c := range conns {
		c.Close()
	}
}

func (s *testSSHServer) serve() {
	cfg := &ssh.ServerConfig{
		PublicKeyCallback: func(ssh.ConnMetadata, ssh.PublicKey) (*ssh.Permissions, error) {
			return &ssh.Permissions{}, nil
		},
	}
	cfg.AddHostKey(s.hostKey)
	for {
		nc, err := s.ln.Accept()
		if err != nil {
			return
		}
		s.mu.Lock()
		s.live = append(s.live, nc)
		s.mu.Unlock()
		go s.handle(nc, cfg)
	}
}

func (s *testSSHServer) handle(nc net.Conn, cfg *ssh.ServerConfig) {
	sc, chans, reqs, err := ssh.NewServerConn(nc, cfg)
	if err != nil {
		nc.Close()
		return
	}
	s.handshakes.Add(1)
	s.open.Add(1)
	go func() {
		sc.Wait() // returns once the client hangs up or the conn dies
		s.open.Add(-1)
	}()
	go ssh.DiscardRequests(reqs)
	defer sc.Close()
	for nch := range chans {
		s.mu.Lock()
		drop := s.dropOnSession
		s.mu.Unlock()
		if drop {
			nc.Close()
			return
		}
		if nch.ChannelType() != "session" {
			nch.Reject(ssh.UnknownChannelType, "only sessions here")
			continue
		}
		ch, chReqs, err := nch.Accept()
		if err != nil {
			return
		}
		go func(ch ssh.Channel, chReqs <-chan *ssh.Request) {
			defer ch.Close()
			for req := range chReqs {
				if req.Type != "exec" {
					req.Reply(false, nil)
					continue
				}
				req.Reply(true, nil)
				s.execs.Add(1)
				s.mu.Lock()
				out, code, delay := s.stdout, s.exitCode, s.execDelay
				s.mu.Unlock()
				if delay > 0 {
					time.Sleep(delay)
				}
				io.WriteString(ch, out)
				ch.SendRequest("exit-status", false, ssh.Marshal(struct{ Status uint32 }{code}))
				return
			}
		}(ch, chReqs)
	}
}

func mustSigner(t *testing.T) ssh.Signer {
	t.Helper()
	_, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("genkey: %v", err)
	}
	signer, err := ssh.NewSignerFromKey(priv)
	if err != nil {
		t.Fatalf("signer: %v", err)
	}
	return signer
}

// sshTestHost wires a Real to the test server: a client key on disk, and a
// HOME whose known_hosts trusts the server's host key (sshDial verifies against
// it, so without this every dial fails closed).
func sshTestHost(t *testing.T, s *testSSHServer) (*Real, config.Host) {
	return sshTestHostAt(t, s, s.addr())
}

// sshTestHostAt is sshTestHost with the dial address decoupled from the
// server's own, so a test can put something (a blackholing proxy) in between.
// known_hosts is keyed on the address dialled but carries the server's key,
// which is exactly how a real tunnelled host is configured.
func sshTestHostAt(t *testing.T, s *testSSHServer, addr string) (*Real, config.Host) {
	t.Helper()
	dir := t.TempDir()

	_, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("client key: %v", err)
	}
	der, err := ssh.MarshalPrivateKey(priv, "")
	if err != nil {
		t.Fatalf("marshal client key: %v", err)
	}
	keyPath := filepath.Join(dir, "id_ed25519")
	if err := os.WriteFile(keyPath, pem.EncodeToMemory(der), 0o600); err != nil {
		t.Fatalf("write client key: %v", err)
	}

	sshDir := filepath.Join(dir, ".ssh")
	if err := os.MkdirAll(sshDir, 0o700); err != nil {
		t.Fatalf("mkdir .ssh: %v", err)
	}
	kh := knownhosts.Line([]string{addr}, s.hostKey.PublicKey()) + "\n"
	if err := os.WriteFile(filepath.Join(sshDir, "known_hosts"), []byte(kh), 0o600); err != nil {
		t.Fatalf("write known_hosts: %v", err)
	}
	t.Setenv("HOME", dir)

	h := config.Host{ID: "h1", Addr: "tester@" + addr, SSHKey: keyPath}
	r, err := NewReal([]config.Host{h})
	if err != nil {
		t.Fatalf("NewReal: %v", err)
	}
	return r, h
}

// appendKnownHost adds a second server's host key to the HOME set up by
// sshTestHost, so a test can re-point a host at it.
func appendKnownHost(t *testing.T, s *testSSHServer) {
	t.Helper()
	f, err := os.OpenFile(filepath.Join(os.Getenv("HOME"), ".ssh", "known_hosts"), os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		t.Fatalf("append known_hosts: %v", err)
	}
	defer f.Close()
	if _, err := f.WriteString(knownhosts.Line([]string{s.addr()}, s.hostKey.PublicKey()) + "\n"); err != nil {
		t.Fatalf("append known_hosts: %v", err)
	}
}

// The whole point of #258: repeated side-channel reads must cost one handshake,
// not one per read. Before the pool this was 3 handshakes — ~2.9s each to a
// real host — which is what starved loadavg out of its request budget.
func TestSSHRun_ReusesOneConnection(t *testing.T) {
	s := newTestSSHServer(t, "0.42 0.37 0.31 1/512 12345\n")
	r, h := sshTestHost(t, s)

	for i := 0; i < 3; i++ {
		out, _, err := r.sshRun(context.Background(), h.ID, "cat /proc/loadavg")
		if err != nil {
			t.Fatalf("read %d: %v", i, err)
		}
		if !strings.HasPrefix(out, "0.42") {
			t.Fatalf("read %d: unexpected output %q", i, out)
		}
	}
	if got := s.handshakes.Load(); got != 1 {
		t.Errorf("handshakes = %d, want 1 (connection should be pooled)", got)
	}
	if got := s.execs.Load(); got != 3 {
		t.Errorf("execs = %d, want 3", got)
	}
}

// Reuse is only safe if a connection that died between calls is transparent to
// the caller. A restarted sshd, a rebooted host, an idle NAT mapping — all
// surface as a session failure on a pooled client, and must cost one reconnect,
// not an error the operator sees.
func TestSSHRun_ReconnectsAfterDroppedConnection(t *testing.T) {
	s := newTestSSHServer(t, "1.00 1.00 1.00 1/1 1\n")
	r, h := sshTestHost(t, s)

	if _, _, err := r.sshRun(context.Background(), h.ID, "cat /proc/loadavg"); err != nil {
		t.Fatalf("first read: %v", err)
	}
	s.dropLive()

	out, _, err := r.sshRun(context.Background(), h.ID, "cat /proc/loadavg")
	if err != nil {
		t.Fatalf("read after drop: %v", err)
	}
	if !strings.HasPrefix(out, "1.00") {
		t.Fatalf("unexpected output %q", out)
	}
	if got := s.handshakes.Load(); got != 2 {
		t.Errorf("handshakes = %d, want 2 (one initial, one reconnect)", got)
	}
}

// A remote command that exits non-zero says nothing about the connection. The
// old code could not tell the difference because it threw the connection away
// every time; the pool must not turn a failing `cat` into a reconnect loop.
func TestSSHRun_CommandFailureIsNotRetried(t *testing.T) {
	s := newTestSSHServer(t, "")
	s.setExit(1)
	r, h := sshTestHost(t, s)

	if _, _, err := r.sshRun(context.Background(), h.ID, "cat /proc/nope"); err == nil {
		t.Fatal("want an error from a non-zero exit")
	}
	if got := s.handshakes.Load(); got != 1 {
		t.Errorf("handshakes = %d, want 1 (a command failure is not a transport failure)", got)
	}
	if got := s.execs.Load(); got != 1 {
		t.Errorf("execs = %d, want 1 (no retry)", got)
	}
}

// A caller whose deadline has already passed must not pay for a handshake
// whose result is guaranteed to be discarded — the secondary defect in #258.
// ssh.Dial's cfg.Timeout could not express this: it is not a context.
func TestSSHRun_ExpiredContextSkipsTheDial(t *testing.T) {
	s := newTestSSHServer(t, "irrelevant\n")
	r, h := sshTestHost(t, s)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	start := time.Now()
	_, _, err := r.sshRun(ctx, h.ID, "cat /proc/loadavg")
	if err == nil {
		t.Fatal("want an error on an expired context")
	}
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Errorf("took %s; an expired ctx should fail at the dial, not after it", elapsed)
	}
	if got := s.handshakes.Load(); got != 0 {
		t.Errorf("handshakes = %d, want 0 (no dial on an expired context)", got)
	}
}

// A pooled connection must outlive the request that opened it. The dial sets a
// deadline on the raw conn to bound the handshake; leaving it in place would
// kill the pooled connection the moment the first caller's budget elapsed,
// which would look exactly like the flapping #258 reported.
func TestSSHRun_PooledConnectionOutlivesTheDialingContext(t *testing.T) {
	s := newTestSSHServer(t, "2.00 2.00 2.00 1/1 1\n")
	r, h := sshTestHost(t, s)

	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	if _, _, err := r.sshRun(ctx, h.ID, "cat /proc/loadavg"); err != nil {
		t.Fatalf("first read: %v", err)
	}
	cancel()
	time.Sleep(400 * time.Millisecond) // outlive the dialing context's deadline

	if _, _, err := r.sshRun(context.Background(), h.ID, "cat /proc/loadavg"); err != nil {
		t.Fatalf("read after the dialing ctx expired: %v", err)
	}
	if got := s.handshakes.Load(); got != 1 {
		t.Errorf("handshakes = %d, want 1 (the pooled conn must survive its dialer's deadline)", got)
	}
}

// A host whose connection parameters change (edited hosts file, SIGHUP reload)
// must not keep being served from a client pointing at the old endpoint.
func TestSetHosts_DropsPooledSSHClientOnParamChange(t *testing.T) {
	s1 := newTestSSHServer(t, "1.11 1.11 1.11 1/1 1\n")
	r, h := sshTestHost(t, s1)
	if _, _, err := r.sshRun(context.Background(), h.ID, "cat /proc/loadavg"); err != nil {
		t.Fatalf("first read: %v", err)
	}

	// Same id, new address. The old pooled client is now wrong by definition.
	s2 := newTestSSHServer(t, "2.22 2.22 2.22 1/1 1\n")
	kh := knownhosts.Line([]string{s2.addr()}, s2.hostKey.PublicKey()) + "\n"
	f, err := os.OpenFile(filepath.Join(os.Getenv("HOME"), ".ssh", "known_hosts"), os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		t.Fatalf("append known_hosts: %v", err)
	}
	if _, err := f.WriteString(kh); err != nil {
		t.Fatalf("append known_hosts: %v", err)
	}
	f.Close()

	moved := config.Host{ID: h.ID, Addr: "tester@" + s2.addr(), SSHKey: h.SSHKey}
	r.SetHosts([]config.Host{moved})

	out, _, err := r.sshRun(context.Background(), moved.ID, "cat /proc/loadavg")
	if err != nil {
		t.Fatalf("read after move: %v", err)
	}
	if !strings.HasPrefix(out, "2.22") {
		t.Fatalf("got %q; still talking to the old endpoint", out)
	}
}

func TestSSHConnBroken(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{"nil is not broken", nil, false},
		{"remote non-zero exit is the command, not the transport", &ssh.ExitError{}, false},
		{"cancelled ctx is our own budget", context.Canceled, false},
		{"deadline exceeded is our own budget", context.DeadlineExceeded, false},
		{"eof is the transport", io.EOF, true},
		{"unknown error is assumed to be the transport", errors.New("connection reset by peer"), true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := connBroken(tc.err); got != tc.want {
				t.Errorf("connBroken(%v) = %v, want %v", tc.err, got, tc.want)
			}
		})
	}
}

// waitFor polls until cond holds, so tests assert on state the pool reaches
// asynchronously (the wedge watchdog) without sleeping for the full grace.
func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

func (e *sshPoolEntry) pooled() *ssh.Client {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.client
}

// wedgeProxy is a TCP relay that can be told to stop delivering bytes in both
// directions without closing anything — a half-open connection, which is what
// an expired NAT mapping or a silently rebooted host actually looks like. It
// has to be simulated at this layer: a server that simply refuses to answer is
// not the same thing, because the client's own session close still unblocks it
// locally, and the pool would never notice a problem.
type wedgeProxy struct {
	ln      net.Listener
	backend string
	wedged  atomic.Bool
}

func newWedgeProxy(t *testing.T, backend string) *wedgeProxy {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("proxy listen: %v", err)
	}
	p := &wedgeProxy{ln: ln, backend: backend}
	go p.serve()
	t.Cleanup(func() { ln.Close() })
	return p
}

func (p *wedgeProxy) addr() string { return p.ln.Addr().String() }

func (p *wedgeProxy) serve() {
	for {
		c, err := p.ln.Accept()
		if err != nil {
			return
		}
		go func(c net.Conn) {
			b, err := net.Dial("tcp", p.backend)
			if err != nil {
				c.Close()
				return
			}
			go p.relay(b, c)
			p.relay(c, b)
		}(c)
	}
}

// relay forwards until wedged, after which it keeps reading (so neither side
// sees a closed socket) and drops everything on the floor.
func (p *wedgeProxy) relay(dst, src net.Conn) {
	buf := make([]byte, 4096)
	for {
		n, err := src.Read(buf)
		if n > 0 && !p.wedged.Load() {
			if _, werr := dst.Write(buf[:n]); werr != nil {
				return
			}
		}
		if err != nil {
			return
		}
	}
}

// The failure mode pooling introduces that dial-per-read could not have: a
// half-open connection answers nothing, so it surfaces ONLY as the caller's
// own ctx deadline — which connBroken deliberately does not treat as a
// transport fault. Left there, the dead client stays pooled and every later
// call fails the same way forever, with no recovery short of a restart, while
// the host itself is perfectly reachable via libpod.
func TestSSHRun_WedgedConnectionIsEvictedAndRecovers(t *testing.T) {
	s := newTestSSHServer(t, "0.42 0.37 0.31 1/1 1\n")
	p := newWedgeProxy(t, s.addr())
	r, h := sshTestHostAt(t, s, p.addr())
	r.hooks = &testHooks{wedgeGrace: 100 * time.Millisecond}

	// Establish the pooled connection while bytes still flow.
	if _, _, err := r.sshRun(context.Background(), h.ID, "cat /proc/loadavg"); err != nil {
		t.Fatalf("first read: %v", err)
	}
	e, _, err := r.sshEntryFor(h.ID)
	if err != nil {
		t.Fatalf("entry: %v", err)
	}
	wedged := e.pooled()

	p.wedged.Store(true)
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	if _, _, err := r.sshRun(ctx, h.ID, "cat /proc/loadavg"); err == nil {
		t.Fatal("want an error from the wedged connection")
	}

	waitFor(t, "the wedged client to be evicted from the pool", func() bool {
		return e.pooled() != wedged
	})

	// Recovery must be automatic: the host is fine, so the next call dials.
	p.wedged.Store(false)
	out, _, err := r.sshRun(context.Background(), h.ID, "cat /proc/loadavg")
	if err != nil {
		t.Fatalf("read after eviction: %v", err)
	}
	if !strings.HasPrefix(out, "0.42") {
		t.Fatalf("unexpected output %q", out)
	}
	if got := s.handshakes.Load(); got != 2 {
		t.Errorf("handshakes = %d, want 2 (one initial, one after eviction)", got)
	}
}

// The other half of the same judgement: a caller with a short deadline on a
// perfectly healthy connection must NOT cost the pool its client. Eviction is
// for connections that never answer, not for callers who stopped waiting.
func TestSSHRun_ShortDeadlineDoesNotEvictAHealthyConnection(t *testing.T) {
	s := newTestSSHServer(t, "0.42 0.37 0.31 1/1 1\n")
	r, h := sshTestHost(t, s)
	r.hooks = &testHooks{wedgeGrace: 2 * time.Second}

	if _, _, err := r.sshRun(context.Background(), h.ID, "cat /proc/loadavg"); err != nil {
		t.Fatalf("first read: %v", err)
	}
	e, _, err := r.sshEntryFor(h.ID)
	if err != nil {
		t.Fatalf("entry: %v", err)
	}
	before := e.pooled()

	// Answers in 150ms; the caller gives up at 30ms — well inside the grace.
	s.setExecDelay(150 * time.Millisecond)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	if _, _, err := r.sshRun(ctx, h.ID, "cat /proc/loadavg"); err == nil {
		t.Fatal("want a deadline error")
	}
	s.setExecDelay(0)

	time.Sleep(400 * time.Millisecond) // long past the session, well short of the grace
	if e.pooled() != before {
		t.Fatal("a healthy connection was evicted because one caller's deadline was short")
	}
	if _, _, err := r.sshRun(context.Background(), h.ID, "cat /proc/loadavg"); err != nil {
		t.Fatalf("read on the still-pooled connection: %v", err)
	}
	if got := s.handshakes.Load(); got != 1 {
		t.Errorf("handshakes = %d, want 1 (the connection should have been kept)", got)
	}
}

// Callers queued behind someone else's dial must still honour their own
// deadline. Otherwise an unreachable host serializes N callers into N
// consecutive 5s dials, and each one blows the budget it was given before it
// even starts — the inverse of the ctx-aware dial this change is about.
func TestConnect_QueuedCallerHonoursItsOwnDeadline(t *testing.T) {
	s := newTestSSHServer(t, "irrelevant\n")
	r, h := sshTestHost(t, s)
	e, _, err := r.sshEntryFor(h.ID)
	if err != nil {
		t.Fatalf("entry: %v", err)
	}

	// Stand in for a caller currently dialing a blackholed host.
	e.gate <- struct{}{}
	defer func() { <-e.gate }()

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	start := time.Now()
	if _, err := e.connect(ctx, h, time.Now); err == nil {
		t.Fatal("want an error when the caller's deadline expires while queued")
	}
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Errorf("waited %s; a queued caller must give up at its own deadline", elapsed)
	}
	if got := s.handshakes.Load(); got != 0 {
		t.Errorf("handshakes = %d, want 0 (the caller gave up before dialing)", got)
	}
}

// A call that raced a SIGHUP removal must not re-create a pool entry for a
// host SetHosts has already swept: nothing would ever close the connection it
// then dialed, since the removal loop only iterates hosts still configured.
func TestSSHRun_RemovedHostDoesNotResurrectAPoolEntry(t *testing.T) {
	s := newTestSSHServer(t, "0.42 0.37 0.31 1/1 1\n")
	r, h := sshTestHost(t, s)

	r.SetHosts(nil) // host removed by a reload

	if _, _, err := r.sshRun(context.Background(), h.ID, "cat /proc/loadavg"); err == nil {
		t.Fatal("want an error for a host that is no longer configured")
	}
	r.mu.Lock()
	_, resurrected := r.sshPool[h.ID]
	r.mu.Unlock()
	if resurrected {
		t.Error("a pool entry was created for a removed host; its connection would never be closed")
	}
	if got := s.handshakes.Load(); got != 0 {
		t.Errorf("handshakes = %d, want 0", got)
	}
}

// sshEntryFor hands out the entry under r.mu, but the dial happens after that
// lock is released — so a SIGHUP reload can drop the entry from the pool while
// a request to that host is still connecting. The freshly dialed client would
// then be stored on an entry nothing references any more, and nothing would
// ever close it: one leaked file descriptor per host-config edit that races a
// request, plus a live connection to an endpoint the operator has moved away
// from.
func TestConnect_RetiredEntryDoesNotOrphanADialedClient(t *testing.T) {
	s := newTestSSHServer(t, "0.42 0.37 0.31 1/1 1\n")
	r, h := sshTestHost(t, s)

	// A request that has already resolved its pool entry...
	e, _, err := r.sshEntryFor(h.ID)
	if err != nil {
		t.Fatalf("entry: %v", err)
	}
	// ...and a SIGHUP that removes the host before that request connects.
	r.SetHosts(nil)

	if _, err := e.connect(context.Background(), h, time.Now); !errors.Is(err, errRetiredHost) {
		t.Fatalf("connect returned %v, want errRetiredHost", err)
	}
	if e.pooled() != nil {
		t.Error("a client was stored on a retired entry; nothing would ever close it")
	}
	waitFor(t, "no connection left open to the server", func() bool { return s.open.Load() == 0 })
}

// The same race with the host merely *moved* rather than removed: the entry is
// retired either way, and a client dialed against the old address must not be
// stored — it would answer for an endpoint the config no longer names.
func TestConnect_RetiredDuringDialIsDiscardedAndClosed(t *testing.T) {
	s := newTestSSHServer(t, "0.42 0.37 0.31 1/1 1\n")
	r, h := sshTestHost(t, s)

	e, _, err := r.sshEntryFor(h.ID)
	if err != nil {
		t.Fatalf("entry: %v", err)
	}
	// Retire mid-flight, as SetHosts does for a changed addr/ssh_key.
	if cl := e.retire(); cl != nil {
		cl.Close()
	}

	if _, err := e.connect(context.Background(), h, time.Now); !errors.Is(err, errRetiredHost) {
		t.Fatalf("connect returned %v, want errRetiredHost", err)
	}
	if e.pooled() != nil {
		t.Error("a client was stored on a retired entry")
	}
	waitFor(t, "no connection left open to the server", func() bool { return s.open.Load() == 0 })

	// A retired entry stays retired: the next call goes through sshEntryFor,
	// which builds a fresh one because SetHosts removed this from the pool.
	if _, err := e.connect(context.Background(), h, time.Now); !errors.Is(err, errRetiredHost) {
		t.Fatalf("second connect returned %v, want errRetiredHost", err)
	}
}

// A live pooled connection must still be closed when its host is dropped —
// the retire path has to keep doing what the old close path did.
func TestSetHosts_ClosesTheLivePooledConnection(t *testing.T) {
	s := newTestSSHServer(t, "0.42 0.37 0.31 1/1 1\n")
	r, h := sshTestHost(t, s)

	if _, _, err := r.sshRun(context.Background(), h.ID, "cat /proc/loadavg"); err != nil {
		t.Fatalf("first read: %v", err)
	}
	if s.open.Load() != 1 {
		t.Fatalf("open = %d, want 1 before the reload", s.open.Load())
	}

	r.SetHosts(nil)
	waitFor(t, "the pooled connection to be closed on host removal", func() bool { return s.open.Load() == 0 })
}

// A cancel-only context — getHost passes r.Context(), which cancels when the
// client disconnects and carries no deadline — must still abort an in-flight
// handshake. A wall-clock deadline cannot express that: without watching
// ctx.Done() the dial runs the full sshDialTimeout for a result nobody wants.
func TestSSHDial_CancelAbortsTheHandshake(t *testing.T) {
	// Accepts TCP, then says nothing: the handshake hangs until something
	// closes the connection.
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer ln.Close()
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			defer c.Close()
		}
	}()

	dir := t.TempDir()
	if err := os.MkdirAll(filepath.Join(dir, ".ssh"), 0o700); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dir, ".ssh", "known_hosts"), []byte("\n"), 0o600); err != nil {
		t.Fatalf("known_hosts: %v", err)
	}
	t.Setenv("HOME", dir)

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(50 * time.Millisecond)
		cancel()
	}()

	start := time.Now()
	_, err = sshDial(ctx, config.Host{ID: "h1", Addr: "tester@" + ln.Addr().String()})
	elapsed := time.Since(start)
	if err == nil {
		t.Fatal("want an error when the context is cancelled mid-handshake")
	}
	// sshDialTimeout is 5s; a cancel must not wait it out.
	if elapsed > 2*time.Second {
		t.Errorf("dial took %s; a cancelled context must abort the handshake", elapsed)
	}
}

// Reuse must not be indefinite: the key and known_hosts are only consulted at
// dial time, so a connection kept forever keeps working on the strength of a
// check made arbitrarily long ago. Age retires it.
func TestConnect_RedialsPastTheMaxConnectionAge(t *testing.T) {
	s := newTestSSHServer(t, "0.42 0.37 0.31 1/1 1\n")
	r, h := sshTestHost(t, s)
	e, _, err := r.sshEntryFor(h.ID)
	if err != nil {
		t.Fatalf("entry: %v", err)
	}

	t0 := time.Date(2026, 8, 14, 12, 0, 0, 0, time.UTC)
	if _, err := e.connect(context.Background(), h, func() time.Time { return t0 }); err != nil {
		t.Fatalf("first connect: %v", err)
	}
	// The server counts handshakes on its own accept goroutine, so it can lag
	// the client's connect returning. Wait for the count rather than reading it
	// straight after — asserting it inline is a CI flake, not a check.
	waitFor(t, "the first handshake to register", func() bool { return s.handshakes.Load() == 1 })

	// Still inside the window: same connection.
	if _, err := e.connect(context.Background(), h, func() time.Time { return t0.Add(sshMaxConnAge - time.Second) }); err != nil {
		t.Fatalf("connect inside the window: %v", err)
	}
	if got := s.handshakes.Load(); got != 1 {
		t.Fatalf("handshakes = %d, want 1 while inside the age window", got)
	}

	// Past it: redialed.
	if _, err := e.connect(context.Background(), h, func() time.Time { return t0.Add(sshMaxConnAge) }); err != nil {
		t.Fatalf("connect past the window: %v", err)
	}
	waitFor(t, "the redial to register", func() bool { return s.handshakes.Load() == 2 })
	waitFor(t, "the aged connection to be closed", func() bool { return s.open.Load() == 1 })
}

// The retry exists so a connection that died between calls costs one reconnect
// rather than an error. But if the reconnect handshakes and the session then
// breaks the same way — a flapping host — the replacement is no better than
// what it replaced, and leaving it pooled makes every subsequent call pay two
// round trips to rediscover what this one already knows.
func TestSSHRun_EvictsAClientThatBreaksOnTheRetryToo(t *testing.T) {
	s := newTestSSHServer(t, "0.42 0.37 0.31 1/1 1\n")
	r, h := sshTestHost(t, s)

	if _, _, err := r.sshRun(context.Background(), h.ID, "cat /proc/loadavg"); err != nil {
		t.Fatalf("first read: %v", err)
	}
	e, _, err := r.sshEntryFor(h.ID)
	if err != nil {
		t.Fatalf("entry: %v", err)
	}

	s.setDropOnSession(true) // handshakes still succeed; every session dies
	if _, _, err := r.sshRun(context.Background(), h.ID, "cat /proc/loadavg"); err == nil {
		t.Fatal("want an error while the host is flapping")
	}
	if cl := e.pooled(); cl != nil {
		t.Error("a client broken on the retry stayed pooled; the next caller pays to rediscover it")
	}

	// And once the host settles, the very next call reconnects cleanly rather
	// than first having to fail on the cached bad client.
	s.setDropOnSession(false)
	before := s.handshakes.Load()
	out, _, err := r.sshRun(context.Background(), h.ID, "cat /proc/loadavg")
	if err != nil {
		t.Fatalf("read after the flap: %v", err)
	}
	if !strings.HasPrefix(out, "0.42") {
		t.Fatalf("unexpected output %q", out)
	}
	if got := s.handshakes.Load() - before; got != 1 {
		t.Errorf("handshakes after recovery = %d, want 1 (no wasted attempt on a known-bad client)", got)
	}
}

// The dial must not be allowed to spend the caller's whole budget: on the
// poller's path the budget and sshDialTimeout are both 5s, so "the dial took
// its full allowance" and "the read had no time at all" were the same event —
// a self-inflicted outage on exactly the hosts slow enough to need a reconnect.
func TestSSHDial_LeavesBudgetForTheSession(t *testing.T) {
	// A listener that accepts and never speaks: the dial burns its whole share.
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer ln.Close()
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			defer c.Close()
		}
	}()

	dir := t.TempDir()
	if err := os.MkdirAll(filepath.Join(dir, ".ssh"), 0o700); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dir, ".ssh", "known_hosts"), []byte("\n"), 0o600); err != nil {
		t.Fatalf("known_hosts: %v", err)
	}
	t.Setenv("HOME", dir)

	const budget = 600 * time.Millisecond
	ctx, cancel := context.WithTimeout(context.Background(), budget)
	defer cancel()

	start := time.Now()
	_, err = sshDial(ctx, config.Host{ID: "h1", Addr: "tester@" + ln.Addr().String()})
	elapsed := time.Since(start)
	if err == nil {
		t.Fatal("want an error from a server that never handshakes")
	}
	// The dial must give up with time to spare, not at the caller's deadline.
	if elapsed >= budget {
		t.Errorf("dial consumed %s of a %s budget; nothing was left for the session", elapsed, budget)
	}
	if ctx.Err() != nil {
		t.Error("the caller's context was already expired when the dial returned")
	}
}

// A session that finishes inside the grace window is not wedged — but it may
// still have broken. Nobody is left to act on that error, so leaving the client
// pooled makes the next caller rediscover it at the cost of its own round trip.
func TestSSHSession_BrokenInsideTheGraceWindowStillEvicts(t *testing.T) {
	s := newTestSSHServer(t, "0.42 0.37 0.31 1/1 1\n")
	r, h := sshTestHost(t, s)
	r.hooks = &testHooks{wedgeGrace: 2 * time.Second}

	if _, _, err := r.sshRun(context.Background(), h.ID, "cat /proc/loadavg"); err != nil {
		t.Fatalf("first read: %v", err)
	}
	e, _, err := r.sshEntryFor(h.ID)
	if err != nil {
		t.Fatalf("entry: %v", err)
	}
	pooled := e.pooled()

	// The caller gives up first; the connection then dies while the watchdog is
	// still inside its grace window.
	s.setExecDelay(150 * time.Millisecond)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	if _, _, err := r.sshRun(ctx, h.ID, "cat /proc/loadavg"); err == nil {
		t.Fatal("want a deadline error")
	}
	s.dropLive() // the session now fails with a transport error, inside the grace

	waitFor(t, "the broken client to be evicted", func() bool { return e.pooled() != pooled })
}

// getHost passes the request context straight through with no deadline, so a
// connection that wedges under such a request would park this goroutine —
// and the per-host read gate it holds — forever. Every later request and
// poller tick for that host then queues behind it and gives up, and loadavg
// goes stale permanently and silently. The read caps itself.
func TestSSHRun_CapsItselfWhenTheCallerSetsNoDeadline(t *testing.T) {
	s := newTestSSHServer(t, "0.42 0.37 0.31 1/1 1\n")
	p := newWedgeProxy(t, s.addr())
	r, h := sshTestHostAt(t, s, p.addr())
	r.hooks = &testHooks{readCap: 300 * time.Millisecond}

	if _, _, err := r.sshRun(context.Background(), h.ID, "cat /proc/loadavg"); err != nil {
		t.Fatalf("first read: %v", err)
	}
	p.wedged.Store(true)

	done := make(chan error, 1)
	go func() {
		// context.Background(): no deadline at all, exactly as getHost supplies.
		_, _, err := r.sshRun(context.Background(), h.ID, "cat /proc/loadavg")
		done <- err
	}()

	select {
	case err := <-done:
		if err == nil {
			t.Fatal("want an error from the wedged connection")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the read never returned; a wedged connection parks the caller and the gate forever")
	}
}

// A retired entry does not mean failure; it means a reload happened and this
// caller is holding the previous answer. Looking again yields the new endpoint,
// so a read racing a reload completes against the host's current config.
//
// This matters well beyond loadavg, which can afford to skip a cycle:
// HostBoundPorts feeds a deploy's port-conflict precheck, where every error
// aborts the deploy.
func TestSSHRun_SelfHealsWhenTheEntryIsRetiredMidCall(t *testing.T) {
	s1 := newTestSSHServer(t, "1.11 1.11 1.11 1/1 1\n")
	r, h := sshTestHost(t, s1)

	s2 := newTestSSHServer(t, "2.22 2.22 2.22 1/1 1\n")
	appendKnownHost(t, s2)
	moved := config.Host{ID: h.ID, Addr: "tester@" + s2.addr(), SSHKey: h.SSHKey}

	// Retire the entry in the window between resolving it and connecting,
	// exactly once — the second attempt must find the new endpoint.
	var once bool
	r.hooks = &testHooks{beforeConnect: func() {
		if !once {
			once = true
			r.SetHosts([]config.Host{moved})
		}
	}}

	out, used, err := r.sshRun(context.Background(), h.ID, "cat /proc/loadavg")
	if err != nil {
		t.Fatalf("sshRun: %v; a reload mid-call must not fail the read", err)
	}
	if !strings.HasPrefix(out, "2.22") {
		t.Errorf("got %q, want the new endpoint's output", out)
	}
	if used.Addr != moved.Addr {
		t.Errorf("reported host %q, want the endpoint actually read %q", used.Addr, moved.Addr)
	}
}

// A host that is genuinely gone is still an error: the deploy path is right to
// abort on that one, and self-healing must not paper over it.
func TestSSHRun_RemovedHostStillFails(t *testing.T) {
	s := newTestSSHServer(t, "1.11 1.11 1.11 1/1 1\n")
	r, h := sshTestHost(t, s)

	r.hooks = &testHooks{beforeConnect: func() { r.SetHosts(nil) }}

	_, _, err := r.sshRun(context.Background(), h.ID, "cat /proc/loadavg")
	if err == nil {
		t.Fatal("want an error for a host that no longer exists")
	}
	if !errors.Is(err, errUnknownHost) {
		t.Errorf("got %v, want errUnknownHost", err)
	}
}

// A host edited from remote to local mid-read must be read locally, not
// refused. sshRun is right to refuse the endpoint it was handed — it would
// otherwise dial "unix:22" — but the caller's answer has simply moved to the
// other branch, and for HostBoundPorts refusing means aborting a deploy that
// had nothing wrong with it.
func TestHostBoundPorts_RedispatchesLocallyWhenTheHostBecomesLocal(t *testing.T) {
	s := newTestSSHServer(t, "unused\n")
	r, h := sshTestHost(t, s)

	var once bool
	r.hooks = &testHooks{beforeConnect: func() {
		if !once {
			once = true
			r.SetHosts([]config.Host{{ID: h.ID, Addr: "unix"}})
		}
	}}

	// The local branch reads this process's own /proc/net/tcp, so the daemon's
	// own listener — whatever port the test server took — must show up.
	_, port, err := net.SplitHostPort(s.addr())
	if err != nil {
		t.Fatal(err)
	}
	want, err := strconv.Atoi(port)
	if err != nil {
		t.Fatal(err)
	}

	got, err := r.HostBoundPorts(context.Background(), h.ID, "tcp")
	if err != nil {
		t.Fatalf("HostBoundPorts: %v; a host moved to local mid-read must be read locally", err)
	}
	found := false
	for _, p := range got {
		if p == want {
			found = true
		}
	}
	if !found {
		t.Errorf("port %d not among %v; the local read did not happen", want, got)
	}
}

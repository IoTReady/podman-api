package podman

import (
	"context"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"golang.org/x/crypto/ssh"

	"github.com/iotready/podman-api/internal/config"
)

// sshTunnel owns the SSH client under one ssh:// libpod connection.
//
// podman's bindings build that client themselves and keep it only in a closure
// over the http.Transport's DialContext, with no way to close it (#311, pro
// #328): closing the connection released its HTTP sockets and left one live
// SSH session per connection until process exit — the SSH client's own read
// goroutines keep it reachable, so the garbage collector never reclaimed it.
// Thousands accumulated on the hosts the control plane talks to.
//
// So we dial the client ourselves (sshDial: ctx-aware, strict known_hosts) and
// hand bindings a private unix socket that forwards to the remote podman
// socket through it. The lifecycle is then ours: Close ends the client.
type sshTunnel struct {
	cl  *ssh.Client
	ln  net.Listener
	dir string

	// remote is the podman socket path on the far side.
	remote string

	mu      sync.Mutex
	active  int  // forwarded connections still open
	closing bool // Close was called; the client goes when active reaches 0
	closed  bool
	done    chan struct{} // closed once the client has been closed
	force   *time.Timer
}

// sshTunnelDrainGrace bounds how long Close waits for in-flight forwarded
// connections before closing the client anyway. Eviction is deliberately
// trigger-happy (see invalidateConn), so a healthy connection another
// goroutine is mid-request on can be closed; letting it finish keeps that
// mistake as cheap as the doc there promises. Longer than the longest call
// timeout (a 10 minute exec) so nothing legitimate is cut, short enough that a
// connection whose last request left a keep-alive socket idle cannot hold the
// client forever.
const sshTunnelDrainGrace = 12 * time.Minute

// sshTunnelKeepalive is how often the client pings the server. A peer that
// vanished without a FIN (reboot, dropped NAT mapping) would otherwise leave
// the client — and its listener — looking healthy indefinitely.
const sshTunnelKeepalive = 30 * time.Second

// sshTunnelDialBudget bounds opening a tunnel: handshake plus, for a host with
// no configured socket, the `podman info` lookup. sshDial additionally caps the
// handshake itself at sshDialTimeout.
const sshTunnelDialBudget = 15 * time.Second

// openSSHTunnel dials h and starts forwarding a local unix socket to the
// remote podman socket. The caller owns the result and must Close it.
func openSSHTunnel(ctx context.Context, h config.Host) (*sshTunnel, error) {
	ctx, cancel := context.WithTimeout(ctx, sshTunnelDialBudget)
	defer cancel()

	cl, err := sshDial(ctx, h)
	if err != nil {
		return nil, err
	}
	remote := h.Socket
	if remote == "" {
		// Same lookup bindings did when handed a URI with no path.
		remote, err = remoteSocketPath(cl)
		if err != nil {
			cl.Close()
			return nil, err
		}
	}

	// XDG_RUNTIME_DIR when set: a tmpfs the tmpfiles sweep leaves alone, where
	// a /tmp socket old enough could be reaped from under a live connection.
	dir, err := os.MkdirTemp(os.Getenv("XDG_RUNTIME_DIR"), "papi-ssh-")
	if err != nil {
		cl.Close()
		return nil, err
	}
	// MkdirTemp is 0700: the socket inside is reachable only by this user, which
	// matters because it fronts a remote podman socket.
	ln, err := net.Listen("unix", filepath.Join(dir, "s"))
	if err != nil {
		cl.Close()
		os.RemoveAll(dir)
		return nil, err
	}
	t := &sshTunnel{cl: cl, ln: ln, dir: dir, remote: remote, done: make(chan struct{})}
	go t.accept()
	go t.watch()
	return t, nil
}

func remoteSocketPath(cl *ssh.Client) (string, error) {
	sess, err := cl.NewSession()
	if err != nil {
		return "", err
	}
	defer sess.Close()
	out, err := sess.Output("podman info --format '{{.Host.RemoteSocket.Path}}'")
	if err != nil {
		return "", fmt.Errorf("find remote podman socket: %w", err)
	}
	p := strings.TrimSpace(string(out))
	if p == "" {
		return "", fmt.Errorf("find remote podman socket: empty path")
	}
	return p, nil
}

// uri is what bindings is pointed at.
func (t *sshTunnel) uri() string { return "unix://" + filepath.Join(t.dir, "s") }

// watch ends the tunnel when the SSH connection dies or stops answering, so a
// dead client fails the next local dial fast (which wireInvalidation reads as
// "evict this connection") instead of accepting and then hanging.
func (t *sshTunnel) watch() {
	gone := make(chan struct{})
	go func() { t.cl.Wait(); close(gone) }()
	tick := time.NewTicker(sshTunnelKeepalive)
	defer tick.Stop()
	for {
		select {
		case <-gone:
			t.shutdown()
			return
		case <-t.done:
			return
		case <-tick.C:
			if _, _, err := t.cl.SendRequest("keepalive@openssh.com", true, nil); err != nil {
				t.shutdown()
				return
			}
		}
	}
}

func (t *sshTunnel) accept() {
	for {
		lc, err := t.ln.Accept()
		if err != nil {
			return
		}
		if !t.begin() {
			lc.Close()
			continue
		}
		go func() {
			defer t.end()
			t.forward(lc)
		}()
	}
}

func (t *sshTunnel) begin() bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.closed || t.closing {
		return false
	}
	t.active++
	return true
}

func (t *sshTunnel) end() {
	t.mu.Lock()
	t.active--
	last := t.closing && t.active == 0
	t.mu.Unlock()
	if last {
		t.finish()
	}
}

// forward pipes lc to a fresh channel on the remote socket, propagating
// half-close in both directions: podman's attach closes STDIN's side to tell an
// exec's command its input ended, which must reach the far end.
func (t *sshTunnel) forward(lc net.Conn) {
	defer lc.Close()
	rc, err := t.cl.Dial("unix", t.remote)
	if err != nil {
		return
	}
	defer rc.Close()

	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		io.Copy(rc, lc)
		if cw, ok := rc.(interface{ CloseWrite() error }); ok {
			cw.CloseWrite()
		}
	}()
	go func() {
		defer wg.Done()
		io.Copy(lc, rc)
		if cw, ok := lc.(interface{ CloseWrite() error }); ok {
			cw.CloseWrite()
		}
	}()
	wg.Wait()
}

// Close stops accepting new connections and closes the SSH client once the
// forwarded connections in flight are done, or after sshTunnelDrainGrace.
// Safe to call more than once, and on nil.
func (t *sshTunnel) Close() error {
	if t == nil {
		return nil
	}
	t.mu.Lock()
	if t.closing || t.closed {
		t.mu.Unlock()
		return nil
	}
	t.closing = true
	idle := t.active == 0
	if !idle {
		t.force = time.AfterFunc(sshTunnelDrainGrace, t.finish)
	}
	t.mu.Unlock()
	t.ln.Close()
	if idle {
		t.finish()
	}
	return nil
}

// shutdown is Close for a tunnel that is already dead: nothing is worth
// draining, so it is closed immediately.
func (t *sshTunnel) shutdown() {
	t.mu.Lock()
	t.closing = true
	t.mu.Unlock()
	t.ln.Close()
	t.finish()
}

// finish closes the client and removes the socket; exactly once.
func (t *sshTunnel) finish() {
	t.mu.Lock()
	if t.closed {
		t.mu.Unlock()
		return
	}
	t.closed = true
	if t.force != nil {
		t.force.Stop()
	}
	t.mu.Unlock()
	close(t.done)
	t.cl.Close()
	t.ln.Close()
	os.RemoveAll(t.dir)
}

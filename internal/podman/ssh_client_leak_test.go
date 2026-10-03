package podman

import (
	"context"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// #311 (pro #328): closing a libpod ssh:// connection must close the SSH
// client underneath it. bindings holds that client only in a closure, so
// closeIdleConns released the HTTP sockets and left one live SSH session per
// evicted/released connection — thousands on a long-running control plane.
func TestCloseIdleConns_ClosesTheUnderlyingSSHClient(t *testing.T) {
	s := newTestSSHServer(t, "")
	r, h := sshTestHost(t, s)
	h.Socket = "/run/podman/podman.sock"
	uri := "ssh://" + h.Addr + h.Socket

	const n = 5
	for i := 0; i < n; i++ {
		cc, tun, err := dialConn(h, uri)
		if err != nil {
			t.Fatalf("dial %d: %v", i, err)
		}
		e := &connEntry{ctx: cc, tunnel: tun}
		r.wireInvalidation(e, h.ID, noExecInvalidation)
		e.closeIdleConns()
	}
	if got := s.handshakes.Load(); got != n {
		t.Fatalf("handshakes = %d, want %d", got, n)
	}

	deadline := time.Now().Add(3 * time.Second)
	for s.open.Load() != 0 && time.Now().Before(deadline) {
		time.Sleep(20 * time.Millisecond)
	}
	if got := s.open.Load(); got != 0 {
		t.Fatalf("%d of %d SSH connections still live after closeIdleConns; the SSH client leaks", got, n)
	}
}

func waitOpen(s *testSSHServer, want int64) bool {
	deadline := time.Now().Add(3 * time.Second)
	for s.open.Load() != want && time.Now().Before(deadline) {
		time.Sleep(20 * time.Millisecond)
	}
	return s.open.Load() == want
}

// Eviction is trigger-happy by design (invalidateConn), so Close on a tunnel
// must let a request another goroutine is mid-way through finish: the SSH client
// goes when the last forwarded connection does, not before.
func TestSSHTunnel_CloseDrainsInFlightConnections(t *testing.T) {
	s := newTestSSHServer(t, "")
	_, h := sshTestHost(t, s)
	h.Socket = "/run/podman/podman.sock"

	tun, err := openSSHTunnel(context.Background(), h)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	c, err := net.Dial("unix", filepath.Join(tun.dir, "s"))
	if err != nil {
		t.Fatalf("dial tunnel: %v", err)
	}
	// Wait until the forward is really established before closing.
	deadline := time.Now().Add(3 * time.Second)
	for {
		tun.mu.Lock()
		a := tun.active
		tun.mu.Unlock()
		if a == 1 || time.Now().After(deadline) {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	tun.Close()
	if _, err := net.Dial("unix", filepath.Join(tun.dir, "s")); err == nil {
		t.Fatal("tunnel accepted a new connection after Close")
	}
	time.Sleep(200 * time.Millisecond)
	if got := s.open.Load(); got != 1 {
		t.Fatalf("SSH client closed with a connection in flight: open=%d, want 1", got)
	}
	c.Close()
	if !waitOpen(s, 0) {
		t.Fatalf("SSH client still live after the last forwarded connection ended: open=%d", s.open.Load())
	}
	if _, err := os.Stat(tun.dir); !os.IsNotExist(err) {
		t.Fatalf("socket dir not removed: %v", err)
	}
}

// A client whose server vanished must stop accepting, so the next dial fails
// fast and wireInvalidation evicts the connection rather than hanging on it.
func TestSSHTunnel_DeadClientStopsListening(t *testing.T) {
	s := newTestSSHServer(t, "")
	_, h := sshTestHost(t, s)
	h.Socket = "/run/podman/podman.sock"

	tun, err := openSSHTunnel(context.Background(), h)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer tun.Close()
	s.dropLive()

	deadline := time.Now().Add(3 * time.Second)
	for {
		c, err := net.Dial("unix", filepath.Join(tun.dir, "s"))
		if err != nil {
			return
		}
		c.Close()
		if time.Now().After(deadline) {
			t.Fatal("tunnel still accepting after its SSH connection died")
		}
		time.Sleep(20 * time.Millisecond)
	}
}

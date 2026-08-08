package podman

import (
	"context"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strings"
	"time"

	"golang.org/x/crypto/ssh"
	"golang.org/x/crypto/ssh/knownhosts"

	"github.com/iotready/podman-api/internal/config"
)

// sshReadLoadAvg dials the host over SSH and reads /proc/loadavg.
// It authenticates with the host's configured key (h.SSHKey) and verifies the
// host against ~/.ssh/known_hosts. NOTE: libpod connects via
// NewConnectionWithIdentity, so trust can diverge from this path — if the host
// key is absent from known_hosts, loadavg is silently nil (best-effort).
func sshReadLoadAvg(ctx context.Context, h config.Host) (string, error) {
	return sshReadFile(ctx, h, "cat /proc/loadavg")
}

// sshReadUptime dials the host over SSH and reads /proc/uptime, mirroring
// sshReadLoadAvg. Used by HostUptime for a remote host, deliberately avoiding
// libpod's `info` endpoint (see HostUptime's own doc comment for why).
func sshReadUptime(ctx context.Context, h config.Host) (string, error) {
	return sshReadFile(ctx, h, "cat /proc/uptime")
}

// sshReadProcNetPorts dials the host over SSH and reads /proc/net/<protocol>
// plus (best-effort) /proc/net/<protocol>6, mirroring sshReadUptime/
// sshReadLoadAvg. The primary file's absence is a real error: it is NOT
// covered by the `|| true` below, so a failing first `cat` still fails the
// whole command (and short-circuits past the second `cat` via `&&`) and
// sshReadFile surfaces that error. Only the second (IPv6) cat's failure is
// swallowed — matching readProcNetPortsLocal's "IPv6 file missing is not an
// error" posture, since a host with IPv6 disabled simply lacks that file.
func sshReadProcNetPorts(ctx context.Context, h config.Host, protocol string) (string, error) {
	cmd := fmt.Sprintf("cat /proc/net/%s && (cat /proc/net/%s6 2>/dev/null || true)", protocol, protocol)
	return sshReadFile(ctx, h, cmd)
}

// sshReadFile dials the host over SSH and runs cmd, returning its stdout.
// Shared by sshReadLoadAvg and sshReadUptime: same auth, same known_hosts
// verification, same ctx-bounded session teardown — only the remote command
// differs.
func sshReadFile(ctx context.Context, h config.Host, cmd string) (string, error) {
	user, addr := splitUserHost(h.Addr)
	auth := []ssh.AuthMethod{}
	if h.SSHKey != "" {
		key, err := os.ReadFile(h.SSHKey)
		if err != nil {
			return "", err
		}
		signer, err := ssh.ParsePrivateKey(key)
		if err != nil {
			return "", err
		}
		auth = append(auth, ssh.PublicKeys(signer))
	}
	hostKeyCb, err := knownhosts.New(filepath.Join(os.Getenv("HOME"), ".ssh", "known_hosts"))
	if err != nil {
		return "", err
	}
	cfg := &ssh.ClientConfig{
		User:            user,
		Auth:            auth,
		HostKeyCallback: hostKeyCb,
		Timeout:         5 * time.Second,
	}
	conn, err := ssh.Dial("tcp", addr, cfg)
	if err != nil {
		return "", err
	}
	defer conn.Close()
	sess, err := conn.NewSession()
	if err != nil {
		return "", err
	}
	// Run the remote command in a goroutine so we can select on ctx.Done().
	// Closing the session unblocks Output() in the goroutine; conn.Close() via
	// defer tears down any remaining session state. Session.Close is idempotent.
	type result struct {
		out []byte
		err error
	}
	ch := make(chan result, 1)
	go func() {
		out, err := sess.Output(cmd)
		ch <- result{out, err}
	}()
	select {
	case <-ctx.Done():
		sess.Close() // unblock the goroutine's Output call
		return "", ctx.Err()
	case r := <-ch:
		sess.Close()
		if r.err != nil {
			return "", r.err
		}
		return string(r.out), nil
	}
}

// splitUserHost parses "user@host" or "user@host:port" into (user, "host:port"),
// defaulting to port 22.
func splitUserHost(addr string) (user, hostport string) {
	at := strings.IndexByte(addr, '@')
	if at >= 0 {
		user = addr[:at]
		addr = addr[at+1:]
	}
	if _, _, err := net.SplitHostPort(addr); err != nil {
		addr = net.JoinHostPort(addr, "22")
	}
	return user, addr
}

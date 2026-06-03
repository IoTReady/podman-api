package podman

import (
	"context"
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
		out, err := sess.Output("cat /proc/loadavg")
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

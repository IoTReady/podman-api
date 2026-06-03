package podman

import (
	"net"
	"os"
	"path/filepath"
	"strings"
	"time"

	"golang.org/x/crypto/ssh"
	"golang.org/x/crypto/ssh/knownhosts"

	"github.com/iotready/podman-api/internal/config"
)

// sshReadLoadAvg dials the host over SSH and reads /proc/loadavg. It reuses the
// host's configured key (h.SSHKey) and the user's known_hosts for verification
// — the same trust the libpod SSH connection relies on.
func sshReadLoadAvg(h config.Host) (string, error) {
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
	defer sess.Close()
	out, err := sess.Output("cat /proc/loadavg")
	if err != nil {
		return "", err
	}
	return string(out), nil
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

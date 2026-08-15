package podman

import (
	"context"
	"fmt"
	"net"
	"strings"

	"github.com/iotready/podman-api/internal/config"
)

// sshReadLoadAvg reads /proc/loadavg over the host's pooled SSH connection.
// It authenticates with the host's configured key (h.SSHKey) and verifies the
// host against ~/.ssh/known_hosts.
//
// Two ways this can come back empty, and they look identical from here:
//
//   - Trust divergence. libpod connects via NewConnectionWithIdentity, so a
//     host podman can reach may still be absent from ~/.ssh/known_hosts, and
//     this path alone then fails. Check with `ssh-keygen -F <host>`.
//   - Deadline. The caller's budget ran out. This is the one that bites in
//     practice (#258): the read used to open a fresh TCP+SSH connection per
//     call — ~2.9s to a remote host — while sitting last in a per-host request
//     budget the libpod prelude had already mostly spent, so loadavg went
//     missing for whichever hosts happened to be slowest that second. Hence
//     the connection pool (ssh_pool.go) and the cache in hostLoadAvg.
//
// hostLoadAvg logs which one it was; do not diagnose this by bisecting hosts.
func (r *Real) sshReadLoadAvg(ctx context.Context, hostID string) (string, config.Host, error) {
	return r.sshRun(ctx, hostID, "cat /proc/loadavg")
}

// sshReadUptime reads /proc/uptime, mirroring sshReadLoadAvg. Used by
// HostUptime for a remote host, deliberately avoiding libpod's `info` endpoint
// (see HostUptime's own doc comment for why).
func (r *Real) sshReadUptime(ctx context.Context, hostID string) (string, error) {
	out, _, err := r.sshRun(ctx, hostID, "cat /proc/uptime")
	return out, err
}

// sshReadProcNetPorts reads /proc/net/<protocol> plus (best-effort)
// /proc/net/<protocol>6, mirroring sshReadUptime/sshReadLoadAvg. The primary
// file's absence is a real error: it is NOT covered by the `|| true` below, so
// a failing first `cat` still fails the whole command (and short-circuits past
// the second `cat` via `&&`) and sshRun surfaces that error. Only the second
// (IPv6) cat's failure is swallowed — matching readProcNetPortsLocal's "IPv6
// file missing is not an error" posture, since a host with IPv6 disabled
// simply lacks that file.
func (r *Real) sshReadProcNetPorts(ctx context.Context, hostID, protocol string) (string, error) {
	cmd := fmt.Sprintf("cat /proc/net/%s && (cat /proc/net/%s6 2>/dev/null || true)", protocol, protocol)
	out, _, err := r.sshRun(ctx, hostID, cmd)
	return out, err
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

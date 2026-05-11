#!/usr/bin/env bash
# Opinionated installer for podman-api on a Linux host with systemd.
#
# Creates a dedicated `podman-api` system user, installs the binary to
# /usr/local/bin, drops the systemd unit, and seeds an empty config tree
# under /etc/podman-api. It does NOT start the service — review the config
# first, then `systemctl enable --now podman-api`.
#
# Usage:
#   sudo contrib/install.sh                       # build + install
#   sudo BINARY=/path/to/podman-api install.sh    # use a pre-built binary
set -euo pipefail

if [[ "${EUID}" -ne 0 ]]; then
	echo "must run as root" >&2
	exit 1
fi

BINARY="${BINARY:-}"
SRC_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

if [[ -z "${BINARY}" ]]; then
	echo "==> building podman-api from ${SRC_DIR}"
	(cd "${SRC_DIR}" && go build -o /tmp/podman-api ./cmd/podman-api)
	BINARY=/tmp/podman-api
fi

if [[ ! -x "${BINARY}" ]]; then
	echo "binary not found or not executable: ${BINARY}" >&2
	exit 1
fi

echo "==> creating podman-api system user (idempotent)"
if ! id podman-api >/dev/null 2>&1; then
	useradd --system --home /var/lib/podman-api --create-home --shell /usr/sbin/nologin podman-api
else
	echo "    user already exists"
fi

echo "==> installing binary to /usr/local/bin/podman-api"
install -o root -g root -m 0755 "${BINARY}" /usr/local/bin/podman-api

echo "==> creating /etc/podman-api/{hosts,templates}"
install -d -o root -g root -m 0755 /etc/podman-api
install -d -o root -g podman-api -m 0750 /etc/podman-api/hosts
install -d -o root -g podman-api -m 0750 /etc/podman-api/templates

if [[ ! -f /etc/podman-api/keys.yaml ]]; then
	echo "==> seeding empty /etc/podman-api/keys.yaml (you must add a real key)"
	cat > /etc/podman-api/keys.yaml <<'EOF'
# Add bearer keys here. Generate hashes with:
#   podman-api hash-token "<plaintext>"
keys: []
EOF
	chown root:podman-api /etc/podman-api/keys.yaml
	chmod 0640 /etc/podman-api/keys.yaml
fi

echo "==> installing systemd unit"
install -o root -g root -m 0644 "${SRC_DIR}/contrib/podman-api.service" /etc/systemd/system/podman-api.service
systemctl daemon-reload

cat <<EOF

==> done.

Next steps:
  1. Add at least one bearer key:
       podman-api hash-token "<plaintext>"
       \$EDITOR /etc/podman-api/keys.yaml
  2. Drop your hosts/<name>.yaml into /etc/podman-api/hosts/
  3. (Optional) drop custom templates into /etc/podman-api/templates/ and
     append "-templates-dir=/etc/podman-api/templates" to the service unit.
  4. systemctl enable --now podman-api
  5. Verify:
       curl http://127.0.0.1:8080/healthz
       journalctl -u podman-api -f

To rotate keys without restart:
  \$EDITOR /etc/podman-api/keys.yaml
  systemctl reload podman-api    # or: kill -HUP \$(pidof podman-api)
EOF

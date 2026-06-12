# Security Policy

## Reporting a vulnerability

Please **do not** open a public issue for security vulnerabilities.

Email **security@iotready.co** with:
- A description of the vulnerability and its impact.
- Steps to reproduce or a proof-of-concept.
- The podman-api version affected.

You will receive an acknowledgement within 48 hours and a resolution timeline within 7 days.

## Scope

- The podman-api HTTP server and its authentication layer.
- The SSH tunnel to remote podman sockets.
- Secret storage and encryption (`-spec-key-file`).

Out of scope: the podman daemon itself, the host OS, or anything the operator has explicitly configured to be world-accessible.

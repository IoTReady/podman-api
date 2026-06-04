# Ingress Spike — Findings (#60, Task 0)

**Date:** 2026-06-04
**Host:** `148.113.9.117` (rootless podman 5.4.2, netavark/aardvark), wildcard DNS
`*.podman.iotready.com → 148.113.9.117` (operator-provided, Cloudflare).
**Result:** ✅ **All three design assumptions hold. Phase 2 may proceed.**

The spike stood up a `spike-`prefixed network + nginx backend + Caddy pod, proved
the assumptions, and tore everything down (host restored: `spike-*` objects
removed, `ip_unprivileged_port_start` reset to 1024).

## Assumptions tested

### A1 — Network DNS resolves the backend by name ✅
On a shared podman network, a peer container resolved the nginx backend by name
(`spike-web → 10.89.1.2`) and fetched it over HTTP. End-to-end, Caddy's
`reverse_proxy spike-web:80` served the backend over HTTPS, confirming the proxy
resolves names at request time via aardvark.

**Resolved §11 open question — what name a `kube play` pod is reachable as.**
For a pod created via `podman kube play` (`metadata.name: spike-pod`, container
`name: web`) on the network:

| Candidate name | Resolves? |
|---|---|
| `spike-pod` (pod name = `metadata.name`) | ✅ → pod IP |
| `web` (bare container name) | ✅ → pod IP |
| `spike-pod-web` | ❌ |

Both the pod name and the bare container name resolve to the (shared) pod IP.
**Decision for Phase 2: the route `Backend` must be `<pod-name>:<container-port>`**
(`metadata.name` = podman-api's `<template>-<slug>`, which is globally unique),
**not the bare container name** — container names like `web`/`db` are not unique
across pods on the same network and would collide in DNS.

### A2 — Containerised HTTP-01 issues and persists ✅
Caddy obtained a real Let's Encrypt certificate for `demo.podman.iotready.com`
via HTTP-01 on `:80` within ~9s, stored under
`/data/caddy/certificates/.../demo.podman.iotready.com/` on a podman **volume**.
After `podman restart`, HTTPS still returned 200 and the cert file mtime was
unchanged — **no re-issue**, so the data volume correctly preserves the cert and
ACME account across restarts (no rate-limit exposure).

This also confirms the host's `:80`/`:443` are reachable from the internet
(HTTP-01 could not have completed otherwise).

### A3 — `podman cp` + `caddy reload` over the existing socket ✅
A new Caddyfile was placed into the running Caddy container with `podman cp`,
validated with `caddy validate` ("Valid configuration"), and applied with
`caddy reload` — **no restart**. The change took effect immediately (response
carried the newly-added header) with zero downtime. Proven via the CLI; the
equivalent podman/v5 binding entry points (`containers` exec + copy, `network`
create/exists) are to be pinned during Phase 2 implementation against
`github.com/containers/podman/v5 v5.8.2`.

## Additional findings (feed Phase 2 + provisioning)

1. **Rootless privileged-port binding is a provisioning prerequisite.**
   `net.ipv4.ip_unprivileged_port_start` defaulted to **1024**, so rootless
   podman cannot publish `:80`/`:443` until it is lowered to ≤80 (set
   system-wide, persistently) — *or* the Caddy pod runs rootful. Phase 2 must
   pick one and the host-provisioning docs (wiki) must record it. The spike used
   an ephemeral `sysctl -w net.ipv4.ip_unprivileged_port_start=80`.
2. **DBUS/systemd note is an artifact of the SSH shell, not production.** Running
   podman over a non-login SSH shell logged
   `failed to move the rootless netns pasta process to the systemd user.slice`.
   Lingering is enabled (`Linger=yes`) and podman-api connects via the systemd
   **user socket** (`/run/user/1000/podman/podman.sock`), where a user session/
   DBUS is present — so this does not occur in the daemon's path. Benign.
3. **Caddy as a podman-managed pod is viable as designed** — published ports,
   config volume (or `cp`-target path), and data volume all behaved.

## Net effect on the design
No assumption failed; the design in `2026-06-04-ingress-auto-tls-design.md`
stands. The only new hard requirement is the rootless port-start sysctl (or
rootful Caddy) for Phase 2 + provisioning. The §11 pod-name question is resolved:
**`Backend = <pod-name>:<container-port>`**.

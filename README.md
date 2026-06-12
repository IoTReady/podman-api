# podman-api

A small REST wrapper around [Podman](https://podman.io)'s [libpod REST API](https://docs.podman.io/en/latest/_static/api.html) that lets a CMS (or any orchestrator) deploy and manage **pods** described by **YAML templates** across a fleet of Podman hosts.

It is opinionated, single-binary, and deliberately narrow: one host group, bearer-token auth, audit log to stdout, Prometheus metrics on a separate listener, kubernetes-style secrets, idempotent applies. Use it when Kubernetes is too much and ad-hoc `podman run` over SSH is too little.

## Documentation

This README is the quick reference. The **[wiki](/tej/podman-api/wiki)** is the operator's handbook:

- [Building](/tej/podman-api/wiki/Building) — why the build needs tags, `make` targets, static/cross builds.
- [Provisioning a Podman Host](/tej/podman-api/wiki/Provisioning-a-Podman-Host) — turn a fresh box into an SSH-drivable target.
- [Deploying](/tej/podman-api/wiki/Deploying) — install the daemon: user, config tree, systemd, TLS.
- [Operating](/tej/podman-api/wiki/Operating) — key rotation, audit-log shipping, metrics, health checks.
- [Troubleshooting](/tej/podman-api/wiki/Troubleshooting) — common failures and their fixes.

## What it does

- Lists, applies, upgrades, starts/stops, and deletes **pod instances** on remote Podman hosts via SSH-tunneled `podman.sock`.
- Renders parameterised **templates** (Go `text/template` over Kubernetes-style YAML) into Pod manifests, then plays them with `podman play kube`.
- Manages **per-host** and **per-instance secrets** as Kubernetes Secret resources (so `secretKeyRef` works).
- Pre-pulls every container image before play, so a bad tag fails fast with a clear registry error instead of a 30-second timeout.
- Streams **logs** as plain text or SSE.
- **Migrates and evacuates** instances across hosts as async jobs. Movement is cold-copy (stop → copy volumes → re-apply → verify → reap source) with rollback if the destination doesn't come up.
- **Backs up and restores** instance volumes on demand: `POST .../backup` snapshots every volume; `POST /backups/{id}/restore` restores in-place with content verification. See [Backing up and Restoring](/tej/podman-api/wiki/Backing-up-and-Restoring).
- Exposes **Prometheus metrics** on a separate, opt-in listener so they aren't world-readable on the public port.

## What it doesn't do

- No multi-replica HA; one daemon, one config tree.
- No image registry, no scheduler, no rolling deploy primitives. (`Upgrade` is single-pod replace-in-place.)
- No webhook callbacks; callers poll (including the async migrate/evacuate jobs).
- No daemon-side placement/scheduler — cross-host moves work, but the *client* chooses destinations (the daemon executes the move).
- No live migration — cross-host moves are cold (stop → copy volumes → re-apply → verify → reap the source).

## Architecture

```
   ┌────────────┐  HTTPS    ┌────────────────┐  SSH-tunneled  ┌────────────┐
   │ CMS / curl │ ───────▶  │  podman-api    │ ─────────────▶ │ podman.sock│
   │            │           │  (this binary) │   libpod REST  │  on hostN  │
   └────────────┘           └────────────────┘                └────────────┘
                              │            │
                              │            └─▶  /metrics  (separate addr, optional)
                              │
                              └─▶  audit log (stdout, JSON lines)
```

Hosts are declared once in `hosts/*.yaml`. Templates live in the store. Bearer keys live in `auth/keys.yaml` and reload without restart via SIGHUP.

## Build

```sh
make build          # -> bin/podman-api
```

The podman v5 bindings require build tags to exclude CGO graph-driver headers. `make build` carries them. See [Building](/tej/podman-api/wiki/Building) for cross-compile and CI details.

## Configure and run

See [Deploying](/tej/podman-api/wiki/Deploying) for the full setup guide (installer, systemd unit, TLS, bearer keys). The short version:

```sh
podman-api \
  -addr=127.0.0.1:8080 \
  -metrics-addr=127.0.0.1:9090 \
  -hosts-dir=/etc/podman-api/hosts \
  -keys-file=/etc/podman-api/keys.yaml \
  -state-db=/var/lib/podman-api/state.db \
  -spec-key-file=/etc/podman-api/spec.key
```

A `contrib/install.sh` script creates a dedicated user, installs the binary, and enables the service.

## Admin UI

An embedded, server-rendered admin UI (HTMX + PureCSS) is served at `/ui`. Disabled unless `-operator-file <path>` is set. Pass `-ui-secure-cookie` when serving over HTTPS.

Generate a password hash:
```sh
podman-api hash-token <your-password>
```

## API reference

A complete OpenAPI 3.0 spec lives at [`api/openapi.yaml`](api/openapi.yaml) and is served by the binary at `GET /openapi.yaml` (no auth).

```
GET    /healthz
GET    /metrics                                                      separate listener

GET    /hosts
GET    /hosts/{host}
GET    /hosts/{host}/healthz
GET    /hosts/{host}/ports-in-use

GET    /templates
GET    /templates/{id}
GET    /templates/{id}/render?<params>
POST   /templates                            body: {id, body, ...}
PUT    /templates/{id}                       body: {body, ...}
DELETE /templates/{id}?force=
POST   /templates/{id}/clone                 body: {new_id}

GET    /hosts/{host}/secrets
PUT    /hosts/{host}/secrets/{name}          body: {"value": "..."}
DELETE /hosts/{host}/secrets/{name}

GET    /hosts/{host}/instances?template=<id>
GET    /hosts/{host}/instances/{template}/{slug}
POST   /hosts/{host}/instances
PUT    /hosts/{host}/instances/{template}/{slug}
DELETE /hosts/{host}/instances/{template}/{slug}?prune_volumes=&prune_secrets=

POST   /hosts/{host}/instances/{template}/{slug}/start
POST   /hosts/{host}/instances/{template}/{slug}/stop
POST   /hosts/{host}/instances/{template}/{slug}/restart
POST   /hosts/{host}/instances/{template}/{slug}/upgrade  body: {"image": "..."}

GET    /hosts/{host}/instances/{template}/{slug}/logs?container=&tail=&follow=
GET    /hosts/{host}/instances/{template}/{slug}/volumes
DELETE /hosts/{host}/volumes/{name}

POST   /migrate    body: {from_host, to_host, template, slug, parameters?}
POST   /evacuate   body: {from_host, map:{slug: to_host}}

POST   /hosts/{host}/instances/{template}/{slug}/backup
GET    /hosts/{host}/instances/{template}/{slug}/backups?limit=
POST   /backups/{id}/restore
DELETE /backups/{id}

GET    /jobs?state=&kind=&parent_id=
GET    /jobs/{id}
```

**Notes:**
- `POST /migrate` and `POST /evacuate` require `-state-db` (else `501`), validate synchronously, and return `202 {job_id}`. Poll `GET /jobs/{id}` for progress.
- `?skip_pull=true` on POST/PUT skips the pre-pull step.
- On DELETE, `prune_volumes` and `prune_secrets` default to `false`. Pass both as `true` to reap volumes and secrets. DELETE is idempotent — a prune-requested delete on an already-gone pod still removes orphaned volumes/secrets and returns `204`.

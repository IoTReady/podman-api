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
- **Migrates and evacuates** instances across hosts (opt-in, requires the state store): `POST /migrate` moves one instance, `POST /evacuate` clears a whole host, both as async jobs. Movement is cold-copy (stop → copy volumes → re-apply → verify → reap source) with rollback if the destination doesn't come up.
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

Hosts are declared once in `hosts/*.yaml`. Templates are bundled (or loaded from a directory). Bearer keys live in `auth/keys.yaml` and can be reloaded without restart via SIGHUP.

## Build

Use `make build` — it carries the required build tags:

```sh
make build          # -> bin/podman-api
```

The podman v5 bindings transitively pull in the storage graph drivers (btrfs,
devicemapper) and gpgme, all of which need CGO and system `-dev` headers. This
binary uses only the **remote** libpod client, so we exclude those drivers and
swap gpgme for a pure-Go OpenPGP implementation via build tags. A plain
`go build` *without* these tags fails on a clean machine (`<btrfs/version.h>`
not found / missing `gpgme.pc`):

```sh
go build -tags "containers_image_openpgp exclude_graphdriver_btrfs exclude_graphdriver_devicemapper" \
  -o podman-api ./cmd/podman-api
```

With those tags no CGO system headers are needed. For cross-compile to a Linux
server from a Mac/Linux dev box:

```sh
GOOS=linux GOARCH=amd64 go build \
  -tags "containers_image_openpgp exclude_graphdriver_btrfs exclude_graphdriver_devicemapper" \
  -o podman-api ./cmd/podman-api
```

## Configure

### Hosts

One YAML file per Podman host, dropped into `hosts/`. Each looks like:

```yaml
id: prod-1
addr: podman@10.0.1.10        # ssh://user@host  OR  the literal "unix"
socket: /run/user/1000/podman/podman.sock
ssh_key: /etc/podman-api/keys/prod-1.id_ed25519
labels:
  env: prod
  region: ap-south-1
```

`addr: unix` connects to a local socket directly (no SSH). All other values open an SSH tunnel using the named identity file. The remote user must have a running `podman.socket` on the path you point at.

### Bearer keys

`auth/keys.yaml` holds Argon2id-hashed bearer tokens. Generate a hash with the binary itself:

```sh
podman-api hash-token "my-plaintext-token"
# $argon2id$v=19$m=65536,t=3,p=4$...
```

```yaml
keys:
  - id: cms-prod
    secret_hash: '$argon2id$v=19$m=65536,t=3,p=4$...'
    scopes: [hosts:read, instances:*, secrets:*]
    description: "Prod CMS"
  - id: prom-scraper
    secret_hash: '$argon2id$v=19$m=65536,t=3,p=4$...'
    scopes: [hosts:read]
```

The defined scopes are `hosts:read`, `hosts:write`, `instances:read`, `instances:write`, `secrets:read`, `secrets:write`, `jobs:read` (and `instances:*` / `secrets:*` as shorthand).

**Live key rotation:** edit `auth/keys.yaml`, then `kill -HUP $(pidof podman-api)`. The new key list takes effect on the next inbound request — in-flight log streams are not interrupted. A bad reload (parse error or zero keys) is logged and the previous list stays live, so a fat-fingered edit can't lock you out.

### Templates

Templates are Kubernetes-style YAML with a small metadata header in a YAML comment block. The bundled `templates/postgres.yaml` is the canonical example:

```yaml
# template-meta:
#   id: postgres
#   parameters:
#     required: [slug, image, port, db, user]
#   secrets:
#     per_instance: [password]
#   volumes:
#     - name: data
#       backup: none
---
apiVersion: v1
kind: Pod
metadata:
  name: postgres-{{.slug}}
  labels:
    podman-api/template: postgres
    podman-api/slug: {{.slug}}
spec:
  containers:
    - name: db
      image: {{.image}}
      ...
```

Identifier fields (slug, template id, secret name, container name) must match `^[a-z0-9][a-z0-9-]{0,38}[a-z0-9]$` — DNS-label style, 2–40 chars, lowercase ASCII + digits + hyphen, no leading/trailing dash. This is enforced at every API edge to prevent injection into pod/secret/volume names or rendered YAML.

To use external templates, pass `-templates-dir=/etc/podman-api/templates`. The bundled set is then ignored.

## Run

The opinionated layout for a production host:

```
/etc/podman-api/
├── hosts/
│   └── prod-1.yaml
├── keys.yaml
├── spec.key           # 32-byte AES-256 key; required when -state-db is set
└── templates/         # optional override
```

Then run:

```sh
podman-api \
  -addr=127.0.0.1:8080 \
  -metrics-addr=127.0.0.1:9090 \
  -hosts-dir=/etc/podman-api/hosts \
  -keys-file=/etc/podman-api/keys.yaml
```

To enable the desired-state store (required for migrate/evacuate):

```sh
podman-api \
  -addr=127.0.0.1:8080 \
  -hosts-dir=/etc/podman-api/hosts \
  -keys-file=/etc/podman-api/keys.yaml \
  -state-db=/var/lib/podman-api/state.db \
  -spec-key-file=/etc/podman-api/spec.key
```

A systemd unit and an opinionated installer live in `contrib/`. See [`contrib/install.sh`](contrib/install.sh) — it creates a dedicated `podman-api` user, installs the binary, and enables the service.

## Admin UI

An optional, embedded, server-rendered admin UI (HTMX + PureCSS) is served at `/ui` on the main `-addr` listener. It is **disabled** unless `-operator-file <path>` is set — no flag, no UI, no auth surface.

**Setup:**

1. Copy `auth/operator.example.yaml` to `auth/operator.yaml` (gitignored).
2. Generate a password hash and paste it in:
   ```sh
   ./bin/podman-api hash-token <your-password>
   ```
   The file shape is:
   ```yaml
   username: operator
   password_hash: "$argon2id$v=19$m=65536,t=3,p=4$..."
   ```
3. Start the daemon with `-operator-file auth/operator.yaml`.

The UI provides a single-operator login, a host list, template-based deployment, and instance lifecycle from the browser: start, stop, restart, delete, and a static log tail (last 200 lines on request; live streaming is a later slice) — all without touching the API directly. **Upgrade** is image-only: it reuses the instance's stored parameters and secrets and asks only for a new image, so it requires the desired-state store (`-state-db`) and is hidden when the store is off; rotating a secret is a separate operation (a per-instance secret-management UI is future work).

**Flags:**

- **`-operator-file <path>`** — path to the operator credential YAML; enables the UI.
- **`-ui-secure-cookie`** — marks the session cookie `Secure`; set this when serving over HTTPS or behind a TLS-terminating proxy.

Like `auth/keys.yaml`, the operator file is reloaded on SIGHUP — edit the file, send `kill -HUP $(pidof podman-api)`, and the new credential takes effect on the next login attempt.

Design spec: [`docs/superpowers/specs/2026-06-04-admin-ui-shell-design.md`](docs/superpowers/specs/2026-06-04-admin-ui-shell-design.md).

## Security model

- **TLS** is the responsibility of a reverse proxy. The recommended setup is Caddy in front (see `contrib/Caddyfile.example`), terminating Let's Encrypt. The binary itself binds to `127.0.0.1` and never exposes plaintext on a public port.
- **/metrics** is **not** mounted on the main listener. To expose Prometheus, set `-metrics-addr=127.0.0.1:9090` and either scrape it via SSH tunnel or bind to a VPC-internal address. Metrics labels include host IDs, template IDs, and HTTP paths — not safe to expose publicly.
- **Bearer tokens** are stored as Argon2id PHC strings; the plaintext only exists at issuance time and in the client's config.
- **Slug/template/secret name validation** happens at the API edge (see "Templates" above) — bad inputs never reach the renderer or podman.
- **Secrets** flow through the API in the JSON body of `POST /instances`, are stored as Kubernetes Secret objects on the target host, and are zeroed in the in-memory request struct after use.

### State store (optional)

When `-state-db=<path>` is set, the daemon persists each instance's parameters and AES-256-GCM-encrypted secrets to a local SQLite database. This is off by default and is required for the migrate/evacuate features. With it the daemon becomes a **stateful controller**: it holds desired state (the store) beside the podman actuator, so it can move an instance to another host by name without the client re-supplying secrets.

- **`-state-db <path>`** — enables the desired-state store at this path.
- **`-spec-key-file <path>`** — 32-byte AES-256-GCM key used to encrypt stored secrets. Required when `-state-db` is set; the daemon refuses to start without a readable, valid key. Loaded once at startup (no runtime reload — see the rotation caveat below). Keep the file `0600` and **separate from the database** — leaking secrets requires compromise of both.
- **`-jobs-retention <dur>`** — when set (e.g. `168h`), a background sweep prunes terminal (`succeeded`/`failed`) jobs older than the duration, keeping parent/child families intact (a parent is removed only once it has no surviving child). Default `0` (disabled; the `jobs` table accumulates until manual cleanup).
- **`-evacuate-concurrency <n>`** — max child migrations an evacuate runs at once (default `2`, clamped to `1..32`). A request body's `"concurrency"` overrides it per call.
- **`-job-workers <n>`** — size of the background job worker pool (default `8`). A parent evacuate occupies one worker for its entire fan-out, so this is the headroom that keeps a few concurrent evacuates from starving plain migrate/other jobs; raise it if you run many concurrent evacuates. Values `<=0` fall back to the built-in default.
- **`-migrate-verify-timeout <dur>`** — how long a migrate waits for the destination instance to become ready before it gives up and rolls back (default `60s`). Readiness means the pod and every container are `Running` **and** every container that declares a healthcheck reports `healthy`; raise this for apps with a slow warm-up (DB WAL replay, cache priming). Containers without a healthcheck are gated on liveness alone.
- **`-migrate-verify-volumes`** — verify each copied volume's contents (a sorted path→size+sha256 manifest) against the source before the source is reaped; a mismatch fails the move and rolls back (default `true`). Set `=false` to skip the extra source+dest re-export per volume on very large volumes.

Generate a key:

```sh
head -c 32 /dev/urandom | base64 > /etc/podman-api/spec.key && chmod 600 /etc/podman-api/spec.key
```

> **Key rotation caveat:** the key is loaded once at startup and there is no re-encrypting rotation yet, so changing the key makes existing rows unreadable. The SQLite `-wal`/`-shm` sidecar files also hold (encrypted) secret material — include them (or checkpoint first) when backing up.

### Migrate & evacuate (optional)

With the store enabled, the daemon exposes server-side cross-host moves as async jobs:

- **`POST /migrate`** `{from_host, to_host, template, slug, parameters?}` — move one instance. Validated synchronously (known hosts, distinct source/destination, an existing stored spec), then enqueued. The job stops the source, cold-copies its volumes, re-applies the spec on the destination, **verifies** every container is Running, then reaps the source. If the destination doesn't come up it rolls back (restarts the source, reaps the partial destination). Scope `instances:write`.
- **`POST /evacuate`** `{from_host, map:{slug: to_host, ...}}` — clear a whole host. Enqueues one **parent** job that fans out a child `migrate` job per instance at bounded concurrency. The `map` must cover exactly the instances on the host (strict bijection); placement is the client's choice. A sibling failure doesn't abort the others — the parent succeeds only if every child does, otherwise it fails with per-child detail. Scope `instances:write`.

Both validate the request and return `202 {job_id}`; deeper/destination-side errors surface as job failures, not the POST status. Both return `501` when `-state-db` is not set.

Operations are tracked as **jobs**, readable via:

- `GET /jobs?state=<queued|running|succeeded|failed>&kind=<kind>&parent_id=<id>` — list jobs, optionally filtered (scope `jobs:read`). `parent_id` drills into an evacuate's child migrations. Paginated newest-first: `?limit=` (default 100, max 1000) and `?before=<job_id>` cursor — page by passing the previous page's last id, stopping when fewer than `limit` rows come back.
- `GET /jobs/{id}` — fetch one job by ID, including its progress steps and error (scope `jobs:read`).

Both endpoints return `501` when `-state-db` is not set.

### Host-health automation: scheduled prune (optional)

With the store enabled, the daemon can keep hosts tidy on a schedule, running
`podman` prune on a **safe, opt-in policy** so production hosts don't fill their
container-storage partition. Each run is a `prune` **job** — auditable and
queryable via the jobs API (and surfaced by the UI in a later release). It is
**off by default**; nothing is auto-deleted until an operator turns it on. Like
migrate/evacuate it requires `-state-db` (the daemon refuses to start if
`-prune-enabled` is set without it).

A scheduler evaluates every host roughly once a minute and enqueues a prune when
**either** the interval has elapsed since that host's last successful prune
**or** the host's disk crosses a high-water threshold (whichever comes first).

Global flag defaults (each host may override via a `prune:` block — see below):

- **`-prune-enabled`** — turn the feature on (default `false`).
- **`-prune-interval <dur>`** — routine sweep interval per host (default `24h`). `0` disables the interval trigger (threshold only).
- **`-prune-disk-threshold <pct>`** — disk used-% that triggers an early prune before the interval is due (default `85`). `0` disables the threshold trigger.
- **`-prune-scope <list>`** — comma-separated scopes (default `dangling`). Available: `dangling` (dangling image layers), `all-images` (also unused tagged images — costs a re-pull on next deploy), `containers` (exited containers), `build-cache`, `volumes` (unused/unattached volumes). Only `dangling` runs unless you opt into more.
- **`-prune-dry-run`** — perform a dry run that removes nothing (default `false`). When the `volumes` scope is enabled it reports the volume-reclaimable bytes from `system df`; image/build-cache sizes aren't available in a dry run. Use it to confirm a policy is sane before enabling removal.

Per-host override in `hosts/*.yaml`:

```yaml
id: web1
addr: user@web1
socket: /run/user/1000/podman/podman.sock
prune:
  enabled: true
  interval: 12h
  disk_threshold_pct: 70
  scope: [dangling, build-cache]
  dry_run: false
```

**Safety.** Prune relies on podman's safe-by-default semantics — only
dangling/unused objects are removed, never anything in use, and nothing is
force-removed. Two extra guards protect stateful workloads:

- The scheduler will not start a prune for a host that already has a running
  **migrate** or **evacuate** job, and while any such move is in flight the
  `volumes` scope is dropped from that run — so a migration's transiently
  detached volume can't be reaped.
- The `volumes` scope skips any volume carrying the protect label
  `podman-api.protect=true`. Volumes are opt-in regardless.

Runs appear in the jobs API as kind `prune`
(`GET /jobs?kind=prune&state=succeeded`), and are exported as the Prometheus
counters `podman_api_prune_runs_total` and `podman_api_prune_reclaimed_bytes_total`.

### Ingress + auto-TLS (optional)

`-ingress-enabled` runs a per-host Caddy pod that terminates TLS (HTTP-01 ACME)
and reverse-proxies each instance's `domains` to its pod over the shared
`-ingress-network`. Requires `-state-db` and `-ingress-acme-email`. Apps join the
ingress network and publish no host ports; only Caddy publishes :80/:443.

**Rootless prerequisite:** the host must allow rootless binding of privileged
ports — set `net.ipv4.ip_unprivileged_port_start=80` persistently (e.g. a
`/etc/sysctl.d/` drop-in), or Caddy cannot publish :80/:443. See the wiki
"Provisioning a Podman Host" page.

## Observability

- **Audit log:** every state-changing request (POST/PUT/DELETE) emits one JSON line to stdout — or, if `-audit-log-file=/var/log/podman-api/audit.log` is set, to that file. Each line includes method, path, host, template, slug, status, duration, key_id, and any error.
- **Metrics** (when `-metrics-addr` is set):
  - `podman_api_requests_total{host,template,method,status}` — counter
  - `podman_api_request_duration_seconds{host,template,method}` — histogram
  - plus standard Go/process metrics.

### Shipping the audit log

Pick the pattern that matches your supervisor:

**A) Under systemd (default)** — audit lines go to stdout, which systemd routes into journald. Cap journald's on-disk size with a drop-in:

```ini
# /etc/systemd/journald.conf.d/podman-api.conf
[Journal]
SystemMaxUse=2G
MaxFileSec=1day
```

To extract just podman-api's audit lines:
```sh
journalctl -u podman-api -o cat | jq -c 'select(.method)'
```

**B) On-disk file with logrotate** — set `-audit-log-file=/var/log/podman-api/audit.log`, then drop:

```
# /etc/logrotate.d/podman-api
/var/log/podman-api/audit.log {
    daily
    rotate 14
    compress
    missingok
    notifempty
    copytruncate    # podman-api keeps the fd open; copytruncate avoids needing a SIGHUP
}
```

`copytruncate` is the right choice because the binary holds the file open across rotations. If you'd rather not pay the copy cost, switch to a `create` strategy and add a `postrotate` hook that calls `systemctl restart podman-api` (you'll lose in-flight log streams).

**C) External collector (Vector / Promtail / Fluent Bit)** — the simplest setup is to keep audit on stdout and let the collector tail journald. Vector example:

```toml
# /etc/vector/vector.toml
[sources.podman_api]
type = "journald"
include_units = ["podman-api.service"]

[transforms.audit_only]
type = "filter"
inputs = ["podman_api"]
condition = '.message != null && contains(string!(.message), "\"method\":")'

[transforms.parse]
type = "remap"
inputs = ["audit_only"]
source = '. = parse_json!(.message)'

[sinks.loki]
type = "loki"
inputs = ["parse"]
endpoint = "http://loki.internal:3100"
labels = {job = "podman-api", host = "{{ host }}", template = "{{ template }}"}
```

If you'd rather feed a collector via stdin (e.g. Fluent Bit's `tail` input on a file), use pattern **B** above and point the collector at the file.

## API reference

A complete OpenAPI 3.0 spec lives at [`api/openapi.yaml`](api/openapi.yaml) and is also served by the binary itself at `GET /openapi.yaml` (no auth). Paste either the file or the URL into [editor.swagger.io](https://editor.swagger.io/) for a browsable view.

The summary below is the same routes, grouped by what they do.

## API quick reference

```
GET    /healthz                                                      no auth
GET    /metrics                                                      separate listener

GET    /hosts                                                        hosts:read
GET    /hosts/{host}                                                 hosts:read
GET    /hosts/{host}/healthz                                         hosts:read
GET    /hosts/{host}/ports-in-use                                    hosts:read

GET    /templates                                                    instances:read
GET    /templates/{id}                                               instances:read
GET    /templates/{id}/render?<params>                               instances:read

GET    /hosts/{host}/secrets                                         secrets:read
PUT    /hosts/{host}/secrets/{name}      body: {"value": "..."}      secrets:write
DELETE /hosts/{host}/secrets/{name}                                  secrets:write

GET    /hosts/{host}/instances?template=<id>                         instances:read
GET    /hosts/{host}/instances/{template}/{slug}                     instances:read
POST   /hosts/{host}/instances                                       instances:write
PUT    /hosts/{host}/instances/{template}/{slug}                     instances:write
DELETE /hosts/{host}/instances/{template}/{slug}?prune_volumes=&prune_secrets=  instances:write

POST   /hosts/{host}/instances/{template}/{slug}/start               instances:write
POST   /hosts/{host}/instances/{template}/{slug}/stop                instances:write
POST   /hosts/{host}/instances/{template}/{slug}/restart             instances:write
POST   /hosts/{host}/instances/{template}/{slug}/upgrade  body: {"image": "..."}

GET    /hosts/{host}/instances/{template}/{slug}/logs?container=&tail=&follow=
GET    /hosts/{host}/instances/{template}/{slug}/volumes
DELETE /hosts/{host}/volumes/{name}

POST   /migrate   body: {from_host,to_host,template,slug,parameters?} instances:write
POST   /evacuate   body: {from_host, map:{slug:to_host}}              instances:write

GET    /jobs?state=&kind=&parent_id=                                  jobs:read
GET    /jobs/{id}                                                     jobs:read
```

`POST /migrate` and `POST /evacuate` require `-state-db` (else `501`), validate synchronously, and return `202 {job_id}`; poll `GET /jobs/{id}` for progress. For an evacuate, `GET /jobs?parent_id=<id>` lists its child migrations.

The `?skip_pull=true` query on POST/PUT instances skips the pre-pull step, useful for CI or when the image is known to be local.

On DELETE, `prune_volumes` and `prune_secrets` both default to **false** — the pod is removed but its data volumes and per-instance secrets are kept. Pass `?prune_volumes=true&prune_secrets=true` to also reap them. DELETE is an idempotent reconcile: if the pod is already gone, a delete that requests pruning still removes any orphaned volumes/secrets and returns `204` (rather than `404`), so you can clean up after an earlier prune-less delete.

## FAQ

**Why not just use Kubernetes / Nomad / k3s?**
Smaller blast radius. One binary, one socket per host, no clustered control plane, no etcd. The deploy unit is a single Pod per instance, which is much closer to what most "small fleet of services" deployments actually want.

**Why does podman-api need to be on the same network as the podman hosts?**
It doesn't. SSH tunnel to the remote `podman.sock` is the default. Run podman-api wherever your CMS is — it just needs the SSH key.

**How do I add a new template?**
Drop a YAML file into the directory you point `-templates-dir` at, with the `# template-meta:` header. The next inbound `GET /templates` reflects it. The bundled set in `templates/` is the easiest place to copy from.

**How do I rotate a bearer token without dropping log streams?**
Edit `auth/keys.yaml`, then `kill -HUP $(pidof podman-api)`. The middleware re-reads on the next request.

**What happens if the registry is down during Apply?**
Apply pulls every image first. A pull failure aborts before any secret is written, returns 502 with the registry's message, and leaves no orphan state on the target host.

# podman-api

A small REST wrapper around [Podman](https://podman.io)'s [libpod REST API](https://docs.podman.io/en/latest/_static/api.html) that lets a CMS (or any orchestrator) deploy and manage **pods** described by **YAML templates** across a fleet of Podman hosts.

It is opinionated, single-binary, and deliberately narrow: one host group, bearer-token auth, audit log to stdout, Prometheus metrics on a separate listener, kubernetes-style secrets, idempotent applies. Use it when Kubernetes is too much and ad-hoc `podman run` over SSH is too little.

## What it does

- Lists, applies, upgrades, starts/stops, and deletes **pod instances** on remote Podman hosts via SSH-tunneled `podman.sock`.
- Renders parameterised **templates** (Go `text/template` over Kubernetes-style YAML) into Pod manifests, then plays them with `podman play kube`.
- Manages **per-host** and **per-instance secrets** as Kubernetes Secret resources (so `secretKeyRef` works).
- Pre-pulls every container image before play, so a bad tag fails fast with a clear registry error instead of a 30-second timeout.
- Streams **logs** as plain text or SSE.
- Exposes **Prometheus metrics** on a separate, opt-in listener so they aren't world-readable on the public port.

## What it doesn't do

- No multi-replica HA; one daemon, one config tree.
- No image registry, no scheduler, no rolling deploy primitives. (`Upgrade` is single-pod replace-in-place.)
- No webhook callbacks; callers poll.
- No pod migration across hosts; an instance is bound to the host where it was applied.

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

Pure Go, no CGO required.

```sh
go build -o podman-api ./cmd/podman-api
```

For cross-compile to a Linux server from a Mac/Linux dev box:

```sh
GOOS=linux GOARCH=amd64 go build -o podman-api ./cmd/podman-api
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

The defined scopes are `hosts:read`, `hosts:write`, `instances:read`, `instances:write`, `secrets:read`, `secrets:write` (and `instances:*` / `secrets:*` as shorthand).

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

A systemd unit and an opinionated installer live in `contrib/`. See [`contrib/install.sh`](contrib/install.sh) — it creates a dedicated `podman-api` user, installs the binary, and enables the service.

## Security model

- **TLS** is the responsibility of a reverse proxy. The recommended setup is Caddy in front (see `contrib/Caddyfile.example`), terminating Let's Encrypt. The binary itself binds to `127.0.0.1` and never exposes plaintext on a public port.
- **/metrics** is **not** mounted on the main listener. To expose Prometheus, set `-metrics-addr=127.0.0.1:9090` and either scrape it via SSH tunnel or bind to a VPC-internal address. Metrics labels include host IDs, template IDs, and HTTP paths — not safe to expose publicly.
- **Bearer tokens** are stored as Argon2id PHC strings; the plaintext only exists at issuance time and in the client's config.
- **Slug/template/secret name validation** happens at the API edge (see "Templates" above) — bad inputs never reach the renderer or podman.
- **Secrets** flow through the API in the JSON body of `POST /instances`, are stored as Kubernetes Secret objects on the target host, and are zeroed in the in-memory request struct after use.

## Observability

- **Audit log:** every state-changing request (POST/PUT/DELETE) emits one JSON line to stdout with method, path, host, template, slug, status, duration, key_id, and any error.
- **Metrics** (when `-metrics-addr` is set):
  - `podman_api_requests_total{host,template,method,status}` — counter
  - `podman_api_request_duration_seconds{host,template,method}` — histogram
  - plus standard Go/process metrics.

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
DELETE /hosts/{host}/instances/{template}/{slug}?prune_volumes=...   instances:write

POST   /hosts/{host}/instances/{template}/{slug}/start               instances:write
POST   /hosts/{host}/instances/{template}/{slug}/stop                instances:write
POST   /hosts/{host}/instances/{template}/{slug}/restart             instances:write
POST   /hosts/{host}/instances/{template}/{slug}/upgrade  body: {"image": "..."}

GET    /hosts/{host}/instances/{template}/{slug}/logs?container=&tail=&follow=
GET    /hosts/{host}/instances/{template}/{slug}/volumes
DELETE /hosts/{host}/volumes/{name}
```

The `?skip_pull=true` query on POST/PUT instances skips the pre-pull step, useful for CI or when the image is known to be local.

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

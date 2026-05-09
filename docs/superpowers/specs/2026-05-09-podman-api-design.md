# podman-api design

**Date:** 2026-05-09
**Status:** Approved (design); pending implementation plan

## Summary

A small, stateless HTTP API in Go that lets a CMS manage multi-container customer
deployments on a fleet of Podman hosts. Each customer deployment is modelled as
a Podman **Pod** (typically an app container plus a `litestream` sidecar sharing
a SQLite volume), defined by a Kubernetes-style YAML template, and applied via
`podman play kube` over the libpod REST API. The API holds no inventory; the CMS
is the source of truth for desired state, the host is the source of truth for
observed state, and the API translates between them.

## Goals

- Build on canonical Podman primitives (Pods, `play kube`, podman secrets,
  named volumes) — no shell-scripting around `podman-compose` or quadlets.
- Keep the API stateless: no DB, no background reconciler, restart-safe.
- Single-binary deploy with templates embedded.
- Reach remote hosts via SSH-tunnelled rootless `podman.sock` — no new ports
  exposed on hosts, no new credential system.
- Provide a small, predictable REST surface a CMS can drive directly.

## Non-goals

- Auto-placement / scheduling. The caller picks which host an instance lives on.
- Cross-host migration, multi-region failover, blue/green orchestration.
- A vault. The API never persists secret values; it pushes them into Podman
  secrets and forgets them.
- A UI. The CMS owns presentation.
- Customer-facing self-service. v1 trusts the CMS as the only caller.
- Multi-API-instance horizontal scaling. v1 assumes one API process.

## Architecture

```
   CMS ──HTTPS──▶ Caddy/nginx ──HTTP──▶ podman-api
                                            │
                                            └─ SSH tunnel ──▶ host:podman.sock  (×N hosts)
```

- Single central `podman-api` process. Loads `hosts/*.yaml`,
  `templates/*.yaml`, and `auth/keys.yaml` at boot. Reloads on SIGHUP.
- TLS terminates outside the API (Caddy/nginx). The API speaks plain HTTP on
  `127.0.0.1` or a Unix socket.
- The API connects to each host's rootless `podman.sock`
  (`/run/user/1000/podman/podman.sock`) over SSH using the official
  `containers/podman/v5/pkg/bindings` Go client. One pooled SSH session per
  host; reconnect on failure.

## Data model

The API has four entity types. None of them are persisted by the API itself.

| Entity     | Source                 | Identity                          |
|------------|------------------------|-----------------------------------|
| `Host`     | Static config (`hosts/`) | `id` (slug, e.g. `otp-prod-1`)  |
| `Template` | Static config (`templates/`) | `id` (slug, e.g. `lite-engine`) |
| `Instance` | Live on a host         | `(host_id, template_id, slug)`    |
| `Secret`   | Live on a host         | `(host_id, name)`                 |

- `Host` and `Template` are read-only configuration.
- `Instance` and `Secret` exist only on the host and are queried via libpod;
  the API derives their identity from the deterministic naming pattern
  `{template_id}-{slug}` for pods and `{template_id}-{slug}-{name}` for
  per-instance secrets/volumes/configmaps.

## Templates

A template is a Go `text/template` rendering a Kubernetes-style YAML document
(one or more docs separated by `---`) that `podman play kube` applies. Each
template begins with a `# template-meta:` comment block describing its parameter
contract.

### Example: `templates/lite-engine.yaml`

```yaml
# template-meta:
#   id: lite-engine
#   parameters:
#     required: [slug, image, port, base_url, app_template, s3_bucket, s3_endpoint]
#     optional: []
#   secrets:
#     per_instance: [auth_secret]
#     per_host_referenced: [s3-access-key-id, s3-secret-access-key]
#   volumes:
#     - name: data
#       backup: litestream
---
apiVersion: v1
kind: ConfigMap
metadata:
  name: lite-engine-{{.slug}}-litestream
data:
  litestream.yml: |
    # access-key-id and secret-access-key are read from the
    # LITESTREAM_ACCESS_KEY_ID / LITESTREAM_SECRET_ACCESS_KEY env vars,
    # which are injected from per-host podman secrets (see below).
    dbs:
      - path: /data/engine.db
        replicas:
          - type: s3
            bucket: {{.s3_bucket}}
            path: lite/engine/{{.slug}}/
            endpoint: {{.s3_endpoint}}
            retention: 720h
            sync-interval: 10s
---
apiVersion: v1
kind: Pod
metadata:
  name: lite-engine-{{.slug}}
  labels:
    podman-api/template: lite-engine
    podman-api/slug: {{.slug}}
spec:
  restartPolicy: Always
  containers:
    - name: app
      image: {{.image}}
      ports:
        - containerPort: 30000
          hostPort: {{.port}}
          hostIP: 127.0.0.1
      env:
        - name: NODE_ENV
          value: production
        - name: PORT
          value: "30000"
        - name: BASE_URL
          value: {{.base_url}}
        - name: TRUSTED_ORIGINS
          value: {{.base_url}}
        - name: APP_TEMPLATE
          value: {{.app_template}}
        - name: AUTH_SECRET
          valueFrom:
            secretKeyRef:
              name: lite-engine-{{.slug}}-auth
              key: lite-engine-{{.slug}}-auth
      volumeMounts:
        - name: data
          mountPath: /app/data
    - name: litestream
      image: docker.io/litestream/litestream:latest
      args: ["replicate"]
      env:
        - name: LITESTREAM_ACCESS_KEY_ID
          valueFrom:
            secretKeyRef:
              name: s3-access-key-id
              key: s3-access-key-id
        - name: LITESTREAM_SECRET_ACCESS_KEY
          valueFrom:
            secretKeyRef:
              name: s3-secret-access-key
              key: s3-secret-access-key
      volumeMounts:
        - name: data
          mountPath: /data
        - name: litestream-config
          mountPath: /etc/litestream.yml
          subPath: litestream.yml
  volumes:
    - name: data
      persistentVolumeClaim:
        claimName: lite-engine-{{.slug}}-data
    - name: litestream-config
      configMap:
        name: lite-engine-{{.slug}}-litestream
```

### Template-meta semantics

- `parameters.required` / `parameters.optional`: validated against request body.
  Missing required → `400 invalid_parameters`. Unknown keys → `400`.
- `secrets.per_instance`: required in the request body; pushed via
  `podman secret create {template}-{slug}-{name}` before `play kube`. Values
  are zeroed from process memory after the libpod call.
- `secrets.per_host_referenced`: must already exist on the host. The API
  pre-checks via libpod `secret inspect` and fails the create with
  `422 host_secret_missing` if absent.
- `volumes`: informational. The `backup: litestream` flag surfaces in
  `GET /instances/...` so the CMS can warn before destructive deletes.

### Naming convention (deterministic, no state required)

For instance `(template, slug)` on host `H`:

- Pod: `{template}-{slug}`
- PVC / volume: `{template}-{slug}-{volume_name}`
- Per-instance secret: `{template}-{slug}-{secret_name}`
- ConfigMap: `{template}-{slug}-{purpose}`

The API uses these names for every libpod call, so given just `(host, template, slug)`
it can find every resource it owns.

### Stateless variant

Single-container templates (e.g. `templates/google-groups.yaml`) drop the
ConfigMap, the data volume, and the litestream container. Same template-meta
shape, fewer required keys. No special-case code in the API.

## REST API surface

```
# Hosts (read-only inventory of what the API knows about)
GET    /hosts                                          → [{id, addr, status, podman_version, labels}]
GET    /hosts/{host}                                   → {id, addr, status, podman_version, capabilities}
GET    /hosts/{host}/ports-in-use                      → [{port, pod, container}]   helper for CMS
GET    /hosts/{host}/healthz                           → 200 if libpod reachable

# Host-level secrets (bootstrap / rotation)
GET    /hosts/{host}/secrets                           → [{name, created_at}]       names only, no values
PUT    /hosts/{host}/secrets/{name}                    body: {value}  → 204         creates or rotates
DELETE /hosts/{host}/secrets/{name}                                   → 204

# Templates (read-only; what kinds of things can be deployed)
GET    /templates                                      → [{id, parameters, secrets, volumes}]
GET    /templates/{id}                                 → full meta
GET    /templates/{id}/render?param=...                → rendered YAML (dry run)

# Instances
GET    /hosts/{host}/instances?template={id}           → [{template, slug, status, image, ports, started_at}]
GET    /hosts/{host}/instances/{template}/{slug}       → full instance detail (see "Observed shape")
POST   /hosts/{host}/instances                         body: {template, slug, parameters, secrets} → 201
PUT    /hosts/{host}/instances/{template}/{slug}       body: same shape; equivalent to play kube --replace
DELETE /hosts/{host}/instances/{template}/{slug}?prune_volumes=false&prune_secrets=false → 204

# Lifecycle
POST   /hosts/{host}/instances/{template}/{slug}/start
POST   /hosts/{host}/instances/{template}/{slug}/stop
POST   /hosts/{host}/instances/{template}/{slug}/restart
POST   /hosts/{host}/instances/{template}/{slug}/upgrade  body: {image} → pulls new tag, re-applies

# Logs (passthrough)
GET    /hosts/{host}/instances/{template}/{slug}/logs?container=app|litestream&tail=N&since=...&follow=false
       (follow=true returns text/event-stream)

# Volumes (per-instance, observability + cleanup)
GET    /hosts/{host}/instances/{template}/{slug}/volumes
DELETE /hosts/{host}/volumes/{name}                                   → 204

# Process
GET    /healthz
GET    /metrics                                        → Prometheus
```

### Verb semantics

- `PUT /instances/...` is the primary apply verb. Idempotent. Maps to
  `play kube --replace`. Safe to retry.
- `POST /instances` is create-only and 409s if the pod exists. Reserved for
  callers that want strict-create semantics; most callers should `PUT`.
- Lifecycle verbs (`start`/`stop`/`restart`/`upgrade`) are sub-resources, not
  PATCH on the instance. They're imperative actions, intrinsically idempotent.

### Observed shape (instance detail)

```json
{
  "template": "lite-engine",
  "slug": "iotready",
  "pod": {"status": "Running", "created": "...", "id": "..."},
  "containers": [
    {"name": "app",
     "image": "localhost/lite-engine@sha256:abc...",
     "image_tag": "localhost/lite-engine:latest",
     "status": "Running",
     "started_at": "...",
     "restart_count": 0,
     "ports": [{"host_ip": "127.0.0.1", "host_port": 31001, "container_port": 30000}]},
    {"name": "litestream", "...": "..."}
  ],
  "volumes": [{"name": "lite-engine-iotready-data", "size_bytes": 248123456}],
  "env_summary": {"BASE_URL": "https://engine.iotready.com", "APP_TEMPLATE": "crm"}
}
```

Both `image` (digest) and `image_tag` (the resolvable tag) are returned so the
CMS can detect "tag is `:latest` but the image on disk is older than the
registry's." That's the meaningful drift signal.

### Out of v1

- Batch endpoints. The CMS calls N times; partial failures are clearer when
  each call is independent.
- Server-pushed events stream. Request/response only.
- Template upload via API. Templates ship with the binary.
- `exec` into containers.

## Transport & auth

### API → host

`bindings.NewConnection(ctx, "ssh://ubuntu@otp-prod-1/run/user/1000/podman/podman.sock")`.
SSH key trust is reused; no new credentials. One pooled connection per host.

`hosts/otp-prod-1.yaml`:

```yaml
id: otp-prod-1
addr: ubuntu@otp-prod-1
socket: /run/user/1000/podman/podman.sock
ssh_key: /etc/podman-api/ssh/otp-prod-1   # optional; defaults to ssh-agent
labels: {env: prod, region: in}
```

### CMS → API

Bearer token in `Authorization` header. `auth/keys.yaml`:

```yaml
keys:
  - id: cms-prod
    secret_hash: $argon2id$...
    scopes: [hosts:read, instances:*, secrets:*]
    description: "CMS production"
```

Argon2id for hashing. Scopes are coarse: `hosts:read`, `instances:read`,
`instances:write`, `secrets:read`, `secrets:write`. The CMS can hold a
least-privileged token; a separate read-only token for dashboards needs no
code change.

### TLS

Outside the API. Caddy/nginx in front. The API binds 127.0.0.1 by default.

### Audit log

Every state-changing request emits one structured JSON line to stdout:
`{ts, key_id, method, path, host, template, slug, status, duration_ms, error?}`.
No request bodies (they contain secrets).

## Operational concerns

### Idempotency

- `PUT /instances/...` ↔ `play kube --replace`: safe to retry.
- `POST /instances`: 409 if exists.
- Lifecycle verbs: idempotent at the libpod layer.

### Concurrency

Per-`(host, template, slug)` mutex around any state-changing operation,
in-process. Fine for the v1 single-API-instance assumption. Multi-instance
deployment requires a per-host advisory lock (out of scope for v1).

### Errors

JSON only. Stable `code` enum. HTTP status reflects category; `code`
distinguishes specifics.

```json
{
  "code": "instance_not_found",
  "message": "no instance lite-engine/foo on host otp-prod-1",
  "details": {"host": "otp-prod-1", "template": "lite-engine", "slug": "foo"}
}
```

Initial code set: `invalid_parameters`, `unknown_template`, `unknown_host`,
`instance_not_found`, `instance_already_exists`, `host_secret_missing`,
`host_unreachable`, `host_timeout`, `libpod_error`, `internal`.

### Timeouts

- `play kube`: 120s
- `pod start/stop/restart`: 30s
- `image pull` (during upgrade): 300s
- `logs` (non-follow): 30s
- `logs` (follow): no timeout; client disconnects to end

Configurable per host. Past deadline → HTTP 504 with `code: host_timeout`.

### Readiness

The API does not poll for app health. `POST/PUT instances` returns once
`play kube` returns, meaning containers are *created and started*. The CMS
polls `GET .../instances/...` if it wants to wait for "Running" state.

### Observability

- `/metrics` (Prometheus): request count/duration by route+status, libpod
  call latency by host+verb, SSH connection state per host.
- Structured stdout logs (audit + general).
- No tracing in v1.

## Project layout

```
podman-api/
├── cmd/podman-api/main.go
├── internal/
│   ├── config/                      # load hosts/, templates/, auth/keys.yaml
│   ├── auth/                        # bearer-token middleware, scope checks
│   ├── podman/                      # only package importing libpod bindings
│   ├── render/                      # text/template → YAML; metadata extraction
│   ├── instance/                    # orchestration: locking + render + podman
│   ├── api/                         # HTTP handlers, routing, error mapping
│   └── obs/                         # /metrics, /healthz, audit log
├── templates/                       # embedded via go:embed
│   ├── lite-engine.yaml
│   ├── lite-crm.yaml
│   └── google-groups.yaml
├── docs/superpowers/specs/
│   └── 2026-05-09-podman-api-design.md
├── go.mod
└── Makefile
```

### Boundary discipline

- `internal/podman` is the only package importing libpod bindings. Other
  packages depend on a `podman.Client` interface.
- `internal/render` is pure: parameters in, YAML out. No I/O, no time, no
  globals. Trivially golden-tested.
- `internal/instance.Service` composes `render` + `podman` + locking. HTTP
  handlers stay thin.
- `internal/api/errors.go` owns the `code` enum. Other packages return typed
  errors; the API layer translates to JSON+status. Other packages never
  reach for HTTP types.
- Templates are embedded with `go:embed`. A `--templates-dir` flag overrides
  for dev iteration without recompile.

Informal LOC budget: any file in `internal/api` over ~300 lines or
`internal/podman` over ~400 is a signal to split.

## Testing strategy

### Ring 1 — unit tests (the bulk)

- `internal/render`: golden-file tests per template
  (`testdata/lite-engine/inputs.yaml` → `expected.yaml`). Parameter-validation
  tests for missing required, unknown keys, wrong types.
- `internal/api`: handler tests with a fake `podman.Client`. Asserts on
  HTTP status, JSON shape, error codes. No network.
- `internal/auth`, `internal/config`: small focused tests.

### Ring 2 — integration tests

`make test-integration`. Requires a local rootless podman.sock. Spins up the
API in-process pointed at `localhost`, drives it via HTTP:

1. Apply a small test template (single container, no volume).
2. Assert pod runs.
3. Hit logs, restart, stop, delete.
4. Verify podman state between steps.

Tagged `//go:build integration` so they don't run on every `go test ./...`.

### Ring 3 — smoke test

`make smoke HOST=otp-staging-1`. Applies a throwaway template, runs the
lifecycle, deletes. Manual / pre-release only.

### Explicitly not tested

- Litestream actually replicating to S3 (litestream's contract).
- The CMS (mocked with `curl` examples in `docs/`).
- Podman itself (if `play kube` is broken, that's a podman bug).

## Open questions for implementation

- Specific Go HTTP router (`chi` vs `net/http` 1.22+ with the new mux). Lean
  toward stdlib if path patterns suffice; `chi` if we want middleware ergonomics.
- Argon2id parameters for token hashing. Use defaults from `golang.org/x/crypto/argon2`
  unless we need stronger.
- Confirm `podman play kube` honors `secretKeyRef` against podman-native
  (flat) secrets with `name == key`. If not, fall back to mounting each secret
  as a single-file volume at a per-secret `mountPath`.
- Reload model: SIGHUP for config reload vs full restart. SIGHUP is nicer but
  needs a brief connection drain story.
- Verify the `containers/podman/v5/pkg/bindings` Go client interoperates with
  podman 4.9.x server (otp-prod-1 currently). If not, pin to v4 bindings.

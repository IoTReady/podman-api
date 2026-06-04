# Ingress + Auto-TLS via Per-Host Caddy — Design

**Issue:** #60 (Slice 1, open-core PaaS roadmap — #69)
**Date:** 2026-06-04
**Status:** Design approved; spike pending before implementation.

## 1. Context

podman-api is a multi-host control plane: it reaches each podman host over an
SSH-tunnelled libpod socket and drives pods/volumes/networks via the `podman/v5`
bindings. Apps are deployed as pods identified by `(host, template, slug)` and
their desired state is persisted in an encrypted SQLite spec-store. Today there
is **no ingress, reverse-proxy, or TLS** in the codebase — the README defers TLS
to "a reverse proxy you run yourself (Caddy in front)."

This design makes ingress + auto-TLS a first-class, turnkey capability: an
operator deploys an app with a domain and gets `https://app.example.com` serving
it, with an auto-provisioned Let's Encrypt certificate and nothing exposed on the
host but the proxy.

This is the roadmap's flagged **long pole** ("multi-host TLS termination is the
highest-uncertainty design point") — so a throwaway spike (§8) runs first.

## 2. Decisions (locked during brainstorm)

All hosts are **publicly reachable** (each can open `:80/:443` to the internet).
That constraint drove every choice below.

| Decision | Choice | Why |
|---|---|---|
| **TLS termination** | **Per-host managed Caddy** (topology B) | Hosts are public, so TLS terminates next to the app. Keeps the control plane out of the app data path (no SPOF/bottleneck); each host runs its own ACME, so there is **no central cert store and no multi-host cert sync** — which is exactly the risk the roadmap called out. |
| **Config push** | Generate a **Caddyfile**, `podman cp` it into Caddy's config volume, reload via `podman exec` | Reuses the existing podman socket. No second SSH tunnel, no sensitive admin endpoint to secure on every host. Config is inspectable and persisted; Caddy reload is graceful. |
| **Backend wiring** | **Shared podman ingress network** per host; apps expose **no host ports** for HTTP | Only Caddy publishes `:80/:443`. Apps are reachable solely through Caddy via podman's internal DNS. Closes the TLS-bypass hole entirely (no raw app port on the public IP). |
| **DNS** | **Operator-managed** for v1; `DNSProvider` seam left for later automation | Avoids provider-credential handling now. Caddy retries ACME until DNS resolves. Automation / DNS-01 wildcard is a clean future (and commercial) attach point. |
| **ACME** | Caddy does **HTTP-01** on `:80` automatically; certs + account persisted in a per-host data volume | Restarts don't re-issue and hit Let's Encrypt rate limits. |

This **supersedes** the roadmap's earlier "embedded Caddy (library)" decision,
which only made sense for a central gateway; with public hosts, per-host wins.

## 3. Scope

**In scope (v1):**
- Deploy an app with one or more domains → auto HTTPS.
- Per-host Caddy pod lifecycle, ingress network, Caddyfile generation + reload.
- Cert provisioning via Caddy (HTTP-01), persisted across restarts.
- Route derivation + validation; reconcile on deploy/delete/upgrade + periodic
  drift correction.
- Extension seams: `IngressController`, `DNSProvider`.

**Out of scope (YAGNI for v1):**
- DNS automation, DNS-01, wildcard certs (seam left, not implemented).
- Path-based routing, multi-backend load balancing.
- Non-HTTP (TCP/UDP) ingress.
- The admin UI surface (Slice 1 UI work, separate).
- TLS for the control-plane API itself (independent concern).
- Bind mounts / external data (tracked separately under #73).

## 4. Components

Each has one job and leans on primitives podman-api already drives.

### 4.1 Ingress network (per host)
A reserved podman network (default name `podman-api-ingress`, configurable). Caddy
and every ingress-fronted app pod attach to it. Podman's netavark/aardvark DNS
resolves names so Caddy can target `reverse_proxy <app-container>:<port>`.

### 4.2 Caddy system pod (per host)
A reserved, podman-api-managed pod running a pinned Caddy image. Publishes
`:80/:443` on the host's public IP. Mounts:
- a **config volume** — holds the generated `Caddyfile`;
- a **data volume** — holds ACME certs + account (persisted; never reaped).

Treated as a special **system instance**, reconciled into existence on any host
that has ≥1 route, but kept distinct from user instances (reserved
template/slug, never listed as a user app, never user-deletable).

### 4.3 Ingress controller (in podman-api)
Behind an `IngressController` interface. Default `CaddyController`:
- `EnsureProxy(host)` — ensure the ingress network and Caddy pod exist and run.
- `Apply(host, routes)` — render the Caddyfile, validate it, `podman cp` it into
  the config volume, exec `caddy reload`.

The interface is the **commercial seam** — an alternative controller can be
swapped without touching the deploy path.

## 5. Data model

**Template** gains optional ingress metadata declaring which container+port serves
HTTP:
```yaml
ingress:
  container: web   # container in the pod that serves HTTP
  port: 8080       # container port (NOT a host port)
```
A template with no `ingress:` block is non-web (databases, workers) and never
gets a route.

**Spec / instance** gains operator-supplied domains, persisted in the encrypted
spec-store alongside existing fields, so it survives restarts and **travels with
migrate/evacuate** automatically:
```go
type Spec struct {
    // ...existing: Host, Template, Slug, Parameters, Secrets, Created, Updated
    Domains []string // e.g. ["app.example.com"]
}
```

**Route** is **derived, never stored** — for each host the controller walks that
host's instances:
```
route = { domain ∈ instance.Domains  →  reverse_proxy <instance-container>:<template.ingress.port> }
```
One source of truth; no drift between a route table and what is actually deployed.

**Validation:**
- Domains must be unique across the host (no two apps claim the same domain).
- A domain on an instance whose template has no `ingress:` block → `400` at deploy.
- `domains` supplied while `-ingress-enabled=false` → rejected with a clear error
  (never silently dropped).

## 6. Control flow

**Deploy an app with a domain:**
1. `POST .../instances` with `domains: [...]`. Validate (template has `ingress:`,
   domains unique on host).
2. Create the pod **attached to the ingress network**, with no host port for the
   HTTP container.
3. Controller `EnsureProxy(host)` — create network + Caddy pod on first use.
4. Recompute the host's Caddyfile from all instances → `podman cp` → `caddy reload`.
5. Operator points DNS at the host IP. On first hit Caddy obtains the cert via
   HTTP-01. Status goes green once the cert is issued **and** the backend is
   reachable.

**Delete / stop:** recompute the host's Caddyfile without that instance, reload.
When the last route on a host is gone, the Caddy pod **stays running** (idle, cheap)
to keep the cert volume warm.

**Reconciliation:** the Caddyfile is a pure function of the host's instances, so
the controller runs (a) inline on create/delete/upgrade and (b) a **periodic
drift-correction reconcile** (same pattern as the jobs runner) that re-renders and
reloads if reality has drifted (host reboot, manual change). Idempotent by
construction.

**New flags** (wired in `main()` like `-migrate-verify-timeout`):
`-ingress-enabled`, `-ingress-network`, `-ingress-caddy-image` (pinned),
`-ingress-acme-email`.

## 7. Seams & migrate/evacuate interaction

**Seams (gate Model 1 — built now though commercial is later):**
- `IngressController` — default `CaddyController`; a commercial controller (central
  edge, richer proxy) can replace it.
- `DNSProvider` — default **no-op** (operator manages DNS). The attach point for
  Cloudflare/Route53 automation or DNS-01 wildcard later.

**Migrate / evacuate** (the sharp edge — #54 is touching that code concurrently):
- `Domains` lives in the Spec, so it **moves with the instance for free**; no new
  copy logic in migrate.
- After a successful migrate, ingress reconcile runs on **both** hosts: add the
  route on the destination, remove it on the source. This is an added reconcile
  *call after* migrate completes — it does **not** modify migrate's core, so it
  stays decoupled from the in-flight #54 work.
- **DNS cutover is an operator step:** the record points at the old host until the
  operator repoints it, so the app is not reachable on the new host until DNS
  updates *and* the new host's Caddy issues a cert. v1 **documents** this (migrate
  surfaces the new host IP); it does **not** automate DNS failover.

## 8. De-risking spike (run FIRST, against a real host)

The roadmap wants a throwaway spike for this long pole. Against the provisioned
podman host, prove the three assumptions the design rests on:
1. Podman network DNS resolves a **pod/container name** so `reverse_proxy name:port`
   works (verify exactly what name a pod's container is reachable as).
2. Caddy in a container completes **HTTP-01** with `:80` published and **persists**
   certs across a pod restart via the data volume.
3. `podman cp` + `caddy reload` over the **bindings** (not the CLI) behaves as
   expected.

If any assumption fails, revisit the design before writing production code.

## 9. Error handling & edge cases

- **Invalid rendered Caddyfile** → `caddy validate` (exec) **before** reload; on
  failure keep the last-good config and surface the error. Never reload a broken
  Caddyfile.
- **ACME failure** (DNS not pointed, `:80` blocked) → Caddy retries; the route
  exists, status reports "cert pending." Not a deploy failure.
- **Domain collision on a host** → rejected at deploy (`400`).
- **Caddy crash / host reboot** → periodic reconcile restores it; cert volume
  persists, so no re-issue.
- **`-ingress-enabled=false`** → inert; `domains` on deploy rejected clearly.
- **Concurrent deploys on a host** → per-host reconcile serialized with a mutex
  (same shape as the SQLITE_BUSY write-mutex from #54/#70).

## 10. Testing

- **Spike** (§8) first, on a real host.
- **TDD units:** Caddyfile renderer (pure function, table tests); route derivation
  + validation; controller reconcile against the existing `podman.Mock`.
- **Integration test** behind the `integration` build tag (aligns with #53's style):
  deploy a web template with a domain, assert the route is live and a cert is
  obtained against a local ACME (e.g. an in-cluster step CA / Pebble).

## 11. Open questions for the plan

- Exact reachable name for a pod's HTTP container on the ingress network
  (resolved by the spike).
- Whether the Caddy system pod is modelled as a real `Spec`/template entry or a
  purely synthetic reserved instance (leaning synthetic — not user-visible).
- Periodic reconcile interval + whether it shares the jobs-runner worker pool or
  runs on its own ticker.

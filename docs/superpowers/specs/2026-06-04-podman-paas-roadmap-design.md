# podman-api → Open-Core Podman PaaS: Program Roadmap

**Date:** 2026-06-04
**Status:** Approved (program-level roadmap; each phase gets its own brainstorm → spec → plan)
**Author:** Tej + Claude (brainstorm)

## 1. Context

`podman-api` today is a production-grade **stateful pod orchestrator API** in Go (~12k LoC):
bearer auth + scopes, YAML templates with idempotent applies, an async jobs runner,
cross-host cold-copy migrate/evacuate, an encrypted SQLite spec store, podman bindings
(libpod v5 over SSH/unix), audit logging, and host-level Prometheus metrics. It has **no UI,
no ingress, no app catalog, and no per-app metrics/log aggregation**.

This roadmap turns that base into a two-tier product without coupling to any other codebase.

## 2. Product strategy

Two tiers, an **open-core** split:

- **OSS tier — a genuinely useful, standalone podman PaaS (CapRover-class).** Not a crippled
  teaser. A single system someone would choose to manage podman hosts and containers.
  License leans **AGPL/GPL** (deter SaaS moochers; copyright retained for dual-licensing).
- **Commercial tier — an opinionated SQLite-application platform + premium ops.** The only
  paid offering foreseen.

**`../engine` is NOT coupled in.** Engine remains an independent product and is the *first
customer* of the Layer-2 SQLite platform — not the admin/UI layer. (Engine is Bun/React;
coupling it was explicitly rejected.)

### 2.1 Feature boundary (OSS vs. Commercial)

| Capability | Tier |
|---|---|
| Core orchestrator API (shipped) | OSS |
| Admin UI (host/instance management, deploy) | OSS |
| App catalog + one-click deploy | OSS |
| Ingress + auto-TLS (Caddy, Let's Encrypt) | OSS |
| Live log **tail** | OSS |
| Metrics **dashboards** (per-app + host display) | OSS |
| Scheduled **backups + one-click restore** (general volumes) | OSS |
| Generic **app-readiness healthchecks** | OSS |
| Scheduled `podman prune` / disk cleanup | OSS |
| Single-operator auth | OSS |
| Litestream continuous replication | Commercial |
| Log **query / archive / retention** | Commercial |
| Alerts & thresholds | Commercial |
| Multi-user **RBAC** | Commercial |
| Private registry | Commercial |
| SQLite-specific health (WAL/replication lag) + PITR restore | Commercial |

(The commercial list may grow.)

## 3. Architectural principles

1. **One standalone system.** Single Go binary for the OSS tier. UI assets and Caddy embedded;
   no external SPA build, no separate UI process, no Postgres/Redis. The commercial tier runs
   as a *second process on the same host* (still "one box").
2. **Ingress = Caddy**, embedded as a Go library — its admin API + on-demand TLS drive dynamic
   domain→instance routing.
3. **UI = HTMX + PureCSS**, server-rendered Go `html/template`, served from `embed.FS`.
   Progressive enhancement, no client build pipeline.
4. **Leave commercial seams (gate Model 1).** Even though commercial is built last, every OSS
   subsystem in Slices 1–3 exposes a clean extension point so the commercial tier later attaches
   as a **separate control-plane process** over a stable API — never by forking the core. This
   keeps the AGPL boundary clean (arm's-length API use is not a derivative work). Seams to bake in:
   - **Auth** behind a pluggable interface (single-operator now → RBAC later)
   - **Log streaming** behind a *sink* interface (tail now → archive/query later)
   - **Metrics** emitted on a bus/stream (display now → alerts later)
   - **Image source** behind a *resolver* (public registries now → private registry later)
5. **Each phase ships an end-to-end, demoable story** (vertical slices), not horizontal infra
   layers.
6. **Observability: instrument with OpenTelemetry, egress via a config toggle.** Instrument once
   with OTel (metrics + logs; traces later) and make the destination pluggable behind the
   metrics-bus / log-sink seams — *not* a global commitment to one transport. **OSS default:**
   local Prometheus `/metrics` exposition + structured stdout logs (zero backend; feeds the
   in-binary UI dashboards; works with users' existing Prometheus/Grafana and `podman logs`).
   **Commercial / hosted ops:** flip the toggle to **OTLP push** to a hosted backend — no inbound
   exposed endpoint. Don't go OTLP-only in OSS: the UI needs a *local* metrics source, the
   self-hoster audience is Prometheus-shaped (OTLP-only would force them to run an OTel Collector),
   and `curl /metrics` is zero-config debuggability.

## 4. Roadmap

Sequencing chosen: **vertical slices**. Each slice is independently lovable and demoable.

### Phase 0 — Base hardening + host protection (trustworthy base)
Make the existing base production-solid and protect hosts already in production, before piling
on features. No UI dependency.
- ~~Land in-flight PRs: **#56** (finish-ctx), **#57** (jobs pagination/retention).~~ ✅ Merged
  (closes #44/#45/#51).
- **#52** migrate/evacuate/job metrics · **#53** evacuate e2e integration test · drain **#54**
  backlog (readiness gate, volume-copy integrity check, evacuate-concurrency flag, dry-run preview).
- **Host-health automation** (pulled forward): scheduled `podman prune` / disk cleanup with safe
  policies, as a headless capability (UI surfaces it in a later slice).
- *(License NOT applied yet — repo stays private through Slice 3; see §5.)*

**✅ Done when:** open backlog cleared, hosts self-clean on a schedule, base is release-quality.

### Slice 1 — The deploy loop (the lovable core)
"Zero → an app on an auto-TLS domain, from a dashboard, no CLI."
- **Ingress + auto-TLS** (own spec; the long pole) — Caddy embedded, domain→instance routing,
  Let's Encrypt.
- **App catalog** — registry of curated templates + deploy wizard (params/secrets form).
- **Admin UI shell** — single-operator login, host list, instance list/detail, deploy flow;
  HTMX + PureCSS, embedded.
- *Seams introduced:* pluggable **auth**, image-source **resolver**.

**✅ Done when:** a new user connects a host, deploys from the catalog, gets
`https://app.example.com` auto-provisioned, and sees it live — entirely from the UI.

### Slice 2 — Observe (trustworthy to run)
- **Per-container metrics** collection (cAdvisor-style) + dashboards in the UI.
- **Live log tail** in the UI — behind a *sink* interface.
- **Generic healthchecks / readiness probes** — closes the #54 `waitRunning` gap; gate
  deploys/migrations on readiness; red/green status in the UI.
- *Seams introduced:* metrics **bus**, log **sink**.

**Observability architecture (see §3.6):** OTel instrumentation; egress is a config toggle. OSS
ships local `/metrics` + stdout (feeds the UI); commercial pushes OTLP to a hosted backend.
Two planes stay distinct: **self/control-plane** metrics (#52 — podman-api's own jobs/migrate/
evacuate; low-volume, may be satisfied by structured events on the same pipeline rather than a
separate metrics path) vs. **hosted-workload** metrics (#63 — per-container resource usage).
Candidate hosted backends (verify pricing): Grafana Cloud (least migration), SigNoz/Uptrace
(OTLP-native).

**✅ Done when:** per-app + host CPU/mem/disk graphs, log tail, health status; deploys block
until ready.

### Slice 3 — Protect (data + hosts safe) — completes OSS v1
- **Scheduled backups + one-click restore** (general volumes, on the export/import primitive).
- (Host-health automation already shipped in Phase 0; surface it in the UI here.)

**✅ Done when:** backups run on a schedule and restore in one click.
**→ Apply AGPL/GPL license + public-readiness (LICENSE/headers/CONTRIBUTING/SECURITY) and go public.**

### Slice 4 — Commercial boundary + Layer 2 (SQLite platform)
Its own program; decomposed in a later brainstorm.
- **First:** gate-architecture spike — stand up the **separate control-plane process** against the
  seams from Slices 1–3; validate the AGPL-clean boundary before any commercial feature exists.
- Then: Litestream replication · log query/archive/retention · alerts/thresholds · multi-user RBAC ·
  private registry · SQLite-specific health + PITR.
- **Engine** becomes the first integration/customer.

## 5. Key decisions (locked this session)

- **No engine coupling** — engine is a customer of Layer 2, not the admin layer.
- **Sequencing = vertical slices.**
- **Ingress/TLS = Caddy** (embedded library).
- **UI = HTMX + PureCSS**, server-rendered, embedded.
- **Gate model = Model 1 / "leave seams"** — commercial attaches as a separate control-plane
  process; OSS Slices 1–3 expose clean extension points.
- **License timing** — stay private through Slice 3; apply AGPL/GPL and go public at OSS v1.
- **Host-health automation** — pulled into Phase 0.

## 6. Deferred to each phase's own brainstorm

- **Ingress (Slice 1):** Caddy routing model; **where TLS terminates across *multiple* podman
  hosts** (central gateway vs. per-host proxy) — the single biggest open unknown; cert storage.
- **UI (Slice 1):** session/auth model for the single operator; template/partials structure;
  how HTMX fragments map to the existing JSON API.
- **Metrics (Slice 2):** collection mechanism (cAdvisor sidecar vs. podman stats vs. cgroup
  reads) and storage (embedded TSDB vs. rely on Prometheus).
- **Backups (Slice 3):** snapshot consistency for running volumes; retention policy in OSS vs.
  commercial.
- **Gate architecture (Slice 4):** concrete control-plane API surface + event/stream transport.

## 7. Risks

- **Multi-host TLS termination** is the highest-uncertainty design point; tackle it first in the
  ingress spec, possibly with a throwaway spike.
- **Seam discipline** — if Slices 1–3 skip the extension seams, Slice 4 becomes a rewrite.
  Treat the seams as acceptance criteria, not nice-to-haves.
- **UI is a new muscle** for this repo (no frontend today); HTMX keeps it small, but budget
  for it.
- **Scope creep into the commercial list** — keep the OSS tier genuinely complete so it stands
  alone; resist moving OSS features behind the paywall.

## 8. Next step

Begin **Phase 0** (no new brainstorm needed — it's draining a known backlog + the prune feature).
Each subsequent slice opens with its own brainstorm → spec → plan. The first net-new design work
is the **ingress/TLS spec** at the start of Slice 1.

Related issues: #12 (go-live tracking), #17 (docs), #52, #53, #54. (PRs #56/#57 already merged.)

# Registry browser & upgrade-picker (phase 1)

## Problem

Operators have no way to see what's actually in the container registry
(`100.64.0.23:5000`, plain Docker Registry v2, no auth today) except by
hand-crafting `curl` calls, and no way to upgrade an instance to a specific
image without knowing its digest ahead of time (currently: SSH in, resolve a
digest by hand, then call `upgrade-image` or `scripts/roll.py`).

Separately, the only thing that prunes the registry today is
`registry-gc.sh`, a hand-installed script on `otp-infra-1` run by a bare
systemd timer, with no visibility into the podman-api control plane's actual
in-use digests. It has already caused two near-miss incidents (pro #64, #65):
one run deleted manifests for 11 of 24 live Engine instances, and a
fleet-wide digest consolidation came within one scheduled run of taking out
the *entire* fleet's recreatability, because the script's notion of "in use"
was a heuristic (tag-name pattern matching) rather than the control plane's
actual state.

## Scope

This spec covers **phase 1 only**: a read-only registry browser plus an
upgrade-picker wired into the existing instance upgrade flow. It does **not**
cover prune jobs or retiring `registry-gc.sh` — those are phases 2 and 3,
tracked separately, and depend on phase 1's client existing first.

Out of scope for phase 1:
- Any delete/prune capability (registry mutation of any kind).
- Per-host registries (one global registry address for the whole fleet).
- Bearer-token auth (only `none` and HTTP Basic — see "Auth" below).

## Package: `internal/imgregistry`

Named `imgregistry`, not `registry`, to avoid colliding with the existing
`jobs.Registry` (job-kind registry) — a plain `registry` package name next to
that would be a constant source of import-aliasing and reading confusion.

A small Docker Registry v2 HTTP client:

```go
type Client interface {
    Catalog(ctx context.Context) ([]string, error)
    Tags(ctx context.Context, repo string) ([]TagGroup, error)
    Manifest(ctx context.Context, repo, ref string) (Manifest, error)
}

type TagGroup struct {
    Digest  string
    Tags    []string  // all tags pointing at this digest
    Created time.Time
    Size    int64
}
```

- `Catalog` paginates `/v2/_catalog?n=<page>` explicitly rather than
  requesting an unbounded `n=`. This registry is known to return an **empty
  list with HTTP 200** for `n=` values above ~1000 — indistinguishable from
  "no repos" unless the client treats that as a possible-truncation signal,
  not a definitive empty result. `Catalog` must follow the `Link` header for
  pagination rather than ever relying on a single large `n=`.
- `Tags` calls `/v2/{repo}/tags/list`, then for each **unique digest**
  (multiple tags commonly collapse onto one digest — e.g. a bare-SHA CI tag
  and `latest`) fetches the manifest for `Docker-Content-Digest` and total
  layer size, and the config blob (`/v2/{repo}/blobs/{digest}`) for the
  `created` field. This is one blob fetch per unique digest, not per tag.
- No caching layer in phase 1 — current post-GC repo tag counts (7-13) make
  synchronous fetch-per-request cheap enough. Revisit with an in-memory TTL
  cache only if a repo's tag count grows large again.
- Auth: `none` (default) or HTTP Basic (username/password). Bearer-token
  auth (Docker Hub/GHCR/ECR-style, requiring a separate token-issuing
  service) is explicitly out of scope — this registry has no such service,
  and nothing in this design needs to browse a third-party registry.

Tests: table-driven against `httptest.Server`, covering catalog pagination
(including the empty-200-above-n trap as a named regression test), tag
grouping by digest, and manifest/blob parsing.

## Config

Follows the existing CLI-flag convention in `server/server.go` (same
pattern as `-prune-*`, `-spec-key-file`) rather than a new settings
subsystem:

```
-registry-address string   e.g. 100.64.0.23:5000 (empty = feature disabled)
-registry-auth string      none|basic (default none)
-registry-username string
-registry-password string
```

If `-registry-address` is empty, the registry routes 404 and the UI nav item
does not render — mirrors the existing "the collector is absent, not just
empty" convention used for the inventory poller.

## API routes

New routes in `internal/api`, guarded by the existing `instances:read` scope
(browsing images is informational, same class as `GET .../instances`):

```
GET /registry/repos                          -> []string (catalog)
GET /registry/repos/{repo}/tags              -> []TagGroup
GET /registry/repos/{repo}/manifests/{ref}   -> Manifest (single-item detail)
```

No new write route for upgrades. The upgrade-picker's confirm step calls the
**existing** `POST /hosts/{host}/instances/{template}/{slug}/upgrade-image`
route with the chosen `repo@sha256:...` ref.

Error handling:
- Registry unreachable or non-2xx from it -> `502`-class error, surfaced in
  the UI as "registry unreachable" rather than an empty list. An empty
  result must never be silently indistinguishable from a failed fetch — the
  same fail-closed principle `registry-gc.sh`'s remediation (pro #64/#66)
  already established for this registry.
- Repo not found -> `404` with a clear message (covers a typo, or an
  instance whose image predates a repo rename).

## UI

- New nav item "Registry": repo list -> per-repo tag/digest table (tag
  group, digest, created, size), sorted newest-first. A digest with several
  tags renders as one row with a comma-separated tag list (e.g. `latest,
  v1.2.3, 8d5f281`) rather than one row per tag, so the CalVer/SHA/branch-tag
  overlap already present in this fleet's tags reads as one image, not three.
- Instance detail page gains an "Upgrade image" action:
  1. Derives the repo from the instance's current `image` parameter by
     stripping the tag/digest suffix (same resolution `scripts/roll.py`
     already does against the template body).
  2. Opens a picker backed by `GET /registry/repos/{repo}/tags`.
  3. On confirm, calls the existing `upgrade-image` route with the chosen
     digest.

This reuses the existing UI page/handler-test conventions
(`internal/ui/handlers_*.go` + `handlers_*_test.go`) rather than introducing
a new pattern.

## Testing

- `internal/imgregistry`: unit tests against `httptest.Server` (catalog
  pagination incl. the empty-200 trap, tag grouping, manifest/blob parsing).
- `internal/api`: handler tests using a fake `imgregistry.Client` (mirrors
  the existing `podman.fake` pattern).
- `internal/ui`: handler tests for the new registry pages, plus a focused
  unit test for the upgrade-picker's repo-derivation logic (the one piece of
  business logic worth testing in isolation, since a wrong derivation would
  silently point the picker at the wrong repo).

## Relationship to phases 2-3 (not in this spec)

Phase 2 (in-use-aware prune jobs) and phase 3 (retiring `registry-gc.sh`)
depend on `imgregistry.Client` existing, but add no requirements back onto
phase 1 — the client's read-only surface (`Catalog`/`Tags`/`Manifest`) is
sufficient for a future prune job to build on, and no delete-capable method
is added until that phase is actually designed.

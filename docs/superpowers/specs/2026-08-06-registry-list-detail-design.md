# Registry browser: enriched list + manifest detail view

## Problem

Phase 1 (docs/superpowers/specs/2026-08-06-registry-browser-design.md, OSS
PR #217/#218) shipped a bare repo list (names only) and a per-repo tag table
(tag/digest/created/size). Two gaps:

1. The repo list carries no metadata — an operator has to click into every
   repo to learn how many tags it has, how big it is, or when it was last
   pushed to.
2. There is no detail view for a single image: the tag table's digest is a
   plain string, not a place to see layers, the config digest, or a
   copy-pasteable pull reference.

## Scope

This spec covers both, as one small follow-up phase (not phase 2/3 from the
original design — those are #219/#220, tracked separately and unrelated to
this UI work).

Explicitly out of scope, deferred to land alongside #219 (in-use-aware
prune): a "used by these instances" section on the detail view. That needs
the same in-use-digest fleet scan #219 already has to build (resolve every
instance's running image to a digest, cross-reference against the
registry) — building a separate, cruder version now would mean writing the
same logic twice.

## 1. Repo list: instant count, lazy size/last-pushed

**The cost problem.** Tag count is cheap: a single (possibly paginated)
`GET /v2/{repo}/tags/list` call, no manifest/blob resolution. Size and
last-pushed are NOT cheap — they only exist after `Tags()`'s full
per-digest manifest+blob resolution, which took ~21s for the fleet's
`engine` repo (~1000 tags, 737 unique digests) even with phase-1's
bounded-concurrency fix. Calling `Tags()` for all ~26 catalog repos
synchronously before rendering the list page would make the page as slow
as its single slowest repo.

**Design:**

- New method on `imgregistry.Client`: `TagCount(ctx, repo) (int, error)` —
  a thin wrapper around the existing (already-private) `listTags()`. No
  manifest/blob resolution, so it stays cheap even for `engine`.
- `registryRepos` (the `GET /ui/registry` handler) calls `Catalog()`, then
  fans `TagCount` out across every repo concurrently — one goroutine per
  repo, each writing only its own slice index, mirroring the exact pattern
  already used by `dashboard`'s per-host fan-out in
  `internal/ui/handlers_hosts.go` (unbounded goroutines is fine here: ~26
  repos, same order of magnitude as the host count that pattern already
  handles). Each goroutine gets its own `context.WithTimeout` off the
  request context, same as `dashboard`'s `hostFetchTimeout`. A repo whose
  count fails to resolve renders `?` in that cell rather than failing the
  whole page.
- The rendered list therefore shows real counts on first paint. Size and
  last-pushed render as a `loading…` placeholder cell with
  `hx-get="/ui/registry/{repo}?stats=1" hx-trigger="load"` — each row
  fetches its own stats independently after the page loads, so one slow
  repo never blocks the others or the initial paint.
- `?stats=1` reuses the existing route-folding convention already
  established for `?picker=1` and (below) `?manifest=<ref>` on
  `GET /ui/registry/{repo...}`: it calls the same `Tags()` used by the tag
  table, and renders a small fragment (`total size · updated <date>`)
  computed from the already-fetched `[]TagGroup` — no new expensive call
  beyond what a normal tag-table view already pays.

## 2. Manifest detail view

**Design:**

- Reuses the same route-folding convention a third time:
  `GET /ui/registry/{repo...}?manifest=<ref>` — mirroring the JSON API's
  own `?manifest=<ref>` fold (`internal/api/registry.go`'s
  `getRepoOrManifest`), which exists for the identical reason (Go 1.22's
  `{repo...}` wildcard must be the route's last segment, ruling out a
  separate `.../manifests/{ref}` path).
- This branch is checked **before** the handler calls `Tags()` — a single
  `Manifest()` lookup is enough for one digest's detail, and there's no
  reason to pay for the whole repo's resolution just to view one entry.
  `ref` is validated via the existing `imgregistry.ValidRef` (already
  shared with the API edge) before the lookup.
- Renders: the `repo@digest` pull reference (copy-pasteable), the config
  digest, total size, and a layer table (digest + size per layer). A
  multi-arch manifest list (no `Layers`, since `Manifest()` already leaves
  that empty for an index — see phase 1's Important-4 fix) renders a note
  instead of an empty table, not a blank one that looks broken.
- Each row's digest in the existing tag table (`registry-tags.html`)
  becomes a link into this view:
  `/ui/registry/{repo}?manifest={digest}` — `html/template`'s contextual
  autoescaping URL-encodes the digest's `:` automatically in this
  attribute context, the same way it already correctly handles a
  `/`-containing namespaced repo name in the existing repo-list links (no
  manual escaping needed, verified against production for `espressif/idf`
  during phase 1's rollout).
- No "used by instances" section — see Scope above.

## Interface change and its ripple

Adding `TagCount` to the `imgregistry.Client` interface means every
existing implementer must gain the method to keep compiling, even where
that package doesn't call it:

- `imgregistry.HTTPClient` (the real implementation).
- `internal/api/registry_test.go`'s `fakeRegistry` test double — `internal/api`
  never calls `TagCount` (it has no route for it; count is UI-only), but
  the fake still has to implement the full interface.
- `internal/ui/handlers_registry_test.go`'s `fakeRegistryUI` test double —
  same reasoning, and this one DOES get exercised by the new list-page
  tests.

## Testing

- `imgregistry`: `TestTagCount` — confirms it returns the raw tag count
  without triggering any manifest/blob request (assert the fake server
  never sees a `/manifests/` or `/blobs/` call).
- `internal/ui`: repo-list test with multiple repos, one whose `TagCount`
  errors (renders `?`, not a failed page); a `?stats=1` fragment test
  asserting size/date render correctly and the fragment omits full-page
  chrome (same convention as the existing picker-fragment test); a
  `?manifest=<ref>` detail-view test asserting layers/config
  digest/pull-ref render, plus one for a multi-arch manifest list (no
  layers, "no layers" note) reusing phase 1's existing
  `TestHTTPClient_Tags_MultiArchManifestListResolvesWithZeroCreated`
  fixture shape.

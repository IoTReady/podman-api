# Phase 4: Volume cold-copy primitive — design

**Date:** 2026-06-03
**Status:** Approved (brainstorm)
**Tracking:** Forgejo #33 (part of milestone #29). Independent of #31/#32.
**Umbrella:** `docs/superpowers/specs/2026-06-03-migrate-evacuate-design.md`

## Goal

Give the daemon the ability to copy a named podman volume's contents from one
host to another, streamed through the daemon's network without ever touching the
daemon's disk. This is the data-movement primitive that `migrate` (#34) and
`evacuate` (#35) sit on top of: step 4 of the migrate algorithm is
"for each named volume: `export(src) | import(dst)`".

This phase ships **no HTTP surface and no job kind**. The work is three things:
two new `podman.Client` methods, one streaming orchestration helper on
`instance.Service`, and the fake-client support that lets all of it be tested
without a real podman host. The migrate handler in #34 is the first caller.

This phase is **independent** — it needs neither the state store (#31) nor the
jobs runner (#32), so it can land in any order relative to them.

## Decisions locked in this brainstorm

| Decision | Choice |
| --- | --- |
| Copy helper location | **Method on `*instance.Service`** (`CopyVolume`) — Service owns the `Client` and is where orchestration already lives; migrate calls `svc.CopyVolume` |
| HTTP surface | **None** — export/import/copy are daemon-internal, driven by the migrate job (#34). Matches the umbrella's API table (no volume export/import routes) |
| Transport | **Podman REST `export`/`import` endpoints over the existing bindings connection** — no SSH-tar shelling, no extra connection setup |
| On-disk staging | **None** — source→dest streamed through an `io.Pipe`; data transits the daemon's network (two connections), never its disk |
| Compression | **None** — uncompressed tar, matching libpod's own `Volume.Export`/`Import`. Compression is a future optimisation, not needed for correctness (YAGNI) |
| Volume creation on dest | **Out of scope** — `import` requires the dest volume to already exist. Migrate's `Apply` step (or an explicit create) owns that; this primitive only moves bytes |

## Transport: how export/import reach podman

The high-level `pkg/bindings/volumes` package does **not** wrap volume
export/import, but the podman v5.8.2 REST daemon exposes both, and the bindings
expose the low-level door every other binding already uses:

- `bindings.GetClient(ctx) (*Connection, error)` — pulls the cached
  `*Connection` out of the connection context.
- `(*Connection).DoRequest(ctx, body io.Reader, method, endpoint, query, headers, pathValues...) (*APIResponse, error)`
  — issues a raw HTTP request over that connection. `endpoint` is relative to
  `/v{ver}/libpod`; `pathValues` fill `%s` placeholders.

Endpoints (confirmed in vendored `pkg/api/server/register_volumes.go`):

| Op | Method | Endpoint | Body | Success |
| --- | --- | --- | --- | --- |
| export | `GET` | `/volumes/%s/export` | — | `200`, body is an uncompressed tar stream |
| import | `POST` | `/volumes/%s/import` | uncompressed tar | `204 No Content` |

This is exactly the pattern `volumes.Inspect`/`Remove` already use
(`GetClient` → `DoRequest` → `response.Process`); we just hit two endpoints the
high-level package happens not to wrap.

## Client interface additions

`internal/podman/client.go` — two methods on the `Client` interface:

```go
// VolumeExport streams the named volume's contents from host as an
// uncompressed tar. The caller must Close the returned reader.
VolumeExport(ctx context.Context, hostID, name string) (io.ReadCloser, error)

// VolumeImport unpacks an uncompressed tar (as produced by VolumeExport)
// into the named volume on host. The volume must already exist.
VolumeImport(ctx context.Context, hostID, name string, r io.Reader) error
```

`name` is the volume name. A missing volume maps to `podman.ErrNotFound`
(via the existing `mapNotFound`), consistent with `VolumeInspect`/`VolumeRemove`.

### Real implementation (`real.go`)

Both follow the established shape: `c, err := r.ctxFor(ctx, id)` for the cached
connection context, `conn, _ := bindings.GetClient(c)`, then `DoRequest`.

- **Export.** `GET /volumes/%s/export`. On non-`200`, close the body and return a
  mapped error (`404` → `ErrNotFound`); otherwise return `response.Body` *open*
  as the `io.ReadCloser` — the caller streams and closes it.
- **Import.** `POST /volumes/%s/import` with `r` as the request body, then
  `response.Process(nil)` (which checks status and drains/closes); map `404` →
  `ErrNotFound`.

Like every other method, these pass the long-lived connection context to
`DoRequest`. Per-operation cancellation/timeout of an in-flight stream is **not**
a goal of this phase — abort-on-error is handled by the pipe (below); a wall-clock
timeout for a stuck transfer is a follow-up for migrate (#34), noted under
"Out of scope".

### Fake implementation (`fake/fake.go`)

The fake gains per-host volume *contents* so a round-trip is observable:

- `volData map[string]map[string][]byte` (hostID → name → tar bytes), with a
  lazy `hostVolData(h)` accessor mirroring `hostVolumes`.
- `VolumeExport` returns `io.NopCloser(bytes.NewReader(data))`; unknown volume →
  `ErrNotFound`.
- `VolumeImport` reads the stream to completion and stores the bytes; unknown
  volume → `ErrNotFound` (import requires the volume to exist, matching real).
- Error-injection hooks: `ExportErr`, `ImportErr` (immediate failure), plus an
  `ExportReader func(host, name string) io.ReadCloser` override so a test can
  return a reader that errors **mid-stream** — the input for the
  export-fails-mid-stream → import-aborts test.

The fake is the contract double the api/instance tests already use, so seeding a
source volume and asserting the dest received identical bytes needs only these
additions.

## Orchestration: `Service.CopyVolume`

`internal/instance/service.go`:

```go
// CopyVolume streams a named volume's contents from one host to another,
// through an in-process pipe — data crosses the daemon's network but never
// its disk. The destination volume must already exist.
func (s *Service) CopyVolume(ctx context.Context, fromHost, toHost, name string) error
```

Algorithm:

1. `rc, err := s.client.VolumeExport(ctx, fromHost, name)` — bail on error;
   `defer rc.Close()`.
2. `pr, pw := io.Pipe()`.
3. Goroutine: `_, err := io.Copy(pw, rc); pw.CloseWithError(err)` — a nil `err`
   becomes a clean EOF for the reader; a read failure becomes a pipe error.
4. `err = s.client.VolumeImport(ctx, toHost, name, pr)`.
5. On import error, `pr.CloseWithError(err)` to unblock and tear down the copy
   goroutine (its next `pw` write fails), then return the error. On success the
   goroutine has already drained `rc` and closed `pw`.

Error propagation is symmetric, which is the property the issue's TDD list calls
out:

- **import fails** → `pr.CloseWithError` → the copy goroutine's `pw` write errors
  → `io.Copy` stops → `rc` closed by defer → the export HTTP body is closed,
  aborting the source read. No goroutine leak.
- **export read fails mid-stream** → `pw.CloseWithError(readErr)` → `VolumeImport`'s
  body read returns that error → the import POST aborts → `CopyVolume` returns the
  error.

Because the source is only ever *read*, a failed copy leaves the source volume
untouched — the migrate algorithm relies on this (source is not deleted until the
dest verifies healthy).

## Testing (TDD)

Unit (pure-Go via the fake; no build tags, like the rest of the api/instance
suites):

1. **Fake round-trip.** Seed `volData[src][name]`, `VolumeExport` → `VolumeImport`
   into a dest host, assert the dest bytes equal the source bytes.
2. **`CopyVolume` happy path.** Seed source, create dest volume, `CopyVolume`,
   assert dest contents match and `rc` was closed.
3. **import-fails → returns error, source intact.** `ImportErr` set; `CopyVolume`
   returns it; source `volData` unchanged; no goroutine leak (the test completes,
   i.e. the copy goroutine is not blocked on `pw`).
4. **export-fails-mid-stream → import aborts.** `ExportReader` returns a reader
   that yields N bytes then an error; assert `CopyVolume` returns an error and the
   dest volume did **not** receive a complete/committed copy.
5. **Not-found mapping.** Export/import of an unknown volume → `ErrNotFound`.

Integration (real podman host, `make test-integration`, behind the
`integration` tag like the existing real-client tests):

6. Create a volume on host A, write known files into it (e.g. via a throwaway
   container or `podman volume import` in the fixture), `CopyVolume` A→B
   (pre-creating the dest volume), then assert the files are present on B with
   identical contents. Exercises the real `export`/`import` REST path end to end.

## Out of scope (deferred)

- **HTTP endpoints** for volume export/import/copy — internal-only this phase.
- **Dest volume creation** — owned by migrate's `Apply` (#34).
- **Streaming timeout / mid-transfer cancellation** — the pipe handles
  abort-on-error; a wall-clock guard for a *stuck* (not failing) transfer belongs
  with the migrate handler (#34), alongside its overall job timeout.
- **Compression** of the tar stream — future network optimisation.
- **Cross-version / driver-specific volume nuances** — we move whatever
  `Volume.Export` produces; podman owns the on-disk representation.

## Files touched

| File | Change |
| --- | --- |
| `internal/podman/client.go` | `VolumeExport` / `VolumeImport` on the `Client` interface |
| `internal/podman/real.go` | libpod impls via `GetClient`+`DoRequest` on the export/import REST endpoints |
| `internal/podman/fake/fake.go` | `volData` store, export/import fakes, `ExportErr`/`ImportErr`/`ExportReader` hooks |
| `internal/instance/service.go` | `CopyVolume` pipe orchestration |
| `internal/podman/*_test.go`, `internal/instance/*_test.go` | unit tests (1–5); integration test (6) behind the `integration` tag |

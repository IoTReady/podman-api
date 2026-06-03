# Volume Cold-Copy Primitive Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add `VolumeExport`/`VolumeImport` to `podman.Client` and a pipe-streaming `Service.CopyVolume` that moves a named volume's contents host→host without touching the daemon's disk.

**Architecture:** The real client hits podman's REST `GET /volumes/{name}/export` (tar out) and `POST /volumes/{name}/import` (tar in) over the existing bindings connection via `bindings.GetClient(ctx).DoRequest(...)` — the same low-level door `volumes.Inspect`/`Remove` use. `Service.CopyVolume` wires export→import through an `io.Pipe`, waiting for the copy goroutine so there is no leak and errors propagate both directions. No HTTP surface, no job kind — migrate (#34) is the first caller.

**Tech Stack:** Go, `github.com/containers/podman/v5@v5.8.2` bindings, `net/http`, `io.Pipe`, testify. Unit tests are pure-Go (no build tags); the real-client test is behind the `integration` tag.

**Spec:** `docs/superpowers/specs/2026-06-03-volume-cold-copy-design.md`

---

## File structure

| File | Responsibility | Change |
| --- | --- | --- |
| `internal/podman/client.go` | `Client` interface | add `VolumeExport`/`VolumeImport`; add `io` import |
| `internal/podman/real.go` | libpod-backed client | implement both via `GetClient`+`DoRequest`; add `io`, `net/http` imports |
| `internal/podman/fake/fake.go` | in-memory client double | `volData` store + `hostVolData`, export/import impls, `ExportErr`/`ImportErr`/`ExportReader` hooks, `SetVolumeData`/`VolumeData` test helpers; add `bytes`, `io` imports |
| `internal/podman/fake/fake_volcopy_test.go` | fake unit tests | NEW — round-trip, not-found, mid-stream error |
| `internal/instance/service.go` | orchestration | `CopyVolume` pipe wiring |
| `internal/instance/volcopy_test.go` | service unit tests | NEW — happy path, import-fails, export-mid-stream-fails |
| `internal/podman/real_volcopy_integration_test.go` | real round-trip | NEW — behind `integration` tag |

Because both `real.go:671` (`var _ Client = (*Real)(nil)`) and `fake.go:304` (`var _ podman.Client = (*Fake)(nil)`) assert interface conformance at compile time, adding the two interface methods forces **both** implementors to be updated in the same task — otherwise the `podman` package won't compile and no test in either package will build. Task 1 therefore lands the interface change plus both implementations together; `real.go` is exercised by the integration test in Task 3 (it has no unit test, matching the other real methods).

---

## Task 1: Client interface + real & fake implementations

**Files:**
- Modify: `internal/podman/client.go` (interface block ~lines 27-29, imports ~lines 3-6)
- Modify: `internal/podman/real.go` (add methods after `VolumeRemove` ~line 369; imports ~lines 3-25)
- Modify: `internal/podman/fake/fake.go` (struct ~lines 18-48, `New` ~lines 58-64, accessors ~lines 78-83, methods after `VolumeRemove` ~line 249, imports ~lines 5-15)
- Test: `internal/podman/fake/fake_volcopy_test.go` (new)

- [ ] **Step 1: Write the failing fake tests**

Create `internal/podman/fake/fake_volcopy_test.go`:

```go
package fake

import (
	"bytes"
	"context"
	"errors"
	"io"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/iotready/podman-api/internal/podman"
)

func TestFake_VolumeExportImport_RoundTrip(t *testing.T) {
	f := New()
	ctx := context.Background()
	want := []byte("tarball-bytes")

	// Seed a source volume with contents.
	f.SetVolumeData("src", "vol", want)
	// Destination volume must exist before import (matches real podman).
	f.AddVolume("dst", podman.Volume{Name: "vol"})

	rc, err := f.VolumeExport(ctx, "src", "vol")
	require.NoError(t, err)
	defer rc.Close()
	require.NoError(t, f.VolumeImport(ctx, "dst", "vol", rc))

	assert.Equal(t, want, f.VolumeData("dst", "vol"))
}

func TestFake_VolumeExport_NotFound(t *testing.T) {
	f := New()
	_, err := f.VolumeExport(context.Background(), "src", "missing")
	require.ErrorIs(t, err, podman.ErrNotFound)
}

func TestFake_VolumeImport_NotFound(t *testing.T) {
	f := New()
	err := f.VolumeImport(context.Background(), "dst", "missing", bytes.NewReader([]byte("x")))
	require.ErrorIs(t, err, podman.ErrNotFound)
}

func TestFake_VolumeExport_ErrHook(t *testing.T) {
	f := New()
	boom := errors.New("boom")
	f.ExportErr = boom
	_, err := f.VolumeExport(context.Background(), "src", "vol")
	require.ErrorIs(t, err, boom)
}

func TestFake_VolumeImport_ErrHook(t *testing.T) {
	f := New()
	boom := errors.New("boom")
	f.ImportErr = boom
	err := f.VolumeImport(context.Background(), "dst", "vol", bytes.NewReader([]byte("x")))
	require.ErrorIs(t, err, boom)
}

// errReader yields some bytes then a hard error, simulating a volume export
// stream that breaks mid-transfer.
type errReader struct {
	data []byte
	off  int
	err  error
}

func (r *errReader) Read(p []byte) (int, error) {
	if r.off < len(r.data) {
		n := copy(p, r.data[r.off:])
		r.off += n
		return n, nil
	}
	return 0, r.err
}
func (r *errReader) Close() error { return nil }

func TestFake_VolumeImport_PropagatesReaderError(t *testing.T) {
	f := New()
	f.AddVolume("dst", podman.Volume{Name: "vol"})
	boom := errors.New("stream broke")
	err := f.VolumeImport(context.Background(), "dst", "vol", &errReader{data: []byte("partial"), err: boom})
	require.ErrorIs(t, err, boom)
	// Nothing should have been committed.
	assert.Nil(t, f.VolumeData("dst", "vol"))
}

func TestFake_VolumeExport_ReaderHook(t *testing.T) {
	f := New()
	want := io.NopCloser(bytes.NewReader([]byte("hooked")))
	f.ExportReader = func(host, name string) io.ReadCloser { return want }
	rc, err := f.VolumeExport(context.Background(), "any", "any")
	require.NoError(t, err)
	got, _ := io.ReadAll(rc)
	assert.Equal(t, []byte("hooked"), got)
}
```

- [ ] **Step 2: Run the tests to verify they fail (do not compile)**

Run: `go test ./internal/podman/fake/ -run VolumeExport -run VolumeImport`
Expected: build failure — `f.VolumeExport undefined`, `f.SetVolumeData undefined`, etc. (And `./internal/podman/` will also fail to build once we touch the interface — that's expected until Step 5.)

- [ ] **Step 3: Add the interface methods**

In `internal/podman/client.go`, add `"io"` to the import block:

```go
import (
	"context"
	"errors"
	"io"
)
```

Replace the `// Volumes` block:

```go
	// Volumes
	VolumeInspect(ctx context.Context, hostID, name string) (Volume, error)
	VolumeRemove(ctx context.Context, hostID, name string, force bool) error
	// VolumeExport streams the named volume's contents from host as an
	// uncompressed tar. The caller must Close the returned reader.
	VolumeExport(ctx context.Context, hostID, name string) (io.ReadCloser, error)
	// VolumeImport unpacks an uncompressed tar (as produced by VolumeExport)
	// into the named volume on host. The volume must already exist.
	VolumeImport(ctx context.Context, hostID, name string, r io.Reader) error
```

- [ ] **Step 4: Implement the real client methods**

In `internal/podman/real.go`, add `"io"` and `"net/http"` to the standard-library import group (keep them ordered):

```go
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
```

Add after `VolumeRemove` (after line 369):

```go
// VolumeExport streams a volume's contents as an uncompressed tar. The returned
// reader is the live HTTP response body; the caller must Close it. We hit the
// REST endpoint directly because the high-level volumes binding doesn't wrap it.
func (r *Real) VolumeExport(ctx context.Context, id, name string) (io.ReadCloser, error) {
	c, err := r.ctxFor(ctx, id)
	if err != nil {
		return nil, err
	}
	conn, err := bindings.GetClient(c)
	if err != nil {
		return nil, err
	}
	resp, err := conn.DoRequest(c, nil, http.MethodGet, "/volumes/%s/export", nil, nil, name)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != http.StatusOK {
		defer resp.Body.Close()
		// Process(nil) drains the body and returns podman's error for non-2xx.
		return nil, mapNotFound(resp.Process(nil))
	}
	return resp.Body, nil
}

// VolumeImport unpacks an uncompressed tar into an existing volume on the host.
func (r *Real) VolumeImport(ctx context.Context, id, name string, src io.Reader) error {
	c, err := r.ctxFor(ctx, id)
	if err != nil {
		return err
	}
	conn, err := bindings.GetClient(c)
	if err != nil {
		return err
	}
	resp, err := conn.DoRequest(c, src, http.MethodPost, "/volumes/%s/import", nil, nil, name)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	// Process(nil) returns nil on 2xx (import answers 204) and podman's error otherwise.
	return mapNotFound(resp.Process(nil))
}
```

- [ ] **Step 5: Implement the fake client methods + helpers**

In `internal/podman/fake/fake.go`, add `"bytes"` and `"io"` to the import block:

```go
import (
	"bytes"
	"context"
	"fmt"
	"io"
	"strings"
	"sync"
	"time"

	"gopkg.in/yaml.v3"

	"github.com/iotready/podman-api/internal/podman"
)
```

Add a `volData` field to the `Fake` struct (next to `volumes`):

```go
	volumes map[string]map[string]podman.Volume // hostID -> name -> Volume
	volData map[string]map[string][]byte        // hostID -> name -> tar bytes
```

Add the hooks to the struct's hook section:

```go
	// ExportErr, if non-nil, makes VolumeExport fail immediately.
	ExportErr error
	// ImportErr, if non-nil, makes VolumeImport fail immediately (without
	// reading the supplied reader) — models a destination that rejects the import.
	ImportErr error
	// ExportReader, if non-nil, overrides VolumeExport's reader. Lets a test
	// supply a stream that errors mid-transfer.
	ExportReader func(host, name string) io.ReadCloser
```

Initialise `volData` in `New`:

```go
func New() *Fake {
	return &Fake{
		pods:    map[string]map[string]podman.Pod{},
		secrets: map[string]map[string]podman.Secret{},
		volumes: map[string]map[string]podman.Volume{},
		volData: map[string]map[string][]byte{},
	}
}
```

Add the accessor next to `hostVolumes`:

```go
func (f *Fake) hostVolData(h string) map[string][]byte {
	if _, ok := f.volData[h]; !ok {
		f.volData[h] = map[string][]byte{}
	}
	return f.volData[h]
}
```

Add test helpers next to `AddVolume`:

```go
// SetVolumeData seeds a volume and its contents on a host. Test-only.
func (f *Fake) SetVolumeData(host, name string, data []byte) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.hostVolumes(host)[name] = podman.Volume{Name: name}
	f.hostVolData(host)[name] = data
}

// VolumeData returns the stored contents of a volume (nil if none). Test-only.
func (f *Fake) VolumeData(host, name string) []byte {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.hostVolData(host)[name]
}
```

Add the methods after `VolumeRemove` (after line 249):

```go
func (f *Fake) VolumeExport(_ context.Context, h, name string) (io.ReadCloser, error) {
	if f.ExportReader != nil {
		return f.ExportReader(h, name), nil
	}
	if f.ExportErr != nil {
		return nil, f.ExportErr
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if _, ok := f.hostVolumes(h)[name]; !ok {
		return nil, podman.ErrNotFound
	}
	src := f.hostVolData(h)[name]
	buf := make([]byte, len(src))
	copy(buf, src)
	return io.NopCloser(bytes.NewReader(buf)), nil
}

func (f *Fake) VolumeImport(_ context.Context, h, name string, r io.Reader) error {
	if f.ImportErr != nil {
		return f.ImportErr
	}
	// Read outside the lock — r may be an io.Pipe fed by another goroutine.
	data, err := io.ReadAll(r)
	if err != nil {
		return err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if _, ok := f.hostVolumes(h)[name]; !ok {
		return podman.ErrNotFound
	}
	f.hostVolData(h)[name] = data
	return nil
}
```

- [ ] **Step 6: Run the fake tests to verify they pass**

Run: `go test ./internal/podman/fake/`
Expected: PASS (all `TestFake_Volume*` plus the existing fake tests).

- [ ] **Step 7: Verify the whole tree still builds**

Run: `make build`
Expected: builds `bin/podman-api` with no errors (confirms `real.go` compiles against the bindings and both interface assertions are satisfied).

- [ ] **Step 8: Commit**

```bash
git add internal/podman/client.go internal/podman/real.go internal/podman/fake/fake.go internal/podman/fake/fake_volcopy_test.go
git commit -m "feat(podman): VolumeExport/VolumeImport client primitives (#33)"
```

---

## Task 2: Service.CopyVolume pipe orchestration

**Files:**
- Modify: `internal/instance/service.go` (add method; ensure `io` and `fmt` are imported)
- Test: `internal/instance/volcopy_test.go` (new)

- [ ] **Step 1: Write the failing service tests**

Create `internal/instance/volcopy_test.go`:

```go
package instance

import (
	"context"
	"errors"
	"io"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/iotready/podman-api/internal/config"
	"github.com/iotready/podman-api/internal/podman"
	"github.com/iotready/podman-api/internal/podman/fake"
)

func newVolSvc(f *fake.Fake) *Service {
	hosts := []config.Host{{ID: "a", Addr: "unix", Socket: "/x"}, {ID: "b", Addr: "unix", Socket: "/y"}}
	return NewService(f, hosts, nil)
}

func TestCopyVolume_HappyPath(t *testing.T) {
	f := fake.New()
	want := []byte("the-volume-tar")
	f.SetVolumeData("a", "vol", want)
	f.AddVolume("b", podman.Volume{Name: "vol"})

	svc := newVolSvc(f)
	require.NoError(t, svc.CopyVolume(context.Background(), "a", "b", "vol"))

	assert.Equal(t, want, f.VolumeData("b", "vol"))
}

func TestCopyVolume_ImportFails_SourceIntact(t *testing.T) {
	f := fake.New()
	want := []byte("the-volume-tar")
	f.SetVolumeData("a", "vol", want)
	f.AddVolume("b", podman.Volume{Name: "vol"})
	boom := errors.New("dest rejected")
	f.ImportErr = boom

	svc := newVolSvc(f)
	err := svc.CopyVolume(context.Background(), "a", "b", "vol")
	require.ErrorIs(t, err, boom)

	// Source is read-only — unchanged. Dest never committed.
	assert.Equal(t, want, f.VolumeData("a", "vol"))
	assert.Nil(t, f.VolumeData("b", "vol"))
	// Test returning means the copy goroutine was not left blocked on the pipe.
}

func TestCopyVolume_ExportFailsMidStream_Aborts(t *testing.T) {
	f := fake.New()
	f.AddVolume("b", podman.Volume{Name: "vol"})
	boom := errors.New("source stream broke")
	f.ExportReader = func(host, name string) io.ReadCloser {
		return &midStreamReader{data: []byte("first-chunk"), err: boom}
	}

	svc := newVolSvc(f)
	err := svc.CopyVolume(context.Background(), "a", "b", "vol")
	require.Error(t, err)
	assert.ErrorContains(t, err, "source stream broke")
	assert.Nil(t, f.VolumeData("b", "vol"))
}

type midStreamReader struct {
	data []byte
	off  int
	err  error
}

func (r *midStreamReader) Read(p []byte) (int, error) {
	if r.off < len(r.data) {
		n := copy(p, r.data[r.off:])
		r.off += n
		return n, nil
	}
	return 0, r.err
}
func (r *midStreamReader) Close() error { return nil }
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `go test ./internal/instance/ -run TestCopyVolume`
Expected: build failure — `svc.CopyVolume undefined`.

- [ ] **Step 3: Implement CopyVolume**

In `internal/instance/service.go`, ensure the import block includes `"io"` and `"fmt"` (add whichever is missing). Add the method near the other volume methods (after `DeleteVolume`):

```go
// CopyVolume streams a named volume's contents from one host to another through
// an in-process pipe — the data crosses the daemon's network (two connections)
// but never its disk. The destination volume must already exist. The source is
// only ever read, so a failed copy leaves it untouched (migrate relies on this).
func (s *Service) CopyVolume(ctx context.Context, fromHost, toHost, name string) error {
	rc, err := s.client.VolumeExport(ctx, fromHost, name)
	if err != nil {
		return fmt.Errorf("export volume %q from %s: %w", name, fromHost, err)
	}
	defer rc.Close()

	pr, pw := io.Pipe()
	copyDone := make(chan struct{})
	go func() {
		_, cerr := io.Copy(pw, rc)
		// nil cerr closes the pipe with EOF (clean); an error closes it so the
		// importer's read fails too.
		pw.CloseWithError(cerr)
		close(copyDone)
	}()

	importErr := s.client.VolumeImport(ctx, toHost, name, pr)
	// Unblock the copy goroutine if the importer stopped reading early, then
	// wait for it so we never leak it. CloseWithError(nil) == Close, harmless
	// after a fully-consumed stream.
	pr.CloseWithError(importErr)
	<-copyDone

	if importErr != nil {
		return fmt.Errorf("import volume %q to %s: %w", name, toHost, importErr)
	}
	return nil
}
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `go test ./internal/instance/ -run TestCopyVolume -race`
Expected: PASS for all three (`-race` confirms the pipe goroutine is clean).

- [ ] **Step 5: Run the full unit suite**

Run: `make test`
Expected: PASS across the repo (no regressions; new tests included).

- [ ] **Step 6: Commit**

```bash
git add internal/instance/service.go internal/instance/volcopy_test.go
git commit -m "feat(instance): CopyVolume streams a volume host-to-host (#33)"
```

---

## Task 3: Real-client integration test

**Files:**
- Test: `internal/podman/real_volcopy_integration_test.go` (new, behind `integration` tag)

This exercises the real REST `export`/`import` path against a live podman socket, the same way `real_secrets_integration_test.go` does. It uses the bindings' own `volumes.Create` to make both volumes, seeds the source by importing a known tar, copies via the daemon path (`VolumeExport` → `VolumeImport`), then exports the destination and untars it to assert the file arrived intact (byte-exact tar comparison is avoided — podman owns the on-disk/tar representation).

- [ ] **Step 1: Write the integration test**

Create `internal/podman/real_volcopy_integration_test.go`:

```go
//go:build integration

package podman

import (
	"archive/tar"
	"bytes"
	"context"
	"io"
	"testing"

	"github.com/containers/podman/v5/pkg/bindings/volumes"
	"github.com/containers/podman/v5/pkg/domain/entities/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/iotready/podman-api/internal/config"
)

// makeTar builds an uncompressed tar containing a single file.
func makeTar(t *testing.T, name string, content []byte) []byte {
	t.Helper()
	var buf bytes.Buffer
	tw := tar.NewWriter(&buf)
	require.NoError(t, tw.WriteHeader(&tar.Header{
		Name: name,
		Mode: 0o644,
		Size: int64(len(content)),
	}))
	_, err := tw.Write(content)
	require.NoError(t, err)
	require.NoError(t, tw.Close())
	return buf.Bytes()
}

// readFileFromTar returns the contents of the named file from a tar stream.
func readFileFromTar(t *testing.T, r io.Reader, name string) []byte {
	t.Helper()
	tr := tar.NewReader(r)
	for {
		h, err := tr.Next()
		if err == io.EOF {
			t.Fatalf("file %q not found in tar", name)
		}
		require.NoError(t, err)
		if h.Name == name {
			b, err := io.ReadAll(tr)
			require.NoError(t, err)
			return b
		}
	}
}

func TestReal_VolumeCopy_LocalOnly(t *testing.T) {
	sock := localSocket(t)
	c, err := NewReal([]config.Host{{ID: "local", Addr: "unix", Socket: sock}})
	require.NoError(t, err)
	ctx := context.Background()

	const src, dst = "podman-api-itest-vol-src", "podman-api-itest-vol-dst"
	conn, err := c.ctxFor(ctx, "local")
	require.NoError(t, err)

	for _, name := range []string{src, dst} {
		_, err := volumes.Create(conn, types.VolumeCreateOptions{Name: name}, nil)
		require.NoError(t, err)
	}
	t.Cleanup(func() {
		_ = c.VolumeRemove(context.Background(), "local", src, true)
		_ = c.VolumeRemove(context.Background(), "local", dst, true)
	})

	// Seed the source volume with a known file via VolumeImport.
	want := []byte("cold-copy-payload")
	require.NoError(t, c.VolumeImport(ctx, "local", src, bytes.NewReader(makeTar(t, "hello.txt", want))))

	// Export source, import into dest — the primitive under test.
	rc, err := c.VolumeExport(ctx, "local", src)
	require.NoError(t, err)
	require.NoError(t, c.VolumeImport(ctx, "local", dst, rc))
	require.NoError(t, rc.Close())

	// Export dest and assert the file survived the round-trip.
	out, err := c.VolumeExport(ctx, "local", dst)
	require.NoError(t, err)
	defer out.Close()
	assert.Equal(t, want, readFileFromTar(t, out, "hello.txt"))

	// Not-found mapping.
	_, err = c.VolumeExport(ctx, "local", "podman-api-itest-vol-missing")
	require.ErrorIs(t, err, ErrNotFound)
}
```

- [ ] **Step 2: Confirm it builds under the integration tag**

Run: `go vet -tags integration ./internal/podman/`
Expected: no errors (verifies imports and the `volumes.Create`/`types.VolumeCreateOptions` signature resolve). If `types` import path differs in this podman version, adjust to the package that defines `VolumeCreateOptions` (grep the module cache: `grep -rn "type VolumeCreateOptions" $(go env GOMODCACHE)/github.com/containers/podman/v5@v5.8.2/`).

- [ ] **Step 3: Run the integration test (needs a podman socket)**

Run: `make test-integration` (or `go test -tags integration ./internal/podman/ -run TestReal_VolumeCopy_LocalOnly -v`)
Expected: PASS against the local podman socket. (CI runs the integration suite; if no local socket is available the runner provides one — same as the existing real tests.)

- [ ] **Step 4: Commit**

```bash
git add internal/podman/real_volcopy_integration_test.go
git commit -m "test(podman): integration round-trip for volume cold-copy (#33)"
```

---

## Wrap-up (after all tasks)

- [ ] `gofmt -l .` is empty and `go vet ./...` (with tags) is clean — repo conventions.
- [ ] Open one PR for #33: `forgejo pr create tej/podman-api --head=feat/33-volume-cold-copy --base=main --title="Volume cold-copy primitive (#33)" --body=...` linking the spec/plan and noting migrate (#34) is the first consumer.

---

## Self-review notes

- **Spec coverage:** `VolumeExport`/`VolumeImport` interface + real (Task 1) ✔; fake support with round-trip + injectable failures incl. mid-stream (Task 1) ✔; `Service.CopyVolume` pipe orchestration with symmetric error propagation (Task 2) ✔; the five unit tests and the integration round-trip from the spec's testing section map to Tasks 1–3 ✔; "no HTTP surface / no job kind" honoured (no router/handler changes) ✔; out-of-scope items (dest creation, timeouts, compression) deliberately absent ✔.
- **Type consistency:** `VolumeExport(ctx, hostID, name) (io.ReadCloser, error)` and `VolumeImport(ctx, hostID, name, io.Reader) error` are identical across the interface, real, fake, and all tests; `CopyVolume(ctx, fromHost, toHost, name) error` is consistent between Task 2's impl and tests; fake helpers `SetVolumeData`/`VolumeData`/`ExportErr`/`ImportErr`/`ExportReader` are defined in Task 1 before Task 2 uses them.
- **Compile ordering:** the interface assertions on Real and Fake mean Task 1 must land all of interface+real+fake together; the plan does this and verifies with `make build` (Step 7).

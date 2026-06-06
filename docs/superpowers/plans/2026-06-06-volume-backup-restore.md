# On-Demand Volume Backup + One-Click Restore (#66) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** OSS on-demand primitive: `POST` backup of all of an instance's volumes (stop → export → restart) into a pluggable blob store (local-dir impl), plus one-click in-place restore verified with the existing sha256 manifest machinery.

**Architecture:** Two new job kinds (`backup`, `restore`) on the existing jobs runner. Core logic lives in `internal/instance` (like migrate); thin job adapters in a new `internal/backup` package, which also hosts the `LocalDir` blob store. Backup metadata (including per-volume manifests) lives in a new `backups` table in the state DB. API + admin-UI surface follows existing patterns.

**Tech Stack:** Go, modernc SQLite, net/http ServeMux routing, html/template + htmx UI, testify.

**Spec:** `docs/superpowers/specs/2026-06-06-volume-backup-restore-design.md`. Read it before starting any task.

**Spec deviations (decided during planning — Task 11 amends the spec to match):**
1. `BlobStore.Put` returns a `BlobWriter` with explicit `Commit()`/`Abort()` (not `io.WriteCloser`) — otherwise a failed backup's `Close` would commit a partial blob.
2. `BlobStore` has `DeleteAll(ctx, prefix)` instead of per-key `Delete` — one call removes a backup's whole artifact directory, including partials left by a failed run.
3. Backup restarts the instance afterwards **only if it was running before** (a backup of a deliberately-stopped instance must not start it).
4. Restore checks host drain **upfront** (sync 423) so a draining host can't fail the job *after* teardown.
5. List pagination is `?limit=` only (newest-first, clamped like jobs); no cursor — manual lifecycle keeps counts modest. YAGNI.

## Build/test invocation

Every `go build` / `go test` in this repo needs the remote-client build tags (CLAUDE.md). In every shell:

```bash
export TAGS="containers_image_openpgp exclude_graphdriver_btrfs exclude_graphdriver_devicemapper"
# run a package's tests:    go test -tags "$TAGS" ./internal/store/ -run TestName -v
# full unit suite:          make test
# build:                    make build
```

## Conventions to follow

- Errors: sentinel `errors.New` vars in `internal/instance/service.go`-style, mapped in `internal/api/errors.go:classify`.
- Job progress: `step(name, detail)` callbacks, nil-safe (`if step == nil { step = func(string,string){} }`).
- Times in SQLite: `UnixNano()` integers (same as `jobs`).
- SQLite writes: serialize via the store's write mutex + `retryBusy` — copy the idiom from an existing write method in `internal/store/sqlite.go` (e.g. `PutHostSecret`); if a field/helper name in this plan differs from the file, **the file wins**.
- gofmt-clean + `go vet -tags "$TAGS" ./...` clean before every commit.

---

### Task 1: Store — `backups` table + BackupStore

**Files:**
- Create: `internal/store/backups.go`
- Create: `internal/store/backups_test.go`
- Modify: `internal/store/sqlite.go` (schema + new methods — put methods in a new `internal/store/sqlite_backups.go`)
- Create: `internal/store/sqlite_backups.go`
- Modify: `internal/store/memory.go` (Memory impl)
- Modify: `internal/store/jobs.go` (add `BackupStore` to the `DB` interface)

- [ ] **Step 1: Write the types + interface** in `internal/store/backups.go`:

```go
package store

import (
	"context"
	"encoding/json"
	"time"
)

// BackupState is the lifecycle state of a backup row.
type BackupState string

const (
	BackupCreating BackupState = "creating" // job in flight; not restorable
	BackupComplete BackupState = "complete" // all volumes exported + manifests recorded
	BackupFailed   BackupState = "failed"   // job failed or was interrupted; not restorable
)

// BackupVolume records one exported volume: its full name
// (<template>-<slug>-<vol>), the tar's byte size, and the sha256 per-file
// manifest (the instance package's Manifest, serialized). Manifests live in
// the row — not the blob store — so restore verifies the artifact against
// metadata it does not have to trust.
type BackupVolume struct {
	Name      string          `json:"name"`
	SizeBytes int64           `json:"size_bytes"`
	Manifest  json.RawMessage `json:"manifest"`
}

// Backup is one row of the backups table.
type Backup struct {
	ID       string
	Host     string
	Template string
	Slug     string
	State    BackupState
	Volumes  []BackupVolume
	Image    string // image ref at backup time; informational hint only
	Created  time.Time
	Finished time.Time // zero until complete/failed
}

// BackupStore persists backup metadata. Implemented by *SQLite and *Memory.
type BackupStore interface {
	// CreateBackup inserts a new row in state creating, stamping Created.
	// The caller supplies the ID (NewBackupID).
	CreateBackup(ctx context.Context, b Backup) error
	// CompleteBackup transitions creating → complete, recording the exported
	// volumes and Finished. CAS: returns false (no error) if the row is not
	// currently creating.
	CompleteBackup(ctx context.Context, id string, vols []BackupVolume) (bool, error)
	// FailBackup transitions creating → failed, setting Finished. CAS like
	// CompleteBackup.
	FailBackup(ctx context.Context, id string) (bool, error)
	GetBackup(ctx context.Context, id string) (Backup, error) // ErrNotFound when absent
	// ListBackups returns the instance's backups newest-first. limit <= 0 uses
	// DefaultJobLimit; clamped at MaxJobLimit.
	ListBackups(ctx context.Context, host, template, slug string, limit int) ([]Backup, error)
	// DeleteBackup removes the row; ErrNotFound when absent. Blob deletion is
	// the caller's job (instance.Service.DeleteBackup) — the store only holds
	// metadata.
	DeleteBackup(ctx context.Context, id string) error
}

// NewBackupID returns a sortable backup id: "bk_" + the jobs id scheme
// (time-prefixed hex + random suffix).
func NewBackupID() string { return "bk_" + newJobID() }
```

- [ ] **Step 2: Write failing store tests** in `internal/store/backups_test.go`. Test against BOTH implementations via a shared helper (follow the pattern of existing dual sqlite/memory tests in this package). Core cases:

```go
package store

import (
	"context"
	"encoding/json"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// backupStores returns one of each implementation, named.
func backupStores(t *testing.T) map[string]BackupStore {
	t.Helper()
	sq, err := OpenSQLite(filepath.Join(t.TempDir(), "s.db"), nil)
	require.NoError(t, err)
	t.Cleanup(func() { sq.Close() })
	return map[string]BackupStore{"sqlite": sq, "memory": NewMemory()}
}

func TestBackups_CreateGetRoundTrip(t *testing.T) {
	for name, st := range backupStores(t) {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			id := NewBackupID()
			require.NoError(t, st.CreateBackup(ctx, Backup{
				ID: id, Host: "h1", Template: "pg", Slug: "a", State: BackupCreating, Image: "postgres:16",
			}))
			got, err := st.GetBackup(ctx, id)
			require.NoError(t, err)
			assert.Equal(t, BackupCreating, got.State)
			assert.Equal(t, "postgres:16", got.Image)
			assert.False(t, got.Created.IsZero())
			assert.True(t, got.Finished.IsZero())
		})
	}
}

func TestBackups_CompleteRecordsVolumesAndFinished(t *testing.T) {
	for name, st := range backupStores(t) {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			id := NewBackupID()
			require.NoError(t, st.CreateBackup(ctx, Backup{ID: id, Host: "h1", Template: "pg", Slug: "a", State: BackupCreating}))
			vols := []BackupVolume{{Name: "pg-a-data", SizeBytes: 42, Manifest: json.RawMessage(`{"f":{"type":48}}`)}}
			ok, err := st.CompleteBackup(ctx, id, vols)
			require.NoError(t, err)
			assert.True(t, ok)
			got, _ := st.GetBackup(ctx, id)
			assert.Equal(t, BackupComplete, got.State)
			require.Len(t, got.Volumes, 1)
			assert.Equal(t, int64(42), got.Volumes[0].SizeBytes)
			assert.JSONEq(t, `{"f":{"type":48}}`, string(got.Volumes[0].Manifest))
			assert.False(t, got.Finished.IsZero())
		})
	}
}

func TestBackups_CompleteCAS_NoOpWhenNotCreating(t *testing.T) {
	for name, st := range backupStores(t) {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			id := NewBackupID()
			require.NoError(t, st.CreateBackup(ctx, Backup{ID: id, Host: "h", Template: "t", Slug: "s", State: BackupCreating}))
			ok, err := st.FailBackup(ctx, id)
			require.NoError(t, err)
			require.True(t, ok)
			ok, err = st.CompleteBackup(ctx, id, nil) // already failed
			require.NoError(t, err)
			assert.False(t, ok)
			got, _ := st.GetBackup(ctx, id)
			assert.Equal(t, BackupFailed, got.State)
		})
	}
}

func TestBackups_ListNewestFirstScopedAndLimited(t *testing.T) {
	// create 3 for (h1,pg,a) + 1 for (h1,pg,b); assert List(h1,pg,a,limit 2)
	// returns the 2 newest of the right instance, ordered newest-first.
	// (IDs are time-prefixed so creation order == lexical order.)
	for name, st := range backupStores(t) {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			var ids []string
			for i := 0; i < 3; i++ {
				id := NewBackupID()
				ids = append(ids, id)
				require.NoError(t, st.CreateBackup(ctx, Backup{ID: id, Host: "h1", Template: "pg", Slug: "a", State: BackupCreating}))
			}
			require.NoError(t, st.CreateBackup(ctx, Backup{ID: NewBackupID(), Host: "h1", Template: "pg", Slug: "b", State: BackupCreating}))
			got, err := st.ListBackups(ctx, "h1", "pg", "a", 2)
			require.NoError(t, err)
			require.Len(t, got, 2)
			assert.Equal(t, ids[2], got[0].ID)
			assert.Equal(t, ids[1], got[1].ID)
		})
	}
}

func TestBackups_DeleteAndNotFound(t *testing.T) {
	for name, st := range backupStores(t) {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			_, err := st.GetBackup(ctx, "bk_missing")
			assert.ErrorIs(t, err, ErrNotFound)
			assert.ErrorIs(t, st.DeleteBackup(ctx, "bk_missing"), ErrNotFound)
			id := NewBackupID()
			require.NoError(t, st.CreateBackup(ctx, Backup{ID: id, Host: "h", Template: "t", Slug: "s", State: BackupCreating}))
			require.NoError(t, st.DeleteBackup(ctx, id))
			_, err = st.GetBackup(ctx, id)
			assert.ErrorIs(t, err, ErrNotFound)
		})
	}
}

// SQLite persistence: rows survive close/reopen.
func TestBackups_SQLitePersistsAcrossReopen(t *testing.T) {
	ctx := context.Background()
	p := filepath.Join(t.TempDir(), "s.db")
	sq, err := OpenSQLite(p, nil)
	require.NoError(t, err)
	id := NewBackupID()
	require.NoError(t, sq.CreateBackup(ctx, Backup{ID: id, Host: "h", Template: "t", Slug: "s", State: BackupCreating}))
	require.NoError(t, sq.Close())
	sq, err = OpenSQLite(p, nil)
	require.NoError(t, err)
	defer sq.Close()
	got, err := sq.GetBackup(ctx, id)
	require.NoError(t, err)
	assert.Equal(t, id, got.ID)
}
```

- [ ] **Step 3: Run to verify failure**

Run: `go test -tags "$TAGS" ./internal/store/ -run TestBackups -v`
Expected: compile FAIL (`CreateBackup` undefined etc.)

- [ ] **Step 4: Implement.** Schema addition to `schemaSQL` in `internal/store/sqlite.go`:

```sql
CREATE TABLE IF NOT EXISTS backups (
  id       TEXT PRIMARY KEY,
  host     TEXT NOT NULL,
  template TEXT NOT NULL,
  slug     TEXT NOT NULL,
  state    TEXT NOT NULL,
  volumes  TEXT NOT NULL DEFAULT '[]',
  image    TEXT NOT NULL DEFAULT '',
  created  INTEGER NOT NULL,
  finished INTEGER
);
CREATE INDEX IF NOT EXISTS backups_instance ON backups(host, template, slug);
```

SQLite methods in `internal/store/sqlite_backups.go` (follow the file's existing write idiom — write-mutex + `retryBusy`; times as `UnixNano()`; `volumes` column is `json.Marshal([]BackupVolume)`):

```go
package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"time"
)

func (s *SQLite) CreateBackup(ctx context.Context, b Backup) error {
	vols, err := json.Marshal(b.Volumes)
	if err != nil {
		return fmt.Errorf("marshal volumes: %w", err)
	}
	if len(b.Volumes) == 0 {
		vols = []byte("[]")
	}
	s.wmu.Lock()
	defer s.wmu.Unlock()
	return retryBusy(ctx, func() error {
		_, err := s.db.ExecContext(ctx,
			`INSERT INTO backups (id, host, template, slug, state, volumes, image, created)
			 VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
			b.ID, b.Host, b.Template, b.Slug, string(BackupCreating), string(vols), b.Image, time.Now().UnixNano())
		return err
	})
}

func (s *SQLite) CompleteBackup(ctx context.Context, id string, vols []BackupVolume) (bool, error) {
	raw, err := json.Marshal(vols)
	if err != nil {
		return false, fmt.Errorf("marshal volumes: %w", err)
	}
	if len(vols) == 0 {
		raw = []byte("[]")
	}
	return s.casBackupState(ctx, id, BackupComplete, string(raw))
}

func (s *SQLite) FailBackup(ctx context.Context, id string) (bool, error) {
	return s.casBackupState(ctx, id, BackupFailed, "")
}

// casBackupState moves a creating row to a terminal state, stamping finished.
// volumesJSON == "" leaves the volumes column untouched.
func (s *SQLite) casBackupState(ctx context.Context, id string, state BackupState, volumesJSON string) (bool, error) {
	s.wmu.Lock()
	defer s.wmu.Unlock()
	var n int64
	err := retryBusy(ctx, func() error {
		var res sql.Result
		var err error
		if volumesJSON != "" {
			res, err = s.db.ExecContext(ctx,
				`UPDATE backups SET state = ?, volumes = ?, finished = ? WHERE id = ? AND state = ?`,
				string(state), volumesJSON, time.Now().UnixNano(), id, string(BackupCreating))
		} else {
			res, err = s.db.ExecContext(ctx,
				`UPDATE backups SET state = ?, finished = ? WHERE id = ? AND state = ?`,
				string(state), time.Now().UnixNano(), id, string(BackupCreating))
		}
		if err != nil {
			return err
		}
		n, err = res.RowsAffected()
		return err
	})
	return n > 0, err
}

func (s *SQLite) GetBackup(ctx context.Context, id string) (Backup, error) {
	row := s.db.QueryRowContext(ctx,
		`SELECT id, host, template, slug, state, volumes, image, created, COALESCE(finished, 0) FROM backups WHERE id = ?`, id)
	return scanBackup(row)
}

func (s *SQLite) ListBackups(ctx context.Context, host, template, slug string, limit int) ([]Backup, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT id, host, template, slug, state, volumes, image, created, COALESCE(finished, 0)
		 FROM backups WHERE host = ? AND template = ? AND slug = ?
		 ORDER BY created DESC, id DESC LIMIT ?`,
		host, template, slug, clampJobLimit(limit))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Backup
	for rows.Next() {
		b, err := scanBackup(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, b)
	}
	return out, rows.Err()
}

func (s *SQLite) DeleteBackup(ctx context.Context, id string) error {
	s.wmu.Lock()
	defer s.wmu.Unlock()
	var n int64
	err := retryBusy(ctx, func() error {
		res, err := s.db.ExecContext(ctx, `DELETE FROM backups WHERE id = ?`, id)
		if err != nil {
			return err
		}
		n, err = res.RowsAffected()
		return err
	})
	if err != nil {
		return err
	}
	if n == 0 {
		return ErrNotFound
	}
	return nil
}

type rowScanner interface{ Scan(dest ...any) error }

func scanBackup(r rowScanner) (Backup, error) {
	var b Backup
	var state, vols string
	var created, finished int64
	if err := r.Scan(&b.ID, &b.Host, &b.Template, &b.Slug, &state, &vols, &b.Image, &created, &finished); err != nil {
		if err == sql.ErrNoRows {
			return Backup{}, ErrNotFound
		}
		return Backup{}, err
	}
	b.State = BackupState(state)
	if err := json.Unmarshal([]byte(vols), &b.Volumes); err != nil {
		return Backup{}, fmt.Errorf("backup %s: volumes column corrupt: %w", b.ID, err)
	}
	b.Created = time.Unix(0, created)
	if finished != 0 {
		b.Finished = time.Unix(0, finished)
	}
	return b, nil
}
```

(If `SQLite`'s write-mutex field is named differently than `wmu`, or reads also go through a helper, match the file.)

Memory impl in `internal/store/memory.go` — a `backups map[string]Backup` guarded by the existing mutex, list filtered + sorted by `Created` desc then `ID` desc, deep-copying `Volumes` on read/write the way other Memory methods copy.

Finally add `BackupStore` to the `DB` interface in `internal/store/jobs.go`:

```go
type DB interface {
	Store
	JobStore
	TemplateStore
	BackupStore
	io.Closer
}
```

- [ ] **Step 5: Run tests + vet**

Run: `go test -tags "$TAGS" ./internal/store/ -v -run TestBackups && go vet -tags "$TAGS" ./internal/store/`
Expected: PASS

- [ ] **Step 6: Run the whole store package** (no regressions)

Run: `go test -tags "$TAGS" ./internal/store/`
Expected: PASS

- [ ] **Step 7: Commit**

```bash
git add internal/store/
git commit -m "feat(66): backups table + BackupStore (sqlite + memory)"
```

---

### Task 2: Manifest JSON round-trip

**Files:**
- Modify: `internal/instance/manifest.go`
- Test: `internal/instance/manifest_test.go` (append)

- [ ] **Step 1: Write the failing test** (append to `manifest_test.go`; reuse its existing tar-building helpers if present):

```go
func TestManifest_JSONRoundTrip(t *testing.T) {
	m := Manifest{
		"data/file":   fileInfo{typ: tar.TypeReg, size: 5, sha256: "abc123"},
		"data/link":   fileInfo{typ: tar.TypeSymlink, link: "file"},
		"data":        fileInfo{typ: tar.TypeDir},
	}
	raw, err := json.Marshal(m)
	require.NoError(t, err)
	var got Manifest
	require.NoError(t, json.Unmarshal(raw, &got))
	diff, equal := m.firstDiff(got)
	assert.True(t, equal, "round-trip changed manifest at %q", diff)
}

func TestManifest_JSONRoundTrip_DetectsDiff(t *testing.T) {
	m := Manifest{"f": fileInfo{typ: tar.TypeReg, size: 1, sha256: "aa"}}
	raw, _ := json.Marshal(m)
	var got Manifest
	require.NoError(t, json.Unmarshal(raw, &got))
	got["f"] = fileInfo{typ: tar.TypeReg, size: 1, sha256: "bb"}
	_, equal := m.firstDiff(got)
	assert.False(t, equal)
}
```

- [ ] **Step 2: Run to verify failure**

Run: `go test -tags "$TAGS" ./internal/instance/ -run TestManifest_JSON -v`
Expected: FAIL (fileInfo has no exported fields → `{}` marshals, round-trip loses data; or compile error if json import missing)

- [ ] **Step 3: Implement** — add to `manifest.go`:

```go
// fileInfoJSON is fileInfo's serialized form, used to persist a backup's
// manifest in the backups table (#66). Field set mirrors fileInfo exactly.
type fileInfoJSON struct {
	Type   byte   `json:"type"`
	Size   int64  `json:"size,omitempty"`
	Sha256 string `json:"sha256,omitempty"`
	Link   string `json:"link,omitempty"`
}

func (f fileInfo) MarshalJSON() ([]byte, error) {
	return json.Marshal(fileInfoJSON{Type: f.typ, Size: f.size, Sha256: f.sha256, Link: f.link})
}

func (f *fileInfo) UnmarshalJSON(b []byte) error {
	var j fileInfoJSON
	if err := json.Unmarshal(b, &j); err != nil {
		return err
	}
	*f = fileInfo{typ: j.Type, size: j.Size, sha256: j.Sha256, link: j.Link}
	return nil
}
```

(add `"encoding/json"` to imports)

- [ ] **Step 4: Run tests**

Run: `go test -tags "$TAGS" ./internal/instance/ -run TestManifest -v`
Expected: PASS (including pre-existing manifest tests)

- [ ] **Step 5: Commit**

```bash
git add internal/instance/manifest.go internal/instance/manifest_test.go
git commit -m "feat(66): manifest JSON round-trip for backup metadata"
```

---

### Task 3: BlobStore seam + LocalDir implementation

The interfaces live in `internal/instance` (the consumer) so `instance` never imports `internal/backup` — `backup` imports `instance`, avoiding a cycle. `LocalDir` is the only OSS impl; #107's S3 backend will implement the same interfaces.

**Files:**
- Create: `internal/instance/blobstore.go`
- Create: `internal/backup/localdir.go`
- Create: `internal/backup/localdir_test.go`

- [ ] **Step 1: Define the interfaces** in `internal/instance/blobstore.go`:

```go
package instance

import (
	"context"
	"io"
)

// BlobWriter is one streamed blob write. Exactly one of Commit or Abort must
// be called; only Commit makes the blob visible to Get. This is the
// temp-file+rename contract: a failed backup never leaves a partial blob that
// looks complete.
type BlobWriter interface {
	io.Writer
	Commit() error
	Abort() error
}

// BlobStore is where backup artifacts rest. The OSS implementation is a local
// directory (internal/backup.LocalDir); the commercial S3 backend implements
// the same seam (#107). Keys are slash-separated relative paths
// (fs.ValidPath); Get returns an error satisfying errors.Is(err,
// fs.ErrNotExist) for a missing blob.
type BlobStore interface {
	Put(ctx context.Context, key string) (BlobWriter, error)
	Get(ctx context.Context, key string) (io.ReadCloser, error)
	// DeleteAll removes every blob under the directory-like key prefix.
	// Removing an absent prefix is a no-op.
	DeleteAll(ctx context.Context, prefix string) error
}
```

- [ ] **Step 2: Write failing LocalDir tests** in `internal/backup/localdir_test.go`:

```go
package backup

import (
	"context"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLocalDir_PutGetRoundTrip(t *testing.T) {
	ctx := context.Background()
	l, err := NewLocalDir(t.TempDir())
	require.NoError(t, err)
	w, err := l.Put(ctx, "h1/pg/a/bk_1/pg-a-data.tar")
	require.NoError(t, err)
	_, err = w.Write([]byte("tarbytes"))
	require.NoError(t, err)
	require.NoError(t, w.Commit())

	rc, err := l.Get(ctx, "h1/pg/a/bk_1/pg-a-data.tar")
	require.NoError(t, err)
	defer rc.Close()
	got, err := io.ReadAll(rc)
	require.NoError(t, err)
	assert.Equal(t, "tarbytes", string(got))
}

func TestLocalDir_UncommittedWriteInvisible(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	l, err := NewLocalDir(root)
	require.NoError(t, err)
	w, err := l.Put(ctx, "h/t/s/bk/v.tar")
	require.NoError(t, err)
	_, err = w.Write([]byte("partial"))
	require.NoError(t, err)
	// no Commit:
	_, err = l.Get(ctx, "h/t/s/bk/v.tar")
	assert.ErrorIs(t, err, fs.ErrNotExist)
	// Abort removes the temp file entirely.
	require.NoError(t, w.Abort())
	entries, err := os.ReadDir(filepath.Join(root, "h/t/s/bk"))
	require.NoError(t, err)
	assert.Empty(t, entries)
}

func TestLocalDir_GetMissingIsNotExist(t *testing.T) {
	l, err := NewLocalDir(t.TempDir())
	require.NoError(t, err)
	_, err = l.Get(context.Background(), "no/such/key.tar")
	assert.ErrorIs(t, err, fs.ErrNotExist)
}

func TestLocalDir_DeleteAll(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	l, err := NewLocalDir(root)
	require.NoError(t, err)
	for _, k := range []string{"h/t/s/bk1/a.tar", "h/t/s/bk1/b.tar", "h/t/s/bk2/a.tar"} {
		w, err := l.Put(ctx, k)
		require.NoError(t, err)
		_, _ = w.Write([]byte("x"))
		require.NoError(t, w.Commit())
	}
	require.NoError(t, l.DeleteAll(ctx, "h/t/s/bk1"))
	_, err = l.Get(ctx, "h/t/s/bk1/a.tar")
	assert.ErrorIs(t, err, fs.ErrNotExist)
	_, err = l.Get(ctx, "h/t/s/bk2/a.tar")
	assert.NoError(t, err)
	// absent prefix: no-op
	assert.NoError(t, l.DeleteAll(ctx, "h/t/s/never"))
}

func TestLocalDir_RejectsBadKeys(t *testing.T) {
	l, err := NewLocalDir(t.TempDir())
	require.NoError(t, err)
	ctx := context.Background()
	for _, k := range []string{"", ".", "..", "../escape", "/abs", "a/../../b"} {
		_, err := l.Put(ctx, k)
		assert.Error(t, err, "Put(%q) must be rejected", k)
		_, err = l.Get(ctx, k)
		assert.Error(t, err, "Get(%q) must be rejected", k)
		assert.Error(t, l.DeleteAll(ctx, k), "DeleteAll(%q) must be rejected", k)
	}
}
```

- [ ] **Step 3: Run to verify failure**

Run: `go test -tags "$TAGS" ./internal/backup/ -v`
Expected: compile FAIL (package does not exist)

- [ ] **Step 4: Implement** `internal/backup/localdir.go`:

```go
// Package backup holds the OSS backup primitives around instance.Service:
// the local-directory blob store and the backup/restore job adapters (#66).
package backup

import (
	"context"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"

	"github.com/iotready/podman-api/internal/instance"
)

// LocalDir is the OSS BlobStore: blobs are plain files under a root
// directory, written via temp-file + rename so a partial write is never
// visible as a complete blob.
type LocalDir struct {
	root string
}

// NewLocalDir creates (if needed) and opens the root directory.
func NewLocalDir(root string) (*LocalDir, error) {
	if err := os.MkdirAll(root, 0o750); err != nil {
		return nil, fmt.Errorf("backup dir: %w", err)
	}
	return &LocalDir{root: root}, nil
}

// path validates key (slash-separated, relative, no "..") and resolves it
// under root.
func (l *LocalDir) path(key string) (string, error) {
	if !fs.ValidPath(key) || key == "." {
		return "", fmt.Errorf("invalid blob key %q", key)
	}
	return filepath.Join(l.root, filepath.FromSlash(key)), nil
}

func (l *LocalDir) Put(_ context.Context, key string) (instance.BlobWriter, error) {
	p, err := l.path(key)
	if err != nil {
		return nil, err
	}
	if err := os.MkdirAll(filepath.Dir(p), 0o750); err != nil {
		return nil, err
	}
	f, err := os.CreateTemp(filepath.Dir(p), ".tmp-*")
	if err != nil {
		return nil, err
	}
	return &fileWriter{f: f, final: p}, nil
}

type fileWriter struct {
	f     *os.File
	final string
}

func (w *fileWriter) Write(p []byte) (int, error) { return w.f.Write(p) }

// Commit fsyncs, closes, and renames the temp file into place — the blob
// becomes visible atomically and durably.
func (w *fileWriter) Commit() error {
	if err := w.f.Sync(); err != nil {
		w.f.Close()
		os.Remove(w.f.Name())
		return err
	}
	if err := w.f.Close(); err != nil {
		os.Remove(w.f.Name())
		return err
	}
	return os.Rename(w.f.Name(), w.final)
}

// Abort discards the temp file; the key remains absent.
func (w *fileWriter) Abort() error {
	w.f.Close()
	return os.Remove(w.f.Name())
}

func (l *LocalDir) Get(_ context.Context, key string) (io.ReadCloser, error) {
	p, err := l.path(key)
	if err != nil {
		return nil, err
	}
	return os.Open(p) // missing file → *PathError wrapping fs.ErrNotExist
}

func (l *LocalDir) DeleteAll(_ context.Context, prefix string) error {
	p, err := l.path(prefix)
	if err != nil {
		return err
	}
	return os.RemoveAll(p) // absent → nil
}

var _ instance.BlobStore = (*LocalDir)(nil)
```

- [ ] **Step 5: Run tests**

Run: `go test -tags "$TAGS" ./internal/backup/ -v && go vet -tags "$TAGS" ./internal/backup/ ./internal/instance/`
Expected: PASS

- [ ] **Step 6: Commit**

```bash
git add internal/instance/blobstore.go internal/backup/
git commit -m "feat(66): BlobStore seam + LocalDir temp-file/rename impl"
```

---

### Task 4: `instance.Service.Backup`

**Files:**
- Create: `internal/instance/backup.go`
- Create: `internal/instance/backup_test.go`
- Modify: `internal/instance/service.go` (add `store.BackupStore` to the `Store` interface; add `blobs` field + `SetBlobStore`)

**Flow recap (spec):** lock → validate → record image + was-running → insert `creating` row → stop → per volume: export teed into blob+manifest → complete row → restart (deferred-style, only if previously running). Failure: mark row failed, delete partial blobs, restart.

- [ ] **Step 1: Modify `service.go`.** In the `Store` interface add the backup store; in `Service` add the blob store:

```go
type Store interface {
	store.Store
	store.TemplateStore
	store.BackupStore
}
```

```go
	// in type Service struct:
	blobs BlobStore // backup artifact store; set via SetBlobStore (nil → backups disabled)
```

```go
// SetBlobStore wires the backup artifact store. Backups/restores are refused
// (ErrBackupsDisabled) until this is set; main always sets it.
func (s *Service) SetBlobStore(bs BlobStore) { s.blobs = bs }
```

Add sentinels next to the existing ones in `service.go`:

```go
	ErrBackupNotFound      = errors.New("backup not found")
	ErrBackupNotRestorable = errors.New("backup is not restorable")
	ErrBackupBusy          = errors.New("backup has a restore in flight")
	ErrBackupsDisabled     = errors.New("backups require a blob store (-backup-dir)")
```

- [ ] **Step 2: Write failing tests** in `internal/instance/backup_test.go`. Use the package's existing test scaffolding style (see `migrate_test.go` / `volcopy_test.go` for how fakes + Memory store + templates are wired; reuse helpers where they exist). The fake (`internal/podman/fake`) supports volumes, `ExportErr`, pods. LocalDir can't be used here (instance can't import backup) — write a tiny in-memory BlobStore test double in this file:

```go
// memBlob is an in-memory BlobStore: committed blobs land in data; aborted
// writes vanish. PutErr forces Put to fail.
type memBlob struct {
	mu     sync.Mutex
	data   map[string][]byte
	PutErr error
}

func newMemBlob() *memBlob { return &memBlob{data: map[string][]byte{}} }

func (m *memBlob) Put(_ context.Context, key string) (BlobWriter, error) {
	if m.PutErr != nil {
		return nil, m.PutErr
	}
	return &memBlobWriter{m: m, key: key}, nil
}

type memBlobWriter struct {
	m   *memBlob
	key string
	buf bytes.Buffer
}

func (w *memBlobWriter) Write(p []byte) (int, error) { return w.buf.Write(p) }
func (w *memBlobWriter) Commit() error {
	w.m.mu.Lock()
	defer w.m.mu.Unlock()
	w.m.data[w.key] = w.buf.Bytes()
	return nil
}
func (w *memBlobWriter) Abort() error { return nil }

func (m *memBlob) Get(_ context.Context, key string) (io.ReadCloser, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	b, ok := m.data[key]
	if !ok {
		return nil, fs.ErrNotExist
	}
	return io.NopCloser(bytes.NewReader(b)), nil
}

func (m *memBlob) DeleteAll(_ context.Context, prefix string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	for k := range m.data {
		if strings.HasPrefix(k, prefix+"/") || k == prefix {
			delete(m.data, k)
		}
	}
	return nil
}
```

Key test cases (build a running instance on the fake — template with one volume, pod running, volume containing a known tar — the same way volcopy/migrate tests seed theirs):

```go
func TestBackup_HappyPath(t *testing.T) {
	// seed: host h1, template pg (1 volume "data"), instance pg/a running,
	// volume "pg-a-data" present on the fake with tar content.
	svc, f, mem, blob := newBackupSvc(t) // helper built in this file
	id := store.NewBackupID()
	var steps []string
	err := svc.Backup(context.Background(), BackupRequest{
		BackupID: id, Host: "h1", Template: "pg", Slug: "a",
	}, func(s, d string) { steps = append(steps, s) })
	require.NoError(t, err)

	b, err := mem.GetBackup(context.Background(), id)
	require.NoError(t, err)
	assert.Equal(t, store.BackupComplete, b.State)
	require.Len(t, b.Volumes, 1)
	assert.Equal(t, "pg-a-data", b.Volumes[0].Name)
	assert.Greater(t, b.Volumes[0].SizeBytes, int64(0))
	assert.NotEmpty(t, b.Volumes[0].Manifest)

	// blob exists under the documented key layout
	_, err = blob.Get(context.Background(), "h1/pg/a/"+id+"/pg-a-data.tar")
	assert.NoError(t, err)

	// instance restarted (was running before)
	p, err := f.PodInspect(context.Background(), "h1", "pg-a")
	require.NoError(t, err)
	assert.Equal(t, "Running", p.Status)
	assert.Contains(t, steps, "stop")
	assert.Contains(t, steps, "export-volume")
}

func TestBackup_StoppedInstanceStaysStopped(t *testing.T) {
	// same seed but pod stopped before backup; after success the pod must
	// NOT be running.
}

func TestBackup_ExportFailureMarksFailedRestartsAndCleansBlobs(t *testing.T) {
	// f.ExportErr = errors.New("boom"); Backup fails; row state failed;
	// blob.data empty; pod running again.
}

func TestBackup_UnknownHost(t *testing.T)       // → ErrUnknownHost
func TestBackup_NoSpec(t *testing.T)            // spec absent → ErrInstanceNotFound
func TestBackup_NoBlobStore(t *testing.T)       // SetBlobStore never called → ErrBackupsDisabled
```

Write all six fully — the abbreviated ones above follow the happy-path mechanics with one knob changed.

- [ ] **Step 3: Run to verify failure**

Run: `go test -tags "$TAGS" ./internal/instance/ -run TestBackup -v`
Expected: compile FAIL (`BackupRequest` undefined)

- [ ] **Step 4: Implement** `internal/instance/backup.go`:

```go
package instance

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"

	"github.com/iotready/podman-api/internal/store"
)

// BackupRequest is the backup job's args. BackupID is generated at enqueue
// time (store.NewBackupID) so POST can return it before the job runs.
type BackupRequest struct {
	BackupID string `json:"backup_id"`
	Host     string `json:"host"`
	Template string `json:"template"`
	Slug     string `json:"slug"`
}

// backupBlobKey is the blob layout: <host>/<template>/<slug>/<backup-id>/<volume>.tar
func backupBlobKey(host, tmpl, slug, id, volume string) string {
	return host + "/" + tmpl + "/" + slug + "/" + id + "/" + volume + ".tar"
}

// backupBlobPrefix addresses every blob of one backup (for DeleteAll).
func backupBlobPrefix(host, tmpl, slug, id string) string {
	return host + "/" + tmpl + "/" + slug + "/" + id
}

// CheckBackupable runs the cheap synchronous validation the POST handler
// needs: known host, known template, stored spec present, blob store wired.
func (s *Service) CheckBackupable(ctx context.Context, host, tmpl, slug string) error {
	if s.blobs == nil {
		return ErrBackupsDisabled
	}
	if _, err := s.lookup(ctx, host, tmpl); err != nil {
		return err
	}
	if _, err := s.store.GetSpec(ctx, host, tmpl, slug); err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return ErrInstanceNotFound
		}
		return err
	}
	return nil
}

// Backup snapshots every volume of an instance into the blob store: stop,
// export each volume (teed into the blob write and the manifest build in one
// pass), record metadata, restart. The instance is restarted even on failure;
// it is only restarted at all if it was running to begin with. step is a
// best-effort progress callback (may be nil).
func (s *Service) Backup(ctx context.Context, req BackupRequest, step func(step, detail string)) error {
	if step == nil {
		step = func(string, string) {}
	}
	// Same lock as migrate: backup/restore/migrate of one instance serialize.
	lk := s.migrateLock(req.Template, req.Slug)
	lk.Lock()
	defer lk.Unlock()

	if err := s.CheckBackupable(ctx, req.Host, req.Template, req.Slug); err != nil {
		return err
	}

	// Image hint + prior run-state. Get also confirms the pod exists.
	obs, err := s.Get(ctx, req.Host, req.Template, req.Slug)
	if err != nil {
		return err
	}
	wasRunning := obs.Pod.Status == "Running"
	image := ""
	if len(obs.Containers) > 0 {
		image = obs.Containers[0].Image
	}
	step("load", req.Host+"/"+req.Template+"/"+req.Slug)

	if err := s.store.CreateBackup(ctx, store.Backup{
		ID: req.BackupID, Host: req.Host, Template: req.Template, Slug: req.Slug,
		State: store.BackupCreating, Image: image,
	}); err != nil {
		return fmt.Errorf("record backup: %w", err)
	}

	// Cleanup helpers run on a detached context: the failure may BE a ctx
	// cancellation, and the row must still be marked failed / the instance
	// restarted (same pattern as migrate's rollback).
	fail := func(cause error) error {
		dctx := context.WithoutCancel(ctx)
		if _, ferr := s.store.FailBackup(dctx, req.BackupID); ferr != nil {
			step("mark-failed-failed", ferr.Error())
		}
		if derr := s.blobs.DeleteAll(dctx, backupBlobPrefix(req.Host, req.Template, req.Slug, req.BackupID)); derr != nil {
			step("cleanup-blobs-failed", derr.Error())
		}
		return cause
	}
	restart := func() {
		if !wasRunning {
			return
		}
		if rerr := s.Start(context.WithoutCancel(ctx), req.Host, req.Template, req.Slug); rerr != nil {
			step("restart-failed", rerr.Error())
		} else {
			step("restart", req.Host)
		}
	}

	if err := s.Stop(ctx, req.Host, req.Template, req.Slug); err != nil {
		return fail(fmt.Errorf("stop instance: %w", err))
	}
	step("stop", req.Host)

	vols, err := s.InstanceVolumes(ctx, req.Host, req.Template, req.Slug)
	if err != nil {
		restart()
		return fail(fmt.Errorf("list volumes: %w", err))
	}
	var bvols []store.BackupVolume
	for _, v := range vols {
		bv, err := s.backupVolume(ctx, req, v.Name)
		if err != nil {
			restart()
			return fail(fmt.Errorf("backup volume %q: %w", v.Name, err))
		}
		bvols = append(bvols, bv)
		step("export-volume", v.Name)
	}

	ok, err := s.store.CompleteBackup(ctx, req.BackupID, bvols)
	if err != nil {
		restart()
		return fail(fmt.Errorf("complete backup: %w", err))
	}
	if !ok {
		// Row left creating-state while we held the lock — only a concurrent
		// reconciler marking it failed can do that, which cannot happen while
		// the job itself is live. Defensive.
		restart()
		return fail(fmt.Errorf("backup %s no longer in creating state", req.BackupID))
	}
	restart()
	step("complete", req.BackupID)
	return nil
}

// backupVolume exports one volume, teeing the tar into the blob store and
// the manifest builder in a single pass. The blob is committed only after a
// clean EOF + manifest build.
func (s *Service) backupVolume(ctx context.Context, req BackupRequest, name string) (store.BackupVolume, error) {
	rc, err := s.client.VolumeExport(ctx, req.Host, name)
	if err != nil {
		return store.BackupVolume{}, fmt.Errorf("export: %w", err)
	}
	defer rc.Close()

	w, err := s.blobs.Put(ctx, backupBlobKey(req.Host, req.Template, req.Slug, req.BackupID, name))
	if err != nil {
		return store.BackupVolume{}, fmt.Errorf("open blob: %w", err)
	}
	cw := &countingWriter{w: w}
	m, err := buildManifest(io.TeeReader(rc, cw))
	if err != nil {
		_ = w.Abort()
		return store.BackupVolume{}, fmt.Errorf("read tar: %w", err)
	}
	if err := w.Commit(); err != nil {
		return store.BackupVolume{}, fmt.Errorf("commit blob: %w", err)
	}
	raw, err := json.Marshal(m)
	if err != nil {
		return store.BackupVolume{}, fmt.Errorf("marshal manifest: %w", err)
	}
	return store.BackupVolume{Name: name, SizeBytes: cw.n, Manifest: raw}, nil
}

// countingWriter counts bytes through to w.
type countingWriter struct {
	w io.Writer
	n int64
}

func (c *countingWriter) Write(p []byte) (int, error) {
	n, err := c.w.Write(p)
	c.n += int64(n)
	return n, err
}
```

- [ ] **Step 5: Run tests**

Run: `go test -tags "$TAGS" ./internal/instance/ -run TestBackup -v`
Expected: PASS

- [ ] **Step 6: Run the whole instance package** (the `Store` interface change must not break existing tests — `store.Memory` already satisfies it after Task 1)

Run: `go test -tags "$TAGS" ./internal/instance/`
Expected: PASS

- [ ] **Step 7: Commit**

```bash
git add internal/instance/
git commit -m "feat(66): Service.Backup — stop, tee export into blob+manifest, restart"
```

---

### Task 5: `instance.Service.Restore` + list/delete/busy helpers

**Files:**
- Modify: `internal/instance/backup.go` (append)
- Modify: `internal/instance/backup_test.go` (append)

- [ ] **Step 1: Write failing tests** (append to `backup_test.go`):

```go
func TestRestore_HappyPath(t *testing.T) {
	// 1. seed running instance with volume content A; Backup it.
	// 2. mutate the volume to content B (fake exposes volume contents —
	//    re-seed the volume the same way the original content was seeded).
	// 3. Restore. Assert: volume content == A again (export + compare),
	//    pod Running, spec still present in store.
}

func TestRestore_VerifyMismatchFails(t *testing.T) {
	// Backup, then corrupt the stored manifest row (mem.CompleteBackup with a
	// manifest whose sha256 differs — easiest: GetBackup, tweak
	// Volumes[0].Manifest JSON, write back via direct Memory mutation or
	// re-create the row). Restore must fail with ErrVolumeIntegrity and NOT
	// reach Apply (pod stays absent).
}

func TestRestore_NotComplete(t *testing.T) {
	// row in creating state → CheckRestorable returns ErrBackupNotRestorable.
}

func TestRestore_MissingBlob(t *testing.T) {
	// complete row but blob.DeleteAll'd → Restore fails wrapping
	// ErrBackupNotRestorable.
}

func TestRestore_MissingBackup(t *testing.T) {
	// no row → ErrBackupNotFound.
}

func TestRestore_InstanceGone(t *testing.T) {
	// spec deleted → ErrInstanceNotFound (in-place restore requires the
	// instance to exist).
}

func TestRestore_DrainingHostRefusedUpfront(t *testing.T) {
	// host cfg Drain=true → ErrHostDraining BEFORE any teardown (pod still
	// present afterwards).
}

func TestRestoreInFlight(t *testing.T) {
	// Enqueue a restore job for bk_X in a Memory job store →
	// RestoreInFlight(ctx, mem, "bk_X") == true; for another id == false;
	// after Finish(succeeded) == false.
}
```

Implement each fully using the same seeding helpers as Task 4. For restore tests `instance.SetVerifyTimeout` / package-level `verifyTimeout` may need shortening when the fake pod doesn't report Running — the fake's pods report Running after `PodStart`/`PlayKube`, so the happy path verifies quickly; for failure tests set `verifyTimeout` low via the package-level var (same-package test, direct assignment with `t.Cleanup` restore).

- [ ] **Step 2: Run to verify failure**

Run: `go test -tags "$TAGS" ./internal/instance/ -run 'TestRestore' -v`
Expected: compile FAIL

- [ ] **Step 3: Implement** (append to `internal/instance/backup.go`):

```go
// RestoreRequest is the restore job's args.
type RestoreRequest struct {
	BackupID string `json:"backup_id"`
}

// CheckRestorable runs the synchronous validation the POST handler needs and
// returns the backup row: row exists and is complete, host known and not
// draining, instance (spec) still present. The drain check is upfront so a
// draining host can't fail the job after teardown.
func (s *Service) CheckRestorable(ctx context.Context, backupID string) (store.Backup, error) {
	if s.blobs == nil {
		return store.Backup{}, ErrBackupsDisabled
	}
	b, err := s.store.GetBackup(ctx, backupID)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return store.Backup{}, fmt.Errorf("%w: %s", ErrBackupNotFound, backupID)
		}
		return store.Backup{}, err
	}
	if b.State != store.BackupComplete {
		return store.Backup{}, fmt.Errorf("%w: state %s", ErrBackupNotRestorable, b.State)
	}
	hostCfg, ok := s.host(b.Host)
	if !ok {
		return store.Backup{}, ErrUnknownHost
	}
	if hostCfg.Drain {
		return store.Backup{}, ErrHostDraining
	}
	if _, err := s.store.GetSpec(ctx, b.Host, b.Template, b.Slug); err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return store.Backup{}, ErrInstanceNotFound
		}
		return store.Backup{}, err
	}
	return b, nil
}

// Restore replaces an instance's volumes in place from a backup: stop, tear
// down containers + volumes, recreate volumes from blobs, verify each against
// the stored manifest, re-apply the CURRENT spec, wait healthy. There is no
// rollback: a failure leaves the instance stopped with the job error naming
// the failed step (that is why each volume is verified before declaring
// success). step is a best-effort progress callback (may be nil).
func (s *Service) Restore(ctx context.Context, req RestoreRequest, step func(step, detail string)) error {
	if step == nil {
		step = func(string, string) {}
	}
	b, err := s.CheckRestorable(ctx, req.BackupID)
	if err != nil {
		return err
	}

	lk := s.migrateLock(b.Template, b.Slug)
	lk.Lock()
	defer lk.Unlock()

	// Re-check under the lock (a concurrent delete may have raced us).
	b, err = s.CheckRestorable(ctx, req.BackupID)
	if err != nil {
		return err
	}
	spec, err := s.store.GetSpec(ctx, b.Host, b.Template, b.Slug)
	if err != nil {
		return err
	}
	step("load", b.Host+"/"+b.Template+"/"+b.Slug)

	// Teardown: pod + volumes (a referenced volume can't be removed). Keep
	// per-instance secrets — Apply below re-pushes them from the spec anyway,
	// and host-scoped secrets must survive. Delete also reconciles away the
	// spec row; Apply re-persists it. Tolerate an already-gone pod.
	if err := s.Delete(ctx, b.Host, b.Template, b.Slug, DeleteOptions{PruneVolumes: true}); err != nil && !errors.Is(err, ErrInstanceNotFound) {
		return fmt.Errorf("teardown: %w", err)
	}
	step("teardown", b.Host)

	for _, bv := range b.Volumes {
		if err := s.restoreVolume(ctx, b, bv); err != nil {
			return fmt.Errorf("restore volume %q: %w", bv.Name, err)
		}
		step("restore-volume", bv.Name)
	}

	if err := s.Apply(ctx, b.Host, ApplyRequest{
		Template: b.Template, Slug: b.Slug,
		Parameters: spec.Parameters, Secrets: spec.Secrets, Domains: spec.Domains,
	}, ApplyOptions{Replace: false}); err != nil {
		return fmt.Errorf("apply: %w", err)
	}
	step("apply", b.Host)

	if err := s.waitRunning(ctx, b.Host, b.Template, b.Slug); err != nil {
		return fmt.Errorf("verify: %w", err)
	}
	step("verify", b.Host)
	return nil
}

// restoreVolume recreates one volume from its blob and verifies the imported
// content against the manifest recorded at backup time (migrate's verify).
func (s *Service) restoreVolume(ctx context.Context, b store.Backup, bv store.BackupVolume) error {
	if err := s.client.VolumeCreate(ctx, b.Host, bv.Name); err != nil {
		return fmt.Errorf("create: %w", err)
	}
	rc, err := s.blobs.Get(ctx, backupBlobKey(b.Host, b.Template, b.Slug, b.ID, bv.Name))
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return fmt.Errorf("%w: blob for volume %q missing", ErrBackupNotRestorable, bv.Name)
		}
		return fmt.Errorf("open blob: %w", err)
	}
	defer rc.Close()
	if err := s.client.VolumeImport(ctx, b.Host, bv.Name, rc); err != nil {
		return fmt.Errorf("import: %w", err)
	}

	var want Manifest
	if err := json.Unmarshal(bv.Manifest, &want); err != nil {
		return fmt.Errorf("stored manifest corrupt: %w", err)
	}
	got, err := s.volumeManifest(ctx, b.Host, bv.Name)
	if err != nil {
		return fmt.Errorf("re-export for verify: %w", err)
	}
	if diff, ok := want.firstDiff(got); !ok {
		return fmt.Errorf("%w: volume %q differs at %q", ErrVolumeIntegrity, bv.Name, diff)
	}
	return nil
}

// ListBackups returns an instance's backups, newest first.
func (s *Service) ListBackups(ctx context.Context, host, tmpl, slug string, limit int) ([]store.Backup, error) {
	if _, ok := s.host(host); !ok {
		return nil, ErrUnknownHost
	}
	return s.store.ListBackups(ctx, host, tmpl, slug, limit)
}

// GetBackup returns one backup row, mapping absence to ErrBackupNotFound.
func (s *Service) GetBackup(ctx context.Context, id string) (store.Backup, error) {
	b, err := s.store.GetBackup(ctx, id)
	if errors.Is(err, store.ErrNotFound) {
		return store.Backup{}, fmt.Errorf("%w: %s", ErrBackupNotFound, id)
	}
	return b, err
}

// DeleteBackup removes a backup's blobs, then its row — in that order, so a
// crash between the two leaves a harmless blob-less row rather than orphaned
// blobs. The caller (API/UI) must check RestoreInFlight first.
func (s *Service) DeleteBackup(ctx context.Context, id string) error {
	if s.blobs == nil {
		return ErrBackupsDisabled
	}
	b, err := s.GetBackup(ctx, id)
	if err != nil {
		return err
	}
	if err := s.blobs.DeleteAll(ctx, backupBlobPrefix(b.Host, b.Template, b.Slug, b.ID)); err != nil {
		return fmt.Errorf("delete blobs: %w", err)
	}
	if err := s.store.DeleteBackup(ctx, id); err != nil && !errors.Is(err, store.ErrNotFound) {
		return err
	}
	return nil
}

// RestoreInFlight reports whether any active (queued/running/reconciling)
// restore job targets backupID. Shared by the API and UI delete handlers to
// refuse deleting a backup mid-restore (ErrBackupBusy).
func RestoreInFlight(ctx context.Context, js store.JobStore, backupID string) (bool, error) {
	for _, st := range []store.JobState{store.JobQueued, store.JobRunning, store.JobReconciling} {
		jobsList, err := js.ListJobs(ctx, store.JobFilter{State: st, Kind: "restore", Limit: store.MaxJobLimit})
		if err != nil {
			return false, err
		}
		for _, j := range jobsList {
			var req RestoreRequest
			if err := json.Unmarshal(j.Args, &req); err != nil {
				continue
			}
			if req.BackupID == backupID {
				return true, nil
			}
		}
	}
	return false, nil
}
```

(add `"io/fs"` to imports)

- [ ] **Step 4: Run tests**

Run: `go test -tags "$TAGS" ./internal/instance/ -run 'TestBackup|TestRestore' -v && go test -tags "$TAGS" ./internal/instance/`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add internal/instance/
git commit -m "feat(66): Service.Restore with manifest verify + backup list/delete/busy helpers"
```

---

### Task 6: Job adapters — `backup` and `restore` kinds + backup reconciler

**Files:**
- Create: `internal/backup/handler.go`
- Create: `internal/backup/reconciler.go`
- Create: `internal/backup/handler_test.go`
- Create: `internal/backup/reconciler_test.go`

Mirror `internal/migrate/handler.go` / `reconciler.go` exactly in shape.

**Reconcile policy (from spec + planning):**
- `backup` is reconcilable: an interrupted backup is safely resolved by marking the row failed (CAS — no-op if it completed), deleting its blobs, and starting the instance (best-effort; Start of an already-running pod is harmless). Always resolves terminal-failed unless the host is unreachable (then inconclusive → retry next sweep).
- `restore` is **not** reconcilable: boot `FailRunning` marks it failed ("interrupted by daemon restart"); the operator simply re-runs the restore, which is idempotent from the blob. Document this in the handler comment.

- [ ] **Step 1: Write failing handler tests** in `internal/backup/handler_test.go`:

```go
package backup

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/iotready/podman-api/internal/instance"
	"github.com/iotready/podman-api/internal/jobs"
	"github.com/iotready/podman-api/internal/store"
)

func TestHandler_RunDecodesArgsAndBacksUp(t *testing.T) {
	// Seed a runnable Service the same way internal/instance backup tests do
	// (fake podman + store.Memory + LocalDir over t.TempDir()), enqueue a job
	// whose args marshal a BackupRequest, run the handler with
	// jobs.NewJobContext(mem, job.ID), and assert the backup row completes.
}

func TestHandler_BadArgsFails(t *testing.T) {
	h := &Handler{}
	err := h.Run(context.Background(), store.Job{Args: json.RawMessage(`{`)}, jobs.NewJobContext(store.NewMemory(), "j1"))
	assert.Error(t, err)
}

func TestRestoreHandler_BadArgsFails(t *testing.T) {
	h := &RestoreHandler{}
	err := h.Run(context.Background(), store.Job{Args: json.RawMessage(`{`)}, jobs.NewJobContext(store.NewMemory(), "j1"))
	assert.Error(t, err)
}
```

Plus a `TestRestoreHandler_RunRoundTrip` mirroring the instance-level happy path through the handler. Reconciler tests in `reconciler_test.go`:

```go
func TestReconciler_MarksCreatingFailedCleansBlobsAndRestarts(t *testing.T) {
	// Seed: complete backup machinery, then manually CreateBackup a row in
	// creating state + a committed stray blob under its prefix + stopped pod.
	// Reconcile a job whose args name that backup. Assert: resolved=true,
	// state=JobFailed, row state failed, blobs gone, pod started.
}

func TestReconciler_CompletedRowResolvesSucceeded(t *testing.T) {
	// Row already complete (job died between CompleteBackup and runner
	// finish): FailBackup CAS no-ops → reconciler resolves JobSucceeded.
}

func TestReconciler_BadArgsResolvesFailed(t *testing.T) {
	// Unparseable args → terminal failure, resolved=true.
}
```

- [ ] **Step 2: Run to verify failure**

Run: `go test -tags "$TAGS" ./internal/backup/ -run 'TestHandler|TestRestoreHandler|TestReconciler' -v`
Expected: compile FAIL

- [ ] **Step 3: Implement** `internal/backup/handler.go`:

```go
package backup

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/iotready/podman-api/internal/instance"
	"github.com/iotready/podman-api/internal/jobs"
	"github.com/iotready/podman-api/internal/store"
)

// Handler runs "backup" jobs by delegating to instance.Service.Backup.
type Handler struct {
	Svc *instance.Service
}

func (h *Handler) Run(ctx context.Context, job store.Job, jc *jobs.JobContext) error {
	var req instance.BackupRequest
	if err := json.Unmarshal(job.Args, &req); err != nil {
		return fmt.Errorf("decode backup args: %w", err)
	}
	return h.Svc.Backup(ctx, req, jc.Step)
}

// RestoreHandler runs "restore" jobs by delegating to instance.Service.Restore.
//
// restore has no reconciler on purpose: an interrupted restore is resolved by
// boot recovery failing the job, and the operator re-running the restore —
// which is idempotent from the blob (it re-does teardown + import + verify).
type RestoreHandler struct {
	Svc *instance.Service
}

func (h *RestoreHandler) Run(ctx context.Context, job store.Job, jc *jobs.JobContext) error {
	var req instance.RestoreRequest
	if err := json.Unmarshal(job.Args, &req); err != nil {
		return fmt.Errorf("decode restore args: %w", err)
	}
	return h.Svc.Restore(ctx, req, jc.Step)
}

var (
	_ jobs.Handler = (*Handler)(nil)
	_ jobs.Handler = (*RestoreHandler)(nil)
)
```

`internal/backup/reconciler.go` — needs a small service surface: add to Task 5's `backup.go` a method `ReconcileBackup` (keeps blob/store access inside instance, mirroring `ReconcileMigrate`):

First append to `internal/instance/backup.go`:

```go
// ReconcileBackup drives a backup interrupted by a daemon restart to a
// terminal state: mark the row failed (CAS — a row that already completed
// means the job finished its work and only the terminal write was lost),
// delete any partial blobs, and restart the instance. Returns
// (ok=true) when the backup actually completed, (ok=false, message) when it
// was failed. resolved=false only when the host is unreachable and the
// restart attempt was inconclusive.
func (s *Service) ReconcileBackup(ctx context.Context, req BackupRequest, step func(step, detail string)) (resolved, ok bool, message string, err error) {
	if step == nil {
		step = func(string, string) {}
	}
	lk := s.migrateLock(req.Template, req.Slug)
	lk.Lock()
	defer lk.Unlock()

	b, gerr := s.store.GetBackup(ctx, req.BackupID)
	if gerr != nil {
		if errors.Is(gerr, store.ErrNotFound) {
			// Row never created — the job died before CreateBackup. Nothing on
			// disk, nothing to clean.
			return true, false, "interrupted before the backup row was created", nil
		}
		return false, false, "", gerr
	}
	if b.State == store.BackupComplete {
		// Work finished; only the job's terminal write was lost.
		return true, true, "", nil
	}
	if _, ferr := s.store.FailBackup(ctx, req.BackupID); ferr != nil {
		return false, false, "", ferr
	}
	step("reconcile-mark-failed", req.BackupID)
	if derr := s.blobs.DeleteAll(ctx, backupBlobPrefix(req.Host, req.Template, req.Slug, req.BackupID)); derr != nil {
		step("reconcile-cleanup-blobs-failed", derr.Error())
	}
	// Restart best-effort: Start of a running pod is harmless; an unreachable
	// host leaves the job reconciling for the next sweep.
	if serr := s.Start(ctx, req.Host, req.Template, req.Slug); serr != nil {
		if errors.Is(serr, ErrInstanceNotFound) || errors.Is(serr, podman.ErrNotFound) {
			step("reconcile-restart-skipped", "instance gone")
		} else {
			return false, false, "", fmt.Errorf("restart instance: %w", serr)
		}
	} else {
		step("reconcile-restart", req.Host)
	}
	return true, false, "backup interrupted by daemon restart; instance restarted", nil
}
```

(add `"github.com/iotready/podman-api/internal/podman"` to backup.go's imports)

Then `internal/backup/reconciler.go`:

```go
package backup

import (
	"context"
	"encoding/json"

	"github.com/iotready/podman-api/internal/instance"
	"github.com/iotready/podman-api/internal/jobs"
	"github.com/iotready/podman-api/internal/store"
)

// Reconciler recovers "backup" jobs interrupted by a daemon restart: the
// backup row is failed (unless the work actually completed), partial blobs
// are deleted, and the instance is restarted.
type Reconciler struct {
	Svc *instance.Service
}

func (r *Reconciler) Reconcile(ctx context.Context, job store.Job, jc *jobs.JobContext) (store.JobState, string, bool, error) {
	var req instance.BackupRequest
	if err := json.Unmarshal(job.Args, &req); err != nil {
		jc.Step("reconcile-bad-args", err.Error())
		return store.JobFailed, "interrupted backup could not be decoded", true, nil
	}
	resolved, ok, message, err := r.Svc.ReconcileBackup(ctx, req, jc.Step)
	if err != nil {
		return "", "", false, err
	}
	if !resolved {
		return "", "", false, nil
	}
	if ok {
		return store.JobSucceeded, message, true, nil
	}
	return store.JobFailed, message, true, nil
}

var _ jobs.Reconciler = (*Reconciler)(nil)
```

- [ ] **Step 4: Run tests**

Run: `go test -tags "$TAGS" ./internal/backup/ ./internal/instance/ -v -run 'TestHandler|TestRestoreHandler|TestReconciler|TestBackup|TestRestore'`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add internal/backup/ internal/instance/
git commit -m "feat(66): backup/restore job adapters + backup reconciler"
```

---### Task 7: API routes, error mapping, OpenAPI

**Files:**
- Create: `internal/api/backups.go`
- Create: `internal/api/backups_test.go`
- Modify: `internal/api/errors.go` (3 new cases)
- Modify: `internal/api/router.go` (4 routes)
- Modify: `api/openapi.yaml` (4 paths + tag)
- Modify: `internal/api/openapi_test.go` (spot-check list)

- [ ] **Step 1: Write failing API tests** in `internal/api/backups_test.go`, following `migrate_test.go`'s `newMigrateSrv` shape (fake + Memory + template + svc — plus `svc.SetBlobStore` over a test-double or `backup.LocalDir` with `t.TempDir()`; `internal/api` may import `internal/backup`). Cases:

```go
// helper: newBackupSrv(t) (*httptest.Server, tok string, f *fake.Fake, mem *store.Memory)
//   like newMigrateSrv but: one host h1, template pg with one volume,
//   deployed instance pg/a (seed spec + pod + volume on the fake),
//   svc.SetBlobStore(localdir over t.TempDir()), router with mem as JobStore.

func TestAPI_PostBackup_EnqueuesAndReturnsIDs(t *testing.T) {
	// POST /hosts/h1/instances/pg/a/backup → 202 {"job_id","backup_id"};
	// job exists in mem with kind backup; args carry the backup_id.
}

func TestAPI_PostBackup_UnknownInstance404(t *testing.T) {
	// POST /hosts/h1/instances/pg/nope/backup → 404 instance_not_found
}

func TestAPI_ListBackups(t *testing.T) {
	// seed two complete rows via mem.CreateBackup+CompleteBackup →
	// GET /hosts/h1/instances/pg/a/backups → 200, newest first;
	// ?limit=1 returns one.
}

func TestAPI_PostRestore_Enqueues(t *testing.T) {
	// seed complete row → POST /backups/{id}/restore → 202 {"job_id"}
}

func TestAPI_PostRestore_NotRestorable422(t *testing.T) {
	// creating-state row → 422 backup_not_restorable
}

func TestAPI_PostRestore_Missing404(t *testing.T) {
	// → 404 backup_not_found
}

func TestAPI_DeleteBackup_RemovesRowAndBlobs(t *testing.T) {
	// → 204; row gone; second delete → 404
}

func TestAPI_DeleteBackup_RestoreInFlight409(t *testing.T) {
	// Enqueue (don't run) a restore job for the id → DELETE → 409 backup_busy
}

func TestAPI_Backup_ScopeEnforced(t *testing.T) {
	// token with only instances:read → POST backup 403 (match the existing
	// scope-test assertion style in this package).
}
```

- [ ] **Step 2: Run to verify failure**

Run: `go test -tags "$TAGS" ./internal/api/ -run TestAPI_.*Backup -v`
Expected: FAIL (404s / compile errors)

- [ ] **Step 3: Implement** `internal/api/backups.go`:

```go
package api

import (
	"encoding/json"
	"net/http"
	"strconv"

	"github.com/iotready/podman-api/internal/instance"
	"github.com/iotready/podman-api/internal/store"
)

// postBackup enqueues a backup job for an instance. The backup id is
// generated here so the response can carry it before the job runs.
func (h *handlers) postBackup(w http.ResponseWriter, r *http.Request) {
	if h.jobs == nil {
		WriteError(w, errJobsDisabled)
		return
	}
	host, tmpl, slug := r.PathValue("host"), r.PathValue("template"), r.PathValue("slug")
	if err := h.svc.CheckBackupable(r.Context(), host, tmpl, slug); err != nil {
		WriteError(w, err)
		return
	}
	req := instance.BackupRequest{BackupID: store.NewBackupID(), Host: host, Template: tmpl, Slug: slug}
	args, err := json.Marshal(req)
	if err != nil {
		WriteJSON(w, http.StatusInternalServerError, ErrorBody{Code: "internal", Message: err.Error()})
		return
	}
	job, err := h.jobs.Enqueue(r.Context(), "backup", args, "")
	if err != nil {
		WriteError(w, err)
		return
	}
	WriteJSON(w, http.StatusAccepted, map[string]string{"job_id": job.ID, "backup_id": req.BackupID})
}

// listBackups returns an instance's backups, newest first. ?limit= clamps
// like the jobs list.
func (h *handlers) listBackups(w http.ResponseWriter, r *http.Request) {
	host, tmpl, slug := r.PathValue("host"), r.PathValue("template"), r.PathValue("slug")
	limit := 0
	if v := r.URL.Query().Get("limit"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil {
			WriteJSON(w, http.StatusBadRequest, ErrorBody{Code: "invalid_request", Message: "limit must be an integer"})
			return
		}
		limit = n
	}
	backups, err := h.svc.ListBackups(r.Context(), host, tmpl, slug, limit)
	if err != nil {
		WriteError(w, err)
		return
	}
	WriteJSON(w, http.StatusOK, map[string]any{"backups": toBackupViews(backups)})
}

// BackupView is the JSON shape of one backup. Manifests are internal
// verification metadata and are not exposed; per-volume name+size are.
type BackupView struct {
	ID       string             `json:"id"`
	Host     string             `json:"host"`
	Template string             `json:"template"`
	Slug     string             `json:"slug"`
	State    string             `json:"state"`
	Image    string             `json:"image,omitempty"`
	Volumes  []BackupVolumeView `json:"volumes,omitempty"`
	Created  string             `json:"created"`
	Finished string             `json:"finished,omitempty"`
}

type BackupVolumeView struct {
	Name      string `json:"name"`
	SizeBytes int64  `json:"size_bytes"`
}

func toBackupViews(bs []store.Backup) []BackupView {
	out := make([]BackupView, 0, len(bs))
	for _, b := range bs {
		v := BackupView{
			ID: b.ID, Host: b.Host, Template: b.Template, Slug: b.Slug,
			State: string(b.State), Image: b.Image,
			Created: b.Created.UTC().Format(timeFormat),
		}
		if !b.Finished.IsZero() {
			v.Finished = b.Finished.UTC().Format(timeFormat)
		}
		for _, bv := range b.Volumes {
			v.Volumes = append(v.Volumes, BackupVolumeView{Name: bv.Name, SizeBytes: bv.SizeBytes})
		}
		out = append(out, v)
	}
	return out
}

// postRestore enqueues a restore job for a backup.
func (h *handlers) postRestore(w http.ResponseWriter, r *http.Request) {
	if h.jobs == nil {
		WriteError(w, errJobsDisabled)
		return
	}
	id := r.PathValue("id")
	if _, err := h.svc.CheckRestorable(r.Context(), id); err != nil {
		WriteError(w, err)
		return
	}
	args, err := json.Marshal(instance.RestoreRequest{BackupID: id})
	if err != nil {
		WriteJSON(w, http.StatusInternalServerError, ErrorBody{Code: "internal", Message: err.Error()})
		return
	}
	job, err := h.jobs.Enqueue(r.Context(), "restore", args, "")
	if err != nil {
		WriteError(w, err)
		return
	}
	WriteJSON(w, http.StatusAccepted, map[string]string{"job_id": job.ID})
}

// deleteBackup synchronously removes a backup's blobs and row; refused while
// a restore of it is in flight.
func (h *handlers) deleteBackup(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if h.jobs != nil {
		busy, err := instance.RestoreInFlight(r.Context(), h.jobs, id)
		if err != nil {
			WriteError(w, err)
			return
		}
		if busy {
			WriteError(w, instance.ErrBackupBusy)
			return
		}
	}
	if err := h.svc.DeleteBackup(r.Context(), id); err != nil {
		WriteError(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}
```

`timeFormat`: use whatever constant/format the package already uses for timestamps in JSON (check `jobs.go`'s view; if it inlines `time.RFC3339`, use `time.RFC3339` here too and drop the constant).

Routes in `router.go` (after the Migrate block):

```go
	// Backups: enqueue/list per instance; restore/delete per backup (#66).
	mux.Handle("POST /hosts/{host}/instances/{template}/{slug}/backup", guard("instances:write", http.HandlerFunc(h.postBackup)))
	mux.Handle("GET /hosts/{host}/instances/{template}/{slug}/backups", guard("instances:read", http.HandlerFunc(h.listBackups)))
	mux.Handle("POST /backups/{id}/restore", guard("instances:write", http.HandlerFunc(h.postRestore)))
	mux.Handle("DELETE /backups/{id}", guard("instances:write", http.HandlerFunc(h.deleteBackup)))
```

Error mapping in `errors.go` `classify` (before the `default`):

```go
	case errors.Is(err, instance.ErrBackupNotFound):
		return "backup_not_found", http.StatusNotFound, err.Error()
	case errors.Is(err, instance.ErrBackupNotRestorable):
		return "backup_not_restorable", http.StatusUnprocessableEntity, err.Error()
	case errors.Is(err, instance.ErrBackupBusy):
		return "backup_busy", http.StatusConflict, err.Error()
	case errors.Is(err, instance.ErrBackupsDisabled):
		return "not_implemented", http.StatusNotImplemented, err.Error()
```

- [ ] **Step 4: Update `api/openapi.yaml`** — add a `backups` tag and the four paths, documenting request/response bodies and error codes exactly as implemented (`202 {job_id, backup_id}`, `202 {job_id}`, `200 {backups: [...]}`, `204`; errors `backup_not_found` 404, `backup_not_restorable` 422, `backup_busy` 409, `host_draining` 423, `instance_not_found` 404). Follow the style of the existing `/migrate` entry. Add to the spot-check list in `openapi_test.go`:

```go
		"/hosts/{host}/instances/{template}/{slug}/backup",
		"/hosts/{host}/instances/{template}/{slug}/backups",
		"/backups/{id}/restore",
		"/backups/{id}",
```

- [ ] **Step 5: Run tests**

Run: `go test -tags "$TAGS" ./internal/api/ -v -run 'TestAPI|TestOpenAPI' && go test -tags "$TAGS" ./internal/api/`
Expected: PASS

- [ ] **Step 6: Commit**

```bash
git add internal/api/ api/openapi.yaml
git commit -m "feat(66): backup/restore/delete API routes + error mapping + OpenAPI"
```

---

### Task 8: Admin UI

**Files:**
- Create: `internal/ui/handlers_backups.go`
- Create: `internal/ui/handlers_backups_test.go`
- Modify: `internal/ui/handlers_instances.go` (`instanceView` gains backups)
- Modify: `internal/ui/templates/instance-detail.html`
- Modify: `internal/ui/ui.go` (routes)

- [ ] **Step 1: Write failing UI tests** in `handlers_backups_test.go`, following the existing `handlers_*_test.go` setup (authenticated session + CSRF helpers already exist in this package — reuse them). Cases:

```go
// TestUI_InstanceDetailShowsBackups: seed a complete backup row → GET
//   instance detail → body contains the backup id and a "Back up now" button.
// TestUI_BackupNow: POST /ui/hosts/h1/instances/pg/a/backup → 200, body
//   contains "Backup started"; a backup job exists in the job store.
// TestUI_RestoreEnqueues: seed complete row → POST /ui/backups/{id}/restore
//   → 200, restore job enqueued, body contains "Restore started".
// TestUI_DeleteBackup: seed row → POST /ui/backups/{id}/delete → 200, row gone.
// TestUI_DeleteBusyShowsError: enqueue restore job for the id first → POST
//   delete → body contains the busy error, row still present.
```

- [ ] **Step 2: Run to verify failure**

Run: `go test -tags "$TAGS" ./internal/ui/ -run TestUI_.*Backup -v`
Expected: FAIL

- [ ] **Step 3: Implement.**

`instanceView` in `handlers_instances.go` needs the backups list (best-effort, like `HasSecrets`):

```go
func (u *UI) instanceView(ctx context.Context, host string, obs instance.Observed) map[string]any {
	backups, _ := u.cfg.Svc.ListBackups(ctx, host, obs.Template, obs.Slug, 0) // best-effort; nil on error
	return map[string]any{
		"Host":       host,
		"Inst":       obs,
		"CanUpgrade": true,
		"HasSecrets": len(u.templatePerInstanceSecrets(ctx, obs.Template)) > 0,
		"Backups":    backups,
	}
}
```

`internal/ui/handlers_backups.go`:

```go
package ui

import (
	"encoding/json"
	"net/http"

	"github.com/iotready/podman-api/internal/instance"
	"github.com/iotready/podman-api/internal/store"
)

// renderInstanceNotice re-renders the instance detail with a notice banner.
func (u *UI) renderInstanceNotice(w http.ResponseWriter, r *http.Request, host, tmpl, slug, notice string) {
	obs, err := u.cfg.Svc.Get(r.Context(), host, tmpl, slug)
	if err != nil {
		u.renderError(w, r, err)
		return
	}
	data := u.instanceView(r.Context(), host, obs)
	data["Notice"] = notice
	u.render(w, r, http.StatusOK, "instance-detail", u.pageData(data))
}

// renderInstanceActionError re-renders the instance detail with an error
// banner (mirrors lifecycle's failure path).
func (u *UI) renderInstanceActionError(w http.ResponseWriter, r *http.Request, host, tmpl, slug string, actionErr error) {
	obs, gerr := u.cfg.Svc.Get(r.Context(), host, tmpl, slug)
	if gerr != nil {
		u.renderError(w, r, actionErr)
		return
	}
	data := u.instanceView(r.Context(), host, obs)
	data["ActionError"] = actionErr.Error()
	u.render(w, r, errorStatus(actionErr), "instance-detail", u.pageData(data))
}

// backupNow enqueues a backup job and re-renders the detail with a notice.
func (u *UI) backupNow(w http.ResponseWriter, r *http.Request) {
	host, tmpl, slug := r.PathValue("host"), r.PathValue("template"), r.PathValue("slug")
	ctx := r.Context()
	if err := u.cfg.Svc.CheckBackupable(ctx, host, tmpl, slug); err != nil {
		u.renderInstanceActionError(w, r, host, tmpl, slug, err)
		return
	}
	req := instance.BackupRequest{BackupID: store.NewBackupID(), Host: host, Template: tmpl, Slug: slug}
	args, err := json.Marshal(req)
	if err != nil {
		u.renderError(w, r, err)
		return
	}
	job, err := u.cfg.Jobs.Enqueue(ctx, "backup", args, "")
	if err != nil {
		u.renderInstanceActionError(w, r, host, tmpl, slug, err)
		return
	}
	u.renderInstanceNotice(w, r, host, tmpl, slug, "Backup started — job "+job.ID)
}

// restoreBackup enqueues a restore job for a backup id.
func (u *UI) restoreBackup(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	ctx := r.Context()
	b, err := u.cfg.Svc.CheckRestorable(ctx, id)
	if err != nil {
		u.renderError(w, r, err)
		return
	}
	args, merr := json.Marshal(instance.RestoreRequest{BackupID: id})
	if merr != nil {
		u.renderError(w, r, merr)
		return
	}
	job, err := u.cfg.Jobs.Enqueue(ctx, "restore", args, "")
	if err != nil {
		u.renderInstanceActionError(w, r, b.Host, b.Template, b.Slug, err)
		return
	}
	u.renderInstanceNotice(w, r, b.Host, b.Template, b.Slug, "Restore started — job "+job.ID)
}

// deleteBackup removes a backup (blobs + row); refused mid-restore.
func (u *UI) deleteBackup(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	ctx := r.Context()
	b, err := u.cfg.Svc.GetBackup(ctx, id)
	if err != nil {
		u.renderError(w, r, err)
		return
	}
	if u.cfg.Jobs != nil {
		busy, berr := instance.RestoreInFlight(ctx, u.cfg.Jobs, id)
		if berr != nil {
			u.renderError(w, r, berr)
			return
		}
		if busy {
			u.renderInstanceActionError(w, r, b.Host, b.Template, b.Slug, instance.ErrBackupBusy)
			return
		}
	}
	if err := u.cfg.Svc.DeleteBackup(ctx, id); err != nil {
		u.renderInstanceActionError(w, r, b.Host, b.Template, b.Slug, err)
		return
	}
	u.renderInstanceNotice(w, r, b.Host, b.Template, b.Slug, "Backup "+id+" deleted")
}
```

(`errorStatus` already exists in this package — used by `lifecycle`.)

Routes in `ui.go`'s `Handler()` (with the other guarded routes — note the literal `backup` route must be registered; Go 1.22 ServeMux prefers the more specific literal over the `{action}` wildcard, so `lifecycle` is unaffected):

```go
	mux.Handle("POST /ui/hosts/{host}/instances/{template}/{slug}/backup", guardW(u.backupNow))
	mux.Handle("POST /ui/backups/{id}/restore", guardW(u.restoreBackup))
	mux.Handle("POST /ui/backups/{id}/delete", guardW(u.deleteBackup))
```

Template — add to `instance-detail.html` after the VOLUMES block:

```html
<div class="label">BACKUPS</div>
<button hx-post="/ui/hosts/{{.Host}}/instances/{{.Inst.Template}}/{{.Inst.Slug}}/backup" hx-target="#main"
        hx-confirm="Back up {{.Inst.Template}}/{{.Inst.Slug}} now? The instance is stopped for the duration of the backup.">Back up now</button>
{{range .Backups}}
<div>
  {{.ID}} · {{.Created.Format "2006-01-02 15:04:05"}} · {{.State}}{{if .Image}} · {{.Image}}{{end}}
  {{if eq (printf "%s" .State) "complete"}}
  <button hx-post="/ui/backups/{{.ID}}/restore" hx-target="#main"
          hx-confirm="Restore {{.ID}}? This stops the instance and OVERWRITES its current data.">Restore</button>
  {{end}}
  <button hx-post="/ui/backups/{{.ID}}/delete" hx-target="#main"
          hx-confirm="Delete backup {{.ID}}?">Delete</button>
</div>
{{end}}
```

- [ ] **Step 4: Run tests**

Run: `go test -tags "$TAGS" ./internal/ui/ -v -run TestUI && go test -tags "$TAGS" ./internal/ui/`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add internal/ui/
git commit -m "feat(66): admin UI — back up now, backups list, restore/delete"
```

---

### Task 9: main.go wiring + full verification

**Files:**
- Modify: `cmd/podman-api/main.go`

- [ ] **Step 1: Wire the flag, blob store, and job kinds.**

Flag (with the other storage flags):

```go
		backupDir = flag.String("backup-dir", "", "directory for volume backup artifacts; empty derives <state-db dir>/backups")
```

After `svc.SetStore(db)`:

```go
	// Backup artifacts rest on local disk next to the state DB by default;
	// the BlobStore seam is where the commercial S3 backend slots in (#66).
	bdir := *backupDir
	if bdir == "" {
		bdir = filepath.Join(filepath.Dir(*stateDB), "backups")
	}
	blobs, err := backuppkg.NewLocalDir(bdir)
	if err != nil {
		log.Fatalf("backup dir: %v", err)
	}
	svc.SetBlobStore(blobs)
	log.Printf("backups enabled: %s", bdir)
```

Import as `backuppkg "github.com/iotready/podman-api/internal/backup"` (avoids shadowing). Registry + reconcilers:

```go
	registry := jobs.Registry{
		"migrate":  &migrate.Handler{Svc: svc},
		"evacuate": &evacuate.Handler{Svc: svc, Jobs: db, Concurrency: *evacConc},
		"prune":    &prune.Handler{Client: client, Jobs: db, Metrics: pruneMetrics},
		"backup":   &backuppkg.Handler{Svc: svc},
		"restore":  &backuppkg.RestoreHandler{Svc: svc},
	}
	...
	runner.SetReconcilers(jobs.Reconcilers{
		"migrate": &migrate.Reconciler{Svc: svc},
		"backup":  &backuppkg.Reconciler{Svc: svc},
	})
```

- [ ] **Step 2: Build + vet + gofmt + full unit suite**

Run: `make build && go vet -tags "$TAGS" ./... && test -z "$(gofmt -l .)" && make test`
Expected: all PASS, gofmt list empty

- [ ] **Step 3: Run main_test.go specifically** (flag/wiring tests live there)

Run: `go test -tags "$TAGS" ./cmd/podman-api/ -v`
Expected: PASS

- [ ] **Step 4: Commit**

```bash
git add cmd/podman-api/main.go
git commit -m "feat(66): wire -backup-dir, LocalDir blob store, backup/restore job kinds"
```

---

### Task 10: Integration test (real podman)

**Files:**
- Modify: `cmd/podman-api/e2e_integration_test.go` (or a new sibling file `backup_integration_test.go` with the same build tag — check the existing file's `//go:build integration` tag and helpers first and follow them)

- [ ] **Step 1: Write the round-trip test** following the existing e2e harness (real podman socket, deployed test instance):

Scenario:
1. Deploy a test instance with one volume (existing e2e helpers).
2. Write a marker file into the volume (`ContainerExec` with `sh -c 'echo v1 > /data/marker'` — match how existing integration tests exec).
3. `POST .../backup`; poll `GET /jobs/{id}` until succeeded.
4. Overwrite the marker (`echo v2 > /data/marker`).
5. `POST /backups/{id}/restore`; poll until succeeded.
6. Exec `cat /data/marker` → expect `v1`; assert instance Running.
7. `DELETE /backups/{id}` → 204.

- [ ] **Step 2: Run it** (requires a podman host — skip gracefully like the existing integration tests if unavailable):

Run: `make test-integration`
Expected: PASS (or skip where the environment lacks podman)

- [ ] **Step 3: Commit**

```bash
git add cmd/podman-api/
git commit -m "test(66): e2e backup/restore round-trip integration test"
```

---

### Task 11: Docs — spec amendments, README, wiki

**Files:**
- Modify: `docs/superpowers/specs/2026-06-06-volume-backup-restore-design.md` (record the 5 deviations from the plan header as decided amendments — edit the BlobStore interface snippet, restart-only-if-running, upfront drain check, limit-only pagination)
- Modify: `README.md` (one bullet in the feature list + flag in the flags table, if those sections exist — match existing style)
- Wiki: new page `Backing-up-and-Restoring` (Forgejo wiki repo) covering: what a backup is (per-instance, stop-export-restart, downtime note), `-backup-dir`, API examples (curl for backup/list/restore/delete), job polling, the restore-overwrites warning, failed-backup semantics, and that scheduling/retention/offsite are out of scope (commercial #107)

- [ ] **Step 1: Amend the spec + README; commit**

```bash
git add docs/ README.md
git commit -m "docs(66): spec amendments + README pointer for volume backup/restore"
```

- [ ] **Step 2: Draft the wiki page and publish** (wiki has no PR flow — `forgejo api POST /repos/tej/podman-api/wiki/new` or push to `podman-api.wiki.git`). Confirm with the user before publishing.

---

## Final verification (after all tasks)

- [ ] `make build && make test` — green
- [ ] `gofmt -l .` — empty; `go vet -tags "$TAGS" ./...` — clean
- [ ] Walk the spec's Decisions table and confirm each is implemented (per-instance granularity, stop-only consistency, in-place restore, manual lifecycle, volumes-only, blob seam)
- [ ] Open PR: `forgejo pr create tej/podman-api --title="feat: on-demand volume backup + one-click restore (#66)" --head=<branch> --base=main --body="..."`

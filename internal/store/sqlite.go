package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand"
	"net/url"
	"strings"
	"sync"
	"time"

	sqlite "modernc.org/sqlite"

	"github.com/iotready/podman-api/internal/render"
)

const schemaSQL = `
CREATE TABLE IF NOT EXISTS specs (
  host       TEXT NOT NULL,
  template   TEXT NOT NULL,
  slug       TEXT NOT NULL,
  parameters TEXT NOT NULL,
  secrets    BLOB,
  domains    TEXT NOT NULL DEFAULT '[]',
  created    INTEGER NOT NULL,
  updated    INTEGER NOT NULL,
  PRIMARY KEY (host, template, slug)
);
CREATE TABLE IF NOT EXISTS jobs (
  id        TEXT PRIMARY KEY,
  kind      TEXT NOT NULL,
  args      TEXT NOT NULL,
  state     TEXT NOT NULL,
  steps     TEXT NOT NULL DEFAULT '[]',
  parent_id TEXT,
  error     TEXT,
  created   INTEGER NOT NULL,
  started   INTEGER,
  finished  INTEGER
);
CREATE INDEX IF NOT EXISTS jobs_state ON jobs(state);
CREATE TABLE IF NOT EXISTS host_secrets (
  host    TEXT NOT NULL,
  name    TEXT NOT NULL,
  value   BLOB NOT NULL,
  created INTEGER NOT NULL,
  updated INTEGER NOT NULL,
  PRIMARY KEY (host, name)
);
CREATE TABLE IF NOT EXISTS templates (
  id      TEXT PRIMARY KEY,
  body    TEXT NOT NULL,
  meta    TEXT NOT NULL,
  origin  TEXT NOT NULL,
  created INTEGER NOT NULL,
  updated INTEGER NOT NULL
);`

// maxOpenConns bounds the SQLite connection pool. WAL allows many concurrent
// readers + one writer; setting the pool above the job worker count
// (jobs.DefaultWorkers, 8) leaves read headroom so GET /jobs is not starved when
// a worker holds the write connection. A competing writer waits up to
// busy_timeout rather than failing with "database is locked". Operators raising
// -job-workers well above the default may want a correspondingly larger pool.
const maxOpenConns = 12

// retryBusyTimeout bounds how long a write retries past a transient
// SQLITE_BUSY/LOCKED before giving up. busy_timeout(5000) handles most
// contention, but modernc can still return BUSY without invoking the busy
// handler under write-write races; this is a thin application-level backstop.
// Shutdown paths pass a cancellable ctx (e.g. finishTimeout), so ctx
// cancellation — not this ceiling — bounds them.
const retryBusyTimeout = 5 * time.Second

// retryBackoffCap bounds the per-attempt backoff before jitter.
const retryBackoffCap = 50 * time.Millisecond

// isBusy reports whether err is a transient SQLite BUSY/LOCKED worth retrying.
func isBusy(err error) bool {
	var se *sqlite.Error
	if !errors.As(err, &se) {
		return false
	}
	switch se.Code() & 0xff { // strip extended result-code bits
	case 5, 6: // SQLITE_BUSY, SQLITE_LOCKED
		return true
	default:
		return false
	}
}

// retry runs fn, retrying while retryable(err) is true until retryBusyTimeout
// elapses or ctx is done. Success and non-retryable errors return immediately.
//
// The sleep is randomized in [backoff/2, backoff] (capped-exponential with
// jitter). Jitter is essential, not cosmetic: the runner's N workers hit BUSY
// together, and a fixed schedule would have them all wake and re-collide in
// lockstep (a thundering herd that never converges under load). Decorrelating
// the retriers lets one win each round.
func retry(ctx context.Context, retryable func(error) bool, fn func() error) error {
	deadline := time.Now().Add(retryBusyTimeout)
	backoff := time.Millisecond
	for {
		err := fn()
		if err == nil || !retryable(err) {
			return err
		}
		if ctx.Err() != nil || time.Now().After(deadline) {
			return err
		}
		half := backoff / 2
		sleep := half + time.Duration(rand.Int63n(int64(backoff-half)+1))
		select {
		case <-ctx.Done():
			return err
		case <-time.After(sleep):
		}
		if backoff < retryBackoffCap {
			backoff *= 2
		}
	}
}

// retryBusy retries fn on a transient SQLite BUSY/LOCKED. See retry / isBusy.
func retryBusy(ctx context.Context, fn func() error) error {
	return retry(ctx, isBusy, fn)
}

// SQLite is the durable Store backed by a single SQLite file. Secrets are
// sealed with the key held in keys, read fresh on every Put/Get so a SIGHUP
// key swap takes effect immediately.
type SQLite struct {
	db   *sql.DB
	keys *KeyStore
	// wmu serializes writes. This daemon is the sole writer to the file and
	// SQLite permits only one writer at a time, so serializing in-process moves
	// the wait from the DB file-lock (which returns SQLITE_BUSY immediately under
	// the modernc driver) to a clean Go lock — eliminating the write-write
	// thundering herd among the runner's workers. Reads do NOT take wmu: WAL keeps
	// them concurrent with the in-flight write. retryBusy remains a backstop for
	// the rare non-write-write BUSY (e.g. WAL checkpoint contention).
	wmu sync.Mutex
}

// write serializes fn behind wmu and retries it past a transient BUSY/LOCKED.
// Every mutating store method funnels through here.
func (s *SQLite) write(ctx context.Context, fn func() error) error {
	s.wmu.Lock()
	defer s.wmu.Unlock()
	return retryBusy(ctx, fn)
}

// OpenSQLite opens (creating if needed) the SQLite file at path and ensures the
// schema exists. keys supplies the AES-256-GCM secret key.
func OpenSQLite(path string, keys *KeyStore) (*SQLite, error) {
	// file: URI so modernc applies _pragma to every pooled connection. Escape
	// the path: modernc splits the DSN on the first '?', so a path containing
	// '?' or '#' would otherwise corrupt the path/query split.
	dsn := "file:" + url.PathEscape(path) + "?_pragma=busy_timeout(5000)"
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(maxOpenConns)
	db.SetMaxIdleConns(maxOpenConns)
	// WAL + busy_timeout: WAL gives many concurrent readers + one writer (the
	// API reads /jobs while the background runner writes); busy_timeout makes a
	// competing writer wait rather than failing with "database is locked".
	if _, err := db.Exec(`PRAGMA journal_mode = WAL`); err != nil {
		_ = db.Close()
		return nil, err
	}
	// belt-and-suspenders: the DSN _pragma above covers every pooled connection;
	// this explicit Exec surfaces a driver/pragma failure as an OpenSQLite error
	// (the DSN path's per-connection errors are retried internally, not returned).
	if _, err := db.Exec(`PRAGMA busy_timeout = 5000`); err != nil {
		_ = db.Close()
		return nil, err
	}
	if _, err := db.Exec(schemaSQL); err != nil {
		_ = db.Close()
		return nil, err
	}
	if err := migrateSchema(db); err != nil {
		_ = db.Close()
		return nil, err
	}
	return &SQLite{db: db, keys: keys}, nil
}

// migrateSchema brings an existing DB up to the current schema version.
// v4 added specs.domains. v5 added the templates table.
// v6 made specs.secrets nullable (NULL = no secrets; key-less open allowed).
// Each step is guarded by user_version so OpenSQLite is idempotent.
func migrateSchema(db *sql.DB) error {
	var v int
	if err := db.QueryRow(`PRAGMA user_version`).Scan(&v); err != nil {
		return err
	}
	if v < 4 {
		// specs.domains is created by schemaSQL on a fresh DB but absent on a
		// pre-v4 DB. Add it only when missing — checking the catalog avoids
		// depending on the driver's duplicate-column error text.
		has, err := columnExists(db, "specs", "domains")
		if err != nil {
			return fmt.Errorf("migrateSchema: check domains column: %w", err)
		}
		if !has {
			if _, err := db.Exec(`ALTER TABLE specs ADD COLUMN domains TEXT NOT NULL DEFAULT '[]'`); err != nil {
				return fmt.Errorf("migrateSchema: add domains column: %w", err)
			}
		}
		if _, err := db.Exec(`PRAGMA user_version = 4`); err != nil {
			return fmt.Errorf("migrateSchema: set user_version: %w", err)
		}
		v = 4
	}
	if v < 5 {
		// templates table is created by schemaSQL on a fresh DB but absent on a
		// pre-v5 DB. CREATE TABLE IF NOT EXISTS is safe for both cases.
		if _, err := db.Exec(`CREATE TABLE IF NOT EXISTS templates (
  id      TEXT PRIMARY KEY,
  body    TEXT NOT NULL,
  meta    TEXT NOT NULL,
  origin  TEXT NOT NULL,
  created INTEGER NOT NULL,
  updated INTEGER NOT NULL
)`); err != nil {
			return fmt.Errorf("migrateSchema: create templates table: %w", err)
		}
		if _, err := db.Exec(`PRAGMA user_version = 5`); err != nil {
			return fmt.Errorf("migrateSchema: set user_version: %w", err)
		}
		v = 5
	}
	if v < 6 {
		// specs.secrets changes from BLOB NOT NULL to BLOB (nullable).
		// SQLite does not support DROP NOT NULL via ALTER COLUMN; recreate the
		// table using the standard rename-copy-drop sequence.
		// All DDL steps run inside a single transaction so a crash mid-migration
		// cannot leave the DB without a specs table.
		tx, err := db.Begin()
		if err != nil {
			return fmt.Errorf("migrateSchema v6: begin: %w", err)
		}
		steps := []string{
			`ALTER TABLE specs RENAME TO specs_v5`,
			`CREATE TABLE specs (
  host       TEXT NOT NULL,
  template   TEXT NOT NULL,
  slug       TEXT NOT NULL,
  parameters TEXT NOT NULL,
  secrets    BLOB,
  domains    TEXT NOT NULL DEFAULT '[]',
  created    INTEGER NOT NULL,
  updated    INTEGER NOT NULL,
  PRIMARY KEY (host, template, slug)
)`,
			`INSERT INTO specs SELECT host, template, slug, parameters, secrets, domains, created, updated FROM specs_v5`,
			`DROP TABLE specs_v5`,
		}
		for _, stmt := range steps {
			if _, err := tx.Exec(stmt); err != nil {
				_ = tx.Rollback()
				return fmt.Errorf("migrateSchema v6: %w", err)
			}
		}
		if _, err := tx.Exec(`PRAGMA user_version = 6`); err != nil {
			_ = tx.Rollback()
			return fmt.Errorf("migrateSchema v6: set user_version: %w", err)
		}
		if err := tx.Commit(); err != nil {
			return fmt.Errorf("migrateSchema v6: commit: %w", err)
		}
	}
	return nil
}

// columnExists reports whether table has a column named col, read from the
// SQLite schema catalog (pragma_table_info) rather than inferred from an error.
func columnExists(db *sql.DB, table, col string) (bool, error) {
	var n int
	if err := db.QueryRow(`SELECT count(*) FROM pragma_table_info(?) WHERE name = ?`, table, col).Scan(&n); err != nil {
		return false, err
	}
	return n > 0, nil
}

// Close releases the underlying database handle.
func (s *SQLite) Close() error { return s.db.Close() }

func (s *SQLite) PutSpec(ctx context.Context, sp Spec) error {
	params, err := json.Marshal(sp.Parameters)
	if err != nil {
		return err
	}
	var blob []byte
	if len(sp.Secrets) > 0 {
		if s.keys == nil {
			return ErrSecretsNeedKey
		}
		secJSON, err := json.Marshal(sp.Secrets)
		if err != nil {
			return err
		}
		blob, err = seal(s.keys.Load(), secJSON)
		if err != nil {
			return err
		}
	}
	if sp.Domains == nil {
		sp.Domains = []string{}
	}
	domJSON, err := json.Marshal(sp.Domains)
	if err != nil {
		return err
	}
	now := time.Now().Unix()
	return s.write(ctx, func() error {
		_, err := s.db.ExecContext(ctx, `
INSERT INTO specs (host, template, slug, parameters, secrets, domains, created, updated)
VALUES (?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT(host, template, slug) DO UPDATE SET
  parameters = excluded.parameters,
  secrets    = excluded.secrets,
  domains    = excluded.domains,
  updated    = excluded.updated`,
			sp.Host, sp.Template, sp.Slug, string(params), blob, string(domJSON), now, now)
		return err
	})
}

func (s *SQLite) GetSpec(ctx context.Context, host, template, slug string) (Spec, error) {
	row := s.db.QueryRowContext(ctx,
		`SELECT parameters, secrets, domains, created, updated FROM specs WHERE host=? AND template=? AND slug=?`,
		host, template, slug)
	var (
		paramsJSON       string
		blob             []byte
		domainsJSON      string
		created, updated int64
	)
	if err := row.Scan(&paramsJSON, &blob, &domainsJSON, &created, &updated); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return Spec{}, ErrNotFound
		}
		return Spec{}, err
	}
	var params map[string]any
	if err := json.Unmarshal([]byte(paramsJSON), &params); err != nil {
		return Spec{}, fmt.Errorf("%w: parameters: %v", ErrSpecCorrupt, err)
	}
	var secrets map[string]string
	if len(blob) > 0 {
		if s.keys == nil {
			return Spec{}, ErrSecretsNeedKey
		}
		secJSON, err := open(s.keys.Load(), blob)
		if err != nil {
			return Spec{}, fmt.Errorf("%w: decrypt secrets: %v", ErrSecretsUndecryptable, err)
		}
		if err := json.Unmarshal(secJSON, &secrets); err != nil {
			return Spec{}, fmt.Errorf("%w: secrets: %v", ErrSpecCorrupt, err)
		}
	}
	var domains []string
	if err := json.Unmarshal([]byte(domainsJSON), &domains); err != nil {
		return Spec{}, fmt.Errorf("%w: domains: %v", ErrSpecCorrupt, err)
	}
	return Spec{
		Host: host, Template: template, Slug: slug,
		Parameters: params, Secrets: secrets, Domains: domains,
		Created: time.Unix(created, 0), Updated: time.Unix(updated, 0),
	}, nil
}

func (s *SQLite) DeleteSpec(ctx context.Context, host, template, slug string) error {
	return s.write(ctx, func() error {
		res, err := s.db.ExecContext(ctx,
			`DELETE FROM specs WHERE host=? AND template=? AND slug=?`, host, template, slug)
		if err != nil {
			return err
		}
		n, err := res.RowsAffected()
		if err != nil {
			return err
		}
		if n == 0 {
			return ErrNotFound
		}
		return nil
	})
}

func (s *SQLite) ListSpecKeys(ctx context.Context, host string) ([]SpecKey, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT template, slug FROM specs WHERE host=? ORDER BY template, slug`, host)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []SpecKey{}
	for rows.Next() {
		var k SpecKey
		if err := rows.Scan(&k.Template, &k.Slug); err != nil {
			return nil, err
		}
		out = append(out, k)
	}
	return out, rows.Err()
}

func (s *SQLite) PutHostSecret(ctx context.Context, host, name string, value []byte) error {
	if s.keys == nil {
		return ErrSecretsNeedKey
	}
	blob, err := seal(s.keys.Load(), value)
	if err != nil {
		return err
	}
	now := time.Now().Unix()
	return s.write(ctx, func() error {
		_, err := s.db.ExecContext(ctx, `
INSERT INTO host_secrets (host, name, value, created, updated)
VALUES (?, ?, ?, ?, ?)
ON CONFLICT(host, name) DO UPDATE SET
  value   = excluded.value,
  updated = excluded.updated`,
			host, name, blob, now, now)
		return err
	})
}

func (s *SQLite) GetHostSecret(ctx context.Context, host, name string) ([]byte, error) {
	if s.keys == nil {
		return nil, ErrSecretsNeedKey
	}
	var blob []byte
	err := s.db.QueryRowContext(ctx,
		`SELECT value FROM host_secrets WHERE host=? AND name=?`, host, name).Scan(&blob)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, ErrNotFound
		}
		return nil, err
	}
	val, err := open(s.keys.Load(), blob)
	if err != nil {
		// A wrong/missing key is recoverable by a restart with the correct
		// -spec-key-file; surface the typed sentinel so the API/UI classify it
		// 422 (coherent with GetSpec), not a raw 500. (#117)
		return nil, fmt.Errorf("%w: decrypt host secret: %v", ErrSecretsUndecryptable, err)
	}
	return val, nil
}

func (s *SQLite) DeleteHostSecret(ctx context.Context, host, name string) error {
	return s.write(ctx, func() error {
		_, err := s.db.ExecContext(ctx,
			`DELETE FROM host_secrets WHERE host=? AND name=?`, host, name)
		return err
	})
}

// SecretsEnabled reports whether this store can persist secrets — true only when
// it was opened with an encryption key (-spec-key-file).
func (s *SQLite) SecretsEnabled() bool { return s.keys != nil }

// ---------------------------------------------------------------------------
// JobStore implementation
// ---------------------------------------------------------------------------

const jobColumns = `id, kind, args, state, steps, parent_id, error, created, started, finished`

type rowScanner interface {
	Scan(dest ...any) error
}

func scanJob(sc rowScanner) (Job, error) {
	var (
		j                 Job
		args, steps       string
		parent, errMsg    sql.NullString
		created           int64
		started, finished sql.NullInt64
	)
	if err := sc.Scan(&j.ID, &j.Kind, &args, &j.State, &steps, &parent, &errMsg, &created, &started, &finished); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return Job{}, ErrNotFound
		}
		return Job{}, err
	}
	j.Args = json.RawMessage(args)
	if err := json.Unmarshal([]byte(steps), &j.Steps); err != nil {
		return Job{}, err
	}
	j.ParentID = parent.String
	j.Error = errMsg.String
	// Job timestamps are stored as Unix nanoseconds (see Enqueue/Finish) for
	// sub-second durations and FIFO tiebreaks.
	j.Created = time.Unix(0, created)
	if started.Valid {
		j.Started = time.Unix(0, started.Int64)
	}
	if finished.Valid {
		j.Finished = time.Unix(0, finished.Int64)
	}
	return j, nil
}

func (s *SQLite) Enqueue(ctx context.Context, kind string, args json.RawMessage, parentID string) (Job, error) {
	id := newJobID()
	now := time.Now().UnixNano()
	if len(args) == 0 {
		args = json.RawMessage("null")
	}
	var parent any
	if parentID != "" {
		parent = parentID
	}
	err := s.write(ctx, func() error {
		_, e := s.db.ExecContext(ctx, `
INSERT INTO jobs (id, kind, args, state, steps, parent_id, error, created, started, finished)
VALUES (?, ?, ?, 'queued', '[]', ?, NULL, ?, NULL, NULL)`,
			id, kind, string(args), parent, now)
		return e
	})
	if err != nil {
		return Job{}, err
	}
	return s.GetJob(ctx, id)
}

func (s *SQLite) StartChild(ctx context.Context, kind string, args json.RawMessage, parentID string) (Job, error) {
	id := newJobID()
	now := time.Now().UnixNano()
	if len(args) == 0 {
		args = json.RawMessage("null")
	}
	var parent any
	if parentID != "" {
		parent = parentID
	}
	err := s.write(ctx, func() error {
		_, e := s.db.ExecContext(ctx, `
INSERT INTO jobs (id, kind, args, state, steps, parent_id, error, created, started, finished)
VALUES (?, ?, ?, 'running', '[]', ?, NULL, ?, ?, NULL)`,
			id, kind, string(args), parent, now, now)
		return e
	})
	if err != nil {
		return Job{}, err
	}
	return s.GetJob(ctx, id)
}

func (s *SQLite) GetJob(ctx context.Context, id string) (Job, error) {
	row := s.db.QueryRowContext(ctx, `SELECT `+jobColumns+` FROM jobs WHERE id=?`, id)
	return scanJob(row)
}

func (s *SQLite) ListJobs(ctx context.Context, f JobFilter) ([]Job, error) {
	q := `SELECT ` + jobColumns + ` FROM jobs`
	var where []string
	var args []any
	if f.State != "" {
		where = append(where, "state=?")
		args = append(args, string(f.State))
	}
	if f.Kind != "" {
		where = append(where, "kind=?")
		args = append(args, f.Kind)
	}
	if f.ParentID != "" {
		where = append(where, "parent_id=?")
		args = append(args, f.ParentID)
	}
	if f.Before != "" {
		where = append(where, "id < ?")
		args = append(args, f.Before)
	}
	if len(where) > 0 {
		q += " WHERE " + strings.Join(where, " AND ")
	}
	// Order by id alone: the id is fixed-width "<unixnano>-<rand>", so its
	// lexicographic order is a total order on creation time. The Before cursor
	// (id < ?) must use the SAME key as ORDER BY — created and the id time-prefix
	// come from two separate clock reads and can disagree under concurrent
	// inserts, so ordering by created here would let the id cursor skip a row.
	q += " ORDER BY id DESC LIMIT ?"
	args = append(args, clampJobLimit(f.Limit))
	rows, err := s.db.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []Job{}
	for rows.Next() {
		j, err := scanJob(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, j)
	}
	return out, rows.Err()
}

func (s *SQLite) ClaimNext(ctx context.Context) (Job, bool, error) {
	var (
		j  Job
		ok bool
	)
	// Stamp once outside the retry closure (consistent with the other writes), so
	// a write that retries past a transient BUSY records the pre-retry claim time.
	now := time.Now().UnixNano()
	err := s.write(ctx, func() error {
		row := s.db.QueryRowContext(ctx, `
UPDATE jobs SET state='running', started=?
WHERE id = (SELECT id FROM jobs WHERE state='queued' ORDER BY created, id LIMIT 1)
  AND state='queued'
RETURNING `+jobColumns, now)
		jj, e := scanJob(row)
		if errors.Is(e, ErrNotFound) {
			j, ok = Job{}, false
			return nil
		}
		if e != nil {
			return e
		}
		j, ok = jj, true
		return nil
	})
	if err != nil {
		return Job{}, false, err
	}
	return j, ok, nil
}

func (s *SQLite) AppendStep(ctx context.Context, id string, step JobStep) error {
	// Only the worker running this job appends steps, so there is no concurrent
	// AppendStep for the same id — a read-modify-write is safe.
	var steps string
	if err := s.db.QueryRowContext(ctx, `SELECT steps FROM jobs WHERE id=?`, id).Scan(&steps); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return ErrNotFound
		}
		return err
	}
	var arr []JobStep
	if err := json.Unmarshal([]byte(steps), &arr); err != nil {
		return err
	}
	if n := len(arr); n > 0 && arr[n-1].Step == step.Step && arr[n-1].Detail == step.Detail {
		// Collapse a consecutive identical step (e.g. a reconcile loop stuck on
		// the same condition): bump the occurrence count and refresh the
		// timestamp to the latest attempt rather than growing the array. (#117)
		if arr[n-1].Count == 0 {
			arr[n-1].Count = 1
		}
		arr[n-1].Count++
		arr[n-1].TS = step.TS
	} else {
		arr = append(arr, step)
	}
	b, err := json.Marshal(arr)
	if err != nil {
		return err
	}
	return s.write(ctx, func() error {
		_, err := s.db.ExecContext(ctx, `UPDATE jobs SET steps=? WHERE id=?`, string(b), id)
		return err
	})
}

func (s *SQLite) Finish(ctx context.Context, id string, state JobState, errMsg string) error {
	if state != JobSucceeded && state != JobFailed && state != JobCanceled {
		return fmt.Errorf("store.Finish: invalid terminal state %q", state)
	}
	now := time.Now().UnixNano()
	var e any
	if errMsg != "" {
		e = errMsg
	}
	return s.write(ctx, func() error {
		res, err := s.db.ExecContext(ctx, `UPDATE jobs SET state=?, error=?, finished=? WHERE id=?`,
			string(state), e, now, id)
		if err != nil {
			return err
		}
		n, err := res.RowsAffected()
		if err != nil {
			return err
		}
		if n == 0 {
			return ErrNotFound
		}
		return nil
	})
}

func (s *SQLite) FailRunning(ctx context.Context, reason string) (int, error) {
	now := time.Now().UnixNano()
	var n int64
	err := s.write(ctx, func() error {
		res, e := s.db.ExecContext(ctx, `UPDATE jobs SET state='failed', error=?, finished=? WHERE state='running'`,
			reason, now)
		if e != nil {
			return e
		}
		n, e = res.RowsAffected()
		return e
	})
	if err != nil {
		return 0, err
	}
	return int(n), nil
}

func (s *SQLite) MarkReconciling(ctx context.Context, kinds []string) (int, error) {
	if len(kinds) == 0 {
		return 0, nil
	}
	placeholders := make([]string, len(kinds))
	args := make([]any, len(kinds))
	for i, k := range kinds {
		placeholders[i] = "?"
		args[i] = k
	}
	query := `UPDATE jobs SET state='reconciling' WHERE state='running' AND kind IN (` +
		strings.Join(placeholders, ",") + `)`
	var n int64
	err := s.write(ctx, func() error {
		res, e := s.db.ExecContext(ctx, query, args...)
		if e != nil {
			return e
		}
		n, e = res.RowsAffected()
		return e
	})
	if err != nil {
		return 0, err
	}
	return int(n), nil
}

func (s *SQLite) ResolveReconciling(ctx context.Context, id string, state JobState, errMsg string) (bool, error) {
	if state != JobSucceeded && state != JobFailed {
		return false, fmt.Errorf("store.ResolveReconciling: invalid terminal state %q", state)
	}
	now := time.Now().UnixNano()
	var e any
	if errMsg != "" {
		e = errMsg
	}
	var n int64
	err := s.write(ctx, func() error {
		res, err := s.db.ExecContext(ctx,
			`UPDATE jobs SET state=?, error=?, finished=? WHERE id=? AND state='reconciling'`,
			string(state), e, now, id)
		if err != nil {
			return err
		}
		n, err = res.RowsAffected()
		return err
	})
	if err != nil {
		return false, err
	}
	return n > 0, nil
}

func (s *SQLite) CancelReconciling(ctx context.Context, id string) (bool, error) {
	now := time.Now().UnixNano()
	var n int64
	err := s.write(ctx, func() error {
		res, err := s.db.ExecContext(ctx,
			`UPDATE jobs SET state='canceled', finished=? WHERE id=? AND state='reconciling'`, now, id)
		if err != nil {
			return err
		}
		n, err = res.RowsAffected()
		return err
	})
	if err != nil {
		return false, err
	}
	return n > 0, nil
}

func (s *SQLite) CancelQueued(ctx context.Context, id string) (bool, error) {
	now := time.Now().UnixNano()
	var n int64
	err := s.write(ctx, func() error {
		res, e := s.db.ExecContext(ctx,
			`UPDATE jobs SET state='canceled', finished=? WHERE id=? AND state='queued'`, now, id)
		if e != nil {
			return e
		}
		n, e = res.RowsAffected()
		return e
	})
	if err != nil {
		return false, err
	}
	return n > 0, nil
}

func (s *SQLite) PruneJobs(ctx context.Context, olderThan time.Time) (int, error) {
	cutoff := olderThan.UnixNano()
	var total int64
	err := s.write(ctx, func() error {
		tx, err := s.db.BeginTx(ctx, nil)
		if err != nil {
			return err
		}
		defer tx.Rollback() //nolint:errcheck // no-op after a successful Commit

		// 1) Old terminal children first, so their parents can then be considered.
		rc, err := tx.ExecContext(ctx, `
DELETE FROM jobs
WHERE parent_id IS NOT NULL AND state IN ('succeeded','failed','canceled')
  AND finished IS NOT NULL AND finished < ?`, cutoff)
		if err != nil {
			return err
		}
		nChild, err := rc.RowsAffected()
		if err != nil {
			return err
		}

		// 2) Old terminal jobs not referenced as a parent by any surviving row.
		rp, err := tx.ExecContext(ctx, `
DELETE FROM jobs
WHERE state IN ('succeeded','failed','canceled') AND finished IS NOT NULL AND finished < ?
  AND id NOT IN (SELECT parent_id FROM jobs WHERE parent_id IS NOT NULL)`, cutoff)
		if err != nil {
			return err
		}
		nParent, err := rp.RowsAffected()
		if err != nil {
			return err
		}
		if err := tx.Commit(); err != nil {
			return err
		}
		total = nChild + nParent
		return nil
	})
	if err != nil {
		return 0, err
	}
	return int(total), nil
}

// ---------------------------------------------------------------------------
// TemplateStore implementation
// ---------------------------------------------------------------------------

func (s *SQLite) ListTemplates(ctx context.Context) ([]Template, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT id, meta, body, origin, created, updated FROM templates ORDER BY id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []Template{}
	for rows.Next() {
		t, err := scanTemplate(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, t)
	}
	return out, rows.Err()
}

func (s *SQLite) GetTemplate(ctx context.Context, id string) (Template, error) {
	row := s.db.QueryRowContext(ctx,
		`SELECT id, meta, body, origin, created, updated FROM templates WHERE id=?`, id)
	t, err := scanTemplate(row)
	if errors.Is(err, ErrNotFound) {
		return Template{}, ErrNotFound
	}
	return t, err
}

func (s *SQLite) PutTemplate(ctx context.Context, t Template) error {
	metaJSON, err := json.Marshal(t.Meta)
	if err != nil {
		return err
	}
	now := time.Now().Unix()
	return s.write(ctx, func() error {
		_, err := s.db.ExecContext(ctx, `
INSERT INTO templates (id, body, meta, origin, created, updated)
VALUES (?, ?, ?, ?, ?, ?)
ON CONFLICT(id) DO UPDATE SET
  body    = excluded.body,
  meta    = excluded.meta,
  origin  = excluded.origin,
  updated = excluded.updated`,
			t.Meta.ID, t.Body, string(metaJSON), t.Origin, now, now)
		return err
	})
}

func (s *SQLite) DeleteTemplate(ctx context.Context, id string) error {
	return s.write(ctx, func() error {
		_, err := s.db.ExecContext(ctx, `DELETE FROM templates WHERE id=?`, id)
		return err
	})
}

func (s *SQLite) CountTemplates(ctx context.Context) (int, error) {
	var n int
	err := s.db.QueryRowContext(ctx, `SELECT count(*) FROM templates`).Scan(&n)
	return n, err
}

func scanTemplate(sc rowScanner) (Template, error) {
	var (
		id, metaJSON, body, origin string
		created, updated           int64
	)
	if err := sc.Scan(&id, &metaJSON, &body, &origin, &created, &updated); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return Template{}, ErrNotFound
		}
		return Template{}, err
	}
	var meta render.Meta
	if err := json.Unmarshal([]byte(metaJSON), &meta); err != nil {
		return Template{}, err
	}
	return Template{
		Meta:    meta,
		Body:    body,
		Origin:  origin,
		Created: time.Unix(created, 0),
		Updated: time.Unix(updated, 0),
	}, nil
}

var _ DB = (*SQLite)(nil)

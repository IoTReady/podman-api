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
)

const schemaSQL = `
CREATE TABLE IF NOT EXISTS specs (
  host       TEXT NOT NULL,
  template   TEXT NOT NULL,
  slug       TEXT NOT NULL,
  parameters TEXT NOT NULL,
  secrets    BLOB NOT NULL,
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
CREATE INDEX IF NOT EXISTS jobs_state ON jobs(state);`

// maxOpenConns bounds the SQLite connection pool. WAL allows many concurrent
// readers + one writer; setting the pool above jobs.DefaultWorkers (4) leaves
// read headroom so GET /jobs is not starved when every worker is writing. A
// competing writer waits up to busy_timeout rather than failing with
// "database is locked".
const maxOpenConns = 8

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
	// Reserved schema-version stamp for future migrations. At v1 this is set
	// unconditionally; a real version gate is added if/when the schema changes.
	if _, err := db.Exec(`PRAGMA user_version = 3`); err != nil {
		_ = db.Close()
		return nil, err
	}
	return &SQLite{db: db, keys: keys}, nil
}

// Close releases the underlying database handle.
func (s *SQLite) Close() error { return s.db.Close() }

func (s *SQLite) PutSpec(ctx context.Context, sp Spec) error {
	params, err := json.Marshal(sp.Parameters)
	if err != nil {
		return err
	}
	secJSON, err := json.Marshal(sp.Secrets)
	if err != nil {
		return err
	}
	key := s.keys.Load()
	blob, err := seal(key, secJSON)
	if err != nil {
		return err
	}
	now := time.Now().Unix()
	return s.write(ctx, func() error {
		_, err := s.db.ExecContext(ctx, `
INSERT INTO specs (host, template, slug, parameters, secrets, created, updated)
VALUES (?, ?, ?, ?, ?, ?, ?)
ON CONFLICT(host, template, slug) DO UPDATE SET
  parameters = excluded.parameters,
  secrets    = excluded.secrets,
  updated    = excluded.updated`,
			sp.Host, sp.Template, sp.Slug, string(params), blob, now, now)
		return err
	})
}

func (s *SQLite) GetSpec(ctx context.Context, host, template, slug string) (Spec, error) {
	row := s.db.QueryRowContext(ctx,
		`SELECT parameters, secrets, created, updated FROM specs WHERE host=? AND template=? AND slug=?`,
		host, template, slug)
	var (
		paramsJSON       string
		blob             []byte
		created, updated int64
	)
	if err := row.Scan(&paramsJSON, &blob, &created, &updated); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return Spec{}, ErrNotFound
		}
		return Spec{}, err
	}
	var params map[string]any
	if err := json.Unmarshal([]byte(paramsJSON), &params); err != nil {
		return Spec{}, err
	}
	secJSON, err := open(s.keys.Load(), blob)
	if err != nil {
		return Spec{}, err
	}
	var secrets map[string]string
	if err := json.Unmarshal(secJSON, &secrets); err != nil {
		return Spec{}, err
	}
	return Spec{
		Host: host, Template: template, Slug: slug,
		Parameters: params, Secrets: secrets,
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
	arr = append(arr, step)
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
WHERE parent_id IS NOT NULL AND state IN ('succeeded','failed')
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
WHERE state IN ('succeeded','failed') AND finished IS NOT NULL AND finished < ?
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

var _ DB = (*SQLite)(nil)

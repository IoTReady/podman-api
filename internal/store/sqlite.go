package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"net/url"
	"strings"
	"time"

	_ "modernc.org/sqlite"
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

// maxOpenConns bounds the SQLite connection pool. >1 enables WAL reader
// concurrency (API reads while the job runner writes); a competing writer waits
// up to busy_timeout rather than failing with "database is locked".
const maxOpenConns = 4

// SQLite is the durable Store backed by a single SQLite file. Secrets are
// sealed with the key held in keys, read fresh on every Put/Get so a SIGHUP
// key swap takes effect immediately.
type SQLite struct {
	db   *sql.DB
	keys *KeyStore
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
	if _, err := db.Exec(`PRAGMA user_version = 2`); err != nil {
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
	_, err = s.db.ExecContext(ctx, `
INSERT INTO specs (host, template, slug, parameters, secrets, created, updated)
VALUES (?, ?, ?, ?, ?, ?, ?)
ON CONFLICT(host, template, slug) DO UPDATE SET
  parameters = excluded.parameters,
  secrets    = excluded.secrets,
  updated    = excluded.updated`,
		sp.Host, sp.Template, sp.Slug, string(params), blob, now, now)
	return err
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
	j.Created = time.Unix(created, 0)
	if started.Valid {
		j.Started = time.Unix(started.Int64, 0)
	}
	if finished.Valid {
		j.Finished = time.Unix(finished.Int64, 0)
	}
	return j, nil
}

func (s *SQLite) Enqueue(ctx context.Context, kind string, args json.RawMessage, parentID string) (Job, error) {
	id := newJobID()
	now := time.Now().Unix()
	if len(args) == 0 {
		args = json.RawMessage("null")
	}
	var parent any
	if parentID != "" {
		parent = parentID
	}
	_, err := s.db.ExecContext(ctx, `
INSERT INTO jobs (id, kind, args, state, steps, parent_id, error, created, started, finished)
VALUES (?, ?, ?, 'queued', '[]', ?, NULL, ?, NULL, NULL)`,
		id, kind, string(args), parent, now)
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
	if len(where) > 0 {
		q += " WHERE " + strings.Join(where, " AND ")
	}
	q += " ORDER BY created DESC, id DESC"
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
	now := time.Now().Unix()
	row := s.db.QueryRowContext(ctx, `
UPDATE jobs SET state='running', started=?
WHERE id = (SELECT id FROM jobs WHERE state='queued' ORDER BY created, id LIMIT 1)
  AND state='queued'
RETURNING `+jobColumns, now)
	j, err := scanJob(row)
	if errors.Is(err, ErrNotFound) {
		return Job{}, false, nil
	}
	if err != nil {
		return Job{}, false, err
	}
	return j, true, nil
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
	_, err = s.db.ExecContext(ctx, `UPDATE jobs SET steps=? WHERE id=?`, string(b), id)
	return err
}

func (s *SQLite) Finish(ctx context.Context, id string, state JobState, errMsg string) error {
	now := time.Now().Unix()
	var e any
	if errMsg != "" {
		e = errMsg
	}
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
}

func (s *SQLite) FailRunning(ctx context.Context, reason string) (int, error) {
	now := time.Now().Unix()
	res, err := s.db.ExecContext(ctx, `UPDATE jobs SET state='failed', error=?, finished=? WHERE state='running'`,
		reason, now)
	if err != nil {
		return 0, err
	}
	n, err := res.RowsAffected()
	if err != nil {
		return 0, err
	}
	return int(n), nil
}

var _ DB = (*SQLite)(nil)

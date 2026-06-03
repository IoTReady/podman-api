package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"net/url"
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

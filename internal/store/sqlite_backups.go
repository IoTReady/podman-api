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
	return s.write(ctx, func() error {
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
	var n int64
	err := s.write(ctx, func() error {
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
	var n int64
	err := s.write(ctx, func() error {
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

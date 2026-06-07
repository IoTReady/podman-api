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

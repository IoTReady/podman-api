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
	rc, err := l.Get(ctx, "h/t/s/bk2/a.tar")
	assert.NoError(t, err)
	if err == nil {
		rc.Close()
	}
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

package store

import (
	"context"
	"errors"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// hostSecretStore is the slice of Store exercised here.
type hostSecretStore interface {
	PutHostSecret(ctx context.Context, host, name string, value []byte) error
	GetHostSecret(ctx context.Context, host, name string) ([]byte, error)
	DeleteHostSecret(ctx context.Context, host, name string) error
}

func hostSecretStores(t *testing.T) map[string]hostSecretStore {
	t.Helper()
	keys := &KeyStore{}
	keys.Store([32]byte{1, 2, 3})
	sq, err := OpenSQLite(filepath.Join(t.TempDir(), "s.db"), keys)
	require.NoError(t, err)
	t.Cleanup(func() { _ = sq.Close() })
	return map[string]hostSecretStore{"sqlite": sq, "memory": NewMemory()}
}

func TestHostSecret_RoundTrip(t *testing.T) {
	ctx := context.Background()
	for name, st := range hostSecretStores(t) {
		t.Run(name, func(t *testing.T) {
			require.NoError(t, st.PutHostSecret(ctx, "h1", "shared-token", []byte("v1")))
			got, err := st.GetHostSecret(ctx, "h1", "shared-token")
			require.NoError(t, err)
			assert.Equal(t, []byte("v1"), got)
		})
	}
}

func TestHostSecret_Upsert(t *testing.T) {
	ctx := context.Background()
	for name, st := range hostSecretStores(t) {
		t.Run(name, func(t *testing.T) {
			require.NoError(t, st.PutHostSecret(ctx, "h1", "k", []byte("old")))
			require.NoError(t, st.PutHostSecret(ctx, "h1", "k", []byte("new")))
			got, err := st.GetHostSecret(ctx, "h1", "k")
			require.NoError(t, err)
			assert.Equal(t, []byte("new"), got)
		})
	}
}

func TestHostSecret_NotFound(t *testing.T) {
	ctx := context.Background()
	for name, st := range hostSecretStores(t) {
		t.Run(name, func(t *testing.T) {
			_, err := st.GetHostSecret(ctx, "h1", "missing")
			assert.ErrorIs(t, err, ErrNotFound)
		})
	}
}

func TestHostSecret_ScopedByHost(t *testing.T) {
	ctx := context.Background()
	for name, st := range hostSecretStores(t) {
		t.Run(name, func(t *testing.T) {
			require.NoError(t, st.PutHostSecret(ctx, "h1", "k", []byte("a")))
			require.NoError(t, st.PutHostSecret(ctx, "h2", "k", []byte("b")))
			g1, _ := st.GetHostSecret(ctx, "h1", "k")
			g2, _ := st.GetHostSecret(ctx, "h2", "k")
			assert.Equal(t, []byte("a"), g1)
			assert.Equal(t, []byte("b"), g2)
		})
	}
}

func TestHostSecret_Delete(t *testing.T) {
	ctx := context.Background()
	for name, st := range hostSecretStores(t) {
		t.Run(name, func(t *testing.T) {
			require.NoError(t, st.PutHostSecret(ctx, "h1", "k", []byte("v")))
			require.NoError(t, st.DeleteHostSecret(ctx, "h1", "k"))
			_, err := st.GetHostSecret(ctx, "h1", "k")
			assert.ErrorIs(t, err, ErrNotFound)
			// Delete is idempotent: removing an absent key is not an error.
			require.NoError(t, st.DeleteHostSecret(ctx, "h1", "k"))
		})
	}
}

// SQLite seals at rest: the encrypted blob must not contain the plaintext.
func TestHostSecret_SQLiteSealsAtRest(t *testing.T) {
	keys := &KeyStore{}
	keys.Store([32]byte{9})
	sq, err := OpenSQLite(filepath.Join(t.TempDir(), "s.db"), keys)
	require.NoError(t, err)
	t.Cleanup(func() { _ = sq.Close() })
	ctx := context.Background()
	require.NoError(t, sq.PutHostSecret(ctx, "h1", "k", []byte("SUPERSECRET")))
	var blob []byte
	require.NoError(t, sq.db.QueryRowContext(ctx,
		`SELECT value FROM host_secrets WHERE host='h1' AND name='k'`).Scan(&blob))
	assert.NotContains(t, string(blob), "SUPERSECRET")
	assert.False(t, errors.Is(nil, ErrNotFound)) // keep errors import used
}

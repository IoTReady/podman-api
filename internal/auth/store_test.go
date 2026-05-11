package auth

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/iotready/podman-api/internal/config"
)

func keysWithIDs(t *testing.T, ids ...string) []config.APIKey {
	t.Helper()
	out := make([]config.APIKey, 0, len(ids))
	for _, id := range ids {
		hash, err := config.HashToken(id + "-tok")
		require.NoError(t, err)
		out = append(out, config.APIKey{ID: id, SecretHash: hash, Scopes: []string{"hosts:read"}})
	}
	return out
}

func TestKeyStore_RoundTrip(t *testing.T) {
	store := NewKeyStore(keysWithIDs(t, "a"))
	assert.Len(t, store.Load(), 1)

	store.Store(keysWithIDs(t, "a", "b", "c"))
	assert.Len(t, store.Load(), 3)

	store.Store(nil)
	assert.Empty(t, store.Load(), "nil store is allowed and means deny-all")
}

func TestKeyStore_StoreCopiesInput(t *testing.T) {
	// The slice passed to Store is defensively copied so that callers who
	// keep references to it (e.g. main holding the parsed slice for logging)
	// can't accidentally mutate the live snapshot afterwards.
	original := keysWithIDs(t, "a")
	store := NewKeyStore(original)
	original[0].ID = "tampered"
	got := store.Load()
	require.Len(t, got, 1)
	assert.Equal(t, "a", got[0].ID, "Store must defensively copy its input")
}

// TestKeyStore_ConcurrentReadWrite verifies the atomic swap: many readers
// and one writer running together never observe a partial state. This is
// the property that makes SIGHUP reload safe under load.
func TestKeyStore_ConcurrentReadWrite(t *testing.T) {
	store := NewKeyStore(keysWithIDs(t, "a"))
	stop := make(chan struct{})

	// Writer: continuously rotate between 1-key and 3-key snapshots until
	// readers finish.
	writerDone := make(chan struct{})
	go func() {
		defer close(writerDone)
		small := keysWithIDs(t, "a")
		big := keysWithIDs(t, "a", "b", "c")
		for {
			select {
			case <-stop:
				return
			default:
			}
			store.Store(small)
			store.Store(big)
		}
	}()

	// Readers: assert every snapshot has either 1 or 3 entries (never 0,
	// 2, or partial).
	var readers sync.WaitGroup
	for i := 0; i < 8; i++ {
		readers.Add(1)
		go func() {
			defer readers.Done()
			for j := 0; j < 5000; j++ {
				n := len(store.Load())
				assert.Truef(t, n == 1 || n == 3, "saw torn snapshot of length %d", n)
			}
		}()
	}

	readers.Wait()
	close(stop)
	<-writerDone
}

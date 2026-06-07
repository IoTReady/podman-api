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

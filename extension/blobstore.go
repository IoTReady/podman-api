package extension

import (
	"context"
	"io"
)

type BlobWriter interface {
	io.Writer
	Commit() error
	Abort() error
}

type BlobStore interface {
	Put(ctx context.Context, key string) (BlobWriter, error)
	Get(ctx context.Context, key string) (io.ReadCloser, error)
	DeleteAll(ctx context.Context, prefix string) error
}

package podman

import (
	"context"
	"fmt"
	"sync"

	"github.com/containers/podman/v5/pkg/bindings"
	"github.com/containers/podman/v5/pkg/bindings/system"

	"github.com/iotready/podman-api/internal/config"
)

// Real is the production podman.Client implementation backed by libpod
// over SSH (production) or a local unix socket (dev).
//
// Per-host context.Contexts are created lazily and cached. The bindings
// library stores the underlying connection on the context, so callers must
// pass the cached context (returned by ctxFor) to every libpod call.
//
// Uses github.com/containers/podman/v5 v5.8.2.
// bindings.NewConnection signature: func(ctx context.Context, uri string) (context.Context, error)
type Real struct {
	hosts map[string]config.Host

	mu  sync.Mutex
	ctx map[string]context.Context // hostID -> connection-bearing ctx
}

// NewReal validates host configs and registers them. Connections are not
// opened here; first use opens them.
func NewReal(hosts []config.Host) (*Real, error) {
	r := &Real{hosts: map[string]config.Host{}, ctx: map[string]context.Context{}}
	for _, h := range hosts {
		if h.ID == "" {
			return nil, fmt.Errorf("host with empty id")
		}
		if _, dup := r.hosts[h.ID]; dup {
			return nil, fmt.Errorf("duplicate host id %q", h.ID)
		}
		r.hosts[h.ID] = h
	}
	return r, nil
}

// Knows reports whether the host is registered.
func (r *Real) Knows(id string) bool {
	_, ok := r.hosts[id]
	return ok
}

// URIFor returns the libpod URI for hostID. unix-only when addr=="unix".
func (r *Real) URIFor(id string) (string, error) {
	h, ok := r.hosts[id]
	if !ok {
		return "", fmt.Errorf("unknown host %q", id)
	}
	if h.Addr == "unix" {
		return "unix://" + h.Socket, nil
	}
	return "ssh://" + h.Addr + h.Socket, nil
}

// ctxFor returns a libpod-ready context for hostID, opening the connection
// on first use.
func (r *Real) ctxFor(parent context.Context, id string) (context.Context, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if c, ok := r.ctx[id]; ok {
		return c, nil
	}
	h, ok := r.hosts[id]
	if !ok {
		return nil, fmt.Errorf("unknown host %q", id)
	}
	uri, err := r.URIFor(id)
	if err != nil {
		return nil, err
	}
	var c context.Context
	if h.Addr != "unix" && h.SSHKey != "" {
		// SSH host with explicit key file. The fourth arg (`machine`) is false
		// for non-machine connections per the bindings API.
		c, err = bindings.NewConnectionWithIdentity(parent, uri, h.SSHKey, false)
	} else {
		c, err = bindings.NewConnection(parent, uri)
	}
	if err != nil {
		return nil, fmt.Errorf("connect to host %q: %w", id, err)
	}
	r.ctx[id] = c
	return c, nil
}

func (r *Real) Ping(ctx context.Context, id string) error {
	c, err := r.ctxFor(ctx, id)
	if err != nil {
		return err
	}
	_, err = system.Info(c, &system.InfoOptions{})
	return err
}

func (r *Real) Version(ctx context.Context, id string) (string, error) {
	c, err := r.ctxFor(ctx, id)
	if err != nil {
		return "", err
	}
	info, err := system.Info(c, &system.InfoOptions{})
	if err != nil {
		return "", err
	}
	return info.Version.Version, nil
}

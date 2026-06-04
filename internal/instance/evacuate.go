package instance

import (
	"context"
	"errors"
	"fmt"
	"sort"
)

// ErrInvalidEvacuation means the request cannot be planned against the stored
// specs: an instance on the host has no destination in the map, a map entry
// names no instance, or a slug is ambiguous across templates. The API maps it to
// 400 invalid_request.
var ErrInvalidEvacuation = errors.New("invalid evacuation request")

// EvacuateRequest is the POST /evacuate body and the evacuate job's args. Map is
// slug -> destination host; it carries no template (resolved from the stored
// spec) and no parameters (Migrate merges the spec's own).
type EvacuateRequest struct {
	FromHost string            `json:"from_host"`
	Map      map[string]string `json:"map"`
}

// ResolveEvacuation validates the request against the specs stored on FromHost
// and returns the per-instance migrate plan, sorted by slug for determinism. It
// is pure read/validation (no mutation) and is called both synchronously by the
// POST handler (fast-fail, result discarded) and by the evacuate job handler at
// execution time (state may have drifted since enqueue).
func (s *Service) ResolveEvacuation(ctx context.Context, req EvacuateRequest) ([]MigrateRequest, error) {
	if _, ok := s.host(req.FromHost); !ok {
		return nil, ErrUnknownHost
	}
	if s.store == nil {
		return nil, ErrStoreDisabled
	}
	keys, err := s.store.ListSpecKeys(ctx, req.FromHost)
	if err != nil {
		return nil, err
	}

	tmplBySlug := make(map[string]string, len(keys))
	for _, k := range keys {
		if prev, dup := tmplBySlug[k.Slug]; dup && prev != k.Template {
			return nil, fmt.Errorf("%w: slug %q exists under templates %q and %q on %s",
				ErrInvalidEvacuation, k.Slug, prev, k.Template, req.FromHost)
		}
		tmplBySlug[k.Slug] = k.Template
	}

	// Every instance on the host must have a destination (true evacuate).
	for slug := range tmplBySlug {
		if _, ok := req.Map[slug]; !ok {
			return nil, fmt.Errorf("%w: no destination for slug %q on %s",
				ErrInvalidEvacuation, slug, req.FromHost)
		}
	}

	moves := make([]MigrateRequest, 0, len(req.Map))
	for slug, dest := range req.Map {
		tmpl, ok := tmplBySlug[slug]
		if !ok {
			return nil, fmt.Errorf("%w: no such instance %q on %s",
				ErrInvalidEvacuation, slug, req.FromHost)
		}
		if dest == req.FromHost {
			return nil, ErrSameHost
		}
		if _, ok := s.host(dest); !ok {
			return nil, ErrUnknownHost
		}
		moves = append(moves, MigrateRequest{
			FromHost: req.FromHost, ToHost: dest, Template: tmpl, Slug: slug,
		})
	}
	sort.Slice(moves, func(i, j int) bool { return moves[i].Slug < moves[j].Slug })
	return moves, nil
}

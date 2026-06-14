package instance

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"

	"github.com/iotready/podman-api/internal/store"
)

// ErrInvalidEvacuation means the request cannot be planned against the stored
// specs: an instance on the host has no destination in the map, a move names
// no instance, a slug is ambiguous across templates (slug-keyed form only),
// or a map/move entry names an unknown destination host. The API maps it to
// 400 invalid_request, giving every bad-map case one consistent status.
var ErrInvalidEvacuation = errors.New("invalid evacuation request")

// Move is one instance's destination in an evacuation request. Template+Slug
// together identify the instance uniquely, resolving the ambiguity when two
// templates share a slug on one host.
type Move struct {
	Template string `json:"template"`
	Slug     string `json:"slug"`
	ToHost   string `json:"to_host"`
}

// EvacuateRequest is the POST /evacuate body and the evacuate job's args.
//
// Moves lists each instance's destination by (template, slug) — the composite
// identity used throughout the system. Map (slug -> destination host) is the
// legacy form kept for backward compatibility; it is rejected when a slug is
// ambiguous across templates on the source host.
//
// At most one of {Map, Moves} should be set. If Moves is non-empty it wins;
// otherwise ResolveEvacuation falls back to Map.
type EvacuateRequest struct {
	FromHost string            `json:"from_host"`
	Map      map[string]string `json:"map,omitempty"`
	Moves    []Move            `json:"moves,omitempty"`
	// Concurrency, if >0, overrides the server's default for how many child
	// migrations run at once (clamped to [1,32] by the handler). Request-only:
	// it does not affect the migrate plan, so ResolveEvacuation ignores it.
	Concurrency int `json:"concurrency,omitempty"`
}

// ResolveEvacuation validates the request against the specs stored on FromHost
// and returns the per-instance migrate plan, sorted by (template, slug) for
// determinism. It is pure read/validation (no mutation) and is called both
// synchronously by the POST handler (fast-fail, result discarded) and by the
// evacuate job handler at execution time (state may have drifted since enqueue).
func (s *Service) ResolveEvacuation(ctx context.Context, req EvacuateRequest) ([]MigrateRequest, error) {
	if _, ok := s.host(req.FromHost); !ok {
		return nil, ErrUnknownHost
	}
	keys, err := s.store.ListSpecKeys(ctx, req.FromHost)
	if err != nil {
		return nil, err
	}

	hostKeys := make(map[store.SpecKey]struct{}, len(keys))
	for _, k := range keys {
		hostKeys[k] = struct{}{}
	}

	moves, err := resolveMoves(req, hostKeys)
	if err != nil {
		return nil, err
	}

	// Bijection: every host instance has a move, every move names a host
	// instance, no duplicate moves.
	seen := make(map[store.SpecKey]struct{}, len(moves))
	for _, m := range moves {
		sk := store.SpecKey{Template: m.Template, Slug: m.Slug}
		if _, ok := hostKeys[sk]; !ok {
			return nil, fmt.Errorf("%w: no such instance %q/%q on %s",
				ErrInvalidEvacuation, m.Template, m.Slug, req.FromHost)
		}
		if _, dup := seen[sk]; dup {
			return nil, fmt.Errorf("%w: duplicate move for %q/%q on %s",
				ErrInvalidEvacuation, m.Template, m.Slug, req.FromHost)
		}
		seen[sk] = struct{}{}
		if m.ToHost == req.FromHost {
			return nil, ErrSameHost
		}
		if _, ok := s.host(m.ToHost); !ok {
			return nil, fmt.Errorf("%w: no such destination host %q for %q/%q",
				ErrInvalidEvacuation, m.ToHost, m.Template, m.Slug)
		}
	}

	var missing []string
	for sk := range hostKeys {
		if _, ok := seen[sk]; !ok {
			missing = append(missing, fmt.Sprintf("%s/%s", sk.Template, sk.Slug))
		}
	}
	if len(missing) > 0 {
		slices.Sort(missing)
		return nil, fmt.Errorf("%w: no destination for instance(s) %v on %s",
			ErrInvalidEvacuation, missing, req.FromHost)
	}

	result := make([]MigrateRequest, 0, len(moves))
	for _, m := range moves {
		result = append(result, MigrateRequest{
			FromHost: req.FromHost, ToHost: m.ToHost, Template: m.Template, Slug: m.Slug,
		})
	}
	slices.SortFunc(result, func(a, b MigrateRequest) int {
		return cmp.Or(strings.Compare(a.Template, b.Template), strings.Compare(a.Slug, b.Slug))
	})
	return result, nil
}

// resolveMoves resolves the effective move list from either the Moves array or
// the legacy slug-keyed Map.
//
//   - Moves form: returned as-is (each move must name a host instance; that is
//     validated by the caller via the bijection check).
//   - Map form: slug -> destination is expanded to (template, slug) -> dest
//     using the host's spec keys. Ambiguous slugs are rejected.
func resolveMoves(req EvacuateRequest, hostKeys map[store.SpecKey]struct{}) ([]Move, error) {
	if len(req.Moves) > 0 {
		return req.Moves, nil
	}

	// Backward-compatible slug-keyed map path.
	// Sort host keys for deterministic template collision reporting.
	skSorted := make([]store.SpecKey, 0, len(hostKeys))
	for sk := range hostKeys {
		skSorted = append(skSorted, sk)
	}
	slices.SortFunc(skSorted, func(a, b store.SpecKey) int {
		return cmp.Or(strings.Compare(a.Template, b.Template), strings.Compare(a.Slug, b.Slug))
	})
	tmplBySlug := make(map[string]string, len(hostKeys))
	for _, sk := range skSorted {
		if prev, dup := tmplBySlug[sk.Slug]; dup && prev != sk.Template {
			return nil, fmt.Errorf("%w: slug %q exists under templates %q and %q on %s",
				ErrInvalidEvacuation, sk.Slug, prev, sk.Template, req.FromHost)
		}
		tmplBySlug[sk.Slug] = sk.Template
	}

	// Collect missing slugs first so the error is deterministic.
	slugs := make([]string, 0, len(tmplBySlug))
	for slug := range tmplBySlug {
		slugs = append(slugs, slug)
	}
	slices.Sort(slugs)
	var missing []string
	for _, slug := range slugs {
		if _, ok := req.Map[slug]; !ok {
			missing = append(missing, slug)
		}
	}
	if len(missing) > 0 {
		return nil, fmt.Errorf("%w: no destination for slug(s) %v on %s",
			ErrInvalidEvacuation, missing, req.FromHost)
	}

	moves := make([]Move, 0, len(req.Map))
	mapSlugs := make([]string, 0, len(req.Map))
	for slug := range req.Map {
		mapSlugs = append(mapSlugs, slug)
	}
	slices.Sort(mapSlugs)
	for _, slug := range mapSlugs {
		dest := req.Map[slug]
		tmpl, ok := tmplBySlug[slug]
		if !ok {
			return nil, fmt.Errorf("%w: no such instance %q on %s",
				ErrInvalidEvacuation, slug, req.FromHost)
		}
		moves = append(moves, Move{Template: tmpl, Slug: slug, ToHost: dest})
	}
	return moves, nil
}

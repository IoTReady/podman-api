package ui

import (
	"context"
	"log"
	"net/http"
	"sync"
	"time"

	"github.com/iotready/podman-api/internal/imgregistry"
)

// tagsRequestTimeout mirrors internal/api/registry.go's bound of the same
// name: a repo with enough accumulated tags (the fleet's "engine" repo has
// ~1000) can take a while to resolve even with imgregistry.Tags' bounded
// concurrency, and without a bound a slow/wedged registry hangs the request
// indefinitely instead of failing with a clear error page.
const tagsRequestTimeout = 45 * time.Second

// repoCountTimeout bounds one repo's TagCount during the list page's
// fan-out. Mirrors hostFetchTimeout's role for the dashboard's per-host
// fan-out: one unreachable repo must not stall the whole page.
const repoCountTimeout = 5 * time.Second

// repoSummary is one row of the registry list page. CountOK distinguishes
// "this repo has zero tags" from "we could not find out", which the template
// renders differently — collapsing them would report an unreachable repo as
// empty, the same class of misleading signal the API's 404-vs-502 split
// exists to prevent.
type repoSummary struct {
	Name     string
	TagCount int
	CountOK  bool
}

func (u *UI) registryRepos(w http.ResponseWriter, r *http.Request) {
	if u.cfg.Registry == nil {
		http.NotFound(w, r)
		return
	}
	repos, err := u.cfg.Registry.Catalog(r.Context())
	if err != nil {
		u.renderError(w, r, err)
		return
	}

	// Fan out the (cheap, one-call) tag counts concurrently — same pattern
	// as dashboard's per-host fan-out in handlers_hosts.go. Each goroutine
	// writes only its own slice index, so no synchronization beyond the
	// WaitGroup is needed. Size and last-pushed are NOT fetched here: they
	// need full Tags() resolution, which the template lazy-loads per row.
	summaries := make([]repoSummary, len(repos))
	var wg sync.WaitGroup
	for i, repo := range repos {
		wg.Add(1)
		go func(i int, repo string) {
			defer wg.Done()
			rctx, cancel := context.WithTimeout(r.Context(), repoCountTimeout)
			defer cancel()
			s := repoSummary{Name: repo}
			if n, err := u.cfg.Registry.TagCount(rctx, repo); err == nil {
				s.TagCount, s.CountOK = n, true
			} else {
				log.Printf("ui: registry: TagCount(%q): %v", repo, err)
			}
			summaries[i] = s
		}(i, repo)
	}
	wg.Wait()

	u.render(w, r, http.StatusOK, "registry-repos", u.pageData(map[string]any{
		"Repos": summaries,
	}))
}

func (u *UI) registryTags(w http.ResponseWriter, r *http.Request) {
	if u.cfg.Registry == nil {
		http.NotFound(w, r)
		return
	}
	repo := r.PathValue("repo")
	// Mirror the API edge's validation (internal/api/registry.go): an
	// unvalidated repo went straight into u.cfg.Registry.Tags before this
	// fix, while the API path already checked it — same shared validator now,
	// both edges.
	if !imgregistry.ValidRepoName(repo) {
		http.Error(w, "invalid repository name", http.StatusBadRequest)
		return
	}
	// A single-digest detail view needs one Manifest lookup, not the whole
	// repo's Tags() resolution — so this branch returns before that cost is
	// paid. Mirrors the JSON API's own ?manifest=<ref> fold on the same
	// route shape (internal/api/registry.go), which exists because Go's
	// {repo...} wildcard must be a route's last segment.
	if ref := r.URL.Query().Get("manifest"); ref != "" {
		if !imgregistry.ValidRef(ref) {
			http.Error(w, "invalid manifest reference", http.StatusBadRequest)
			return
		}
		m, err := u.cfg.Registry.Manifest(r.Context(), repo, ref)
		if err != nil {
			u.renderError(w, r, err)
			return
		}
		u.render(w, r, http.StatusOK, "registry-manifest", u.pageData(map[string]any{
			"Repo":     repo,
			"Ref":      ref,
			"Manifest": m,
		}))
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), tagsRequestTimeout)
	defer cancel()
	groups, err := u.cfg.Registry.Tags(ctx, repo)

	// The stats cell is a small lazy-loaded fragment nested inside the list
	// page's table (registry-repos.html: hx-trigger="load" on each row), not
	// a full navigation — so it must catch its own Tags() error rather than
	// falling into the shared u.renderError path below. renderError returns
	// a non-2xx status, and HTMX does not swap a non-2xx response by
	// default, so a failing repo's cell would keep its "loading…"
	// placeholder forever instead of resolving to a visible failure marker;
	// worse, if it did swap, renderError's fragment carries full error-card
	// chrome (headings, card divs) that doesn't belong inside a <td>. So this
	// branch always renders 200 with a small in-cell marker, and logs the
	// real error for diagnosis instead of surfacing it to the browser.
	if r.URL.Query().Get("stats") == "1" {
		if err != nil {
			log.Printf("ui: registry: stats Tags(%q): %v", repo, err)
			u.render(w, r, http.StatusOK, "registry-repo-stats", u.pageData(map[string]any{
				"StatsError": true,
			}))
			return
		}
		var total int64
		var newest time.Time
		for _, g := range groups {
			// Sums each unique digest's manifest size once. Layers shared
			// between digests (the common case: most of engine's 737
			// digests share most of their base-image layers) are counted
			// once per digest that references them, so this is each image's
			// own uncompressed size, NOT the repo's deduplicated on-disk
			// footprint — TagGroup carries no data that would let us
			// deduplicate by layer digest across groups. The column header
			// says so explicitly (registry-repos.html) rather than implying
			// a number this handler cannot produce.
			total += g.Size
			if g.Created.After(newest) {
				newest = g.Created
			}
		}
		u.render(w, r, http.StatusOK, "registry-repo-stats", u.pageData(map[string]any{
			"TotalSize": total,
			"Newest":    newest,
		}))
		return
	}

	if err != nil {
		u.renderError(w, r, err)
		return
	}
	if r.URL.Query().Get("picker") == "1" {
		u.render(w, r, http.StatusOK, "registry-tags-picker", u.pageData(map[string]any{
			"Repo":   repo,
			"Groups": groups,
		}))
		return
	}
	u.render(w, r, http.StatusOK, "registry-tags", u.pageData(map[string]any{
		"Repo":   repo,
		"Groups": groups,
	}))
}

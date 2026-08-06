package ui

import (
	"context"
	"net/http"
	"time"

	"github.com/iotready/podman-api/internal/imgregistry"
)

// tagsRequestTimeout mirrors internal/api/registry.go's bound of the same
// name: a repo with enough accumulated tags (the fleet's "engine" repo has
// ~1000) can take a while to resolve even with imgregistry.Tags' bounded
// concurrency, and without a bound a slow/wedged registry hangs the request
// indefinitely instead of failing with a clear error page.
const tagsRequestTimeout = 45 * time.Second

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
	u.render(w, r, http.StatusOK, "registry-repos", u.pageData(map[string]any{
		"ActiveHost": "",
		"Repos":      repos,
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
	ctx, cancel := context.WithTimeout(r.Context(), tagsRequestTimeout)
	defer cancel()
	groups, err := u.cfg.Registry.Tags(ctx, repo)
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

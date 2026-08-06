package ui

import "net/http"

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
	groups, err := u.cfg.Registry.Tags(r.Context(), repo)
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

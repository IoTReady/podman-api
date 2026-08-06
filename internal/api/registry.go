package api

import (
	"errors"
	"net/http"

	"github.com/iotready/podman-api/internal/imgregistry"
)

func (h *handlers) listRepos(w http.ResponseWriter, r *http.Request) {
	if h.registry == nil {
		WriteJSON(w, http.StatusNotFound, ErrorBody{Code: "not_found", Message: "registry browsing is not configured"})
		return
	}
	repos, err := h.registry.Catalog(r.Context())
	if err != nil {
		WriteJSON(w, http.StatusBadGateway, ErrorBody{Code: "registry_unreachable", Message: err.Error()})
		return
	}
	WriteJSON(w, http.StatusOK, repos)
}

// getRepoOrManifest serves GET /registry/repos/{repo...}. With no query
// string it lists the repo's tags (grouped by digest); with "?manifest=<ref>"
// it resolves that single tag/digest instead. Both live behind one route
// because Go 1.22's {repo...} wildcard (needed so a "/"-containing repo name
// like "iotready/engine" is addressable at all) must be the mux's last path
// segment, ruling out a separate .../manifests/{ref} route.
func (h *handlers) getRepoOrManifest(w http.ResponseWriter, r *http.Request) {
	if h.registry == nil {
		WriteJSON(w, http.StatusNotFound, ErrorBody{Code: "not_found", Message: "registry browsing is not configured"})
		return
	}
	repo := r.PathValue("repo")
	if !imgregistry.ValidRepoName(repo) {
		writeInvalidName(w, "repo", repo)
		return
	}
	if ref := r.URL.Query().Get("manifest"); ref != "" {
		h.getManifest(w, r, repo, ref)
		return
	}
	h.listRepoTags(w, r, repo)
}

func (h *handlers) listRepoTags(w http.ResponseWriter, r *http.Request, repo string) {
	groups, err := h.registry.Tags(r.Context(), repo)
	if err != nil {
		writeRegistryError(w, err)
		return
	}
	WriteJSON(w, http.StatusOK, groups)
}

func (h *handlers) getManifest(w http.ResponseWriter, r *http.Request, repo, ref string) {
	m, err := h.registry.Manifest(r.Context(), repo, ref)
	if err != nil {
		writeRegistryError(w, err)
		return
	}
	WriteJSON(w, http.StatusOK, m)
}

// writeRegistryError maps a Tags/Manifest error to its HTTP status: a genuine
// registry 404 (imgregistry.ErrNotFound) is "not found", everything else
// (transport failure, non-404 non-2xx, undecodable body —
// imgregistry.ErrUnreachable) is "registry unreachable". Collapsing the two
// into a blanket 404 is exactly the misleading signal this route exists to
// avoid: an operator must never be told a repo/tag doesn't exist when the
// real problem is that the registry could not be reached.
func writeRegistryError(w http.ResponseWriter, err error) {
	if errors.Is(err, imgregistry.ErrNotFound) {
		WriteJSON(w, http.StatusNotFound, ErrorBody{Code: "not_found", Message: err.Error()})
		return
	}
	WriteJSON(w, http.StatusBadGateway, ErrorBody{Code: "registry_unreachable", Message: err.Error()})
}

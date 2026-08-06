package api

import (
	"net/http"
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

func (h *handlers) listRepoTags(w http.ResponseWriter, r *http.Request) {
	if h.registry == nil {
		WriteJSON(w, http.StatusNotFound, ErrorBody{Code: "not_found", Message: "registry browsing is not configured"})
		return
	}
	repo := r.PathValue("repo")
	if !validName(repo) {
		writeInvalidName(w, "repo", repo)
		return
	}
	groups, err := h.registry.Tags(r.Context(), repo)
	if err != nil {
		WriteJSON(w, http.StatusNotFound, ErrorBody{Code: "not_found", Message: err.Error()})
		return
	}
	WriteJSON(w, http.StatusOK, groups)
}

func (h *handlers) getManifest(w http.ResponseWriter, r *http.Request) {
	if h.registry == nil {
		WriteJSON(w, http.StatusNotFound, ErrorBody{Code: "not_found", Message: "registry browsing is not configured"})
		return
	}
	repo := r.PathValue("repo")
	ref := r.PathValue("ref")
	if !validName(repo) {
		writeInvalidName(w, "repo", repo)
		return
	}
	m, err := h.registry.Manifest(r.Context(), repo, ref)
	if err != nil {
		WriteJSON(w, http.StatusNotFound, ErrorBody{Code: "not_found", Message: err.Error()})
		return
	}
	WriteJSON(w, http.StatusOK, m)
}

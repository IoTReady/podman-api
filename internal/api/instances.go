package api

import (
	"encoding/json"
	"net/http"
	"strconv"

	"github.com/iotready/podman-api/internal/instance"
)

func (h *handlers) listInstances(w http.ResponseWriter, r *http.Request) {
	host := r.PathValue("host")
	template := r.URL.Query().Get("template")
	if template == "" {
		WriteJSON(w, http.StatusBadRequest, ErrorBody{
			Code: "invalid_query", Message: "template query parameter is required",
		})
		return
	}
	out, err := h.svc.List(r.Context(), host, template)
	if err != nil {
		WriteError(w, err)
		return
	}
	WriteJSON(w, http.StatusOK, out)
}

func (h *handlers) getInstance(w http.ResponseWriter, r *http.Request) {
	host := r.PathValue("host")
	tmpl := r.PathValue("template")
	slug := r.PathValue("slug")
	obs, err := h.svc.Get(r.Context(), host, tmpl, slug)
	if err != nil {
		WriteError(w, err)
		return
	}
	WriteJSON(w, http.StatusOK, obs)
}

func (h *handlers) createInstance(w http.ResponseWriter, r *http.Request) {
	host := r.PathValue("host")
	req, err := decodeApply(r)
	if err != nil {
		WriteJSON(w, http.StatusBadRequest, ErrorBody{Code: "invalid_body", Message: err.Error()})
		return
	}
	if err := h.svc.Apply(r.Context(), host, req, false); err != nil {
		WriteError(w, err)
		return
	}
	obs, err := h.svc.Get(r.Context(), host, req.Template, req.Slug)
	if err != nil {
		WriteError(w, err)
		return
	}
	WriteJSON(w, http.StatusCreated, obs)
}

func (h *handlers) applyInstance(w http.ResponseWriter, r *http.Request) {
	host := r.PathValue("host")
	req, err := decodeApply(r)
	if err != nil {
		WriteJSON(w, http.StatusBadRequest, ErrorBody{Code: "invalid_body", Message: err.Error()})
		return
	}
	if got := r.PathValue("template"); req.Template != "" && req.Template != got {
		WriteJSON(w, http.StatusBadRequest, ErrorBody{Code: "invalid_body", Message: "template in URL does not match body"})
		return
	}
	if got := r.PathValue("slug"); req.Slug != "" && req.Slug != got {
		WriteJSON(w, http.StatusBadRequest, ErrorBody{Code: "invalid_body", Message: "slug in URL does not match body"})
		return
	}
	req.Template = r.PathValue("template")
	req.Slug = r.PathValue("slug")

	if err := h.svc.Apply(r.Context(), host, req, true); err != nil {
		WriteError(w, err)
		return
	}
	obs, err := h.svc.Get(r.Context(), host, req.Template, req.Slug)
	if err != nil {
		WriteError(w, err)
		return
	}
	WriteJSON(w, http.StatusOK, obs)
}

func (h *handlers) deleteInstance(w http.ResponseWriter, r *http.Request) {
	host := r.PathValue("host")
	tmpl := r.PathValue("template")
	slug := r.PathValue("slug")
	opts := instance.DeleteOptions{
		PruneVolumes: queryBool(r, "prune_volumes"),
		PruneSecrets: queryBool(r, "prune_secrets"),
	}
	if err := h.svc.Delete(r.Context(), host, tmpl, slug, opts); err != nil {
		WriteError(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func decodeApply(r *http.Request) (instance.ApplyRequest, error) {
	var req instance.ApplyRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		return req, err
	}
	return req, nil
}

func queryBool(r *http.Request, key string) bool {
	v := r.URL.Query().Get(key)
	if v == "" {
		return false
	}
	b, _ := strconv.ParseBool(v)
	return b
}

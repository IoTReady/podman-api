package api

import (
	"encoding/json"
	"net/http"
	"strconv"

	"github.com/iotready/podman-api/internal/ingress"
	"github.com/iotready/podman-api/internal/instance"
)

func (h *handlers) listInstances(w http.ResponseWriter, r *http.Request) {
	host := r.PathValue("host")
	template := r.URL.Query().Get("template")
	if template == "" {
		// No template filter: list every podman-api-managed pod on the host
		// across all loaded templates. Useful for the CMS to enumerate a
		// host's tenants without N round-trips.
		out, err := h.svc.ListAllInstances(r.Context(), host)
		if err != nil {
			WriteError(w, err)
			return
		}
		WriteJSON(w, http.StatusOK, out)
		return
	}
	if !validName(template) {
		writeInvalidName(w, "template", template)
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
	if !validInstancePath(w, tmpl, slug) {
		return
	}
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
	if !validInstancePath(w, req.Template, req.Slug) {
		return
	}
	if err := ingress.ValidateDomains(req.Domains); err != nil {
		WriteJSON(w, http.StatusBadRequest, ErrorBody{Code: "invalid_domains", Message: err.Error()})
		return
	}
	opts := instance.ApplyOptions{Replace: false, SkipPull: queryBool(r, "skip_pull")}
	if err := h.svc.Apply(r.Context(), host, req, opts); err != nil {
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
	pathTmpl := r.PathValue("template")
	pathSlug := r.PathValue("slug")
	if !validInstancePath(w, pathTmpl, pathSlug) {
		return
	}
	req, err := decodeApply(r)
	if err != nil {
		WriteJSON(w, http.StatusBadRequest, ErrorBody{Code: "invalid_body", Message: err.Error()})
		return
	}
	if req.Template != "" && req.Template != pathTmpl {
		WriteJSON(w, http.StatusBadRequest, ErrorBody{Code: "invalid_body", Message: "template in URL does not match body"})
		return
	}
	if req.Slug != "" && req.Slug != pathSlug {
		WriteJSON(w, http.StatusBadRequest, ErrorBody{Code: "invalid_body", Message: "slug in URL does not match body"})
		return
	}
	req.Template = pathTmpl
	req.Slug = pathSlug

	if err := ingress.ValidateDomains(req.Domains); err != nil {
		WriteJSON(w, http.StatusBadRequest, ErrorBody{Code: "invalid_domains", Message: err.Error()})
		return
	}
	opts := instance.ApplyOptions{Replace: true, SkipPull: queryBool(r, "skip_pull")}
	if err := h.svc.Apply(r.Context(), host, req, opts); err != nil {
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
	if !validInstancePath(w, tmpl, slug) {
		return
	}
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

func (h *handlers) startInstance(w http.ResponseWriter, r *http.Request) {
	tmpl, slug := r.PathValue("template"), r.PathValue("slug")
	if !validInstancePath(w, tmpl, slug) {
		return
	}
	if err := h.svc.Start(r.Context(), r.PathValue("host"), tmpl, slug); err != nil {
		WriteError(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (h *handlers) stopInstance(w http.ResponseWriter, r *http.Request) {
	tmpl, slug := r.PathValue("template"), r.PathValue("slug")
	if !validInstancePath(w, tmpl, slug) {
		return
	}
	if err := h.svc.Stop(r.Context(), r.PathValue("host"), tmpl, slug); err != nil {
		WriteError(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (h *handlers) restartInstance(w http.ResponseWriter, r *http.Request) {
	tmpl, slug := r.PathValue("template"), r.PathValue("slug")
	if !validInstancePath(w, tmpl, slug) {
		return
	}
	if err := h.svc.Restart(r.Context(), r.PathValue("host"), tmpl, slug); err != nil {
		WriteError(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (h *handlers) instanceVolumes(w http.ResponseWriter, r *http.Request) {
	host := r.PathValue("host")
	tmpl := r.PathValue("template")
	slug := r.PathValue("slug")
	if !validInstancePath(w, tmpl, slug) {
		return
	}
	vols, err := h.svc.InstanceVolumes(r.Context(), host, tmpl, slug)
	if err != nil {
		WriteError(w, err)
		return
	}
	out := make([]map[string]any, 0, len(vols))
	for _, v := range vols {
		out = append(out, map[string]any{"name": v.Name, "size_bytes": v.SizeBytes})
	}
	WriteJSON(w, http.StatusOK, out)
}

func (h *handlers) upgradeInstance(w http.ResponseWriter, r *http.Request) {
	host := r.PathValue("host")
	tmpl := r.PathValue("template")
	slug := r.PathValue("slug")
	if !validInstancePath(w, tmpl, slug) {
		return
	}
	var body struct {
		Image      string            `json:"image"`
		Parameters map[string]any    `json:"parameters"`
		Secrets    map[string]string `json:"secrets"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		WriteJSON(w, http.StatusBadRequest, ErrorBody{Code: "invalid_body", Message: err.Error()})
		return
	}
	req := instance.ApplyRequest{
		Template:   tmpl,
		Slug:       slug,
		Parameters: body.Parameters,
		Secrets:    body.Secrets,
	}
	if err := h.svc.Upgrade(r.Context(), host, req, body.Image); err != nil {
		WriteError(w, err)
		return
	}
	obs, err := h.svc.Get(r.Context(), host, tmpl, slug)
	if err != nil {
		WriteError(w, err)
		return
	}
	WriteJSON(w, http.StatusOK, obs)
}

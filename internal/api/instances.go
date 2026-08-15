package api

import (
	"net/http"
	"strconv"
	"strings"

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
	req, ok := decodeApplyRequest(w, r)
	if !ok {
		return
	}
	if !validInstancePath(w, req.Template, req.Slug) {
		return
	}
	if !validSlugParameter(w, req.Parameters, req.Slug) {
		return
	}
	if err := ingress.ValidateDomains(req.Domains); err != nil {
		WriteJSON(w, http.StatusBadRequest, ErrorBody{Code: "invalid_domains", Message: err.Error()})
		return
	}
	opts := instance.ApplyOptions{Replace: false, SkipPull: queryBool(r, "skip_pull")}
	obs, err := h.svc.ApplyAndObserve(r.Context(), host, req, opts)
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
	req, ok := decodeApplyRequest(w, r)
	if !ok {
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
	// An absent/empty PUT body decodes to a zero-value ApplyRequest
	// (Parameters, Secrets, and Domains all nil). applyInstance always runs
	// with Replace:true, so — unlike createInstance, where a genuinely empty
	// Template/Slug is already rejected by validInstancePath above —
	// forwarding that zero value here would silently discard every
	// previously-set optional parameter, every configured ingress domain,
	// and any per-instance secret, then re-render/restart the pod from
	// template defaults alone. render.Validate only catches this when the
	// template happens to declare a required parameter or per-instance
	// secret; it says nothing about domains and nothing about an
	// all-optional template. There is no sane "no body" interpretation for a
	// PUT against an existing instance, so reject it explicitly here rather
	// than relying on validation happening to catch it downstream (#254
	// review).
	if len(req.Parameters) == 0 && len(req.Secrets) == 0 && len(req.Domains) == 0 {
		WriteJSON(w, http.StatusBadRequest, ErrorBody{Code: "invalid_body", Message: "body is required: at least one of parameters, secrets, or domains must be set"})
		return
	}
	if !validSlugParameter(w, req.Parameters, req.Slug) {
		return
	}

	if err := ingress.ValidateDomains(req.Domains); err != nil {
		WriteJSON(w, http.StatusBadRequest, ErrorBody{Code: "invalid_domains", Message: err.Error()})
		return
	}
	opts := instance.ApplyOptions{Replace: true, SkipPull: queryBool(r, "skip_pull")}
	obs, err := h.svc.ApplyAndObserve(r.Context(), host, req, opts)
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

// decodeApplyRequest decodes an ApplyRequest with DisallowUnknownFields,
// writing a 400 and returning false on failure.
func decodeApplyRequest(w http.ResponseWriter, r *http.Request) (instance.ApplyRequest, bool) {
	var req instance.ApplyRequest
	ok := decodeBody(w, r, &req)
	return req, ok
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
	obs, err := h.svc.Start(r.Context(), r.PathValue("host"), tmpl, slug)
	if err != nil {
		WriteError(w, err)
		return
	}
	WriteJSON(w, http.StatusOK, obs)
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
	if !decodeBody(w, r, &body) {
		return
	}
	// decodeBody rejects an entirely absent body itself, but a present-but-
	// empty body (`{}`) still decodes to Image=="", which Service.Upgrade
	// rejects with a plain error that classify() has no sentinel for — it
	// would fall through to a 500 "internal" instead of a 400. Check here so
	// a missing image always stays a 400 (#254 review finding 2).
	if strings.TrimSpace(body.Image) == "" {
		WriteJSON(w, http.StatusBadRequest, ErrorBody{Code: "invalid_body", Message: "image is required"})
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

// upgradeImageInstance is the bearer-auth mirror of the admin UI's image-only
// upgrade: it reuses the instance's stored spec (parameters + sealed
// per-instance secrets) and overrides only the image, applying with
// Replace+AllowMissingSecrets via Service.UpgradeImage. Unlike upgradeInstance
// (POST .../upgrade) it requires no secrets in the request, so a scripted
// rollout can roll every instance to a new image with a plain bearer key — the
// sealed secrets are unrecoverable plaintext and never need to be re-supplied.
func (h *handlers) upgradeImageInstance(w http.ResponseWriter, r *http.Request) {
	host := r.PathValue("host")
	tmpl := r.PathValue("template")
	slug := r.PathValue("slug")
	if !validInstancePath(w, tmpl, slug) {
		return
	}
	var body struct {
		Image string `json:"image"`
	}
	if !decodeBody(w, r, &body) {
		return
	}
	if strings.TrimSpace(body.Image) == "" {
		WriteJSON(w, http.StatusBadRequest, ErrorBody{Code: "invalid_body", Message: "image is required"})
		return
	}
	if err := h.svc.UpgradeImage(r.Context(), host, tmpl, slug, body.Image); err != nil {
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

// patchInstanceParameters overlays the request's parameters onto the instance's
// stored spec and re-applies. Unlike PUT it requires no secrets: the sealed
// per-instance secrets are reused from the store, which is what makes a
// parameter change possible at all on an instance whose secret plaintext the
// operator can no longer read (pro#74). Omitted parameter names keep their
// stored value; a parameter cannot be deleted through this route. A "slug" key
// is rejected outright — slug drives metadata.name/secretKeyRef/claimName in
// the rendered manifest, so silently accepting a change would replay it against
// a different pod than the one this route's path (and lock) name; use POST
// .../rename instead. Because this route replaces the pod, a rejected manifest
// can leave the instance down until a boot converge or manual re-apply — see
// Service.UpdateInstanceParameters.
func (h *handlers) patchInstanceParameters(w http.ResponseWriter, r *http.Request) {
	host := r.PathValue("host")
	tmpl := r.PathValue("template")
	slug := r.PathValue("slug")
	if !validInstancePath(w, tmpl, slug) {
		return
	}
	var body struct {
		Parameters map[string]any `json:"parameters"`
	}
	if !decodeBody(w, r, &body) {
		return
	}
	if len(body.Parameters) == 0 {
		WriteJSON(w, http.StatusBadRequest, ErrorBody{Code: "invalid_body", Message: "parameters is required and must not be empty"})
		return
	}
	if _, ok := body.Parameters["slug"]; ok {
		WriteJSON(w, http.StatusBadRequest, ErrorBody{Code: "invalid_body", Message: "slug cannot be changed through this route; use POST .../rename"})
		return
	}
	if err := h.svc.UpdateInstanceParameters(r.Context(), host, tmpl, slug, body.Parameters); err != nil {
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

// patchInstanceSecrets merges the request's secrets into the instance's stored
// sealed per-instance secrets and re-applies. Names absent from the request keep
// their stored value, so an operator can add a secret the template gained after
// deploy without resupplying plaintext they can no longer read — PUT demands the
// full declared set, and no route ever hands a sealed value back, which before
// this route made such an addition impossible (#207). Values are request-only:
// the 200 body is the usual Observed and carries no plaintext. A blank value is
// rejected — it would wipe an unrecoverable secret, and since this route cannot
// delete one (use PUT with the full spec for that) a blank can only be a
// mistake. Like .../parameters this replaces the pod, so a manifest podman
// refuses can leave the instance down until a boot converge or manual re-apply
// — see Service.RotateInstanceSecrets.
func (h *handlers) patchInstanceSecrets(w http.ResponseWriter, r *http.Request) {
	host := r.PathValue("host")
	tmpl := r.PathValue("template")
	slug := r.PathValue("slug")
	if !validInstancePath(w, tmpl, slug) {
		return
	}
	var body struct {
		Secrets map[string]string `json:"secrets"`
	}
	if !decodeBody(w, r, &body) {
		return
	}
	if len(body.Secrets) == 0 {
		WriteJSON(w, http.StatusBadRequest, ErrorBody{Code: "invalid_body", Message: "secrets is required and must not be empty"})
		return
	}
	for name, value := range body.Secrets {
		if value == "" {
			// The name is safe to echo (it is template-declared metadata, and
			// the manage-secrets UI already lists it); the value never is.
			WriteJSON(w, http.StatusBadRequest, ErrorBody{
				Code:    "invalid_body",
				Message: "secret " + strconv.Quote(name) + " has an empty value; this route cannot delete a secret",
			})
			return
		}
	}
	if err := h.svc.RotateInstanceSecrets(r.Context(), host, tmpl, slug, body.Secrets); err != nil {
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

func (h *handlers) renameInstance(w http.ResponseWriter, r *http.Request) {
	host := r.PathValue("host")
	tmpl := r.PathValue("template")
	slug := r.PathValue("slug")
	if !validInstancePath(w, tmpl, slug) {
		return
	}
	var req instance.RenameRequest
	if !decodeBody(w, r, &req) {
		return
	}
	if req.NewSlug == "" {
		WriteJSON(w, http.StatusBadRequest, ErrorBody{Code: "invalid_body", Message: "new_slug is required"})
		return
	}
	if !validName(req.NewSlug) {
		writeInvalidName(w, "new_slug", req.NewSlug)
		return
	}
	if err := h.svc.CheckRenameable(r.Context(), host, tmpl, slug, req); err != nil {
		WriteError(w, err)
		return
	}
	if err := h.svc.Rename(r.Context(), host, tmpl, slug, req, nil); err != nil {
		WriteError(w, err)
		return
	}
	obs, err := h.svc.Get(r.Context(), host, tmpl, req.NewSlug)
	if err != nil {
		WriteError(w, err)
		return
	}
	WriteJSON(w, http.StatusOK, obs)
}

package ui

import (
	"context"
	"fmt"
	"net/http"
	"slices"
	"strings"

	"github.com/iotready/podman-api/internal/instance"
	"github.com/iotready/podman-api/internal/store"
)

// hostSecretRef is a per-host-referenced secret name plus whether it currently
// exists on the host (display-only; not a form input).
type hostSecretRef struct {
	Name    string
	Present bool
}

// hostExists reports whether host is a configured host.
func (u *UI) hostExists(host string) bool {
	for _, h := range u.cfg.Svc.Hosts() {
		if h.ID == host {
			return true
		}
	}
	return false
}

// sortedTemplates returns the templates ordered by id, so the deploy form's
// <select> is stable across renders (Service.Templates() iterates a map). A
// store error is propagated so the handler can surface it rather than render an
// empty catalog.
func (u *UI) sortedTemplates(ctx context.Context) ([]store.Template, error) {
	tmpls, err := u.cfg.Svc.Templates(ctx)
	if err != nil {
		return nil, err
	}
	slices.SortFunc(tmpls, func(a, b store.Template) int {
		return strings.Compare(a.Meta.ID, b.Meta.ID)
	})
	return tmpls, nil
}

// hostSecretRefs computes the present/absent status of each per-host-referenced
// secret the template declares, checked against the secrets actually on the
// host. A HostSecrets error is treated as "none present" (every ref absent),
// matching the form's display-only intent. The caller supplies the already
// resolved template, so this issues no template query of its own.
func (u *UI) hostSecretRefs(r *http.Request, host string, tmpl store.Template) []hostSecretRef {
	if len(tmpl.Meta.Secrets.PerHostReferenced) == 0 {
		return nil
	}
	present := map[string]bool{}
	if secs, err := u.cfg.Svc.HostSecrets(r.Context(), host); err == nil {
		for _, s := range secs {
			present[s.Name] = true
		}
	}
	var refs []hostSecretRef
	for _, name := range tmpl.Meta.Secrets.PerHostReferenced {
		refs = append(refs, hostSecretRef{Name: name, Present: present[name]})
	}
	return refs
}

// formValues collects param.* and secret.* fields, skipping empties so a blank
// required field surfaces as a validation error rather than an empty value.
func formValues(form map[string][]string) (params map[string]any, secrets map[string]string) {
	params = map[string]any{}
	secrets = map[string]string{}
	for k, vs := range form {
		v := vs[0]
		if v == "" {
			continue
		}
		switch {
		case strings.HasPrefix(k, "param."):
			params[strings.TrimPrefix(k, "param.")] = v
		case strings.HasPrefix(k, "secret."):
			secrets[strings.TrimPrefix(k, "secret.")] = v
		}
	}
	return params, secrets
}

// typedValues collects param.* and secret.* fields verbatim, keyed by full field
// name (e.g. "param.db", "secret.password"), for re-populating the deploy form so
// a template switch or a failed deploy does not discard what the operator typed.
// Unlike formValues (the apply path), it does NOT skip empty values: a key the
// operator submitted empty (a deliberately cleared field) is preserved as empty
// and must not be back-filled with the parameter default (see mergeParamDefaults).
func typedValues(form map[string][]string) map[string]string {
	vals := map[string]string{}
	for k, vs := range form {
		if strings.HasPrefix(k, "param.") || strings.HasPrefix(k, "secret.") {
			vals[k] = vs[0]
		}
	}
	return vals
}

// mergeParamDefaults fills each parameter's one-click default into values, but
// only for keys the request did not submit at all. This resolves the
// typed-value-vs-default precedence server-side: the template can't tell a
// missing key from a typed-empty one (index returns "" for both), so doing it
// here lets a fresh form show defaults while a field the operator cleared
// (submitted empty) stays empty rather than silently reverting to the default.
//
// This is display-only. The apply path treats empty and absent the same: a
// cleared defaulted field is dropped by formValues, then back-filled by
// render.ApplyDefaults in Service.Apply, so deploying it still applies the
// default — the parameter model has no way to express "explicitly empty". The
// form communicates that by advertising the default as the input's placeholder
// (see instance-fields.html). NB: this is the UI's copy of the same
// fill-absent-from-defaults rule implemented in render.ApplyDefaults and
// api/templates.go; keep them consistent.
func mergeParamDefaults(values map[string]string, tmpl store.Template) {
	for _, p := range tmpl.Meta.Parameters {
		if p.Default == nil {
			continue
		}
		key := "param." + p.Name
		if _, ok := values[key]; !ok {
			values[key] = fmt.Sprint(p.Default)
		}
	}
}

// paramPlaceholders builds the placeholder text for each parameter, keyed by
// param name. An explicit ParamDef.Placeholder wins; otherwise a parameter with
// a default advertises it as "default: <value>", communicating that deploying
// the field empty applies that default (see mergeParamDefaults). The default
// hint is gated on the SAME Default != nil check as mergeParamDefaults — not
// template truthiness — so a falsy non-nil default (false, 0) still gets a hint
// (#100). Computing this server-side keeps the value-fill and placeholder halves
// of the policy in one place with one nil-check semantics.
func paramPlaceholders(tmpl store.Template) map[string]string {
	ph := map[string]string{}
	for _, p := range tmpl.Meta.Parameters {
		switch {
		case p.Placeholder != "":
			ph[p.Name] = p.Placeholder
		case p.Default != nil:
			ph[p.Name] = fmt.Sprintf("default: %v", p.Default)
		}
	}
	return ph
}

// deployFormData assembles the data map the "deploy-form" template needs. vals
// are the raw typed param.*/secret.* values to re-populate; mergeParamDefaults
// is applied here so every caller shares one defaults policy. A store error
// from the template list is returned for the caller to surface via renderError.
// The caller adds an "Error" key when re-rendering after a failed deploy.
//
// defaultFirst selects the first template when selected is empty — wanted on
// the initial GET load (so a fresh form shows a real template) but NOT on the
// POST switch or the failed-deploy re-render, where an empty selection is the
// operator's actual input and must not be silently replaced with the first
// template (and its merged defaults). The template is resolved from the list
// already fetched here, so no second template query is issued.
func (u *UI) deployFormData(r *http.Request, host, selected, slug string, vals map[string]string, defaultFirst bool) (map[string]any, error) {
	tmpls, err := u.sortedTemplates(r.Context())
	if err != nil {
		return nil, err
	}
	if defaultFirst && selected == "" && len(tmpls) > 0 {
		selected = tmpls[0].Meta.ID
	}
	var tmpl store.Template
	for _, t := range tmpls {
		if t.Meta.ID == selected {
			tmpl = t
			break
		}
	}
	refs := u.hostSecretRefs(r, host, tmpl)
	mergeParamDefaults(vals, tmpl)
	return map[string]any{
		"Host":         host,
		"Templates":    tmpls,
		"Selected":     selected,
		"Tmpl":         tmpl,
		"HostRefs":     refs,
		"Slug":         slug,
		"Values":       vals,
		"Placeholders": paramPlaceholders(tmpl),
	}, nil
}

func (u *UI) deployForm(w http.ResponseWriter, r *http.Request) {
	host := r.PathValue("host")
	if !u.hostExists(host) {
		u.renderError(w, r, instance.ErrUnknownHost)
		return
	}
	q := r.URL.Query()
	data, err := u.deployFormData(r, host, q.Get("template"), q.Get("slug"), typedValues(q), true)
	if err != nil {
		u.renderError(w, r, err)
		return
	}
	u.render(w, r, http.StatusOK, "deploy-form", u.pageData(data))
}

// deployFormPost re-renders the deploy form for a newly selected template. The
// template <select> POSTs here (rather than GETs the deploy route) so typed
// per-instance secrets travel in the request body, not the URL (#99). It mirrors
// deployForm but reads the selected template, slug, and typed values from the
// POST body.
func (u *UI) deployFormPost(w http.ResponseWriter, r *http.Request) {
	host := r.PathValue("host")
	if !u.hostExists(host) {
		u.renderError(w, r, instance.ErrUnknownHost)
		return
	}
	if err := r.ParseForm(); err != nil {
		http.Error(w, "bad form", http.StatusBadRequest)
		return
	}
	data, err := u.deployFormData(r, host, r.PostFormValue("template"), r.PostFormValue("slug"), typedValues(r.PostForm), false)
	if err != nil {
		u.renderError(w, r, err)
		return
	}
	u.render(w, r, http.StatusOK, "deploy-form", u.pageData(data))
}

func (u *UI) deployCreate(w http.ResponseWriter, r *http.Request) {
	host := r.PathValue("host")
	if err := r.ParseForm(); err != nil {
		http.Error(w, "bad form", http.StatusBadRequest)
		return
	}
	params, secrets := formValues(r.PostForm)
	req := instance.ApplyRequest{
		Template:   r.FormValue("template"),
		Slug:       r.FormValue("slug"),
		Parameters: params,
		Secrets:    secrets,
	}
	if applyErr := u.cfg.Svc.Apply(r.Context(), host, req, instance.ApplyOptions{Replace: false}); applyErr != nil {
		data, derr := u.deployFormData(r, host, req.Template, req.Slug, typedValues(r.PostForm), false)
		if derr != nil {
			u.renderError(w, r, derr)
			return
		}
		data["Error"] = applyErr.Error()
		u.render(w, r, errorStatus(applyErr), "deploy-form", u.pageData(data))
		return
	}
	obs, err := u.cfg.Svc.Get(r.Context(), host, req.Template, req.Slug)
	if err != nil {
		u.renderError(w, r, err)
		return
	}
	u.render(w, r, http.StatusOK, "instance-detail", u.pageData(u.instanceView(host, obs)))
}

// upgradeForm renders the image-only upgrade form. The upgrade reuses the
// instance's stored parameters and secrets (the operator supplies only a new
// image). The desired-state store is always present, so upgrade is always
// available.
func (u *UI) upgradeForm(w http.ResponseWriter, r *http.Request) {
	host, tmplID, slug := r.PathValue("host"), r.PathValue("template"), r.PathValue("slug")
	obs, err := u.cfg.Svc.Get(r.Context(), host, tmplID, slug)
	if err != nil {
		u.renderError(w, r, err)
		return
	}
	u.render(w, r, http.StatusOK, "upgrade-form", u.pageData(map[string]any{
		"Host":         host,
		"Template":     tmplID,
		"Slug":         slug,
		"CurrentImage": firstContainerImage(obs),
	}))
}

func (u *UI) upgradeApply(w http.ResponseWriter, r *http.Request) {
	host, tmplID, slug := r.PathValue("host"), r.PathValue("template"), r.PathValue("slug")
	image := strings.TrimSpace(r.FormValue("image"))
	if image == "" {
		obs, _ := u.cfg.Svc.Get(r.Context(), host, tmplID, slug)
		u.render(w, r, http.StatusBadRequest, "upgrade-form", u.pageData(map[string]any{
			"Host": host, "Template": tmplID, "Slug": slug,
			"CurrentImage": firstContainerImage(obs),
			"Error":        "image is required",
		}))
		return
	}
	if err := u.cfg.Svc.UpgradeImage(r.Context(), host, tmplID, slug, image); err != nil {
		obs, _ := u.cfg.Svc.Get(r.Context(), host, tmplID, slug)
		u.render(w, r, errorStatus(err), "upgrade-form", u.pageData(map[string]any{
			"Host": host, "Template": tmplID, "Slug": slug,
			"CurrentImage": firstContainerImage(obs),
			"Error":        err.Error(),
		}))
		return
	}
	obs, err := u.cfg.Svc.Get(r.Context(), host, tmplID, slug)
	if err != nil {
		u.renderError(w, r, err)
		return
	}
	u.render(w, r, http.StatusOK, "instance-detail", u.pageData(u.instanceView(host, obs)))
}

// firstContainerImage returns the first container's image, for prefilling the
// upgrade form; "" when the instance has no observed containers.
func firstContainerImage(obs instance.Observed) string {
	if len(obs.Containers) > 0 {
		return obs.Containers[0].Image
	}
	return ""
}

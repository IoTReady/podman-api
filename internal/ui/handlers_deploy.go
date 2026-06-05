package ui

import (
	"net/http"
	"strings"

	"github.com/iotready/podman-api/internal/config"
	"github.com/iotready/podman-api/internal/instance"
)

// hostSecretRef is a per-host-referenced secret name plus whether it currently
// exists on the host (display-only; not a form input).
type hostSecretRef struct {
	Name    string
	Present bool
}

// fieldData resolves the template by id and computes the present/absent status
// of each per-host-referenced secret on the host.
func (u *UI) fieldData(r *http.Request, host, tmplID string) (config.Template, []hostSecretRef) {
	var tmpl config.Template
	for _, t := range u.cfg.Svc.Templates() {
		if t.Meta.ID == tmplID {
			tmpl = t
			break
		}
	}
	var refs []hostSecretRef
	if len(tmpl.Meta.Secrets.PerHostReferenced) > 0 {
		present := map[string]bool{}
		if secs, err := u.cfg.Svc.HostSecrets(r.Context(), host); err == nil {
			for _, s := range secs {
				present[s.Name] = true
			}
		}
		for _, name := range tmpl.Meta.Secrets.PerHostReferenced {
			refs = append(refs, hostSecretRef{Name: name, Present: present[name]})
		}
	}
	return tmpl, refs
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

func (u *UI) deployForm(w http.ResponseWriter, r *http.Request) {
	host := r.PathValue("host")
	tmpls := u.cfg.Svc.Templates()
	sel := r.URL.Query().Get("template")
	if sel == "" && len(tmpls) > 0 {
		sel = tmpls[0].Meta.ID
	}
	tmpl, refs := u.fieldData(r, host, sel)
	u.render(w, r, http.StatusOK, "deploy-form", u.pageData(map[string]any{
		"Host":      host,
		"Templates": tmpls,
		"Selected":  sel,
		"Tmpl":      tmpl,
		"HostRefs":  refs,
		"Slug":      r.URL.Query().Get("slug"),
	}))
}

func (u *UI) deployCreate(w http.ResponseWriter, r *http.Request) {
	host := r.PathValue("host")
	if err := r.ParseForm(); err != nil {
		http.Error(w, "bad form", http.StatusBadRequest)
		return
	}
	params, secrets := formValues(r.Form)
	req := instance.ApplyRequest{
		Template:   r.FormValue("template"),
		Slug:       r.FormValue("slug"),
		Parameters: params,
		Secrets:    secrets,
	}
	if err := u.cfg.Svc.Apply(r.Context(), host, req, instance.ApplyOptions{Replace: false}); err != nil {
		tmpl, refs := u.fieldData(r, host, req.Template)
		u.render(w, r, errorStatus(err), "deploy-form", u.pageData(map[string]any{
			"Host":      host,
			"Templates": u.cfg.Svc.Templates(),
			"Selected":  req.Template,
			"Tmpl":      tmpl,
			"HostRefs":  refs,
			"Slug":      req.Slug,
			"Error":     err.Error(),
		}))
		return
	}
	obs, err := u.cfg.Svc.Get(r.Context(), host, req.Template, req.Slug)
	if err != nil {
		u.renderError(w, r, err)
		return
	}
	u.render(w, r, http.StatusOK, "instance-detail", u.pageData(map[string]any{"Host": host, "Inst": obs}))
}

func (u *UI) upgradeForm(w http.ResponseWriter, r *http.Request) {
	host, tmplID, slug := r.PathValue("host"), r.PathValue("template"), r.PathValue("slug")
	tmpl, refs := u.fieldData(r, host, tmplID)
	if tmpl.Meta.ID == "" {
		u.renderError(w, r, instance.ErrUnknownTemplate)
		return
	}
	u.render(w, r, http.StatusOK, "upgrade-form", u.pageData(map[string]any{
		"Host":     host,
		"Template": tmplID,
		"Slug":     slug,
		"Tmpl":     tmpl,
		"HostRefs": refs,
	}))
}

func (u *UI) upgradeApply(w http.ResponseWriter, r *http.Request) {
	host, tmplID, slug := r.PathValue("host"), r.PathValue("template"), r.PathValue("slug")
	if err := r.ParseForm(); err != nil {
		http.Error(w, "bad form", http.StatusBadRequest)
		return
	}
	params, secrets := formValues(r.Form)
	req := instance.ApplyRequest{Template: tmplID, Slug: slug, Parameters: params, Secrets: secrets}
	if err := u.cfg.Svc.Upgrade(r.Context(), host, req, r.FormValue("image")); err != nil {
		tmpl, refs := u.fieldData(r, host, tmplID)
		u.render(w, r, errorStatus(err), "upgrade-form", u.pageData(map[string]any{
			"Host":     host,
			"Template": tmplID,
			"Slug":     slug,
			"Tmpl":     tmpl,
			"HostRefs": refs,
			"Error":    err.Error(),
		}))
		return
	}
	obs, err := u.cfg.Svc.Get(r.Context(), host, tmplID, slug)
	if err != nil {
		u.renderError(w, r, err)
		return
	}
	u.render(w, r, http.StatusOK, "instance-detail", u.pageData(map[string]any{"Host": host, "Inst": obs}))
}

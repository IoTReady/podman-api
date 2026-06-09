package ui

import (
	"context"
	"errors"
	"net/http"

	"github.com/iotready/podman-api/internal/instance"
	"github.com/iotready/podman-api/internal/store"
)

// secretsFormData builds the manage-secrets render data: the template's declared
// per-instance secret names and, per name, whether a value is currently stored
// (presence only — values are never read back). On a corrupt/undecryptable spec
// it sets Corrupt and omits Set so the form degrades to a cleanup notice.
func (u *UI) secretsFormData(ctx context.Context, host, tmpl, slug string) (map[string]any, error) {
	data := map[string]any{
		"Host":       host,
		"ActiveHost": host,
		"Template":   tmpl,
		"Slug":       slug,
		"Names":      u.templatePerInstanceSecrets(ctx, tmpl),
	}
	set, err := u.cfg.Svc.InstanceSecretState(ctx, host, tmpl, slug)
	if err != nil {
		if errors.Is(err, store.ErrSpecCorrupt) || errors.Is(err, store.ErrSecretsUndecryptable) {
			data["Corrupt"] = true
			return data, nil
		}
		return nil, err
	}
	data["Set"] = set
	return data, nil
}

// secretsForm (GET) renders the manage-secrets page for one instance.
func (u *UI) secretsForm(w http.ResponseWriter, r *http.Request) {
	host, tmpl, slug := r.PathValue("host"), r.PathValue("template"), r.PathValue("slug")
	if !u.hostExists(host) {
		u.renderError(w, r, instance.ErrUnknownHost)
		return
	}
	data, err := u.secretsFormData(r.Context(), host, tmpl, slug)
	if err != nil {
		u.renderError(w, r, err)
		return
	}
	u.render(w, r, http.StatusOK, "secrets-form", u.pageData(data))
}

// secretsRotate (POST) rotates the submitted per-instance secrets and re-applies
// the instance. Secrets are read from the request body only — never the URL
// (#99). A blank submit re-renders the form at 400 rather than restarting the
// instance for no change.
func (u *UI) secretsRotate(w http.ResponseWriter, r *http.Request) {
	host, tmpl, slug := r.PathValue("host"), r.PathValue("template"), r.PathValue("slug")
	if !u.hostExists(host) {
		u.renderError(w, r, instance.ErrUnknownHost)
		return
	}
	if err := r.ParseForm(); err != nil {
		http.Error(w, "bad form", http.StatusBadRequest)
		return
	}
	_, secrets := formValues(r.PostForm) // body-only; skips blanks
	if len(secrets) == 0 {
		data, derr := u.secretsFormData(r.Context(), host, tmpl, slug)
		if derr != nil {
			u.renderError(w, r, derr)
			return
		}
		data["Error"] = "enter a new value for at least one secret"
		u.render(w, r, http.StatusBadRequest, "secrets-form", u.pageData(data))
		return
	}
	if err := u.cfg.Svc.RotateInstanceSecrets(r.Context(), host, tmpl, slug, secrets); err != nil {
		data, derr := u.secretsFormData(r.Context(), host, tmpl, slug)
		if derr != nil {
			u.renderError(w, r, err) // can't even rebuild the form: surface the original error
			return
		}
		data["Error"] = err.Error()
		u.render(w, r, errorStatus(err), "secrets-form", u.pageData(data))
		return
	}
	obs, err := u.cfg.Svc.Get(r.Context(), host, tmpl, slug)
	if err != nil {
		u.renderError(w, r, err)
		return
	}
	data := u.instanceView(r.Context(), host, obs)
	data["Notice"] = "Secrets rotated; instance re-applied."
	u.render(w, r, http.StatusOK, "instance-detail", u.pageData(data))
}

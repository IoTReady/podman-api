package ui

import (
	"context"
	"net/http"
	"strings"

	"github.com/iotready/podman-api/internal/instance"
)

// instanceView builds the instance-detail render data. Upgrade is always
// available since the desired-state store is always present. HasSecrets gates
// the manage-secrets control on the template declaring any per-instance secrets.
func (u *UI) instanceView(ctx context.Context, host string, obs instance.Observed) map[string]any {
	backups, _ := u.cfg.Svc.ListBackups(ctx, host, obs.Template, obs.Slug, 20) // best-effort; nil on error
	return map[string]any{
		"Host":       host,
		"ActiveHost": host,
		"Inst":       obs,
		"CanUpgrade": true,
		"HasSecrets": len(u.templatePerInstanceSecrets(ctx, obs.Template)) > 0,
		"Backups":    backups,
	}
}

// templatePerInstanceSecrets returns the per-instance secret names the template
// declares, or nil when the template is unknown or can't be read (best-effort: a
// lookup failure degrades to "no secrets", never an error here). Uses a point
// lookup rather than listing the whole catalog, since this runs on every
// instance-detail render.
func (u *UI) templatePerInstanceSecrets(ctx context.Context, tmplID string) []string {
	tmpl, err := u.cfg.Svc.Template(ctx, tmplID)
	if err != nil {
		return nil
	}
	return tmpl.Meta.Secrets.PerInstance
}

func (u *UI) instanceDetail(w http.ResponseWriter, r *http.Request) {
	host, tmpl, slug := r.PathValue("host"), r.PathValue("template"), r.PathValue("slug")
	obs, err := u.cfg.Svc.Get(r.Context(), host, tmpl, slug)
	if err != nil {
		u.renderError(w, r, err)
		return
	}
	u.render(w, r, http.StatusOK, "instance-detail", u.pageData(u.instanceView(r.Context(), host, obs)))
}

// renameForm renders the rename form. The Get call acts as an existence check
// — it renders an error if the instance doesn't exist before showing the form.
func (u *UI) renameForm(w http.ResponseWriter, r *http.Request) {
	host, tmplID, slug := r.PathValue("host"), r.PathValue("template"), r.PathValue("slug")
	if _, err := u.cfg.Svc.Get(r.Context(), host, tmplID, slug); err != nil {
		u.renderError(w, r, err)
		return
	}
	domains := ""
	spec, err := u.cfg.Svc.StoredSpec(r.Context(), host, tmplID, slug)
	if err == nil {
		domains = strings.Join(spec.Domains, ", ")
	}
	u.render(w, r, http.StatusOK, "rename-form", u.pageData(map[string]any{
		"Host":       host,
		"ActiveHost": host,
		"Template":   tmplID,
		"Slug":       slug,
		"Domains":    domains,
	}))
}

// renameApply handles the rename form submission.
func (u *UI) renameApply(w http.ResponseWriter, r *http.Request) {
	host, tmplID, slug := r.PathValue("host"), r.PathValue("template"), r.PathValue("slug")
	newSlug := strings.TrimSpace(r.FormValue("new_slug"))
	domainsRaw := strings.TrimSpace(r.FormValue("domains"))
	keepOld := r.FormValue("keep_old_standby") == "true"

	if newSlug == "" {
		u.render(w, r, http.StatusBadRequest, "rename-form", u.pageData(map[string]any{
			"Host": host, "ActiveHost": host, "Template": tmplID, "Slug": slug,
			"Error": "new slug is required",
		}))
		return
	}

	var domains []string
	if domainsRaw != "" {
		for _, d := range strings.Split(domainsRaw, ",") {
			d = strings.TrimSpace(d)
			if d != "" {
				domains = append(domains, d)
			}
		}
	}

	req := instance.RenameRequest{
		NewSlug:        newSlug,
		Domains:        domains,
		KeepOldStandby: keepOld,
	}

	if err := u.cfg.Svc.CheckRenameable(r.Context(), host, tmplID, slug, req); err != nil {
		spec, _ := u.cfg.Svc.StoredSpec(r.Context(), host, tmplID, slug)
		domainsStr := ""
		if spec.Domains != nil {
			domainsStr = strings.Join(spec.Domains, ", ")
		}
		u.render(w, r, errorStatus(err), "rename-form", u.pageData(map[string]any{
			"Host": host, "ActiveHost": host, "Template": tmplID, "Slug": slug,
			"Domains": domainsStr, "Error": err.Error(),
		}))
		return
	}

	if err := u.cfg.Svc.Rename(r.Context(), host, tmplID, slug, req, nil); err != nil {
		spec, _ := u.cfg.Svc.StoredSpec(r.Context(), host, tmplID, slug)
		domainsStr := ""
		if spec.Domains != nil {
			domainsStr = strings.Join(spec.Domains, ", ")
		}
		u.render(w, r, errorStatus(err), "rename-form", u.pageData(map[string]any{
			"Host": host, "ActiveHost": host, "Template": tmplID, "Slug": slug,
			"Domains": domainsStr, "Error": err.Error(),
		}))
		return
	}

	// Render the new instance's detail page. The form's hx-target="#main"
	// and hx-push-url="true" on the form element will update the URL.
	obs, err := u.cfg.Svc.Get(r.Context(), host, tmplID, newSlug)
	if err != nil {
		u.renderError(w, r, err)
		return
	}
	u.render(w, r, http.StatusOK, "instance-detail", u.pageData(u.instanceView(r.Context(), host, obs)))
}

// lifecycle dispatches start/stop/restart/delete, then re-renders the instance
// detail (or the host instance list, after a delete). Upgrade is NOT handled
// here — it is a separate form flow.
func (u *UI) lifecycle(w http.ResponseWriter, r *http.Request) {
	host, tmpl, slug := r.PathValue("host"), r.PathValue("template"), r.PathValue("slug")
	action := r.PathValue("action")
	ctx := r.Context()

	var (
		err    error
		notice string
	)
	switch action {
	case "start":
		var obs instance.Observed
		obs, err = u.cfg.Svc.Start(ctx, host, tmpl, slug)
		if err == nil && len(obs.Warnings) > 0 {
			notice = strings.Join(obs.Warnings, "; ")
		}
	case "stop":
		err = u.cfg.Svc.Stop(ctx, host, tmpl, slug)
	case "restart":
		err = u.cfg.Svc.Restart(ctx, host, tmpl, slug)
	case "delete":
		err = u.cfg.Svc.Delete(ctx, host, tmpl, slug, instance.DeleteOptions{})
	default:
		http.Error(w, "unknown action", http.StatusBadRequest)
		return
	}
	if err != nil {
		obs, gerr := u.cfg.Svc.Get(ctx, host, tmpl, slug)
		if gerr != nil {
			u.renderError(w, r, err)
			return
		}
		data := u.instanceView(ctx, host, obs)
		data["ActionError"] = err.Error()
		u.render(w, r, errorStatus(err), "instance-detail", u.pageData(data))
		return
	}
	if action == "delete" {
		obs, lerr := u.cfg.Svc.ListAllInstances(ctx, host)
		if lerr != nil {
			u.renderError(w, r, lerr)
			return
		}
		u.render(w, r, http.StatusOK, "host-instances", u.pageData(map[string]any{"Host": host, "ActiveHost": host, "Instances": obs}))
		return
	}
	obs, gerr := u.cfg.Svc.Get(ctx, host, tmpl, slug)
	if gerr != nil {
		u.renderError(w, r, gerr)
		return
	}
	data := u.instanceView(ctx, host, obs)
	if notice != "" {
		data["Notice"] = notice
	}
	u.render(w, r, http.StatusOK, "instance-detail", u.pageData(data))
}

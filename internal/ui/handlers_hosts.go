package ui

import "net/http"

func (u *UI) dashboard(w http.ResponseWriter, r *http.Request) {
	u.render(w, r, http.StatusOK, "dashboard", map[string]any{
		"Hosts": u.cfg.Svc.Hosts(),
	})
}

func (u *UI) hostInstances(w http.ResponseWriter, r *http.Request) {
	host := r.PathValue("host")
	obs, err := u.cfg.Svc.ListAllInstances(r.Context(), host)
	if err != nil {
		u.renderError(w, err)
		return
	}
	u.render(w, r, http.StatusOK, "host-instances", map[string]any{
		"Host":      host,
		"Instances": obs,
		"Templates": u.cfg.Svc.Templates(),
	})
}

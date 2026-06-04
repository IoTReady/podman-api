package ui

import "net/http"

func (u *UI) dashboard(w http.ResponseWriter, r *http.Request) {
	u.render(w, r, http.StatusOK, "dashboard", u.pageData(nil))
}

func (u *UI) hostInstances(w http.ResponseWriter, r *http.Request) {
	host := r.PathValue("host")
	obs, err := u.cfg.Svc.ListAllInstances(r.Context(), host)
	if err != nil {
		u.renderError(w, r, err)
		return
	}
	u.render(w, r, http.StatusOK, "host-instances", u.pageData(map[string]any{
		"Host":      host,
		"Instances": obs,
	}))
}

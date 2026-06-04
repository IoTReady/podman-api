package ui

import "net/http"

func (u *UI) dashboard(w http.ResponseWriter, r *http.Request) {
	u.render(w, r, http.StatusOK, "dashboard", map[string]any{})
}

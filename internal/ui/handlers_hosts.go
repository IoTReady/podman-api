package ui

import "net/http"

func (u *UI) dashboard(w http.ResponseWriter, r *http.Request) {
	u.render(w, r, "dashboard", map[string]any{})
}

package ui

import "net/http"

func (u *UI) loginForm(w http.ResponseWriter, r *http.Request) {
	u.render(w, r, "login", map[string]any{})
}

func (u *UI) login(w http.ResponseWriter, r *http.Request) {
	id, err := u.cfg.Auth.Authenticate(r.FormValue("username"), r.FormValue("password"))
	if err != nil {
		w.WriteHeader(http.StatusUnauthorized)
		u.render(w, r, "login", map[string]any{"Error": ErrAuth.Error()})
		return
	}
	tok, err := u.cfg.Sessions.Create(id)
	if err != nil {
		http.Error(w, "session error", http.StatusInternalServerError)
		return
	}
	http.SetCookie(w, &http.Cookie{
		Name:     sessionCookie,
		Value:    tok,
		Path:     "/ui",
		HttpOnly: true,
		Secure:   u.cfg.Secure,
		SameSite: http.SameSiteLaxMode,
	})
	http.Redirect(w, r, "/ui", http.StatusSeeOther)
}

func (u *UI) logout(w http.ResponseWriter, r *http.Request) {
	if c, err := r.Cookie(sessionCookie); err == nil {
		u.cfg.Sessions.Delete(c.Value)
	}
	http.SetCookie(w, &http.Cookie{Name: sessionCookie, Value: "", Path: "/ui", MaxAge: -1})
	http.Redirect(w, r, "/ui/login", http.StatusSeeOther)
}

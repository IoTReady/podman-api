package ui

import "net/http"

func (u *UI) loginForm(w http.ResponseWriter, r *http.Request) {
	u.render(w, r, http.StatusOK, "login", map[string]any{})
}

func (u *UI) login(w http.ResponseWriter, r *http.Request) {
	id, err := u.cfg.Auth.Authenticate(r.FormValue("username"), r.FormValue("password"))
	if err != nil {
		u.render(w, r, http.StatusUnauthorized, "login", map[string]any{"Error": ErrAuth.Error()})
		return
	}
	tok, err := u.cfg.Sessions.Create(id)
	if err != nil {
		http.Error(w, "session error", http.StatusInternalServerError)
		return
	}
	http.SetCookie(w, u.sessionCookie(tok, 0))
	http.Redirect(w, r, "/ui", http.StatusSeeOther)
}

func (u *UI) logout(w http.ResponseWriter, r *http.Request) {
	if c, err := r.Cookie(sessionCookie); err == nil {
		u.cfg.Sessions.Delete(c.Value)
	}
	// Clearing cookie must mirror the set cookie's attributes (notably HttpOnly)
	// or some browsers refuse to overwrite it.
	http.SetCookie(w, u.sessionCookie("", -1))
	http.Redirect(w, r, "/ui/login", http.StatusSeeOther)
}

// sessionCookie builds the session cookie with consistent security attributes.
// maxAge 0 leaves it a browser-session cookie (set on login); maxAge -1 expires
// it immediately (logout).
func (u *UI) sessionCookie(value string, maxAge int) *http.Cookie {
	return &http.Cookie{
		Name:     sessionCookie,
		Value:    value,
		Path:     "/ui",
		MaxAge:   maxAge,
		HttpOnly: true,
		Secure:   u.cfg.Secure,
		SameSite: http.SameSiteLaxMode,
	}
}

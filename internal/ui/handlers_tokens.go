package ui

import (
	"errors"
	"log"
	"net/http"

	"github.com/iotready/podman-api/internal/auth"
)

func (u *UI) tokensList(w http.ResponseWriter, r *http.Request) {
	keys, err := u.cfg.TokenMgr.List()
	if err != nil {
		u.renderError(w, r, err)
		return
	}
	u.render(w, r, http.StatusOK, "tokens", u.pageData(map[string]any{
		"ActivePage": "tokens",
		"Keys":       keys,
		"AllScopes":  auth.AllScopes,
	}))
}

func (u *UI) tokensCreate(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseForm(); err != nil {
		http.Error(w, "bad form", http.StatusBadRequest)
		return
	}
	id := r.FormValue("id")
	description := r.FormValue("description")
	scopes := r.Form["scopes"]

	if id == "" {
		keys, _ := u.cfg.TokenMgr.List()
		u.render(w, r, http.StatusBadRequest, "tokens", u.pageData(map[string]any{
			"ActivePage": "tokens",
			"Keys":       keys,
			"AllScopes":  auth.AllScopes,
			"Error":      "id is required",
		}))
		return
	}

	plain, err := u.cfg.TokenMgr.Create(id, description, scopes)
	if err != nil {
		keys, _ := u.cfg.TokenMgr.List()
		msg := "failed to create token"
		switch {
		case errors.Is(err, auth.ErrTokenIDExists):
			msg = "a token with that id already exists"
		case errors.Is(err, auth.ErrTokenScopesRequired):
			msg = "select at least one scope"
		default:
			log.Printf("token create %q: %v", id, err)
		}
		u.render(w, r, http.StatusBadRequest, "tokens", u.pageData(map[string]any{
			"ActivePage": "tokens",
			"Keys":       keys,
			"AllScopes":  auth.AllScopes,
			"Error":      msg,
		}))
		return
	}

	keys, _ := u.cfg.TokenMgr.List()
	u.render(w, r, http.StatusOK, "tokens", u.pageData(map[string]any{
		"ActivePage":    "tokens",
		"Keys":          keys,
		"AllScopes":     auth.AllScopes,
		"CreatedID":     id,
		"CreatedSecret": plain,
	}))
}

func (u *UI) tokensRevoke(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if err := u.cfg.TokenMgr.Revoke(id); err != nil && !errors.Is(err, auth.ErrTokenNotFound) {
		u.renderError(w, r, err)
		return
	}
	http.Redirect(w, r, "/ui/tokens", http.StatusSeeOther)
}

package api

import (
	"encoding/json"
	"net/http"
)

func (h *handlers) listSecrets(w http.ResponseWriter, r *http.Request) {
	host := r.PathValue("host")
	secrets, err := h.svc.HostSecrets(r.Context(), host)
	if err != nil {
		WriteError(w, err)
		return
	}
	out := make([]map[string]any, 0, len(secrets))
	for _, s := range secrets {
		out = append(out, map[string]any{
			"name":       s.Name,
			"created_at": s.CreatedAt,
		})
	}
	WriteJSON(w, http.StatusOK, out)
}

func (h *handlers) putSecret(w http.ResponseWriter, r *http.Request) {
	host := r.PathValue("host")
	name := r.PathValue("name")
	if !validName(name) {
		writeInvalidName(w, "name", name)
		return
	}
	var body struct {
		Value   string `json:"value"`
		Persist *bool  `json:"persist"` // optional; defaults to true
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		WriteJSON(w, http.StatusBadRequest, ErrorBody{Code: "invalid_body", Message: err.Error()})
		return
	}
	if body.Value == "" {
		WriteJSON(w, http.StatusBadRequest, ErrorBody{Code: "invalid_body", Message: "value is required"})
		return
	}
	persist := true
	if body.Persist != nil {
		persist = *body.Persist
	}
	if err := h.svc.PutHostSecret(r.Context(), host, name, []byte(body.Value), persist); err != nil {
		WriteError(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (h *handlers) deleteSecret(w http.ResponseWriter, r *http.Request) {
	host := r.PathValue("host")
	name := r.PathValue("name")
	if !validName(name) {
		writeInvalidName(w, "name", name)
		return
	}
	if err := h.svc.DeleteHostSecret(r.Context(), host, name); err != nil {
		WriteError(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

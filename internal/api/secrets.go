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
	var body struct {
		Value string `json:"value"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		WriteJSON(w, http.StatusBadRequest, ErrorBody{Code: "invalid_body", Message: err.Error()})
		return
	}
	if body.Value == "" {
		WriteJSON(w, http.StatusBadRequest, ErrorBody{Code: "invalid_body", Message: "value is required"})
		return
	}
	if err := h.svc.PutHostSecret(r.Context(), host, name, []byte(body.Value)); err != nil {
		WriteError(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (h *handlers) deleteSecret(w http.ResponseWriter, r *http.Request) {
	host := r.PathValue("host")
	name := r.PathValue("name")
	if err := h.svc.DeleteHostSecret(r.Context(), host, name); err != nil {
		WriteError(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

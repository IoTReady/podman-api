package api

import (
	"net/http"
	"strconv"
)

func (h *handlers) deleteVolume(w http.ResponseWriter, r *http.Request) {
	host := r.PathValue("host")
	name := r.PathValue("name")
	force, _ := strconv.ParseBool(r.URL.Query().Get("force"))
	if err := h.svc.DeleteVolume(r.Context(), host, name, force); err != nil {
		WriteError(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

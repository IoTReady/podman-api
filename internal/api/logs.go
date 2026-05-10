package api

import (
	"fmt"
	"net/http"
	"strconv"

	"github.com/iotready/podman-api/internal/podman"
)

func (h *handlers) logsInstance(w http.ResponseWriter, r *http.Request) {
	host := r.PathValue("host")
	tmpl := r.PathValue("template")
	slug := r.PathValue("slug")
	if !validInstancePath(w, tmpl, slug) {
		return
	}
	container := r.URL.Query().Get("container")
	if container == "" {
		WriteJSON(w, http.StatusBadRequest, ErrorBody{Code: "invalid_query", Message: "container is required"})
		return
	}
	if !validName(container) {
		writeInvalidName(w, "container", container)
		return
	}
	tail, _ := strconv.Atoi(r.URL.Query().Get("tail"))
	follow, _ := strconv.ParseBool(r.URL.Query().Get("follow"))
	opts := podman.LogOptions{Tail: tail, Since: r.URL.Query().Get("since"), Follow: follow}

	ch, err := h.svc.Logs(r.Context(), host, tmpl, slug, container, opts)
	if err != nil {
		WriteError(w, err)
		return
	}

	if follow {
		w.Header().Set("Content-Type", "text/event-stream")
		w.Header().Set("Cache-Control", "no-cache")
	} else {
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	}
	w.WriteHeader(http.StatusOK)

	flusher, _ := w.(http.Flusher)
	for {
		select {
		case <-r.Context().Done():
			return
		case line, ok := <-ch:
			if !ok {
				return
			}
			if follow {
				_, _ = fmt.Fprintf(w, "event: log\ndata: %s\n\n", line.Line)
			} else {
				_, _ = fmt.Fprintln(w, line.Line)
			}
			if flusher != nil {
				flusher.Flush()
			}
		}
	}
}

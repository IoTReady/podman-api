package api

import (
	"net/http"

	"github.com/iotready/podman-api/internal/instance"
)

func (h *handlers) listHosts(w http.ResponseWriter, r *http.Request) {
	hosts := h.svc.Hosts()
	out := make([]map[string]any, 0, len(hosts))
	for _, host := range hosts {
		entry := map[string]any{
			"id":     host.ID,
			"addr":   host.Addr,
			"labels": host.Labels,
			"status": "unknown",
		}
		if err := h.svc.Ping(r.Context(), host.ID); err == nil {
			entry["status"] = "ok"
			if v, err := h.svc.Version(r.Context(), host.ID); err == nil {
				entry["podman_version"] = v
			}
		} else {
			entry["status"] = "unreachable"
		}
		out = append(out, entry)
	}
	WriteJSON(w, http.StatusOK, out)
}

func (h *handlers) getHost(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("host")
	for _, host := range h.svc.Hosts() {
		if host.ID == id {
			entry := map[string]any{
				"id": host.ID, "addr": host.Addr, "labels": host.Labels, "status": "unknown",
			}
			if err := h.svc.Ping(r.Context(), id); err == nil {
				entry["status"] = "ok"
				if v, err := h.svc.Version(r.Context(), id); err == nil {
					entry["podman_version"] = v
				}
			} else {
				entry["status"] = "unreachable"
			}
			WriteJSON(w, http.StatusOK, entry)
			return
		}
	}
	WriteError(w, instance.ErrUnknownHost)
}

func (h *handlers) hostHealthz(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("host")
	known := false
	for _, host := range h.svc.Hosts() {
		if host.ID == id {
			known = true
			break
		}
	}
	if !known {
		WriteError(w, instance.ErrUnknownHost)
		return
	}
	if _, err := h.svc.PortsInUse(r.Context(), id); err != nil {
		WriteError(w, err)
		return
	}
	WriteJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (h *handlers) portsInUse(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("host")
	ports, err := h.svc.PortsInUse(r.Context(), id)
	if err != nil {
		WriteError(w, err)
		return
	}
	out := make([]map[string]any, 0, len(ports))
	for _, p := range ports {
		out = append(out, map[string]any{
			"port":      p.HostPort,
			"pod":       p.Pod,
			"container": p.Container,
			"protocol":  p.Protocol,
			"host_ip":   p.HostIP,
		})
	}
	WriteJSON(w, http.StatusOK, out)
}

package api

import (
	"net/http"

	"github.com/iotready/podman-api/internal/config"
	"github.com/iotready/podman-api/internal/instance"
	"github.com/iotready/podman-api/internal/podman"
)

func (h *handlers) listHosts(w http.ResponseWriter, r *http.Request) {
	hosts := h.svc.Hosts()
	out := make([]map[string]any, 0, len(hosts))
	for _, host := range hosts {
		out = append(out, h.hostView(r, host))
	}
	WriteJSON(w, http.StatusOK, out)
}

func (h *handlers) getHost(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("host")
	for _, host := range h.svc.Hosts() {
		if host.ID == id {
			WriteJSON(w, http.StatusOK, h.hostView(r, host))
			return
		}
	}
	WriteError(w, instance.ErrUnknownHost)
}

// hostView is the canonical JSON shape for a host: identity + reachability +
// drain state + a live count of managed instances. Reachability calls run
// per-request (not cached) so operators get a true picture.
func (h *handlers) hostView(r *http.Request, host config.Host) map[string]any {
	entry := map[string]any{
		"id":       host.ID,
		"addr":     host.Addr,
		"labels":   host.Labels,
		"status":   "unknown",
		"draining": host.Drain,
	}
	reachable := false
	if err := h.svc.Ping(r.Context(), host.ID); err == nil {
		reachable = true
		entry["status"] = "ok"
		if v, err := h.svc.Version(r.Context(), host.ID); err == nil {
			entry["podman_version"] = v
		}
	} else {
		entry["status"] = "unreachable"
	}
	if reachable {
		if ic, cc, err := h.svc.HostCounts(r.Context(), host.ID); err == nil {
			entry["instance_count"] = ic
			entry["container_count"] = cc
		}
		if info, err := h.svc.HostLoad(r.Context(), host.ID); err == nil {
			entry["load"] = loadView(info)
		}
	}
	return entry
}

// loadView renders a HostInfo as the canonical JSON load object. Pointer
// metrics absent from the source are omitted entirely (null-by-omission).
func loadView(info podman.HostInfo) map[string]any {
	m := map[string]any{
		"cpus":         info.CPUs,
		"mem_total":    info.MemTotal,
		"mem_free":     info.MemFree,
		"mem_used_pct": info.MemUsedPct,
		"disk": map[string]any{
			"total":       info.Disk.Total,
			"used":        info.Disk.Used,
			"free":        info.Disk.Free,
			"reclaimable": info.Disk.Reclaimable,
		},
	}
	if info.CPUPct != nil {
		m["cpu_pct"] = *info.CPUPct
	}
	if info.LoadAvg != nil {
		m["loadavg"] = []float64{info.LoadAvg[0], info.LoadAvg[1], info.LoadAvg[2]}
	}
	return m
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

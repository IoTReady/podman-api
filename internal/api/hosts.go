package api

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"sync"
	"time"

	"github.com/iotready/podman-api/internal/config"
	"github.com/iotready/podman-api/internal/instance"
	"github.com/iotready/podman-api/internal/podman"
)

func (h *handlers) listHosts(w http.ResponseWriter, r *http.Request) {
	hosts := h.svc.Hosts()
	out := make([]map[string]any, len(hosts))
	const perHostTimeout = 5 * time.Second
	var wg sync.WaitGroup
	for i, host := range hosts {
		wg.Add(1)
		go func(i int, host config.Host) {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(r.Context(), perHostTimeout)
			defer cancel()
			out[i] = h.hostViewCtx(ctx, host)
		}(i, host)
	}
	wg.Wait()
	WriteJSON(w, http.StatusOK, out)
}

func (h *handlers) getHost(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("host")
	for _, host := range h.svc.Hosts() {
		if host.ID == id {
			WriteJSON(w, http.StatusOK, h.hostViewCtx(r.Context(), host))
			return
		}
	}
	WriteError(w, instance.ErrUnknownHost)
}

// hostViewCtx is the canonical JSON shape for a host: identity + reachability +
// drain state + a live count of managed instances. Reachability calls run
// per-request (not cached) so operators get a true picture.
func (h *handlers) hostViewCtx(ctx context.Context, host config.Host) map[string]any {
	entry := map[string]any{
		"id":       host.ID,
		"addr":     host.Addr,
		"labels":   host.Labels,
		"status":   "unknown",
		"draining": host.Drain,
	}
	reachable := false
	if err := h.svc.Ping(ctx, host.ID); err == nil {
		reachable = true
		entry["status"] = "ok"
		if v, err := h.svc.Version(ctx, host.ID); err == nil {
			entry["podman_version"] = v
		}
	} else {
		entry["status"] = "unreachable"
	}
	if reachable {
		if ic, cc, err := h.svc.HostCounts(ctx, host.ID); err == nil {
			entry["instance_count"] = ic
			entry["container_count"] = cc
		}
		if info, err := h.svc.HostLoad(ctx, host.ID); err == nil {
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

func (h *handlers) renameHost(w http.ResponseWriter, r *http.Request) {
	// "Feature absent" is checked before anything else, so an unwired server
	// never answers a rename with "unknown host" or "conflict" — answers that
	// describe the request when the real answer is that the route does nothing
	// here.
	if h.hostRenamer == nil {
		WriteJSON(w, http.StatusNotImplemented, ErrorBody{Code: "not_implemented", Message: "host rename is not configured on this server"})
		return
	}

	oldID := r.PathValue("host")
	var req struct {
		NewID string `json:"new_id"`
	}
	if !decodeBody(w, r, &req) {
		return
	}
	if req.NewID == "" {
		WriteJSON(w, http.StatusBadRequest, ErrorBody{Code: "invalid_body", Message: "new_id is required"})
		return
	}
	if !validName(req.NewID) {
		writeInvalidName(w, "new_id", req.NewID)
		return
	}
	if req.NewID == oldID {
		WriteJSON(w, http.StatusBadRequest, ErrorBody{Code: "invalid_body", Message: "new_id must differ from the current host id"})
		return
	}

	found := false
	for _, hh := range h.svc.Hosts() {
		if hh.ID == oldID {
			found = true
		}
		if hh.ID == req.NewID {
			WriteJSON(w, http.StatusConflict, ErrorBody{Code: "host_already_exists", Message: fmt.Sprintf("host %q already exists", req.NewID)})
			return
		}
	}
	if !found {
		WriteError(w, instance.ErrUnknownHost)
		return
	}

	// Preflight the store's own refusals. The most common one by far —
	// host_has_backups, true of any host that has ever taken a backup — would
	// otherwise cost a destructive rewrite of hosts/*.yaml followed by a revert,
	// for a request that was always going to be refused. It is advisory only: it
	// takes no lock, so it can be stale by the time RenameHost runs, and
	// RenameHost's own checks inside the host lock remain the source of truth.
	if err := h.svc.CanRenameHost(r.Context(), oldID, req.NewID); err != nil {
		WriteError(w, err)
		return
	}

	// The config rewrite is handed to RenameHost rather than done here so it
	// runs inside the same host lock as the store migration, together with its
	// revert. Doing it here — two unsynchronized mutation points — let two
	// concurrent renames of the same host lose one write and then have the
	// loser's revert land after the winner committed, leaving the file
	// permanently disagreeing with the store. See Service.RenameHost's doc.
	var fileErr error
	rewrite := func() (func() error, error) {
		path, original, err := h.hostRenamer.RenameHostFile(oldID, req.NewID)
		if err != nil {
			// Remembered so the response can stay a 500 "internal" rather than
			// being classified as one of the rename-specific conflicts.
			fileErr = err
			return nil, err
		}
		return func() error {
			if err := config.WriteFileAtomic(path, original); err != nil {
				return fmt.Errorf("restore %s: %w", path, err)
			}
			return nil
		}, nil
	}

	if err := h.svc.RenameHost(r.Context(), oldID, req.NewID, rewrite); err != nil {
		if fileErr != nil {
			WriteJSON(w, http.StatusInternalServerError, ErrorBody{Code: "internal", Message: fileErr.Error()})
			return
		}
		WriteError(w, err)
		return
	}

	// The rename is committed at this point (config file + store + the
	// service's own host map). What is left is the rest of the process:
	// the podman client's host map is updated by Service.RenameHost, but the
	// server's background loops (ingress reconcile, prune policies) read their
	// own host list, which only a reload refreshes. A failure here is logged,
	// never surfaced as a failed rename — the rename did happen.
	switch {
	case h.hostsReloader == nil:
		log.Printf("host rename %s -> %s: no hosts reloader wired; background loops keep working the old id until SIGHUP", oldID, req.NewID)
	default:
		if err := h.hostsReloader(); err != nil {
			log.Printf("host rename %s -> %s: succeeded, but reloading the host list failed (%v); background loops keep working the old id until SIGHUP", oldID, req.NewID, err)
		}
	}

	WriteJSON(w, http.StatusOK, map[string]string{"old_id": oldID, "new_id": req.NewID})
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

package api

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/iotready/podman-api/internal/config"
	"github.com/iotready/podman-api/internal/instance"
	"github.com/iotready/podman-api/internal/podman"
)

// listHostsPerHostTimeout bounds one host's whole render inside GET /hosts, so
// one slow host cannot stretch the list response for every other. It is a var
// only so tests can shrink it.
//
// It is the binding budget on this path — tighter than probeTimeout, which
// each libpod diagnostic carries on its own — and that is precisely why
// hostView below must make exactly ONE libpod call. See its doc comment.
var listHostsPerHostTimeout = 5 * time.Second

func (h *handlers) listHosts(w http.ResponseWriter, r *http.Request) {
	hosts := h.svc.Hosts()
	out := make([]map[string]any, len(hosts))
	issues := make([]hostViewIssue, len(hosts))
	var wg sync.WaitGroup
	for i, host := range hosts {
		wg.Add(1)
		go func(i int, host config.Host) {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(r.Context(), listHostsPerHostTimeout)
			defer cancel()
			out[i], issues[i] = h.hostView(ctx, host)
		}(i, host)
	}
	wg.Wait()
	// Logged from the list path only, and only on a change. GET /hosts/{id}
	// runs the same render on a much larger budget, so letting it report too
	// would have the two paths' different verdicts flap against each other in
	// the log for a host that is merely slow.
	for i, host := range hosts {
		h.logHostViewTransition(host.ID, issues[i])
	}
	WriteJSON(w, http.StatusOK, out)
}

func (h *handlers) getHost(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("host")
	for _, host := range h.svc.Hosts() {
		if host.ID == id {
			view, _ := h.hostView(r.Context(), host)
			WriteJSON(w, http.StatusOK, view)
			return
		}
	}
	WriteError(w, instance.ErrUnknownHost)
}

// hostViewIssue records why a rendered host view came back incomplete. kind is
// a stable category ("" when the view is complete) so the transition log keys
// on WHAT went missing rather than on the error text, which can vary between
// otherwise identical failures; detail carries the underlying errors for the
// human reading the line.
type hostViewIssue struct {
	kind   string
	detail string
}

// hostView is the canonical JSON shape for a host: identity + reachability +
// drain state + version + load + a live count of managed instances. Reachability
// is resolved per-request (not cached) so operators get a true picture.
//
// It makes exactly ONE libpod call — HostLoad, i.e. `system.Info` — and derives
// reachability and the podman version from it (#289). It used to call Ping,
// then Version, then HostLoad, which is that same `system.Info` three times
// over, sequentially, inside listHostsPerHostTimeout. On a host whose `info` is
// slow that overran the budget with the calls that carry the interesting
// payload last, so GET /hosts reported the host "ok" and then omitted both its
// version and its whole load object — the "loadavg silently missing for one
// host" symptom of #258/#289, whose cause was never in the loadavg path at all:
// HostInfo returns before it ever reads loadavg when `system.Info` fails.
//
// Measured on the fleet that found it: local `podman info` is ~90ms on
// engine-infra and podman-1, ~430ms on vedanta, and ~1.7s on dev (a large
// graphroot on spinning disks) — so dev alone paid 3x1.7s > 5s and dev alone
// lost its load object, every single call.
//
// Ordering is deliberate: the counts sweep, which scales with the number of
// containers on the host, runs after the fields that are cheap and interesting.
func (h *handlers) hostView(ctx context.Context, host config.Host) (map[string]any, hostViewIssue) {
	entry := map[string]any{
		"id":       host.ID,
		"addr":     host.Addr,
		"labels":   host.Labels,
		"status":   "unknown",
		"draining": host.Drain,
	}
	info, err := h.svc.HostLoad(ctx, host.ID)
	if err != nil {
		entry["status"] = "unreachable"
		return entry, hostViewIssue{kind: "unreachable", detail: fmt.Sprintf("libpod info: %v", err)}
	}
	entry["status"] = "ok"
	if info.PodmanVersion != "" {
		entry["podman_version"] = info.PodmanVersion
	}
	entry["load"] = loadView(info)

	var kinds, details []string
	if info.LoadAvg == nil {
		// The one thing HostInfo reports best-effort rather than failing on.
		// Every path in podman.Real.hostLoadAvg that gives up returns a bare
		// nil, and three of them do so without logging — so without this a
		// host could report no loadavg forever and produce no signal at all,
		// which is exactly how #258's residue survived a fix that was meant to
		// make it impossible.
		kinds = append(kinds, "loadavg")
		details = append(details, "load.loadavg absent (no cached sample and the read-through produced nothing)")
	}
	if ic, cc, cerr := h.svc.HostCounts(ctx, host.ID); cerr == nil {
		entry["instance_count"] = ic
		entry["container_count"] = cc
	} else {
		kinds = append(kinds, "counts")
		details = append(details, fmt.Sprintf("instance/container counts: %v", cerr))
	}
	if len(kinds) == 0 {
		return entry, hostViewIssue{}
	}
	return entry, hostViewIssue{kind: strings.Join(kinds, "+"), detail: strings.Join(details, "; ")}
}

// logHostViewTransition reports an incomplete host view, but only when the
// category differs from the last one recorded for that host.
//
// Gated on the transition for the same reason podman.Real.hostLoadAvg and the
// inventory poller gate theirs: GET /hosts is a scrape target, and a host that
// is permanently degraded would otherwise emit a line per request. Unlike
// those two, though, this observes the RENDER — so a metric that goes missing
// between the sampler and the response, for any reason including ones nobody
// has thought of yet, still produces a signal.
func (h *handlers) logHostViewTransition(id string, issue hostViewIssue) {
	h.hostViewMu.Lock()
	if h.hostViewIssues == nil {
		h.hostViewIssues = map[string]string{}
	}
	prev, seen := h.hostViewIssues[id]
	if prev == issue.kind && seen {
		h.hostViewMu.Unlock()
		return
	}
	h.hostViewIssues[id] = issue.kind
	h.hostViewMu.Unlock()

	if issue.kind == "" {
		if seen {
			log.Printf("api: GET /hosts: host %q renders completely again", id)
		}
		return
	}
	log.Printf("api: GET /hosts: host %q rendered incomplete [%s] within its %s per-host budget: %s",
		id, issue.kind, listHostsPerHostTimeout, issue.detail)
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

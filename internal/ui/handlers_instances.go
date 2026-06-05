package ui

import (
	"net/http"
	"strings"

	"github.com/iotready/podman-api/internal/instance"
	"github.com/iotready/podman-api/internal/podman"
)

func (u *UI) instanceDetail(w http.ResponseWriter, r *http.Request) {
	host, tmpl, slug := r.PathValue("host"), r.PathValue("template"), r.PathValue("slug")
	obs, err := u.cfg.Svc.Get(r.Context(), host, tmpl, slug)
	if err != nil {
		u.renderError(w, r, err)
		return
	}
	u.render(w, r, http.StatusOK, "instance-detail", u.pageData(map[string]any{"Host": host, "Inst": obs}))
}

// lifecycle dispatches start/stop/restart/delete, then re-renders the instance
// detail (or the host instance list, after a delete). Upgrade is NOT handled
// here — it is a separate form flow (Task 9).
func (u *UI) lifecycle(w http.ResponseWriter, r *http.Request) {
	host, tmpl, slug := r.PathValue("host"), r.PathValue("template"), r.PathValue("slug")
	action := r.PathValue("action")
	ctx := r.Context()

	var err error
	switch action {
	case "start":
		err = u.cfg.Svc.Start(ctx, host, tmpl, slug)
	case "stop":
		err = u.cfg.Svc.Stop(ctx, host, tmpl, slug)
	case "restart":
		err = u.cfg.Svc.Restart(ctx, host, tmpl, slug)
	case "delete":
		err = u.cfg.Svc.Delete(ctx, host, tmpl, slug, instance.DeleteOptions{})
	default:
		http.Error(w, "unknown action", http.StatusBadRequest)
		return
	}
	if err != nil {
		u.renderError(w, r, err)
		return
	}
	if action == "delete" {
		obs, lerr := u.cfg.Svc.ListAllInstances(ctx, host)
		if lerr != nil {
			u.renderError(w, r, lerr)
			return
		}
		u.render(w, r, http.StatusOK, "host-instances", u.pageData(map[string]any{"Host": host, "Instances": obs}))
		return
	}
	obs, gerr := u.cfg.Svc.Get(ctx, host, tmpl, slug)
	if gerr != nil {
		u.renderError(w, r, gerr)
		return
	}
	u.render(w, r, http.StatusOK, "instance-detail", u.pageData(map[string]any{"Host": host, "Inst": obs}))
}

// logsTail renders the last N log lines of one container as static text. The
// container is taken from the ?container= query (a name suffix), defaulting to
// the instance's first container. Service.Logs caps output server-side via
// LogOptions.Tail; the drain is bounded by that and is safe for a static render.
func (u *UI) logsTail(w http.ResponseWriter, r *http.Request) {
	host, tmpl, slug := r.PathValue("host"), r.PathValue("template"), r.PathValue("slug")

	container := r.URL.Query().Get("container")
	if container == "" {
		obs, err := u.cfg.Svc.Get(r.Context(), host, tmpl, slug)
		if err != nil {
			u.renderError(w, r, err)
			return
		}
		if len(obs.Containers) == 0 {
			u.render(w, r, http.StatusOK, "logs", u.pageData(map[string]any{"Host": host, "Template": tmpl, "Slug": slug, "Lines": nil}))
			return
		}
		// Service.Logs reconstructs the container name as "<tmpl>-<slug>-<arg>";
		// Observed carries the full container name, so strip that prefix to
		// recover the suffix it expects.
		container = strings.TrimPrefix(obs.Containers[0].Name, tmpl+"-"+slug+"-")
	}

	ch, err := u.cfg.Svc.Logs(r.Context(), host, tmpl, slug, container, podman.LogOptions{Tail: 200})
	if err != nil {
		u.renderError(w, r, err)
		return
	}
	var lines []string
	for ln := range ch {
		lines = append(lines, ln.Line)
	}
	u.render(w, r, http.StatusOK, "logs", u.pageData(map[string]any{"Host": host, "Template": tmpl, "Slug": slug, "Lines": lines}))
}

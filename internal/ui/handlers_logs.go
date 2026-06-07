package ui

import (
	"net/http"
	"strings"

	"github.com/iotready/podman-api/internal/podman"
)

type containerOpt struct {
	Name   string // full container name, e.g. "postgres-main-db"
	Suffix string // suffix after "{tmpl}-{slug}-", e.g. "db"
}

// resolveContainerSuffix returns the first container whose name starts with
// "{tmpl}-{slug}-", stripping the prefix to get the suffix Svc.Logs expects.
// Returns "" if no container matches.
func resolveContainerSuffix(tmpl, slug string, containers []podman.Container) string {
	prefix := tmpl + "-" + slug + "-"
	for _, c := range containers {
		if strings.HasPrefix(c.Name, prefix) {
			return strings.TrimPrefix(c.Name, prefix)
		}
	}
	return ""
}

func (u *UI) logsPage(w http.ResponseWriter, r *http.Request) {
	host, tmpl, slug := r.PathValue("host"), r.PathValue("template"), r.PathValue("slug")

	obs, err := u.cfg.Svc.Get(r.Context(), host, tmpl, slug)
	if err != nil {
		u.renderError(w, r, err)
		return
	}

	prefix := tmpl + "-" + slug + "-"
	var containers []containerOpt
	for _, c := range obs.Containers {
		if strings.HasPrefix(c.Name, prefix) {
			containers = append(containers, containerOpt{
				Name:   c.Name,
				Suffix: strings.TrimPrefix(c.Name, prefix),
			})
		}
	}

	container := r.URL.Query().Get("container")
	if container == "" {
		if len(containers) == 0 {
			u.render(w, r, http.StatusOK, "logs-page", u.pageData(map[string]any{
				"Host": host, "Template": tmpl, "Slug": slug,
				"Container": "", "Containers": containers, "Follow": false, "Lines": nil,
			}))
			return
		}
		http.Redirect(w, r,
			"/ui/hosts/"+host+"/instances/"+tmpl+"/"+slug+"/logs"+
				"?container="+containers[0].Suffix+"&follow=true",
			http.StatusFound)
		return
	}

	follow := r.URL.Query().Get("follow") == "true"

	if follow {
		u.render(w, r, http.StatusOK, "logs-page", u.pageData(map[string]any{
			"Host": host, "Template": tmpl, "Slug": slug,
			"Container": container, "Containers": containers, "Follow": true, "Lines": nil,
		}))
		return
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
	u.render(w, r, http.StatusOK, "logs-page", u.pageData(map[string]any{
		"Host": host, "Template": tmpl, "Slug": slug,
		"Container": container, "Containers": containers, "Follow": false, "Lines": lines,
	}))
}

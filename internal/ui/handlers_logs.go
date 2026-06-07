package ui

import (
	"errors"
	"fmt"
	"html"
	"net/http"
	"net/url"
	"strings"

	"github.com/iotready/podman-api/internal/instance"
	"github.com/iotready/podman-api/internal/podman"
)

type containerOpt struct {
	Name   string // full container name, e.g. "postgres-main-db"
	Suffix string // suffix after "{tmpl}-{slug}-", e.g. "db"
}

// resolveContainerSuffix returns the first container whose name starts with
// "{tmpl}-{slug}-", stripping the prefix to get the suffix Svc.Logs expects.
// Returns "" if no container matches.
func resolveContainerSuffix(tmpl, slug string, names []string) string {
	prefix := tmpl + "-" + slug + "-"
	for _, name := range names {
		if strings.HasPrefix(name, prefix) {
			return strings.TrimPrefix(name, prefix)
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
				"?container="+url.QueryEscape(containers[0].Suffix)+"&follow=true",
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

func (u *UI) logsStream(w http.ResponseWriter, r *http.Request) {
	host, tmpl, slug := r.PathValue("host"), r.PathValue("template"), r.PathValue("slug")

	container := r.URL.Query().Get("container")
	if container == "" {
		obs, err := u.cfg.Svc.Get(r.Context(), host, tmpl, slug)
		if err != nil {
			if errors.Is(err, instance.ErrInstanceNotFound) ||
				errors.Is(err, instance.ErrUnknownHost) ||
				errors.Is(err, instance.ErrUnknownTemplate) {
				http.Error(w, "not found", http.StatusNotFound)
			} else {
				http.Error(w, err.Error(), http.StatusBadGateway)
			}
			return
		}
		names := make([]string, len(obs.Containers))
		for i, c := range obs.Containers {
			names[i] = c.Name
		}
		container = resolveContainerSuffix(tmpl, slug, names)
		if container == "" {
			http.Error(w, "no containers", http.StatusBadRequest)
			return
		}
	} else {
		// Validate the provided suffix belongs to this instance.
		obs, err := u.cfg.Svc.Get(r.Context(), host, tmpl, slug)
		if err != nil {
			if errors.Is(err, instance.ErrInstanceNotFound) ||
				errors.Is(err, instance.ErrUnknownHost) ||
				errors.Is(err, instance.ErrUnknownTemplate) {
				http.Error(w, "not found", http.StatusNotFound)
			} else {
				http.Error(w, err.Error(), http.StatusBadGateway)
			}
			return
		}
		names := make([]string, len(obs.Containers))
		for i, c := range obs.Containers {
			names[i] = c.Name
		}
		if resolveContainerSuffix(tmpl, slug, names) == "" {
			// No containers at all — unusual but possible during startup.
			http.Error(w, "no containers", http.StatusBadRequest)
			return
		}
		// Verify the requested suffix is actually present.
		valid := false
		prefix := tmpl + "-" + slug + "-"
		for _, name := range names {
			if name == prefix+container {
				valid = true
				break
			}
		}
		if !valid {
			http.Error(w, "unknown container", http.StatusBadRequest)
			return
		}
	}

	ch, err := u.cfg.Svc.Logs(r.Context(), host, tmpl, slug, container, podman.LogOptions{Follow: true, Tail: 100})
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadGateway)
		return
	}

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
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
			fmt.Fprintf(w, "event: log\ndata: <span class=\"line\">%s</span>\n\n",
				html.EscapeString(line.Line))
			if flusher != nil {
				flusher.Flush()
			}
		}
	}
}

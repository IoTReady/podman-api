package ui

import (
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

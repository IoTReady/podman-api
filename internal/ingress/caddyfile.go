// Package ingress renders ingress configuration and validates domains. The
// per-host Caddy controller (added later) consumes this package; nothing here
// talks to podman or the network, so it is pure and unit-testable.
package ingress

import (
	"fmt"
	"sort"
	"strings"
)

// Route maps a public domain to the backend address the host's Caddy
// reverse-proxies to. Backend is resolved on the shared ingress network
// (e.g. "web-app1:8080").
type Route struct {
	Domain  string
	Backend string
}

// RenderCaddyfile produces a deterministic Caddyfile for routes. A non-empty
// acmeEmail sets the global ACME contact. Routes are emitted sorted by domain
// so identical inputs yield byte-identical output — a stable file means a
// `caddy reload` is a no-op when nothing actually changed.
func RenderCaddyfile(acmeEmail string, routes []Route) (string, error) {
	var b strings.Builder
	if acmeEmail != "" {
		fmt.Fprintf(&b, "{\n\temail %s\n}\n\n", acmeEmail)
	}
	sorted := append([]Route(nil), routes...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i].Domain < sorted[j].Domain })
	for _, r := range sorted {
		if r.Domain == "" || r.Backend == "" {
			return "", fmt.Errorf("ingress: route has empty domain or backend: %+v", r)
		}
		fmt.Fprintf(&b, "%s {\n\treverse_proxy %s\n}\n", r.Domain, r.Backend)
	}
	return b.String(), nil
}

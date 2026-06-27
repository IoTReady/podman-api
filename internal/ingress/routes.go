// Package ingress derives a host's ingress routes from the store and reconciles
// a per-host Caddy pod to match them via the Caddy admin API. Nothing here other
// than the controller talks to podman or the network.
package ingress

import (
	"context"
	"errors"
	"fmt"
	"log"
	"sort"
	"strconv"

	"github.com/iotready/podman-api/internal/store"
)

// Route maps a public domain to the backend address the host's Caddy
// reverse-proxies to. Backend is resolved on the shared ingress network
// (e.g. "web-app1:8080").
type Route struct {
	Domain  string
	Backend string
}

// podName mirrors instance.podName: an instance's pod is "<template>-<slug>",
// which is globally unique and the name aardvark resolves on the network. The
// route backend points at the pod name (NOT a bare container name, which is not
// unique across pods).
func podName(template, slug string) string { return template + "-" + slug }

// deriveRoutes builds the host's ingress routes from the store. Reconcile
// behaviour:
//   - instances whose template is missing from the store (stale spec) are
//     skipped with a warning; they don't fail the reconcile.
//   - instances whose template declares no ingress are skipped with a warning.
//   - transient store errors fail the reconcile so it retries.
//   - a domain may be claimed by at most one instance on the host.
//   - instances without domains are skipped.
//
// The returned slice is domain-sorted for a stable Caddyfile.
func (c *CaddyController) deriveRoutes(ctx context.Context, host string) ([]Route, error) {
	keys, err := c.store.ListSpecKeys(ctx, host)
	if err != nil {
		return nil, fmt.Errorf("ingress: list specs for %s: %w", host, err)
	}
	owner := map[string]string{} // domain -> "template/slug"
	var routes []Route
	for _, k := range keys {
		sp, err := c.store.GetSpec(ctx, host, k.Template, k.Slug)
		if err != nil {
			return nil, fmt.Errorf("ingress: get spec %s/%s: %w", k.Template, k.Slug, err)
		}
		if len(sp.Domains) == 0 {
			continue
		}
		// Resolve the template's ingress declaration from the store at reconcile
		// time: templates are mutable, so a cached boot-time map would drop
		// routes for templates created/edited after startup.
		tmpl, err := c.store.GetTemplate(ctx, k.Template)
		if err != nil {
			if errors.Is(err, store.ErrNotFound) {
				// Template was deleted while a spec still references it. Skip its
				// routes rather than failing the whole host reconcile; the spec
				// is the operator's to clean up.
				log.Printf("ingress: skipping %s/%s on %s: template %q not found (stale spec)", k.Template, k.Slug, host, k.Template)
				continue
			}
			// A transient store failure must fail this reconcile cycle so it
			// retries, not silently drop routes.
			return nil, fmt.Errorf("ingress: get template for %s/%s: %w", k.Template, k.Slug, err)
		}
		if tmpl.Meta.Ingress == nil {
			// Not an ingress template; it declares no backend, so skip its routes.
			log.Printf("ingress: skipping %s/%s on %s: template %q declares no ingress", k.Template, k.Slug, host, k.Template)
			continue
		}
		backend := podName(k.Template, k.Slug) + ":" + strconv.Itoa(tmpl.Meta.Ingress.Port)
		for _, d := range sp.Domains {
			if prev, dup := owner[d]; dup {
				return nil, fmt.Errorf("ingress: domain %q claimed by both %s and %s/%s", d, prev, k.Template, k.Slug)
			}
			owner[d] = k.Template + "/" + k.Slug
			routes = append(routes, Route{Domain: d, Backend: backend})
		}
	}
	sort.Slice(routes, func(i, j int) bool { return routes[i].Domain < routes[j].Domain })
	return routes, nil
}

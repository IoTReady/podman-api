package ingress

import (
	"context"
	"fmt"
	"sort"
	"strconv"
)

// podName mirrors instance.podName: an instance's pod is "<template>-<slug>",
// which is globally unique and the name aardvark resolves on the network. The
// route backend points at the pod name (NOT a bare container name, which is not
// unique across pods).
func podName(template, slug string) string { return template + "-" + slug }

// deriveRoutes builds the host's ingress routes from the store. It enforces two
// design rules:
//   - an instance carrying domains whose template declares no ingress: is an
//     operator error (rejected), and
//   - a domain may be claimed by at most one instance on the host.
//
// Instances without domains are skipped. The returned slice is domain-sorted
// for a stable Caddyfile.
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
		ti, ok := c.templates[k.Template]
		if !ok {
			return nil, fmt.Errorf("ingress: instance %s/%s has domains but template %q declares no ingress", k.Template, k.Slug, k.Template)
		}
		backend := podName(k.Template, k.Slug) + ":" + strconv.Itoa(ti.Port)
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

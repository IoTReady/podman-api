package ingress

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"time"
)

// Caddy JSON types for the admin API.

type caddyRoute struct {
	Match    []caddyMatch   `json:"match"`
	Handle   []caddyHandler `json:"handle"`
	Terminal bool           `json:"terminal"`
}

type caddyMatch struct {
	Host []string `json:"host"`
}

type caddyHandler struct {
	Handler   string          `json:"handler"`
	Upstreams []caddyUpstream `json:"upstreams"`
}

type caddyUpstream struct {
	Dial string `json:"dial"`
}

// caddyServer is the JSON shape of the podman_api HTTP server we PUT at
// /config/apps/http/servers/podman_api.
type caddyServer struct {
	Listen         []string     `json:"listen"`
	Routes         []caddyRoute `json:"routes"`
	AutomaticHTTPS *struct{}    `json:"automatic_https"`
}

// routesToCaddyJSON converts ingress Routes to Caddy JSON route objects.
func routesToCaddyJSON(routes []Route) []caddyRoute {
	if len(routes) == 0 {
		return nil
	}
	out := make([]caddyRoute, len(routes))
	for i, r := range routes {
		out[i] = caddyRoute{
			Match:    []caddyMatch{{Host: []string{r.Domain}}},
			Handle:   []caddyHandler{{Handler: "reverse_proxy", Upstreams: []caddyUpstream{{Dial: r.Backend}}}},
			Terminal: true,
		}
	}
	return out
}

// defaultAdminClient is the shared http.Client for all admin API calls. A
// 10-second timeout is generous for a loopback or LAN call and avoids
// hanging a reconcile indefinitely.
var defaultAdminClient = &http.Client{Timeout: 10 * time.Second}

// caddyAdminDo is the real adminDo implementation: sends an HTTP request to
// the Caddy admin API at addr (host:port). body may be nil for GET/DELETE.
// Returns (statusCode, responseBody, error). A non-2xx status is NOT
// returned as an error — the caller inspects the code.
func caddyAdminDo(ctx context.Context, addr, method, path string, body []byte) (int, []byte, error) {
	url := "http://" + addr + path
	var r io.Reader
	if len(body) > 0 {
		r = bytes.NewReader(body)
	}
	req, err := http.NewRequestWithContext(ctx, method, url, r)
	if err != nil {
		return 0, nil, fmt.Errorf("ingress: admin request: %w", err)
	}
	if len(body) > 0 {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := defaultAdminClient.Do(req)
	if err != nil {
		return 0, nil, fmt.Errorf("ingress: admin %s %s: %w", method, path, err)
	}
	defer resp.Body.Close()
	respBody, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, respBody, nil
}

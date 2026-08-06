// Package imgregistry is a minimal Docker Registry v2 HTTP client used to
// browse the fleet's image registry and resolve tags/digests for the
// upgrade-picker. Deliberately not named "registry" — the codebase already
// has jobs.Registry (job-kind registry) and a same-named package here would
// collide in every import block needing both.
package imgregistry

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"time"
)

// Auth configures how HTTPClient authenticates to the registry.
type Auth struct {
	Mode     string // "none" (default) or "basic"
	Username string
	Password string
}

// Client is the read-only surface this package exposes. Only Catalog is
// defined in this task; Tags and Manifest are added in Task 2.
type Client interface {
	Catalog(ctx context.Context) ([]string, error)
}

// HTTPClient implements Client against a real Registry v2 server.
type HTTPClient struct {
	BaseURL string // e.g. "http://100.64.0.23:5000", no trailing slash
	Auth    Auth
	HTTP    *http.Client
}

// NewHTTPClient builds an HTTPClient with a sane default timeout.
func NewHTTPClient(baseURL string, auth Auth) *HTTPClient {
	return &HTTPClient{
		BaseURL: strings.TrimRight(baseURL, "/"),
		Auth:    auth,
		HTTP:    &http.Client{Timeout: 10 * time.Second},
	}
}

func (c *HTTPClient) do(ctx context.Context, path string) (*http.Response, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.BaseURL+path, nil)
	if err != nil {
		return nil, err
	}
	if c.Auth.Mode == "basic" {
		req.SetBasicAuth(c.Auth.Username, c.Auth.Password)
	}
	resp, err := c.HTTP.Do(req)
	if err != nil {
		return nil, fmt.Errorf("registry request %s: %w", path, err)
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		resp.Body.Close()
		return nil, fmt.Errorf("registry request %s: status %d", path, resp.StatusCode)
	}
	return resp, nil
}

// catalogPageSize bounds each /v2/_catalog page. This registry returns an
// empty list with HTTP 200 for n= above ~1000, which is indistinguishable
// from "no repos" — staying well under that and following Link explicitly
// (rather than ever requesting one large page) is load-bearing, not a
// tuning choice.
const catalogPageSize = 100

// Catalog lists every repository in the registry, following the Link
// response header across pages rather than requesting one unbounded page.
func (c *HTTPClient) Catalog(ctx context.Context) ([]string, error) {
	var all []string
	path := fmt.Sprintf("/v2/_catalog?n=%d", catalogPageSize)
	for path != "" {
		resp, err := c.do(ctx, path)
		if err != nil {
			return nil, err
		}
		var body struct {
			Repositories []string `json:"repositories"`
		}
		decErr := json.NewDecoder(resp.Body).Decode(&body)
		link := resp.Header.Get("Link")
		resp.Body.Close()
		if decErr != nil {
			return nil, fmt.Errorf("decode catalog page: %w", decErr)
		}
		all = append(all, body.Repositories...)
		path = nextLinkPath(link)
	}
	return all, nil
}

// nextLinkPath extracts the path+query of a Link: <...>; rel="next" header,
// or "" if there is no next page.
func nextLinkPath(link string) string {
	if link == "" {
		return ""
	}
	start := strings.Index(link, "<")
	end := strings.Index(link, ">")
	if start == -1 || end == -1 || end <= start {
		return ""
	}
	raw := link[start+1 : end]
	u, err := url.Parse(raw)
	if err != nil {
		return ""
	}
	if u.RawQuery == "" {
		return u.Path
	}
	return u.Path + "?" + u.RawQuery
}

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
	"sort"
	"strings"
	"time"
)

// Auth configures how HTTPClient authenticates to the registry.
type Auth struct {
	Mode     string // "none" (default) or "basic"
	Username string
	Password string
}

// Layer is one entry in a manifest's layer list.
type Layer struct {
	Digest string
	Size   int64
}

// Manifest is the resolved shape of a single tag/digest reference.
type Manifest struct {
	Digest       string // from Docker-Content-Digest, the canonical content digest
	ConfigDigest string
	Layers       []Layer
	Size         int64 // config size + sum of all layer sizes
}

// TagGroup is every tag that resolves to the same content digest, with the
// digest's creation time (from its config blob) and total size.
type TagGroup struct {
	Digest  string
	Tags    []string
	Created time.Time
	Size    int64
}

// Client is the read-only surface this package exposes.
type Client interface {
	Catalog(ctx context.Context) ([]string, error)
	Tags(ctx context.Context, repo string) ([]TagGroup, error)
	Manifest(ctx context.Context, repo, ref string) (Manifest, error)
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

const manifestAccept = "application/vnd.docker.distribution.manifest.v2+json, application/vnd.oci.image.manifest.v1+json"

// Manifest fetches a single manifest by tag or digest and returns its
// canonical digest (from Docker-Content-Digest) plus size/layer info.
func (c *HTTPClient) Manifest(ctx context.Context, repo, ref string) (Manifest, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet,
		fmt.Sprintf("%s/v2/%s/manifests/%s", c.BaseURL, repo, ref), nil)
	if err != nil {
		return Manifest{}, err
	}
	req.Header.Set("Accept", manifestAccept)
	if c.Auth.Mode == "basic" {
		req.SetBasicAuth(c.Auth.Username, c.Auth.Password)
	}
	resp, err := c.HTTP.Do(req)
	if err != nil {
		return Manifest{}, fmt.Errorf("registry manifest %s/%s: %w", repo, ref, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusNotFound {
		return Manifest{}, fmt.Errorf("manifest %s/%s: not found", repo, ref)
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return Manifest{}, fmt.Errorf("registry manifest %s/%s: status %d", repo, ref, resp.StatusCode)
	}

	var body struct {
		Config struct {
			Digest string `json:"digest"`
			Size   int64  `json:"size"`
		} `json:"config"`
		Layers []struct {
			Digest string `json:"digest"`
			Size   int64  `json:"size"`
		} `json:"layers"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		return Manifest{}, fmt.Errorf("decode manifest %s/%s: %w", repo, ref, err)
	}

	m := Manifest{
		Digest:       resp.Header.Get("Docker-Content-Digest"),
		ConfigDigest: body.Config.Digest,
		Size:         body.Config.Size,
	}
	for _, l := range body.Layers {
		m.Layers = append(m.Layers, Layer{Digest: l.Digest, Size: l.Size})
		m.Size += l.Size
	}
	return m, nil
}

// Tags lists every tag in repo, resolves each to its manifest digest, and
// groups tags that share a digest into one TagGroup. Each unique digest's
// config blob is fetched once (not once per tag) for its Created timestamp.
// Groups are sorted newest-Created-first.
func (c *HTTPClient) Tags(ctx context.Context, repo string) ([]TagGroup, error) {
	tagNames, err := c.listTags(ctx, repo)
	if err != nil {
		return nil, err
	}

	byDigest := map[string]*TagGroup{}
	var order []string
	for _, tag := range tagNames {
		m, err := c.Manifest(ctx, repo, tag)
		if err != nil {
			return nil, fmt.Errorf("resolve tag %s: %w", tag, err)
		}
		g, ok := byDigest[m.Digest]
		if !ok {
			g = &TagGroup{Digest: m.Digest, Size: m.Size}
			byDigest[m.Digest] = g
			order = append(order, m.Digest)

			created, err := c.blobCreated(ctx, repo, m.ConfigDigest)
			if err != nil {
				return nil, fmt.Errorf("resolve created time for %s: %w", m.Digest, err)
			}
			g.Created = created
		}
		g.Tags = append(g.Tags, tag)
	}

	groups := make([]TagGroup, 0, len(order))
	for _, d := range order {
		groups = append(groups, *byDigest[d])
	}
	sort.Slice(groups, func(i, j int) bool {
		return groups[i].Created.After(groups[j].Created)
	})
	return groups, nil
}

func (c *HTTPClient) listTags(ctx context.Context, repo string) ([]string, error) {
	var all []string
	path := fmt.Sprintf("/v2/%s/tags/list?n=%d", repo, catalogPageSize)
	for path != "" {
		resp, err := c.do(ctx, path)
		if err != nil {
			return nil, err
		}
		var body struct {
			Tags []string `json:"tags"`
		}
		decErr := json.NewDecoder(resp.Body).Decode(&body)
		link := resp.Header.Get("Link")
		resp.Body.Close()
		if decErr != nil {
			return nil, fmt.Errorf("decode tags page for %s: %w", repo, decErr)
		}
		all = append(all, body.Tags...)
		path = nextLinkPath(link)
	}
	return all, nil
}

func (c *HTTPClient) blobCreated(ctx context.Context, repo, digest string) (time.Time, error) {
	resp, err := c.do(ctx, fmt.Sprintf("/v2/%s/blobs/%s", repo, digest))
	if err != nil {
		return time.Time{}, err
	}
	defer resp.Body.Close()
	var body struct {
		Created string `json:"created"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		return time.Time{}, fmt.Errorf("decode config blob %s: %w", digest, err)
	}
	if body.Created == "" {
		return time.Time{}, nil
	}
	return time.Parse(time.RFC3339, body.Created)
}

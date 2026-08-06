// Package imgregistry is a minimal Docker Registry v2 HTTP client used to
// browse the fleet's image registry and resolve tags/digests for the
// upgrade-picker. Deliberately not named "registry" — the codebase already
// has jobs.Registry (job-kind registry) and a same-named package here would
// collide in every import block needing both.
package imgregistry

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"sync"
	"time"
)

// ErrNotFound wraps a registry's genuine 404 response (NAME_UNKNOWN /
// MANIFEST_UNKNOWN) — the registry answered and said this repo/tag doesn't
// exist.
var ErrNotFound = errors.New("not found")

// ErrUnreachable wraps a transport failure, a non-404 non-2xx response, or a
// response body this client cannot decode — anything that means "we could
// not get a straight answer from the registry," as distinct from ErrNotFound
// ("the registry answered and said no"). Callers must not collapse the two:
// an operator told "not found" for a registry that is simply down gets the
// exact misleading signal this package exists to prevent.
var ErrUnreachable = errors.New("registry unreachable")

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
		return nil, fmt.Errorf("registry request %s: %w: %w", path, ErrUnreachable, err)
	}
	if resp.StatusCode == http.StatusNotFound {
		resp.Body.Close()
		return nil, fmt.Errorf("registry request %s: status %d: %w", path, resp.StatusCode, ErrNotFound)
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		resp.Body.Close()
		return nil, fmt.Errorf("registry request %s: status %d: %w", path, resp.StatusCode, ErrUnreachable)
	}
	return resp, nil
}

// catalogPageSize bounds each /v2/_catalog page. This registry returns an
// empty list with HTTP 200 for n= above ~1000, which is indistinguishable
// from "no repos" — staying well under that and following Link explicitly
// (rather than ever requesting one large page) is load-bearing, not a
// tuning choice.
const catalogPageSize = 100

// maxPaginationPages bounds the Catalog/listTags follow-the-Link loops. It is
// a backstop against a misbehaving server that keeps sending a next link
// forever — the caller's ctx is the real budget (both methods take one
// already), this just guarantees the loop itself terminates even within a
// generous deadline.
const maxPaginationPages = 1000

// Catalog lists every repository in the registry, following the Link
// response header across pages rather than requesting one unbounded page.
func (c *HTTPClient) Catalog(ctx context.Context) ([]string, error) {
	var all []string
	path := fmt.Sprintf("/v2/_catalog?n=%d", catalogPageSize)
	for pages := 0; path != ""; pages++ {
		if pages >= maxPaginationPages {
			return nil, fmt.Errorf("registry catalog: exceeded %d pages: %w", maxPaginationPages, ErrUnreachable)
		}
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
			return nil, fmt.Errorf("decode catalog page: %w: %w", ErrUnreachable, decErr)
		}
		all = append(all, body.Repositories...)
		path = nextLinkPath(link)
	}
	return all, nil
}

// nextLinkPath extracts the path+query of the Link entry whose rel="next",
// or "" if there is none. The header can carry multiple comma-separated
// relation entries (e.g. a rel="prev" alongside rel="next"), so matching the
// first "<...>" regardless of its rel would misfire — anchor specifically to
// rel="next".
func nextLinkPath(link string) string {
	if link == "" {
		return ""
	}
	for _, part := range strings.Split(link, ",") {
		if !strings.Contains(part, `rel="next"`) {
			continue
		}
		start := strings.Index(part, "<")
		end := strings.Index(part, ">")
		if start == -1 || end == -1 || end <= start {
			continue
		}
		raw := part[start+1 : end]
		u, err := url.Parse(raw)
		if err != nil {
			continue
		}
		if u.RawQuery == "" {
			return u.Path
		}
		return u.Path + "?" + u.RawQuery
	}
	return ""
}

// manifestAccept lists both single-platform manifest media types and the
// multi-arch manifest-list/index types. Without the latter, a multi-arch tag
// (common for base images) gets served whatever the registry's default is
// and may fail to decode as a plain manifest — see the Manifests field below.
const manifestAccept = "application/vnd.docker.distribution.manifest.v2+json, application/vnd.oci.image.manifest.v1+json, application/vnd.docker.distribution.manifest.list.v2+json, application/vnd.oci.image.index.v1+json"

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
		return Manifest{}, fmt.Errorf("registry manifest %s/%s: %w: %w", repo, ref, ErrUnreachable, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusNotFound {
		return Manifest{}, fmt.Errorf("manifest %s/%s: not found: %w", repo, ref, ErrNotFound)
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return Manifest{}, fmt.Errorf("registry manifest %s/%s: status %d: %w", repo, ref, resp.StatusCode, ErrUnreachable)
	}

	var body struct {
		Config *struct {
			Digest string `json:"digest"`
			Size   int64  `json:"size"`
		} `json:"config"`
		Layers []struct {
			Digest string `json:"digest"`
			Size   int64  `json:"size"`
		} `json:"layers"`
		// Manifests is present (even if empty) only on a multi-arch manifest
		// list / OCI image index. Its mere presence, not its content, is what
		// distinguishes an index from a single-platform manifest — a
		// json.RawMessage lets us test for that without trying (and failing)
		// to decode it as config+layers.
		Manifests json.RawMessage `json:"manifests"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		return Manifest{}, fmt.Errorf("decode manifest %s/%s: %w: %w", repo, ref, ErrUnreachable, err)
	}

	m := Manifest{Digest: resp.Header.Get("Docker-Content-Digest")}
	if body.Manifests != nil {
		// Multi-arch manifest list/index: there is no single config blob to
		// inspect here (each platform entry has its own), so record the tag
		// with zero Created/Size rather than misreading "manifests" as
		// config+layers.
		return m, nil
	}
	if body.Config != nil {
		m.ConfigDigest = body.Config.Digest
		m.Size = body.Config.Size
	}
	for _, l := range body.Layers {
		m.Layers = append(m.Layers, Layer{Digest: l.Digest, Size: l.Size})
		m.Size += l.Size
	}
	return m, nil
}

// tagResolveConcurrency bounds how many manifest/blob fetches Tags runs at
// once. A repo can carry hundreds of tags accumulated over months of CI
// builds (the fleet's "engine" repo has ~1000) — resolving them one at a
// time made a single Tags call take minutes; this caps concurrency instead
// of either serializing (too slow) or firing all requests at once (an
// accidental thundering-herd against the registry).
const tagResolveConcurrency = 16

// tagResolution is one tag's resolved manifest, or the error resolving it —
// captured per-goroutine so the grouping pass below can stay single-threaded
// and keep its original deterministic, tag-order-preserving behavior.
type tagResolution struct {
	tag      string
	manifest Manifest
	err      error
}

// digestGroup pairs a TagGroup being built with the config digest needed to
// resolve its Created timestamp — kept alongside rather than added to
// TagGroup itself, since TagGroup is public API and configDigest is only
// needed transiently during grouping.
type digestGroup struct {
	group        *TagGroup
	configDigest string
}

// Tags lists every tag in repo, resolves each to its manifest digest, and
// groups tags that share a digest into one TagGroup. Each unique digest's
// config blob is fetched once (not once per tag) for its Created timestamp.
// Manifest and blob-created lookups both run with bounded concurrency
// (tagResolveConcurrency), not sequentially. Groups are sorted
// newest-Created-first.
func (c *HTTPClient) Tags(ctx context.Context, repo string) ([]TagGroup, error) {
	tagNames, err := c.listTags(ctx, repo)
	if err != nil {
		return nil, err
	}

	// Phase 1: resolve every tag's manifest concurrently. Each goroutine
	// writes only its own index of resolutions, so no synchronization is
	// needed beyond the WaitGroup.
	resolutions := make([]tagResolution, len(tagNames))
	{
		var wg sync.WaitGroup
		sem := make(chan struct{}, tagResolveConcurrency)
		for i, tag := range tagNames {
			wg.Add(1)
			sem <- struct{}{}
			go func(i int, tag string) {
				defer wg.Done()
				defer func() { <-sem }()
				m, err := c.Manifest(ctx, repo, tag)
				resolutions[i] = tagResolution{tag: tag, manifest: m, err: err}
			}(i, tag)
		}
		wg.Wait()
	}

	// Phase 2: group resolved tags by digest, single-threaded (cheap,
	// in-memory) so grouping stays deterministic and tag order within a
	// group matches tagNames' order.
	byDigest := map[string]*digestGroup{}
	var order []string
	for _, r := range resolutions {
		if r.err != nil {
			// One tag this client cannot resolve (an unsupported media type,
			// a transient blip) must not hide every other tag in the repo
			// behind a single error — skip it and keep going. No logger is
			// wired into this package; this comment is the record of the
			// trade-off (see the design finding this fixes).
			continue
		}
		dg, ok := byDigest[r.manifest.Digest]
		if !ok {
			dg = &digestGroup{
				group:        &TagGroup{Digest: r.manifest.Digest, Size: r.manifest.Size},
				configDigest: r.manifest.ConfigDigest,
			}
			byDigest[r.manifest.Digest] = dg
			order = append(order, r.manifest.Digest)
		}
		dg.group.Tags = append(dg.group.Tags, r.tag)
	}

	// Phase 3: resolve each unique digest's Created timestamp concurrently —
	// one blob fetch per digest, not per tag, same as before, but no longer
	// serialized. Each goroutine writes only its own digestGroup's Created
	// field, so again no synchronization beyond the WaitGroup is needed.
	{
		var wg sync.WaitGroup
		sem := make(chan struct{}, tagResolveConcurrency)
		for _, d := range order {
			dg := byDigest[d]
			if dg.configDigest == "" {
				// A multi-arch manifest list has no single config blob;
				// leave Created zero rather than fetching a blob that
				// doesn't exist.
				continue
			}
			wg.Add(1)
			sem <- struct{}{}
			go func(dg *digestGroup) {
				defer wg.Done()
				defer func() { <-sem }()
				// A failed blob fetch leaves Created zero instead of
				// dropping the whole digest group — same visibility
				// trade-off as an unresolvable tag above.
				if created, err := c.blobCreated(ctx, repo, dg.configDigest); err == nil {
					dg.group.Created = created
				}
			}(dg)
		}
		wg.Wait()
	}

	groups := make([]TagGroup, 0, len(order))
	for _, d := range order {
		groups = append(groups, *byDigest[d].group)
	}
	sort.Slice(groups, func(i, j int) bool {
		return groups[i].Created.After(groups[j].Created)
	})
	return groups, nil
}

func (c *HTTPClient) listTags(ctx context.Context, repo string) ([]string, error) {
	var all []string
	path := fmt.Sprintf("/v2/%s/tags/list?n=%d", repo, catalogPageSize)
	for pages := 0; path != ""; pages++ {
		if pages >= maxPaginationPages {
			return nil, fmt.Errorf("list tags for %s: exceeded %d pages: %w", repo, maxPaginationPages, ErrUnreachable)
		}
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
			return nil, fmt.Errorf("decode tags page for %s: %w: %w", repo, ErrUnreachable, decErr)
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

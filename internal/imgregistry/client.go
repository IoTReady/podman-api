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

// DroppedTag is one tag that a listing could not resolve, and why.
type DroppedTag struct {
	Tag string

	// Vanished is true ONLY when, after resolution failed, a fresh read of
	// /v2/<repo>/tags/list no longer listed Tag. That is the registry's one
	// exact statement about a tag's existence, and the ONLY basis on which a
	// caller may treat a drop as benign.
	//
	// It is a field rather than a sentinel wrapped into Err on purpose. A 404
	// from GET/HEAD /manifests/<ref> is content-negotiated: distribution
	// answers 404 MANIFEST_UNKNOWN when its stored media type matches no entry
	// in the request's Accept set, byte-identical to "this tag does not
	// exist". So no manifest error, however classified, can carry this
	// meaning — and if the decision were read back out of Err via
	// errors.Is(..., ErrNotFound), any future edit that wrapped a manifest
	// error's cause with %w instead of %v would silently reclassify an
	// unreadable tag as a deleted one. That mistake ends in deleting a live
	// image, and it would leave every test green. The flag cannot be set by
	// accident: exactly one code path assigns it, from the tags/list read.
	Vanished bool

	Err error
}

// TagListing is a repo's resolved tag groups PLUS the tags that could not be
// resolved and are therefore absent from those groups.
//
// The two travel together on purpose. Resolution drops a tag whose manifest
// fetch failed and carries on (one exotic media type must not hide a whole
// repo behind a single error), which is right for browsing and catastrophic
// for anything that deletes: a dropped "latest" leaves its bare-hex alias
// alone in its group, reclassifying it from "this is latest's own digest"
// to "orphan". A caller that must not reason from a partial view checks
// Dropped; a caller that just wants to render a list ignores it, or uses
// Tags. Crucially, Dropped is derived from the SAME resolution pass as
// Groups, so no two calls, layers or caches can disagree about whether a
// given listing was complete.
type TagListing struct {
	Groups  []TagGroup
	Dropped []DroppedTag
}

// Client is the surface this package exposes: read-only browsing plus the
// one mutating call, Delete, that a prune job needs to reclaim registry
// storage.
type Client interface {
	Catalog(ctx context.Context) ([]string, error)
	Tags(ctx context.Context, repo string) ([]TagGroup, error)
	// ResolveTags is Tags plus the evidence of what it could not resolve.
	// Prefer it over Tags anywhere a partial listing would be acted on.
	ResolveTags(ctx context.Context, repo string) (TagListing, error)
	TagCount(ctx context.Context, repo string) (int, error)
	Manifest(ctx context.Context, repo, ref string) (Manifest, error)

	// Delete removes the manifest at repo/digest. digest must be
	// digest-form (sha256:...), never a tag: deleting by tag would remove
	// the manifest for every tag that currently shares that digest.
	Delete(ctx context.Context, repo, digest string) error
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
//
// It also carries the legacy schema1 type and the OCI artifact type, because
// distribution answers 404 MANIFEST_UNKNOWN — indistinguishable from "this tag
// does not exist" — when it holds a manifest whose media type no entry here
// matches. The fleet's registry mirrors third-party images (espressif/idf,
// koalaman/shellcheck, jioworldcentre/web) whose older tags are still schema1,
// so an Accept set that omits them turns a present tag into a phantom 404.
const manifestAccept = "application/vnd.docker.distribution.manifest.v2+json, application/vnd.oci.image.manifest.v1+json, application/vnd.docker.distribution.manifest.list.v2+json, application/vnd.oci.image.index.v1+json, application/vnd.docker.distribution.manifest.v1+json, application/vnd.oci.artifact.manifest.v1+json"

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
	if m.Digest == "" {
		// Symmetrical with manifestDigest's guard. A 200 with no
		// Docker-Content-Digest is not an answer we can use: it would group
		// this tag under the empty string, and a caller that deletes would
		// then hold a candidate whose "digest" is "" — eligible to reach
		// Delete(repo, ""). Every conforming registry sets the header; the
		// guard costs nothing and closes the direction that ends in a request
		// nobody meant to make.
		return Manifest{}, fmt.Errorf("manifest %s/%s: no Docker-Content-Digest header: %w", repo, ref, ErrUnreachable)
	}
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

// manifestDigest resolves ref to its content digest and NOTHING else, using
// HEAD with the SAME explicit manifestAccept string the GET path sends.
//
// It exists as the last resort before a tag is written off as unresolvable:
// the full Manifest GET also decodes a body, and a body this client cannot
// decode is an error about the body, not about the tag. HEAD skips that
// entirely, and Docker-Content-Digest is all that protecting a digest from
// deletion requires — so a tag resolved this way is protected, at the cost of
// a zero Created (which callers that act on age already treat as unknown).
//
// Sending `Accept: */*` here — the obvious way to write "give me whatever you
// have" — is WRONG, and measurably so. distribution's manifest handler parses
// Accept by exact media-type string into its supported set; `*/*` matches
// nothing, so the wildcard is NARROWER than the explicit list, not broader.
// Measured against real registries (an ordinary image pushed --format oci):
//
//	2.8.3  HEAD /v2/probe/manifests/oci  Accept: */*             -> 404
//	2.8.3  HEAD /v2/probe/manifests/oci  Accept: manifestAccept  -> 200 sha256:4ecaff1a…
//	3.1.1  HEAD /v2/probe/manifests/oci  Accept: */*             -> 404
//
// Worse, on 2.8.x a schema2 manifest requested with an Accept set that omits
// schema2 is DOWN-CONVERTED to schema1 and answered with the converted
// manifest's Docker-Content-Digest — a digest that exists nowhere in storage:
//
//	2.8.3  HEAD .../manifests/v2  Accept: */*             -> sha256:05853901…  (fabricated)
//	2.8.3  HEAD .../manifests/v2  Accept: manifestAccept  -> sha256:7c76b20c…  (real)
//
// A fabricated digest groups a tag away from its real one, so the real digest
// loses that tag's protection and becomes a delete candidate. Never widen
// this to a wildcard.
func (c *HTTPClient) manifestDigest(ctx context.Context, repo, ref string) (string, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodHead,
		fmt.Sprintf("%s/v2/%s/manifests/%s", c.BaseURL, repo, ref), nil)
	if err != nil {
		return "", err
	}
	req.Header.Set("Accept", manifestAccept)
	if c.Auth.Mode == "basic" {
		req.SetBasicAuth(c.Auth.Username, c.Auth.Password)
	}
	resp, err := c.HTTP.Do(req)
	if err != nil {
		return "", fmt.Errorf("registry manifest HEAD %s/%s: %w: %w", repo, ref, ErrUnreachable, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusNotFound {
		return "", fmt.Errorf("manifest HEAD %s/%s: not found: %w", repo, ref, ErrNotFound)
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return "", fmt.Errorf("registry manifest HEAD %s/%s: status %d: %w", repo, ref, resp.StatusCode, ErrUnreachable)
	}
	d := resp.Header.Get("Docker-Content-Digest")
	if d == "" {
		// A 2xx with no digest header is not an answer we can use, and
		// treating it as "gone" would be the unsafe direction.
		return "", fmt.Errorf("manifest HEAD %s/%s: no Docker-Content-Digest header: %w", repo, ref, ErrUnreachable)
	}
	return d, nil
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

// Tags is ResolveTags' groups-only view, for callers that only render a
// listing. Anything that DELETES must use ResolveTags and check Dropped.
func (c *HTTPClient) Tags(ctx context.Context, repo string) ([]TagGroup, error) {
	listing, err := c.ResolveTags(ctx, repo)
	if err != nil {
		return nil, err
	}
	return listing.Groups, nil
}

// ResolveTags lists every tag in repo, resolves each to its manifest digest,
// and groups tags that share a digest into one TagGroup. Each unique digest's
// config blob is fetched once (not once per tag) for its Created timestamp.
// Manifest and blob-created lookups both run with bounded concurrency
// (tagResolveConcurrency), not sequentially. Groups are sorted
// newest-Created-first.
//
// A tag whose manifest cannot be resolved is skipped rather than failing the
// whole call — and reported in TagListing.Dropped, so a caller that cannot
// act on a partial view can tell "this repo has one tag" from "this repo has
// two tags and we could only see one".
func (c *HTTPClient) ResolveTags(ctx context.Context, repo string) (TagListing, error) {
	tagNames, err := c.listTags(ctx, repo)
	if err != nil {
		return TagListing{}, err
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
	var dropped []DroppedTag
	for _, r := range resolutions {
		if r.err != nil {
			// Before writing a tag off, ask for its digest alone (HEAD, same
			// Accept). The full GET also decodes a body, and a body this
			// client cannot decode says nothing about whether the tag exists.
			// For a caller that only needs to know which digests are reachable
			// by tag, the digest IS the answer. Drops are rare, so this runs
			// serially here rather than adding a second concurrent phase.
			if d, herr := c.manifestDigest(ctx, repo, r.tag); herr == nil {
				r.manifest = Manifest{Digest: d} // no config blob: Created stays zero
			} else {
				// Neither the GET's nor the HEAD's status can establish
				// absence. Both are content-negotiated requests, and
				// distribution answers a negotiation it cannot satisfy with
				// the same 404 MANIFEST_UNKNOWN it uses for a tag that was
				// never there. Deriving "gone" from either is how a live image
				// gets deleted.
				//
				// So ask the one endpoint that negotiates nothing. The tag
				// list is the registry's exact statement of which tags exist;
				// re-read it, and call the tag vanished ONLY if it is no
				// longer in it. One extra call per drop, and drops are rare.
				//
				// Fail closed in both other directions: a tag still listed, or
				// a listing we could not read, is an unresolvable tag, not a
				// gone one.
				vanished := false
				var derr error
				listed, lerr := c.tagStillListed(ctx, repo, r.tag)
				switch {
				case lerr != nil:
					derr = fmt.Errorf("%w: could not re-read the tag list to tell whether %s/%s still exists: %w",
						ErrUnreachable, repo, r.tag, lerr)
				case listed:
					derr = fmt.Errorf("%w: %s/%s is still listed but its manifest could not be read (GET: %v; digest-only HEAD: %v)",
						ErrUnreachable, repo, r.tag, r.err, herr)
				default:
					vanished = true
					derr = fmt.Errorf("%w: %s/%s is no longer in the repo's tag list (GET: %v; digest-only HEAD: %v)",
						ErrNotFound, repo, r.tag, r.err, herr)
				}
				dropped = append(dropped, DroppedTag{Tag: r.tag, Vanished: vanished, Err: derr})
				continue
			}
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
				// dropping the whole digest group. NOTE this is NOT reported
				// anywhere — unlike an unresolvable tag, which lands in
				// Dropped, a failed config-blob fetch is indistinguishable
				// from a multi-arch index (which genuinely has no config
				// blob) once this returns. A caller that acts on Created must
				// therefore treat zero as "age unknown" and decline to act,
				// rather than as "old".
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
	return TagListing{Groups: groups, Dropped: dropped}, nil
}

// TagCount returns how many tags repo has, without resolving any of them.
// Deliberately NOT implemented as len(Tags(...)): Tags resolves every tag's
// manifest and every unique digest's config blob (~21s for the fleet's
// "engine" repo), while the repo-list page calls this once per repo on a
// single page load. One tags-list call — paginated, but nothing more — is
// the whole point.
//
// Note this counts tags, not unique digests, so it can exceed len(Tags(...))
// when several tags share a digest. That is the intended meaning for a
// "how many tags does this repo have" column.
func (c *HTTPClient) TagCount(ctx context.Context, repo string) (int, error) {
	tags, err := c.listTags(ctx, repo)
	if err != nil {
		return 0, err
	}
	return len(tags), nil
}

// tagStillListed re-reads /v2/<repo>/tags/list and reports whether tag is
// still in it.
//
// This is the ONLY absence signal this package trusts. /v2/<repo>/tags/list
// negotiates no media type: it is a JSON array of names, identical for a
// schema1 manifest, an OCI image, an OCI artifact and a multi-arch index. A
// manifest endpoint's 404 conflates "no such tag" with "no representation you
// asked for"; this endpoint cannot.
//
// An error is returned as an error, never as absence — a tag list we could not
// read is not evidence that a tag is gone.
func (c *HTTPClient) tagStillListed(ctx context.Context, repo, tag string) (bool, error) {
	tags, err := c.listTags(ctx, repo)
	if err != nil {
		return false, err
	}
	for _, t := range tags {
		if t == tag {
			return true, nil
		}
	}
	return false, nil
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

// Delete issues Registry v2 DELETE /v2/<repo>/manifests/<digest>. digest
// must be digest-form (validated by ValidRef + IsDigestRef); a tag-form or
// otherwise invalid ref is rejected before any request is constructed —
// deleting by tag would remove the manifest for every tag sharing that
// digest. Success is any 2xx (the spec documents 202); a genuine 404 is
// ErrNotFound, anything else non-2xx or a transport failure is
// ErrUnreachable — the two must never collapse into each other.
func (c *HTTPClient) Delete(ctx context.Context, repo, digest string) error {
	if !ValidRepoName(repo) {
		return fmt.Errorf("delete %s/%s: invalid repo name", repo, digest)
	}
	if !ValidRef(digest) || !IsDigestRef(digest) {
		return fmt.Errorf("delete %s/%s: ref must be digest-form (sha256:...), not a tag", repo, digest)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodDelete,
		fmt.Sprintf("%s/v2/%s/manifests/%s", c.BaseURL, repo, digest), nil)
	if err != nil {
		return err
	}
	if c.Auth.Mode == "basic" {
		req.SetBasicAuth(c.Auth.Username, c.Auth.Password)
	}
	resp, err := c.HTTP.Do(req)
	if err != nil {
		return fmt.Errorf("registry delete %s/%s: %w: %w", repo, digest, ErrUnreachable, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusNotFound {
		return fmt.Errorf("delete %s/%s: not found: %w", repo, digest, ErrNotFound)
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("registry delete %s/%s: status %d: %w", repo, digest, resp.StatusCode, ErrUnreachable)
	}
	return nil
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

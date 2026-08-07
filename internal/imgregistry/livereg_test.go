//go:build livereg

// Reproduction harness for the two facts this package's manifest handling
// rests on, verified against real `distribution` servers rather than fakes.
// Build-tagged, so `make test` never runs it; stand the registries up and run:
//
//	podman run -d --name reg283 -p 5999:5000 docker.io/library/registry:2.8.3
//	podman run -d --name reg311 -p 5998:5000 docker.io/library/registry:3.1.1
//	printf 'FROM scratch\nCOPY hello.txt /hello.txt\n' > Dockerfile; echo hi > hello.txt
//	podman build -t probe:local .
//	for R in 5999 5998; do
//	  podman push --tls-verify=false --format docker localhost/probe:local 127.0.0.1:$R/probe:v2
//	  podman push --tls-verify=false --format oci    localhost/probe:local 127.0.0.1:$R/probe:oci
//	  podman push --tls-verify=false --format docker localhost/probe:local 127.0.0.1:$R/live:latest
//	  podman push --tls-verify=false --format docker localhost/probe:local 127.0.0.1:$R/live:abc1234
//	done
//	go test -tags "containers_image_openpgp exclude_graphdriver_btrfs exclude_graphdriver_devicemapper livereg" \
//	    -run LiveReg -v ./internal/imgregistry/
//
// Measured output, 2026-08-07 (see the -v log lines):
//
//	2.8.3  manifestDigest(probe:oci) [manifestAccept] -> sha256:4ecaff1a…
//	2.8.3  HEAD probe:oci  [*/*]                      -> 404 (no digest)
//	3.1.1  HEAD probe:oci  [*/*]                      -> 404 (no digest)
//	2.8.3  manifestDigest(probe:v2)  [manifestAccept] -> sha256:7c76b20c…  (real)
//	2.8.3  HEAD probe:v2   [*/*]                      -> 200 sha256:05853901…  (schema1 down-convert; in no storage)
//
// i.e. `Accept: */*` is narrower than the explicit list AND can fabricate a
// digest. Hence manifestDigest sends manifestAccept, and absence is decided by
// tags/list, never by a manifest 404.
package imgregistry

import (
	"context"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
	"testing"
)

var liveRegs = map[string]string{"2.8.3": "http://127.0.0.1:5999", "3.1.1": "http://127.0.0.1:5998"}

// (a) HEAD with manifestAccept resolves an OCI manifest that `Accept: */*`
// 404s, and returns the REAL digest for a schema2 manifest where `*/*` returns
// a fabricated down-converted one.
func TestLiveReg_ManifestDigestAccept(t *testing.T) {
	for ver, base := range liveRegs {
		c := NewHTTPClient(base, Auth{Mode: "none"})
		for _, tag := range []string{"oci", "v2"} {
			got, err := c.manifestDigest(context.Background(), "probe", tag)
			t.Logf("%s  manifestDigest(probe:%s) [manifestAccept] -> %q err=%v", ver, tag, got, err)

			// The same request with the wildcard, for contrast.
			req, _ := http.NewRequest(http.MethodHead, base+"/v2/probe/manifests/"+tag, nil)
			req.Header.Set("Accept", "*/*")
			resp, err := http.DefaultClient.Do(req)
			if err != nil {
				t.Fatal(err)
			}
			resp.Body.Close()
			t.Logf("%s  HEAD probe:%s [*/*]           -> %d %q", ver, tag, resp.StatusCode, resp.Header.Get("Docker-Content-Digest"))
		}
	}
}

// (b) End-to-end: a manifest GET+HEAD that 502s for `latest` only. The tag is
// still in the registry's tag list, so it must drop NOT-vanished (the run
// aborts) — and `latest` must never be silently absent from Groups.
func TestLiveReg_ResolveTagsFailingManifest(t *testing.T) {
	for ver, base := range liveRegs {
		u, _ := url.Parse(base)
		proxy := httputil.NewSingleHostReverseProxy(u)
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path == "/v2/live/manifests/latest" {
				http.Error(w, "bad gateway", http.StatusBadGateway)
				return
			}
			proxy.ServeHTTP(w, r)
		}))
		listing, err := NewHTTPClient(srv.URL, Auth{Mode: "none"}).ResolveTags(context.Background(), "live")
		srv.Close()
		if err != nil {
			t.Fatalf("%s: %v", ver, err)
		}
		t.Logf("%s  502-on-latest: groups=%+v", ver, listing.Groups)
		for _, d := range listing.Dropped {
			t.Logf("%s  502-on-latest: dropped tag=%s vanished=%v err=%v", ver, d.Tag, d.Vanished, d.Err)
			if d.Vanished {
				t.Fatalf("%s: a tag still in tags/list must not be vanished", ver)
			}
		}
		if len(listing.Dropped) != 1 {
			t.Fatalf("%s: want exactly one drop, got %+v", ver, listing.Dropped)
		}
	}
}

// (b) The other direction: the tag really is gone from tags/list. Proxy 502s
// the manifest for `deleted` and rewrites tags/list to omit it.
func TestLiveReg_ResolveTagsVanishedTag(t *testing.T) {
	for ver, base := range liveRegs {
		u, _ := url.Parse(base)
		proxy := httputil.NewSingleHostReverseProxy(u)
		first := true
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			switch {
			case r.URL.Path == "/v2/live/manifests/latest":
				http.Error(w, "bad gateway", http.StatusBadGateway)
			case r.URL.Path == "/v2/live/tags/list" && !first:
				// Second read: the registry no longer lists it (what a real
				// concurrent `DELETE /v2/live/manifests/<digest>` produces —
				// measured separately on both versions).
				w.Write([]byte(`{"name":"live","tags":["abc1234"]}`))
			default:
				first = false
				proxy.ServeHTTP(w, r)
			}
		}))
		listing, err := NewHTTPClient(srv.URL, Auth{Mode: "none"}).ResolveTags(context.Background(), "live")
		srv.Close()
		if err != nil {
			t.Fatalf("%s: %v", ver, err)
		}
		for _, d := range listing.Dropped {
			t.Logf("%s  delisted: dropped tag=%s vanished=%v err=%v", ver, d.Tag, d.Vanished, d.Err)
			if !d.Vanished {
				t.Fatalf("%s: a tag absent from tags/list must be vanished", ver)
			}
		}
		if len(listing.Dropped) != 1 {
			t.Fatalf("%s: want exactly one drop, got %+v", ver, listing.Dropped)
		}
	}
}

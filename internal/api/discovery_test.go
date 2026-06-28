package api_test

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/iotready/podman-api/internal/api"
	"github.com/iotready/podman-api/internal/auth"
)

func newDiscoveryRouter(version string) http.Handler {
	return api.NewRouter(nil, nil, auth.NewKeyStore(nil), nil, nil, nil, version)
}

func TestMCPDiscovery(t *testing.T) {
	srv := httptest.NewServer(newDiscoveryRouter("v1.2.3"))
	defer srv.Close()

	resp, err := http.Get(srv.URL + "/mcp")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("want 200, got %d", resp.StatusCode)
	}
	if ct := resp.Header.Get("Content-Type"); !strings.HasPrefix(ct, "application/json") {
		t.Fatalf("want application/json, got %q", ct)
	}
	var doc map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&doc); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if doc["protocol_version"] == nil {
		t.Error("missing protocol_version")
	}
	server, ok := doc["server"].(map[string]any)
	if !ok {
		t.Fatal("missing server object")
	}
	if server["version"] != "v1.2.3" {
		t.Errorf("want v1.2.3, got %v", server["version"])
	}
	if doc["auth"] == nil {
		t.Error("missing auth")
	}
	refs, ok := doc["references"].(map[string]any)
	if !ok {
		t.Fatal("missing references")
	}
	if refs["openapi"] != "/openapi.yaml" {
		t.Errorf("want /openapi.yaml, got %v", refs["openapi"])
	}
	if refs["agent_docs"] != "/agent-docs" {
		t.Errorf("want /agent-docs, got %v", refs["agent_docs"])
	}
}

func TestAgentDocs(t *testing.T) {
	srv := httptest.NewServer(newDiscoveryRouter("dev"))
	defer srv.Close()

	resp, err := http.Get(srv.URL + "/agent-docs")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("want 200, got %d", resp.StatusCode)
	}
	body, _ := io.ReadAll(resp.Body)
	s := string(body)
	for _, want := range []string{"Authentication", "Bearer", "instance", "template", "slug", "/openapi.yaml"} {
		if !strings.Contains(s, want) {
			t.Errorf("agent-docs missing %q", want)
		}
	}
}

func TestDiscoveryNoAuth(t *testing.T) {
	srv := httptest.NewServer(newDiscoveryRouter("dev"))
	defer srv.Close()
	for _, path := range []string{"/mcp", "/agent-docs"} {
		resp, err := http.Get(srv.URL + path)
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			t.Errorf("%s: want 200 without auth, got %d", path, resp.StatusCode)
		}
	}
}

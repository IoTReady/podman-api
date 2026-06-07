package ui

import (
	"context"
	"net/http"
	"strings"
	"testing"

	"github.com/iotready/podman-api/internal/config"
	"github.com/iotready/podman-api/internal/instance"
	"github.com/iotready/podman-api/internal/podman"
	"github.com/iotready/podman-api/internal/podman/fake"
	"github.com/iotready/podman-api/internal/render"
	"github.com/iotready/podman-api/internal/store"
)

func TestResolveContainerSuffix(t *testing.T) {
	names := []string{"postgres-main-db", "postgres-main-sidecar"}
	if got := resolveContainerSuffix("postgres", "main", names); got != "db" {
		t.Fatalf("got %q, want %q", got, "db")
	}
}

func TestResolveContainerSuffixSkipsNonPrefixed(t *testing.T) {
	names := []string{"pause", "infra", "postgres-main-app"}
	if got := resolveContainerSuffix("postgres", "main", names); got != "app" {
		t.Fatalf("got %q, want %q", got, "app")
	}
}

func TestResolveContainerSuffixNoMatch(t *testing.T) {
	names := []string{"pause"}
	if got := resolveContainerSuffix("postgres", "main", names); got != "" {
		t.Fatalf("got %q, want empty string", got)
	}
}

func TestLogsPageRedirectsWhenContainerAbsent(t *testing.T) {
	u := uiWithSeededInstance(t)
	// uiWithSeededInstance seeds container "postgres-main-db"; suffix = "db"
	w := authedGet(t, u, "/ui/hosts/edge-1/instances/postgres/main/logs")
	if w.Code != http.StatusFound {
		t.Fatalf("status = %d, want 302 redirect", w.Code)
	}
	loc := w.Header().Get("Location")
	if !strings.Contains(loc, "container=db") {
		t.Errorf("Location %q should contain container=db", loc)
	}
	if !strings.Contains(loc, "follow=true") {
		t.Errorf("Location %q should contain follow=true", loc)
	}
}

func TestLogsPageStaticRenderShowsLines(t *testing.T) {
	u := uiWithSeededInstance(t)
	// ?container=db&follow=false — static 200-line snapshot
	w := authedGet(t, u, "/ui/hosts/edge-1/instances/postgres/main/logs?container=db&follow=false")
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", w.Code)
	}
	if !strings.Contains(w.Body.String(), "database system is ready") {
		t.Error("static logs page should contain the seeded log line")
	}
}

func TestLogsPageFollowRenderHasSSEConnect(t *testing.T) {
	u := uiWithSeededInstance(t)
	// ?container=db&follow=true — SSE streaming mode
	w := authedGet(t, u, "/ui/hosts/edge-1/instances/postgres/main/logs?container=db&follow=true")
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", w.Code)
	}
	if !strings.Contains(w.Body.String(), "sse-connect") {
		t.Error("follow mode page should contain sse-connect attribute")
	}
}

func TestLogsPageNotFoundReturns404(t *testing.T) {
	u := uiWithSeededInstance(t)
	w := authedGet(t, u, "/ui/hosts/edge-1/instances/postgres/ghost/logs?container=db")
	if w.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want 404 for unknown slug", w.Code)
	}
}

func TestLogsStreamSSEHeaders(t *testing.T) {
	u := uiWithSeededInstance(t)
	w := authedGet(t, u, "/ui/hosts/edge-1/instances/postgres/main/logs/stream?container=db")
	if ct := w.Header().Get("Content-Type"); ct != "text/event-stream" {
		t.Errorf("Content-Type = %q, want text/event-stream", ct)
	}
	if !strings.Contains(w.Body.String(), "event: log") {
		t.Error("SSE stream should contain 'event: log' lines")
	}
}

func TestLogsStreamHTMLEscapesLogLines(t *testing.T) {
	fc := fake.New()
	hosts := []config.Host{{ID: "edge-1"}}
	fc.AddPod("edge-1", podman.Pod{
		Name: "postgres-main",
		Containers: []podman.Container{
			{Name: "postgres-main-db", Image: "postgres:16", Status: "Running"},
		},
	})
	fc.LogLines = []podman.LogLine{{Container: "postgres-main-db", Line: `<script>alert(1)</script>`}}
	mem := store.NewMemory()
	_ = mem.PutTemplate(context.Background(), store.Template{Meta: render.Meta{ID: "postgres"}})
	svc := instance.NewService(fc, hosts)
	svc.SetStore(mem)
	hash, _ := config.HashToken("pw")
	u, err := New(Config{
		Svc:  svc,
		Auth: NewOperatorAuthenticator(config.Operator{Username: "op", PasswordHash: hash}),
	})
	if err != nil {
		t.Fatal(err)
	}

	w := authedGet(t, u, "/ui/hosts/edge-1/instances/postgres/main/logs/stream?container=db")
	body := w.Body.String()
	if strings.Contains(body, "<script>") {
		t.Error("log line must be HTML-escaped; raw <script> tag found in SSE output")
	}
	if !strings.Contains(body, "&lt;script&gt;") {
		t.Error("log line should appear as HTML-escaped &lt;script&gt; in SSE output")
	}
}

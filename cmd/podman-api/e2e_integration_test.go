//go:build integration

package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/iotready/podman-api/internal/api"
	"github.com/iotready/podman-api/internal/auth"
	"github.com/iotready/podman-api/internal/config"
	"github.com/iotready/podman-api/internal/instance"
	"github.com/iotready/podman-api/internal/podman"
	"github.com/iotready/podman-api/internal/render"
)

const e2eTemplate = `# template-meta:
#   id: e2e
#   parameters:
#     required: [slug, image, port]
#   secrets:
#     per_instance: []
#     per_host_referenced: []
#   volumes: []
---
apiVersion: v1
kind: Pod
metadata:
  name: e2e-{{.slug}}
  labels:
    podman-api/template: e2e
    podman-api/slug: {{.slug}}
spec:
  restartPolicy: Never
  containers:
    - name: c
      image: {{.image}}
      command: ["sleep", "60"]
`

func localSock(t *testing.T) string {
	t.Helper()
	rt := os.Getenv("XDG_RUNTIME_DIR")
	if rt == "" {
		t.Skip("XDG_RUNTIME_DIR unset")
	}
	p := filepath.Join(rt, "podman", "podman.sock")
	if _, err := os.Stat(p); err != nil {
		t.Skip("local podman socket not available: " + err.Error())
	}
	return p
}

func TestE2E_FullLifecycle_LocalOnly(t *testing.T) {
	sock := localSock(t)

	hosts := []config.Host{{ID: "local", Addr: "unix", Socket: sock}}
	meta, body, err := render.ParseMeta(e2eTemplate)
	require.NoError(t, err)
	tmpls := []config.Template{{Meta: meta, Body: body, Source: "e2e.yaml"}}

	client, err := podman.NewReal(hosts)
	require.NoError(t, err)

	tok := "e2etoken"
	hash, _ := config.HashToken(tok)
	keys := []config.APIKey{{ID: "e2e", SecretHash: hash, Scopes: []string{"instances:*", "hosts:read"}}}
	svc := instance.NewService(client, hosts, tmpls)
	r := api.NewRouter(svc, nil, auth.NewKeyStore(keys), nil, nil)
	srv := httptest.NewServer(r)
	defer srv.Close()

	t.Cleanup(func() {
		_ = client.PodRemove(context.Background(), "local", "e2e-itest", true)
	})

	do := func(method, path, body string) (*http.Response, error) {
		req, _ := http.NewRequest(method, srv.URL+path, bytes.NewBufferString(body))
		req.Header.Set("Authorization", "Bearer "+tok)
		req.Header.Set("Content-Type", "application/json")
		return http.DefaultClient.Do(req)
	}

	// CREATE
	body2 := `{"template":"e2e","slug":"itest","parameters":{"slug":"itest","image":"docker.io/library/alpine:latest","port":31999}}`
	resp, err := do("PUT", "/hosts/local/instances/e2e/itest", body2)
	require.NoError(t, err)
	resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)

	// Wait for Running.
	require.Eventually(t, func() bool {
		r, err := do("GET", "/hosts/local/instances/e2e/itest", "")
		if err != nil {
			return false
		}
		defer r.Body.Close()
		if r.StatusCode != 200 {
			return false
		}
		var got map[string]any
		_ = json.NewDecoder(r.Body).Decode(&got)
		pod, _ := got["pod"].(map[string]any)
		return pod["status"] == "Running"
	}, 30*time.Second, 500*time.Millisecond)

	// STOP
	resp, _ = do("POST", "/hosts/local/instances/e2e/itest/stop", "")
	resp.Body.Close()
	assert.Equal(t, http.StatusNoContent, resp.StatusCode)

	// START
	resp, _ = do("POST", "/hosts/local/instances/e2e/itest/start", "")
	resp.Body.Close()
	assert.Equal(t, http.StatusNoContent, resp.StatusCode)

	// DELETE
	resp, _ = do("DELETE", "/hosts/local/instances/e2e/itest", "")
	resp.Body.Close()
	assert.Equal(t, http.StatusNoContent, resp.StatusCode)

	// GET → 404
	resp, _ = do("GET", "/hosts/local/instances/e2e/itest", "")
	resp.Body.Close()
	assert.Equal(t, http.StatusNotFound, resp.StatusCode)

	// Sanity: no orphan pod.
	_, err = client.PodInspect(context.Background(), "local", "e2e-itest")
	require.ErrorIs(t, err, podman.ErrNotFound, fmt.Sprintf("unexpected: %v", err))
}

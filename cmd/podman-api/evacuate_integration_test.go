//go:build integration

package main

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
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
	backuppkg "github.com/iotready/podman-api/internal/backup"
	"github.com/iotready/podman-api/internal/config"
	"github.com/iotready/podman-api/internal/evacuate"
	"github.com/iotready/podman-api/internal/instance"
	"github.com/iotready/podman-api/internal/jobs"
	"github.com/iotready/podman-api/internal/migrate"
	"github.com/iotready/podman-api/internal/obs"
	"github.com/iotready/podman-api/internal/podman"
	"github.com/iotready/podman-api/internal/prune"
	"github.com/iotready/podman-api/internal/render"
	"github.com/iotready/podman-api/internal/store"
	"github.com/prometheus/client_golang/prometheus"
)

// evacuateTemplate is a simple web-shaped template used by the evacuate e2e test.
// Two templates are defined: one SQLite-backed (web-sql) and one non-SQLite (web)
// so the copy-time-manifest verify path is exercised alongside a simpler instance.
const evacuateTemplateWeb = `# template-meta:
#   id: web
#   parameters:
#     required: [slug, image, port]
#   secrets:
#     per_instance: []
#     per_host_referenced: []
#   volumes:
#     - name: data
#       backup: none
---
apiVersion: v1
kind: Pod
metadata:
  name: web-{{.slug}}
  labels:
    podman-api/template: web
    podman-api/slug: {{.slug}}
spec:
  restartPolicy: Never
  containers:
    - name: c
      image: {{.image}}
      command: ["sleep", "600"]
      volumeMounts:
        - name: data
          mountPath: /data
  volumes:
    - name: data
      persistentVolumeClaim:
        claimName: web-{{.slug}}-data
`

// evacuateTemplateSQL is the same but also carries a SQLite db file in the
// volume, so the copy-time-manifest verify path is exercised (#153).
const evacuateTemplateSQL = `# template-meta:
#   id: web-sql
#   parameters:
#     required: [slug, image, port]
#   secrets:
#     per_instance: []
#     per_host_referenced: []
#   volumes:
#     - name: data
#       backup: none
---
apiVersion: v1
kind: Pod
metadata:
  name: web-sql-{{.slug}}
  labels:
    podman-api/template: web-sql
    podman-api/slug: {{.slug}}
spec:
  restartPolicy: Never
  containers:
    - name: c
      image: {{.image}}
      command: ["sleep", "600"]
      volumeMounts:
        - name: data
          mountPath: /data
  volumes:
    - name: data
      persistentVolumeClaim:
        claimName: web-sql-{{.slug}}-data
`

// sock2 returns the path to the second podman socket used as the evacuate
// destination. Set PODMAN_API_ITEST_SOCK2 to override; defaults to
// /tmp/podman2/podman2.sock.
func sock2(t *testing.T) string {
	t.Helper()
	p := os.Getenv("PODMAN_API_ITEST_SOCK2")
	if p == "" {
		p = "/tmp/podman2/podman2.sock"
	}
	if _, err := os.Stat(p); err != nil {
		t.Skipf("second podman socket not available at %s: %v", p, err)
	}
	return p
}

func TestEvacuate_TwoSockets_LocalOnly(t *testing.T) {
	srcSock := localSock(t)
	dstSock := sock2(t)

	hosts := []config.Host{
		{ID: "src", Addr: "unix", Socket: srcSock},
		{ID: "dst", Addr: "unix", Socket: dstSock},
	}
	client, err := podman.NewReal(hosts)
	require.NoError(t, err)

	ctx := context.Background()

	metaWeb, bodyWeb, err := render.ParseMeta(evacuateTemplateWeb)
	require.NoError(t, err)
	metaSQL, bodySQL, err := render.ParseMeta(evacuateTemplateSQL)
	require.NoError(t, err)

	db, err := store.OpenSQLite(filepath.Join(t.TempDir(), "state.db"), nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	require.NoError(t, db.PutTemplate(ctx, store.Template{Meta: metaWeb, Body: bodyWeb, Origin: "seed"}))
	require.NoError(t, db.PutTemplate(ctx, store.Template{Meta: metaSQL, Body: bodySQL, Origin: "seed"}))

	svc := instance.NewService(client, hosts)
	svc.SetStore(db)
	svc.SetVerifyVolumes(true) // exercise the copy-time-manifest verify path

	pruneMetrics := obs.NewPruneMetrics(prometheus.NewRegistry())
	jobMetrics := obs.NewJobMetrics(prometheus.NewRegistry())
	registry := jobs.Registry{
		"migrate":  &migrate.Handler{Svc: svc, Metrics: jobMetrics},
		"evacuate": &evacuate.Handler{Svc: svc, Jobs: db, Concurrency: 2, Metrics: jobMetrics},
		"prune":    &prune.Handler{Client: client, Jobs: db, Metrics: pruneMetrics},
		"backup":   &backuppkg.Handler{Svc: svc},
		"restore":  &backuppkg.RestoreHandler{Svc: svc},
	}
	reconcilers := jobs.Reconcilers{
		"migrate": &migrate.Reconciler{Svc: svc},
		"backup":  &backuppkg.Reconciler{Svc: svc},
	}
	runner := jobs.NewRunner(db, registry, jobs.DefaultWorkers)
	runner.SetReconcilers(reconcilers)
	runnerCtx, cancelRunner := context.WithCancel(ctx)
	t.Cleanup(cancelRunner)
	runner.Start(runnerCtx)

	tok := "evactoken"
	hash, _ := config.HashToken(tok)
	keys := []config.APIKey{{ID: "evac", SecretHash: hash, Scopes: []string{"instances:*", "hosts:read", "jobs:read"}}}
	r := api.NewRouter(svc, db, auth.NewKeyStore(keys), nil, nil, runner)
	srv := httptest.NewServer(r)
	defer srv.Close()

	t.Cleanup(func() {
		_ = client.PodRemove(ctx, "src", "web-app1", true)
		_ = client.PodRemove(ctx, "src", "web-sql-db1", true)
		_ = client.PodRemove(ctx, "dst", "web-app1", true)
		_ = client.PodRemove(ctx, "dst", "web-sql-db1", true)
		_ = client.VolumeRemove(ctx, "src", "web-app1-data", true)
		_ = client.VolumeRemove(ctx, "src", "web-sql-db1-data", true)
		_ = client.VolumeRemove(ctx, "dst", "web-app1-data", true)
		_ = client.VolumeRemove(ctx, "dst", "web-sql-db1-data", true)
	})

	do := func(method, path, reqBody string) (*http.Response, error) {
		req, _ := http.NewRequest(method, srv.URL+path, bytes.NewBufferString(reqBody))
		req.Header.Set("Authorization", "Bearer "+tok)
		req.Header.Set("Content-Type", "application/json")
		return http.DefaultClient.Do(req)
	}

	// waitRunning polls the instance via the API until its pod reports Running.
	waitRunning := func(host, tmpl, slug string) {
		t.Helper()
		require.Eventually(t, func() bool {
			resp, err := do("GET", "/hosts/"+host+"/instances/"+tmpl+"/"+slug, "")
			if err != nil {
				return false
			}
			defer resp.Body.Close()
			if resp.StatusCode != http.StatusOK {
				return false
			}
			var got map[string]any
			_ = json.NewDecoder(resp.Body).Decode(&got)
			pod, _ := got["pod"].(map[string]any)
			return pod["status"] == "Running"
		}, 60*time.Second, 500*time.Millisecond)
	}

	// waitJob polls GET /jobs/{id} until terminal; fails the test if not succeeded.
	waitJob := func(id string) {
		t.Helper()
		deadline := time.Now().Add(60 * time.Second)
		for {
			resp, err := do("GET", "/jobs/"+id, "")
			require.NoError(t, err)
			body, err := io.ReadAll(resp.Body)
			resp.Body.Close()
			require.NoError(t, err)
			var jv struct {
				State string `json:"state"`
				Error string `json:"error"`
			}
			require.NoError(t, json.Unmarshal(body, &jv))
			switch jv.State {
			case "succeeded":
				return
			case "failed", "canceled":
				t.Fatalf("job %s ended %s: %s", id, jv.State, jv.Error)
			}
			if time.Now().After(deadline) {
				t.Fatalf("job %s did not finish within 60s (last state %q)", id, jv.State)
			}
			time.Sleep(500 * time.Millisecond)
		}
	}

	// DEPLOY two instances on src: a non-SQLite one (web/app1) and a SQLite-backed
	// one (web-sql/db1), so both the simple copy path and the manifest-verify path
	// are exercised.
	createBody := `{"template":"web","slug":"app1","parameters":{"slug":"app1","image":"docker.io/library/alpine:latest","port":31997}}`
	resp, err := do("POST", "/hosts/src/instances", createBody)
	require.NoError(t, err)
	resp.Body.Close()
	require.Equal(t, http.StatusCreated, resp.StatusCode)
	waitRunning("src", "web", "app1")

	createBody2 := `{"template":"web-sql","slug":"db1","parameters":{"slug":"db1","image":"docker.io/library/alpine:latest","port":31996}}`
	resp, err = do("POST", "/hosts/src/instances", createBody2)
	require.NoError(t, err)
	resp.Body.Close()
	require.Equal(t, http.StatusCreated, resp.StatusCode)
	waitRunning("src", "web-sql", "db1")

	// EVACUATE from src → dst.
	movesBody := `{"from_host":"src","moves":[{"template":"web","slug":"app1","to_host":"dst"},{"template":"web-sql","slug":"db1","to_host":"dst"}]}`
	resp, err = do("POST", "/evacuate", movesBody)
	require.NoError(t, err)
	require.Equal(t, http.StatusAccepted, resp.StatusCode)
	var ev struct {
		JobID string `json:"job_id"`
	}
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&ev))
	resp.Body.Close()
	require.NotEmpty(t, ev.JobID)

	// Wait for the evacuate parent job to succeed.
	waitJob(ev.JobID)

	// Assert both instances landed on dst.
	waitRunning("dst", "web", "app1")
	waitRunning("dst", "web-sql", "db1")

	// Assert source is reaped: GET on src returns 404.
	resp, err = do("GET", "/hosts/src/instances/web/app1", "")
	require.NoError(t, err)
	resp.Body.Close()
	assert.Equal(t, http.StatusNotFound, resp.StatusCode)

	resp, err = do("GET", "/hosts/src/instances/web-sql/db1", "")
	require.NoError(t, err)
	resp.Body.Close()
	assert.Equal(t, http.StatusNotFound, resp.StatusCode)

	// Assert children (migrate jobs) are parented to the evacuate job.
	resp, err = do("GET", "/jobs?parent_id="+ev.JobID, "")
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, resp.StatusCode)
	var jl []struct {
		Kind  string `json:"kind"`
		State string `json:"state"`
		ID    string `json:"id"`
	}
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&jl))
	resp.Body.Close()
	require.Len(t, jl, 2)
	ok, bad := 0, 0
	for _, j := range jl {
		assert.Equal(t, "migrate", j.Kind)
		switch j.State {
		case "succeeded":
			ok++
		case "failed", "canceled":
			bad++
		default:
			t.Fatalf("unexpected child job %s state %q", j.ID, j.State)
		}
	}
	assert.Equal(t, 2, ok, "both child migrate jobs should succeed")
	assert.Equal(t, 0, bad)
}

//go:build integration

package main

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"
	"time"

	"github.com/blang/semver/v4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/iotready/podman-api/internal/api"
	"github.com/iotready/podman-api/internal/auth"
	backuppkg "github.com/iotready/podman-api/internal/backup"
	"github.com/iotready/podman-api/internal/config"
	"github.com/iotready/podman-api/internal/instance"
	"github.com/iotready/podman-api/internal/jobs"
	"github.com/iotready/podman-api/internal/obs"
	"github.com/iotready/podman-api/internal/podman"
	"github.com/iotready/podman-api/internal/render"
	"github.com/iotready/podman-api/internal/store"
	"github.com/prometheus/client_golang/prometheus"
)

// backupTemplate is e2eTemplate plus one backable volume named "data" mounted at
// /data. The container sleeps long enough to survive a backup (stop+restart) and
// a restore (teardown + re-apply, which resets the sleep timer). The declared
// volume materializes as the podman volume <template>-<slug>-data (here
// bkup-itest-data) once the pod is applied.
const backupTemplate = `# template-meta:
#   id: bkup
#   parameters:
#     required: [slug, image, port]
#   secrets:
#     per_instance: []
#     per_host_referenced: []
#   volumes:
#     - name: data
#       backup: cold
---
apiVersion: v1
kind: Pod
metadata:
  name: bkup-{{.slug}}
  labels:
    podman-api/template: bkup
    podman-api/slug: {{.slug}}
spec:
  restartPolicy: Always
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
        claimName: bkup-{{.slug}}-data
`

// TestBackupRestore_RoundTrip_LocalOnly exercises the full backup/restore flow
// against a real podman host: deploy an instance with a volume, write a marker,
// back up, mutate the marker, restore, and assert the marker reverted. It skips
// unless the local socket is reachable AND the server's podman is >= the volume
// export/import floor (5.6.0, #85) — older hosts 404 mid-export.
func TestBackupRestore_RoundTrip_LocalOnly(t *testing.T) {
	sock := localSock(t)

	hosts := []config.Host{{ID: "local", Addr: "unix", Socket: sock}}
	client, err := podman.NewReal(hosts)
	require.NoError(t, err)

	ctx := context.Background()

	// Version gate: the volume export/import REST API first shipped in podman
	// 5.6.0 (#85). Below the floor, VolumeExport 404s mid-backup, so skip with a
	// message naming the version we found rather than fail spuriously.
	ver, err := client.Version(ctx, "local")
	require.NoError(t, err)
	v, err := semver.ParseTolerant(ver)
	if err != nil {
		t.Skipf("cannot parse local podman version %q (need >= %s)", ver, podman.MinPodmanVersion)
	}
	if v.LT(semver.MustParse(podman.MinPodmanVersion)) {
		t.Skipf("local podman %s < %s (volume export/import API floor, #85); skipping backup/restore round-trip", ver, podman.MinPodmanVersion)
	}

	meta, body, err := render.ParseMeta(backupTemplate)
	require.NoError(t, err)

	// Stack: a SQLite store in t.TempDir (the real store.DB main uses — the job
	// registry and runner both need the full DB surface), seeded with the
	// template; a service with a LocalDir blob store under t.TempDir; and the
	// same job registry main wires up so backup/restore jobs actually run.
	db, err := store.OpenSQLite(filepath.Join(t.TempDir(), "state.db"), nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	require.NoError(t, db.PutTemplate(ctx, store.Template{Meta: meta, Body: body, Origin: "seed"}))

	svc := instance.NewService(client, hosts)
	svc.SetStore(db)
	blobs, err := backuppkg.NewLocalDir(t.TempDir())
	require.NoError(t, err)
	svc.SetBlobStore(blobs)

	pruneMetrics := obs.NewPruneMetrics(prometheus.NewRegistry())
	jobMetrics := obs.NewJobMetrics(prometheus.NewRegistry())
	registry, reconcilers := buildJobRegistry(svc, client, db, 2, pruneMetrics, jobMetrics)
	runner := jobs.NewRunner(db, registry, jobs.DefaultWorkers)
	runner.SetReconcilers(reconcilers)
	runnerCtx, cancelRunner := context.WithCancel(ctx)
	t.Cleanup(cancelRunner)
	runner.Start(runnerCtx)

	tok := "bkuptoken"
	hash, _ := config.HashToken(tok)
	keys := []config.APIKey{{ID: "bkup", SecretHash: hash, Scopes: []string{"instances:*", "hosts:read"}}}
	r := api.NewRouter(svc, db, auth.NewKeyStore(keys), nil, nil, runner)
	srv := httptest.NewServer(r)
	defer srv.Close()

	const podName = "bkup-itest"
	const container = podName + "-c" // name `kube play` assigns: <pod>-<container>

	t.Cleanup(func() {
		_ = client.PodRemove(context.Background(), "local", podName, true)
		_ = client.VolumeRemove(context.Background(), "local", "bkup-itest-data", true)
	})

	do := func(method, path, reqBody string) (*http.Response, error) {
		req, _ := http.NewRequest(method, srv.URL+path, bytes.NewBufferString(reqBody))
		req.Header.Set("Authorization", "Bearer "+tok)
		req.Header.Set("Content-Type", "application/json")
		return http.DefaultClient.Do(req)
	}

	// waitRunning polls the instance via the API until its pod reports Running.
	waitRunning := func() {
		require.Eventually(t, func() bool {
			resp, err := do("GET", "/hosts/local/instances/bkup/itest", "")
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
			var jv struct {
				State string `json:"state"`
				Error string `json:"error"`
			}
			require.NoError(t, json.NewDecoder(resp.Body).Decode(&jv))
			resp.Body.Close()
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

	exec := func(sh string) podman.ExecResult {
		t.Helper()
		res, err := client.ContainerExec(ctx, "local", container, []string{"sh", "-c", sh})
		require.NoError(t, err)
		require.Equal(t, 0, res.ExitCode, "exec %q output: %s", sh, res.Output)
		return res
	}

	// DEPLOY via the API (POST /hosts/{host}/instances → 201). alpine ships
	// /bin/sh, which the marker writes/reads rely on; CI's podman image can pull it.
	createBody := `{"template":"bkup","slug":"itest","parameters":{"slug":"itest","image":"docker.io/library/alpine:latest","port":31998}}`
	resp, err := do("POST", "/hosts/local/instances", createBody)
	require.NoError(t, err)
	resp.Body.Close()
	require.Equal(t, http.StatusCreated, resp.StatusCode)
	waitRunning()

	// Write v1 marker into the volume, then back up.
	exec("echo v1 > /data/marker")

	resp, err = do("POST", "/hosts/local/instances/bkup/itest/backup", "")
	require.NoError(t, err)
	require.Equal(t, http.StatusAccepted, resp.StatusCode)
	var bk struct {
		JobID    string `json:"job_id"`
		BackupID string `json:"backup_id"`
	}
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&bk))
	resp.Body.Close()
	require.NotEmpty(t, bk.JobID)
	require.NotEmpty(t, bk.BackupID)
	waitJob(bk.JobID)

	// Backup stopped+restarted the pod; wait for it to be Running again before
	// mutating the marker.
	waitRunning()

	// Mutate marker to v2.
	exec("echo v2 > /data/marker")

	// RESTORE the backup (POST /backups/{id}/restore → 202).
	resp, err = do("POST", "/backups/"+bk.BackupID+"/restore", "")
	require.NoError(t, err)
	require.Equal(t, http.StatusAccepted, resp.StatusCode)
	var rs struct {
		JobID string `json:"job_id"`
	}
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&rs))
	resp.Body.Close()
	require.NotEmpty(t, rs.JobID)
	waitJob(rs.JobID)

	// Restore re-applied the spec; the pod must be Running again.
	waitRunning()

	// The marker must have reverted to v1.
	out := exec("cat /data/marker").Output
	assert.Contains(t, out, "v1", "marker should revert to v1 after restore; got %q", out)
	assert.NotContains(t, out, "v2", "marker should not still be v2 after restore")

	// DELETE the backup (→ 204).
	resp, err = do("DELETE", "/backups/"+bk.BackupID, "")
	require.NoError(t, err)
	resp.Body.Close()
	assert.Equal(t, http.StatusNoContent, resp.StatusCode)

	// Teardown: delete the instance and prune its volume.
	resp, err = do("DELETE", "/hosts/local/instances/bkup/itest?prune_volumes=true", "")
	require.NoError(t, err)
	resp.Body.Close()
	assert.Equal(t, http.StatusNoContent, resp.StatusCode)
}

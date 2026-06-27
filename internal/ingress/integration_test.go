//go:build integration

package ingress_test

import (
	"context"
	"net/http"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/iotready/podman-api/internal/config"
	"github.com/iotready/podman-api/internal/ingress"
	"github.com/iotready/podman-api/internal/podman"
	"github.com/iotready/podman-api/internal/render"
	"github.com/iotready/podman-api/internal/store"
)

// ingressITAdminAddr is the Caddy admin API the integration test expects to
// find running. Override with INGRESS_IT_ADMIN env var.
const ingressITAdminAddr = "localhost:2019"

// ingressITNetwork is the dedicated network the dummy web pod joins for this
// end-to-end test.
const ingressITNetwork = "podman-api-ingress-it"

// webPod is a minimal backend the Caddy proxy routes to.
const webPod = `apiVersion: v1
kind: Pod
metadata:
  name: web-it
spec:
  containers:
    - name: web
      image: docker.io/library/nginx:alpine
`

func localSocket(t *testing.T) string {
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

// TestIngressEndToEnd exercises the real Caddy controller against a running
// operator-managed Caddy instance. It pushes a route and confirms it appears
// in Caddy's config via the admin API.
//
// Prerequisites:
//   - XDG_RUNTIME_DIR set (local podman socket reachable)
//   - INGRESS_IT_DOMAIN set to a domain whose A record points at this host
//   - Caddy running with admin API enabled at localhost:2019
//     (set INGRESS_IT_ADMIN to override the address)
func TestIngressEndToEnd(t *testing.T) {
	sock := localSocket(t)

	domain := os.Getenv("INGRESS_IT_DOMAIN")
	if domain == "" {
		t.Skip("INGRESS_IT_DOMAIN unset")
	}

	adminAddr := os.Getenv("INGRESS_IT_ADMIN")
	if adminAddr == "" {
		adminAddr = ingressITAdminAddr
	}

	// Skip if Caddy admin API is not reachable — operator must start Caddy first.
	probeCtx, probeCancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer probeCancel()
	probeReq, _ := http.NewRequestWithContext(probeCtx, http.MethodGet, "http://"+adminAddr+"/config/", nil)
	if resp, err := http.DefaultClient.Do(probeReq); err != nil || resp.StatusCode != http.StatusOK {
		t.Skipf("Caddy admin API not reachable at %s — start Caddy first", adminAddr)
	}

	const host = "local"

	client, err := podman.NewReal([]config.Host{{ID: host, Addr: "unix", Socket: sock}})
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	var key [32]byte
	for i := range key {
		key[i] = byte(i)
	}
	dbPath := filepath.Join(t.TempDir(), "state.db")
	st, err := store.OpenSQLite(dbPath, store.NewKeyStore(key))
	require.NoError(t, err)
	t.Cleanup(func() { _ = st.Close() })

	require.NoError(t, st.PutTemplate(ctx, store.Template{
		Meta: render.Meta{
			ID:      "web",
			Ingress: &render.Ingress{Container: "web", Port: 80},
		},
		Body:   webPod,
		Origin: "user",
	}))
	require.NoError(t, st.PutSpec(ctx, store.Spec{
		Host:     host,
		Template: "web",
		Slug:     "it",
		Domains:  []string{domain},
	}))

	ctl := ingress.NewCaddyController(
		st,
		ingress.Config{
			ACMEEmail:  "it@example.com",
			AdminAddr:  adminAddr,
			HostAdmins: map[string]string{host: adminAddr},
		},
	)

	// Stand up the dummy backend on the ingress network.
	require.NoError(t, client.NetworkEnsure(ctx, host, ingressITNetwork))
	require.NoError(t, client.PlayKube(ctx, host, webPod, true, ingressITNetwork))
	t.Cleanup(func() {
		_ = client.PodRemove(context.Background(), host, "web-it", true)
	})

	require.NoError(t, ctl.Reconcile(ctx, host))

	// Verify the route appears in Caddy's config.
	verifyCtx, verifyCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer verifyCancel()
	verifyReq, _ := http.NewRequestWithContext(verifyCtx, http.MethodGet,
		"http://"+adminAddr+"/config/apps/http/servers/podman_api", nil)
	resp, err := http.DefaultClient.Do(verifyReq)
	require.NoError(t, err)
	defer resp.Body.Close()
	assert.Equal(t, http.StatusOK, resp.StatusCode, "podman_api server should exist in Caddy config after reconcile")

	// Cleanup: remove our server namespace from Caddy.
	t.Cleanup(func() {
		cleanCtx, cleanCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cleanCancel()
		cleanReq, _ := http.NewRequestWithContext(cleanCtx, http.MethodDelete,
			"http://"+adminAddr+"/config/apps/http/servers/podman_api", nil)
		_, _ = http.DefaultClient.Do(cleanReq)
	})
}

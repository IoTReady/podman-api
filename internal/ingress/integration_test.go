//go:build integration

package ingress_test

import (
	"context"
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

// ingressITNetwork is the dedicated network the controller and the dummy web
// pod share for this end-to-end test. It is isolated from the production
// ingress network so a stray run cannot disturb a real deployment.
const ingressITNetwork = "podman-api-ingress-it"

// webPod is a minimal backend the Caddy proxy routes to. It is deployed onto
// the ingress network under the container name "web" so it matches the stored
// "web" template's ingress: declaration the controller derives the upstream from.
const webPod = `apiVersion: v1
kind: Pod
metadata:
  name: web-it
spec:
  containers:
    - name: web
      image: docker.io/library/nginx:alpine
`

// localSocket mirrors the helper in internal/podman's integration tests: the
// suite drives a real, local podman over the user socket pointed at by
// XDG_RUNTIME_DIR. It is replicated here because that helper is unexported and
// this test lives in the external ingress_test package. Skips (rather than
// fails) when no local socket is reachable so CI without a podman host is green.
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

// TestIngressEndToEnd exercises the real Caddy controller against a real
// podman host: it stores a web spec with a public domain, stands up a dummy
// backend on the ingress network, reconciles, and asserts the Caddy system pod
// comes up running.
//
// The host is the local podman socket (host id "local"), matching the rest of
// the integration suite. INGRESS_IT_DOMAIN must name a domain whose A record
// points at this host; absent it (or a podman socket), the test skips.
func TestIngressEndToEnd(t *testing.T) {
	sock := localSocket(t)

	domain := os.Getenv("INGRESS_IT_DOMAIN")
	if domain == "" {
		t.Skip("INGRESS_IT_DOMAIN unset")
	}

	const host = "local"

	client, err := podman.NewReal([]config.Host{{ID: host, Addr: "unix", Socket: sock}})
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	// Open an encrypted store backed by a temp file (the store always
	// round-trips through SQLite; the controller reads specs from it).
	var key [32]byte
	for i := range key {
		key[i] = byte(i)
	}
	dbPath := filepath.Join(t.TempDir(), "state.db")
	st, err := store.OpenSQLite(dbPath, store.NewKeyStore(key))
	require.NoError(t, err)
	t.Cleanup(func() { _ = st.Close() })

	// The controller resolves the backend port from the stored "web" template's
	// ingress: declaration, so seed that template alongside the spec.
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
		client,
		st,
		ingress.Config{
			Network:    ingressITNetwork,
			CaddyImage: "docker.io/library/caddy:2",
			ACMEEmail:  "it@example.com",
		},
	)

	// Stand up the dummy backend on the ingress network so the Caddy upstream
	// (web-it/web:80) resolves once reconciliation wires the route.
	require.NoError(t, client.PlayKube(ctx, host, webPod, true, ingressITNetwork))

	t.Cleanup(func() {
		_ = client.PodRemove(context.Background(), host, "web-it", true)
		_ = client.PodRemove(context.Background(), host, "podman-api-ingress-caddy", true)
	})

	require.NoError(t, ctl.Reconcile(ctx, host))

	// Reconcile already waited for the admin API to become ready (via
	// waitForAdmin). Give the pod a brief moment to settle its health status.
	time.Sleep(1 * time.Second)

	// Deterministic assertion: the Caddy system pod exists and is running.
	// We deliberately do NOT assert live HTTPS here — public ACME issuance
	// needs real DNS plus open :80/:443 and has nondeterministic timing
	// (proven out in the spike), which would make CI flaky. The pod-running
	// check passes on any correctly-reconciled host.
	pod, err := client.PodInspect(ctx, host, "podman-api-ingress-caddy")
	require.NoError(t, err)
	assert.Equal(t, "podman-api-ingress-caddy", pod.Name)
	assert.Equal(t, "Running", pod.Status, "Caddy system pod should be Running after reconcile")

	// Manual HTTPS smoke test (run by hand when INGRESS_IT_DOMAIN resolves
	// publicly and :80/:443 are reachable; left commented to keep CI
	// deterministic):
	//
	//   c := &http.Client{Timeout: 30 * time.Second}
	//   var resp *http.Response
	//   for i := 0; i < 12; i++ { // wait up to ~60s for LE issuance
	//   	resp, err = c.Get("https://" + domain + "/")
	//   	if err == nil {
	//   		break
	//   	}
	//   	time.Sleep(5 * time.Second)
	//   }
	//   require.NoError(t, err)
	//   defer resp.Body.Close()
	//   require.Equal(t, http.StatusOK, resp.StatusCode)
}

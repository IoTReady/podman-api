package ingress

import (
	"context"
	"fmt"
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/iotready/podman-api/internal/podman/fake"
	"github.com/iotready/podman-api/internal/store"
)

// webSpecStore returns a store seeded with one web instance (with a domain) and
// the ingress-declaring "web" template the controller resolves its backend from.
func webSpecStore(t *testing.T) *store.Memory {
	return newMemStore(t,
		[]store.Spec{{Host: "h1", Template: "web", Slug: "blog", Domains: []string{"blog.example.com"}}},
		webTemplate("web", 8080),
	)
}

// adminRecorder returns an adminDo stub that records (method, path, body)
// and returns statusCode for all calls. It also satisfies waitForAdmin's
// GET /config/ probe.
func adminRecorder(statusCode int) (func(context.Context, string, string, string, []byte) (int, []byte, error), *[]adminCall) {
	calls := &[]adminCall{}
	return func(_ context.Context, addr, method, path string, body []byte) (int, []byte, error) {
		*calls = append(*calls, adminCall{addr: addr, method: method, path: path, body: body})
		return statusCode, nil, nil
	}, calls
}

type adminCall struct {
	addr, method, path string
	body               []byte
}

func TestReconcileFreshHostCreatesPodsAndPushesAdminRoutes(t *testing.T) {
	f := fake.New()
	stub, calls := adminRecorder(http.StatusOK)
	c := NewCaddyController(f, webSpecStore(t),
		Config{Network: "podman-api-ingress", CaddyImage: "docker.io/library/caddy:2", ACMEEmail: "ops@example.com"})
	c.adminDo = stub

	require.NoError(t, c.Reconcile(context.Background(), "h1"))

	// Pod should be created (fresh host).
	require.NotEmpty(t, f.PlayCalls)
	caddyYAML := f.PlayCalls[len(f.PlayCalls)-1].YAML
	// Pod YAML must NOT carry route content — routes go via admin API only.
	require.NotContains(t, caddyYAML, "blog.example.com")
	require.NotContains(t, caddyYAML, "reverse_proxy")
	// Admin API should have been called to push routes.
	var putCall *adminCall
	for i := range *calls {
		if (*calls)[i].method == http.MethodPut {
			putCall = &(*calls)[i]
			break
		}
	}
	require.NotNil(t, putCall, "expected a PUT call to admin API")
	require.Contains(t, putCall.path, "podman_api")
	require.Contains(t, string(putCall.body), "blog.example.com")
	require.Contains(t, string(putCall.body), "web-blog:8080")
	// No exec or file copy.
	require.Empty(t, f.ExecCalls)
	require.Empty(t, f.CopyCalls)
}

func TestReconcileExistingHostPushesAdminRoutes(t *testing.T) {
	f := fake.New()
	stub, calls := adminRecorder(http.StatusOK)
	c := NewCaddyController(f, webSpecStore(t),
		Config{Network: "n", CaddyImage: "img", ACMEEmail: "ops@example.com"})
	c.adminDo = stub
	require.NoError(t, c.Reconcile(context.Background(), "h1")) // creates pod
	*calls = nil                                                // reset; only care about the second reconcile

	require.NoError(t, c.Reconcile(context.Background(), "h1")) // pod exists → admin API only

	require.Empty(t, f.ExecCalls, "no exec on existing pod")
	require.Empty(t, f.CopyCalls, "no file copy on existing pod")
	var putCall *adminCall
	for i := range *calls {
		if (*calls)[i].method == http.MethodPut {
			putCall = &(*calls)[i]
			break
		}
	}
	require.NotNil(t, putCall)
	require.Contains(t, string(putCall.body), "blog.example.com")
}

func TestReconcileNoRoutesNoPodIsNoop(t *testing.T) {
	f := fake.New()
	stub, calls := adminRecorder(http.StatusOK)
	c := NewCaddyController(f, store.NewMemory(),
		Config{Network: "n", CaddyImage: "img", ACMEEmail: "ops@example.com"})
	c.adminDo = stub

	require.NoError(t, c.Reconcile(context.Background(), "h1"))

	require.Empty(t, f.PlayCalls, "no Caddy pod should be created when there are no routes")
	require.Empty(t, *calls, "no admin API calls when there are no routes and no pod")
}

func TestReconcileNoRoutesExistingPodDeletesServer(t *testing.T) {
	f := fake.New()
	stub, calls := adminRecorder(http.StatusOK)
	c := NewCaddyController(f, webSpecStore(t),
		Config{Network: "n", CaddyImage: "img", ACMEEmail: "ops@example.com"})
	c.adminDo = stub
	require.NoError(t, c.Reconcile(context.Background(), "h1")) // creates pod + routes

	// Now clear the store so zero routes remain.
	emptyStore := store.NewMemory()
	c.store = emptyStore
	*calls = nil

	require.NoError(t, c.Reconcile(context.Background(), "h1"))

	var deleteCall *adminCall
	for i := range *calls {
		if (*calls)[i].method == http.MethodDelete {
			deleteCall = &(*calls)[i]
			break
		}
	}
	require.NotNil(t, deleteCall, "expected DELETE when routes go to zero")
	require.Contains(t, deleteCall.path, "podman_api")
}

func TestReconcileFailsWhenAdminAPIFails(t *testing.T) {
	f := fake.New()
	c := NewCaddyController(f, webSpecStore(t),
		Config{Network: "n", CaddyImage: "img"})
	// First reconcile: create pod with a working admin stub.
	stub, _ := adminRecorder(http.StatusOK)
	c.adminDo = stub
	require.NoError(t, c.Reconcile(context.Background(), "h1"))

	// Second reconcile: GET /config/ readiness probe succeeds but PUT returns 500.
	c.adminDo = func(_ context.Context, _, method, _ string, _ []byte) (int, []byte, error) {
		if method == http.MethodGet {
			return http.StatusOK, nil, nil
		}
		return http.StatusInternalServerError, []byte("internal error"), nil
	}
	err := c.Reconcile(context.Background(), "h1")
	require.Error(t, err)
	require.Contains(t, err.Error(), "admin")
}

func TestReconcileFailsWhenAdminAPINetworkError(t *testing.T) {
	f := fake.New()
	c := NewCaddyController(f, webSpecStore(t),
		Config{Network: "n", CaddyImage: "img"})
	// First reconcile: create pod.
	stub, _ := adminRecorder(http.StatusOK)
	c.adminDo = stub
	require.NoError(t, c.Reconcile(context.Background(), "h1"))

	// Second reconcile: network error on PUT.
	c.adminDo = func(_ context.Context, _, method, _ string, _ []byte) (int, []byte, error) {
		if method == http.MethodGet {
			return http.StatusOK, nil, nil
		}
		return 0, nil, fmt.Errorf("connection refused")
	}
	err := c.Reconcile(context.Background(), "h1")
	require.Error(t, err)
	require.Contains(t, err.Error(), "connection refused")
}

func TestReconcileUsesPerHostAdminAddr(t *testing.T) {
	f := fake.New()
	var gotAddr string
	c := NewCaddyController(f, webSpecStore(t), Config{
		Network:    "n",
		CaddyImage: "img",
		AdminAddr:  "default:2019",
		HostAdmins: map[string]string{"h1": "custom-host:2019"},
	})
	c.adminDo = func(_ context.Context, addr, method, _ string, _ []byte) (int, []byte, error) {
		if method == http.MethodPut {
			gotAddr = addr
		}
		return http.StatusOK, nil, nil
	}

	require.NoError(t, c.Reconcile(context.Background(), "h1"))
	require.Equal(t, "custom-host:2019", gotAddr)
}

func TestReconcileACMEEmailPushedToTLS(t *testing.T) {
	f := fake.New()
	stub, calls := adminRecorder(http.StatusOK)
	c := NewCaddyController(f, webSpecStore(t),
		Config{Network: "n", CaddyImage: "img", ACMEEmail: "ops@example.com"})
	c.adminDo = stub

	require.NoError(t, c.Reconcile(context.Background(), "h1"))

	// A PUT to the TLS automation path should carry the email.
	var tlsCall *adminCall
	for i := range *calls {
		if (*calls)[i].method == http.MethodPut && (*calls)[i].path == "/config/apps/tls/automation/email" {
			tlsCall = &(*calls)[i]
			break
		}
	}
	require.NotNil(t, tlsCall)
	require.Equal(t, `"ops@example.com"`, string(tlsCall.body))
}

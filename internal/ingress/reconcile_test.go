package ingress

import (
	"context"
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

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

// findPut returns the first recorded PUT to path, or nil. Order-independent so
// tests don't depend on whether the TLS or server PUT is sent first.
func findPut(calls *[]adminCall, path string) *adminCall {
	for i := range *calls {
		if (*calls)[i].method == http.MethodPut && (*calls)[i].path == path {
			return &(*calls)[i]
		}
	}
	return nil
}

func TestReconcilePushesAdminRoutes(t *testing.T) {
	stub, calls := adminRecorder(http.StatusOK)
	c := NewCaddyController(webSpecStore(t),
		Config{ACMEEmail: "ops@example.com"})
	c.adminDo = stub

	require.NoError(t, c.Reconcile(context.Background(), "h1"))

	// Admin API should have been called to push the routes (server) config.
	putCall := findPut(calls, "/config/apps/http/servers/podman_api")
	require.NotNil(t, putCall, "expected a PUT to the podman_api server")
	require.Contains(t, string(putCall.body), "blog.example.com")
	require.Contains(t, string(putCall.body), "web-blog:8080")
}

func TestReconcileSecondCallPushesAdminRoutes(t *testing.T) {
	stub, calls := adminRecorder(http.StatusOK)
	c := NewCaddyController(webSpecStore(t),
		Config{ACMEEmail: "ops@example.com"})
	c.adminDo = stub
	require.NoError(t, c.Reconcile(context.Background(), "h1"))
	*calls = nil // reset; only care about the second reconcile

	require.NoError(t, c.Reconcile(context.Background(), "h1"))

	putCall := findPut(calls, "/config/apps/http/servers/podman_api")
	require.NotNil(t, putCall)
	require.Contains(t, string(putCall.body), "blog.example.com")
}

func TestReconcileNoRoutesIsNoop(t *testing.T) {
	stub, calls := adminRecorder(http.StatusOK)
	c := NewCaddyController(store.NewMemory(),
		Config{ACMEEmail: "ops@example.com"})
	c.adminDo = stub

	require.NoError(t, c.Reconcile(context.Background(), "h1"))

	// Zero routes: a best-effort DELETE is sent; no PUT to the server.
	var putCall *adminCall
	for i := range *calls {
		if (*calls)[i].method == http.MethodPut {
			putCall = &(*calls)[i]
			break
		}
	}
	require.Nil(t, putCall, "no PUT when there are no routes")
}

func TestReconcileNoRoutesDeletesServer(t *testing.T) {
	stub, calls := adminRecorder(http.StatusOK)
	c := NewCaddyController(webSpecStore(t), Config{})
	c.adminDo = stub
	require.NoError(t, c.Reconcile(context.Background(), "h1")) // push routes

	// Now clear the store so zero routes remain.
	c.store = store.NewMemory()
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

func TestReconcileNoRoutesAdminUnreachableIsNoop(t *testing.T) {
	// When routes=0 and Caddy is not running, Reconcile should return nil
	// (best-effort cleanup — Caddy not running means nothing to clean up).
	c := NewCaddyController(store.NewMemory(), Config{})
	c.adminDo = func(_ context.Context, _, _, _ string, _ []byte) (int, []byte, error) {
		return 0, nil, fmt.Errorf("connection refused")
	}

	require.NoError(t, c.Reconcile(context.Background(), "h1"))
}

func TestReconcileFailsWhenAdminAPIFails(t *testing.T) {
	c := NewCaddyController(webSpecStore(t), Config{})
	// GET /config/ readiness probe succeeds but PUT returns 500.
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
	c := NewCaddyController(webSpecStore(t), Config{})
	c.adminDo = func(_ context.Context, _, _, _ string, _ []byte) (int, []byte, error) {
		return 0, nil, fmt.Errorf("connection refused")
	}
	err := c.Reconcile(context.Background(), "h1")
	require.Error(t, err)
	require.Contains(t, err.Error(), "connection refused")
}

func TestReconcileUsesPerHostAdminAddr(t *testing.T) {
	var gotAddr string
	c := NewCaddyController(webSpecStore(t), Config{
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
	stub, calls := adminRecorder(http.StatusOK)
	c := NewCaddyController(webSpecStore(t),
		Config{ACMEEmail: "ops@example.com"})
	c.adminDo = stub

	require.NoError(t, c.Reconcile(context.Background(), "h1"))

	// A PUT to /config/apps/tls/automation/policies must carry the email
	// and specify the ACME module.
	var tlsCall *adminCall
	for i := range *calls {
		if (*calls)[i].method == http.MethodPut && (*calls)[i].path == "/config/apps/tls/automation/policies" {
			tlsCall = &(*calls)[i]
			break
		}
	}
	require.NotNil(t, tlsCall, "expected PUT to /config/apps/tls/automation/policies")
	require.Contains(t, string(tlsCall.body), "ops@example.com")
	require.Contains(t, string(tlsCall.body), "acme")
}

func TestReconcileFailsWhenAdminNotReady(t *testing.T) {
	c := NewCaddyController(webSpecStore(t), Config{})
	c.adminDo = func(_ context.Context, _, _, _ string, _ []byte) (int, []byte, error) {
		return http.StatusServiceUnavailable, nil, nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	err := c.Reconcile(ctx, "h1")
	require.Error(t, err)
}

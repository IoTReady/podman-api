package ingress

import (
	"bytes"
	"context"
	"fmt"
	"log"
	"net/http"
	"strings"
	"sync"
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
		Config{})
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
		Config{})
	c.adminDo = stub
	require.NoError(t, c.Reconcile(context.Background(), "h1"))
	*calls = nil // reset; only care about the second reconcile

	require.NoError(t, c.Reconcile(context.Background(), "h1"))

	putCall := findPut(calls, "/config/apps/http/servers/podman_api")
	require.NotNil(t, putCall)
	require.Contains(t, string(putCall.body), "blog.example.com")
}

func TestReconcileNoRoutesSkipsPush(t *testing.T) {
	stub, calls := adminRecorder(http.StatusOK)
	c := NewCaddyController(store.NewMemory(),
		Config{})
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

// --- best-effort cleanup logging (#290) --------------------------------------

// captureLog redirects the standard logger for the duration of the test and
// returns a func yielding everything written so far.
func captureLog(t *testing.T) func() string {
	t.Helper()
	var buf bytes.Buffer
	var mu sync.Mutex
	prevOut, prevFlags := log.Writer(), log.Flags()
	log.SetOutput(writerFunc(func(p []byte) (int, error) {
		mu.Lock()
		defer mu.Unlock()
		return buf.Write(p)
	}))
	log.SetFlags(0)
	t.Cleanup(func() {
		log.SetOutput(prevOut)
		log.SetFlags(prevFlags)
	})
	return func() string {
		mu.Lock()
		defer mu.Unlock()
		return buf.String()
	}
}

type writerFunc func([]byte) (int, error)

func (f writerFunc) Write(p []byte) (int, error) { return f(p) }

// countLines returns how many lines of s contain sub.
func countLines(s, sub string) int {
	n := 0
	for _, line := range strings.Split(s, "\n") {
		if line != "" && strings.Contains(line, sub) {
			n++
		}
	}
	return n
}

// A host that runs no Caddy derives zero routes and its best-effort cleanup
// DELETE is refused every single time. Before #290 that logged an
// ERROR-shaped line on every -ingress-interval tick, forever.
func TestReconcileCleanupFailureLogsOnceNotEveryTick(t *testing.T) {
	logs := captureLog(t)
	refused := fmt.Errorf("dial tcp 100.64.0.23:2019: connect: connection refused")
	c := NewCaddyController(newMemStore(t, nil), Config{})
	c.adminDo = func(context.Context, string, string, string, []byte) (int, []byte, error) {
		return 0, nil, refused
	}

	for i := 0; i < 5; i++ {
		require.NoError(t, c.Reconcile(context.Background(), "h1"))
	}

	require.Equal(t, 1, countLines(logs(), "best-effort cleanup on h1"),
		"a permanently absent admin endpoint must log once, not once per tick:\n%s", logs())
	require.Contains(t, logs(), "connection refused", "the first line still names the real cause")
}

// The gate is on the transition, not on "log at most once ever": a cleanup
// that starts succeeding says so, and a later failure is reported again.
func TestReconcileCleanupLogsBothTransitions(t *testing.T) {
	logs := captureLog(t)
	fail := true
	c := NewCaddyController(newMemStore(t, nil), Config{})
	c.adminDo = func(context.Context, string, string, string, []byte) (int, []byte, error) {
		if fail {
			return 0, nil, fmt.Errorf("connection refused")
		}
		return http.StatusOK, nil, nil
	}

	require.NoError(t, c.Reconcile(context.Background(), "h1"))
	require.NoError(t, c.Reconcile(context.Background(), "h1"))
	require.Equal(t, 1, countLines(logs(), "failed"))

	fail = false
	require.NoError(t, c.Reconcile(context.Background(), "h1"))
	require.NoError(t, c.Reconcile(context.Background(), "h1"))
	require.Equal(t, 1, countLines(logs(), "succeeded again"), logs())

	fail = true
	require.NoError(t, c.Reconcile(context.Background(), "h1"))
	require.NoError(t, c.Reconcile(context.Background(), "h1"))
	require.Equal(t, 2, countLines(logs(), "failed"), logs())
}

// A cleanup that has only ever succeeded is not worth a line.
func TestReconcileCleanupSuccessIsSilent(t *testing.T) {
	logs := captureLog(t)
	c := NewCaddyController(newMemStore(t, nil), Config{})
	stub, _ := adminRecorder(http.StatusOK)
	c.adminDo = stub

	require.NoError(t, c.Reconcile(context.Background(), "h1"))
	require.NoError(t, c.Reconcile(context.Background(), "h1"))

	require.Empty(t, logs(), "a working cleanup says nothing")
}

// State is per host: one host's permanent failure must not suppress another's
// first report.
func TestReconcileCleanupStateIsPerHost(t *testing.T) {
	logs := captureLog(t)
	c := NewCaddyController(newMemStore(t, nil), Config{})
	c.adminDo = func(context.Context, string, string, string, []byte) (int, []byte, error) {
		return 0, nil, fmt.Errorf("connection refused")
	}

	require.NoError(t, c.Reconcile(context.Background(), "h1"))
	require.NoError(t, c.Reconcile(context.Background(), "h2"))
	require.NoError(t, c.Reconcile(context.Background(), "h1"))
	require.NoError(t, c.Reconcile(context.Background(), "h2"))

	require.Equal(t, 1, countLines(logs(), "cleanup on h1"), logs())
	require.Equal(t, 1, countLines(logs(), "cleanup on h2"), logs())

	c.forgetCleanupState("h1")
	require.NoError(t, c.Reconcile(context.Background(), "h1"))
	require.Equal(t, 2, countLines(logs(), "cleanup on h1"), logs())
}

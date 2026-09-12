package api

import (
	"errors"
	"net/http"
	"net/url"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/iotready/podman-api/internal/instance"
)

// --- bulkClassify: every branch --------------------------------------------

// bulkClassify is a sibling of classify() local to bulk.go; assert each arm
// directly since some (e.g. host_draining) are not reachable through the
// start/stop/restart/delete actions a bulk op can request.
func TestBulkClassify_AllBranches(t *testing.T) {
	cases := []struct {
		err  error
		code string
		stat int
	}{
		{instance.ErrUnknownHost, "unknown_host", http.StatusNotFound},
		{instance.ErrUnknownTemplate, "unknown_template", http.StatusNotFound},
		{instance.ErrInstanceNotFound, "instance_not_found", http.StatusNotFound},
		{instance.ErrHostDraining, "host_draining", http.StatusLocked},
		{instance.ErrIngressReconcileFailed, "ingress_reconcile_failed", http.StatusBadGateway},
		{errors.New("anything else"), "internal", http.StatusInternalServerError},
	}
	for _, c := range cases {
		code, stat, msg := bulkClassify(c.err)
		assert.Equal(t, c.code, code)
		assert.Equal(t, c.stat, stat)
		assert.Equal(t, c.err.Error(), msg)
	}
}

// TestBulkClassify_AgreesWithClassify locks bulkClassify() and classify()
// together for every sentinel bulkClassify recognizes. They are hand-written
// mirrors of the same switch (like errorStatus/classify, see
// internal/ui/render_test.go's TestErrorStatus_AgreesWithAPIClassify for the
// prior instance of this exact drift): ErrIngressReconcileFailed was added to
// classify() (#301) and missed here, so a bulk apply/delete against a host
// whose Caddy admin API is failing reported "internal"/500 for every affected
// instance instead of the distinct signal a single-instance call gets --
// hiding that the pod operations themselves succeeded. Both switches are
// package-local here, so classify() is called directly rather than through
// api.WriteError.
func TestBulkClassify_AgreesWithClassify(t *testing.T) {
	shared := []error{
		instance.ErrUnknownHost,
		instance.ErrUnknownTemplate,
		instance.ErrInstanceNotFound,
		instance.ErrHostDraining,
		instance.ErrIngressReconcileFailed,
	}
	for _, err := range shared {
		bCode, bStat, _ := bulkClassify(err)
		cCode, cStat, _ := classify(err)
		if bCode != cCode || bStat != cStat {
			t.Errorf("bulkClassify(%v) = (%q, %d), but classify() = (%q, %d) — the two switches have drifted",
				err, bCode, bStat, cCode, cStat)
		}
	}
}

// --- runBulkOp: the delete action arm --------------------------------------

func TestBulk_DeleteAction(t *testing.T) {
	srv, tok, _ := newSrvFull(t)
	resp := postJSON(t, srv, tok, "POST", "/hosts/h1/instances", createBody("bd"))
	resp.Body.Close()
	require.Equal(t, http.StatusCreated, resp.StatusCode)

	resp = postJSON(t, srv, tok, "POST", "/hosts/h1/bulk",
		`{"ops":[{"action":"delete","template":"app","slug":"bd"}]}`)
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)

	// The instance is gone afterwards.
	got := authedReq(t, srv, tok, "GET", "/hosts/h1/instances/app/bd")
	defer got.Body.Close()
	assert.Equal(t, http.StatusNotFound, got.StatusCode)
}

func TestBulk_MalformedJSON(t *testing.T) {
	srv, tok, _ := newSrvFull(t)
	resp := postJSON(t, srv, tok, "POST", "/hosts/h1/bulk", "{not json")
	defer resp.Body.Close()
	assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
	assert.Contains(t, bodyString(t, resp), `"code":"invalid_body"`)
}

// --- hostHealthz / portsInUse: backend (UsedHostPorts) error ----------------

func TestHostHealthz_PortsBackendError(t *testing.T) {
	srv, tok, f := newSrvFull(t)
	f.UsedHostPortsErr = errors.New("socket gone")

	resp := authedReq(t, srv, tok, "GET", "/hosts/h1/healthz")
	defer resp.Body.Close()
	assert.Equal(t, http.StatusInternalServerError, resp.StatusCode)
	assert.Contains(t, bodyString(t, resp), `"code":"internal"`)
}

func TestPortsInUse_BackendError(t *testing.T) {
	srv, tok, f := newSrvFull(t)
	f.UsedHostPortsErr = errors.New("socket gone")

	resp := authedReq(t, srv, tok, "GET", "/hosts/h1/ports-in-use")
	defer resp.Body.Close()
	assert.Equal(t, http.StatusInternalServerError, resp.StatusCode)
	assert.Contains(t, bodyString(t, resp), `"code":"internal"`)
}

// --- lifecycle: invalid-path rejection on stop/restart ----------------------

// start was already covered at the edge; assert stop and restart reject a bad
// slug too (the validInstancePath false branch in each handler).
func TestLifecycle_BadSlugRejected(t *testing.T) {
	srv, tok, _ := newSrvFull(t)
	for _, action := range []string{"stop", "restart"} {
		path := "/hosts/h1/instances/app/" + url.PathEscape("a*b") + "/" + action
		resp := authedReq(t, srv, tok, "POST", path)
		resp.Body.Close()
		assert.GreaterOrEqual(t, resp.StatusCode, 400, "action %s", action)
		assert.Less(t, resp.StatusCode, 500, "action %s", action)
	}
}

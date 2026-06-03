package api

import (
	"errors"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/iotready/podman-api/internal/podman"
)

// createBody is the standard well-formed apply body for the "app" template.
func createBody(slug string) string {
	return `{"template":"app","slug":"` + slug +
		`","parameters":{"slug":"` + slug + `","image":"i:1"},"secrets":{"auth_secret":"s"}}`
}

// --- Apply / Upgrade: image-pull failure surfaces as 502 --------------------

func TestCreateInstance_PullFailureIsBadGateway(t *testing.T) {
	srv, tok, f := newSrvFull(t)
	f.PullErr = map[string]error{"": errors.New("registry unreachable")}

	resp := postJSON(t, srv, tok, "POST", "/hosts/h1/instances", createBody("pf"))
	defer resp.Body.Close()
	assert.Equal(t, http.StatusBadGateway, resp.StatusCode)
	assert.Contains(t, bodyString(t, resp), `"code":"upstream_error"`)
}

func TestUpgradeInstance_PullFailureIsBadGateway(t *testing.T) {
	srv, tok, f := newSrvFull(t)

	// Create succeeds (pull works), then the registry goes away for the upgrade.
	resp := postJSON(t, srv, tok, "POST", "/hosts/h1/instances", createBody("up"))
	resp.Body.Close()
	require.Equal(t, http.StatusCreated, resp.StatusCode)

	f.PullErr = map[string]error{"": errors.New("registry unreachable")}
	upgrade := `{"image":"i:2","parameters":{"slug":"up","image":"i:1"},"secrets":{"auth_secret":"s"}}`
	resp = postJSON(t, srv, tok, "POST", "/hosts/h1/instances/app/up/upgrade", upgrade)
	defer resp.Body.Close()
	assert.Equal(t, http.StatusBadGateway, resp.StatusCode)
	assert.Contains(t, bodyString(t, resp), `"code":"upstream_error"`)
}

func TestApplyInstance_PullFailureIsBadGateway(t *testing.T) {
	srv, tok, f := newSrvFull(t)
	f.PullErr = map[string]error{"": errors.New("registry unreachable")}

	// PUT (replace) takes the same pre-pull path as create.
	resp := postJSON(t, srv, tok, "PUT", "/hosts/h1/instances/app/ap", createBody("ap"))
	defer resp.Body.Close()
	assert.Equal(t, http.StatusBadGateway, resp.StatusCode)
	assert.Contains(t, bodyString(t, resp), `"code":"upstream_error"`)
}

// --- deleteInstance: service-error branch -----------------------------------

func TestDeleteInstance_GhostIsNotFound(t *testing.T) {
	srv, tok, _ := newSrvFull(t)
	// Deleting an absent instance without prune flags is a 404 (no orphan
	// secrets/volumes to reap), driving the handler's WriteError branch.
	resp := authedReq(t, srv, tok, "DELETE", "/hosts/h1/instances/app/ghost")
	defer resp.Body.Close()
	assert.Equal(t, http.StatusNotFound, resp.StatusCode)
	assert.Contains(t, bodyString(t, resp), `"code":"instance_not_found"`)
}

// --- listInstances: PodList failure surfaces as 500 -------------------------

func TestListInstances_BackendErrorIsInternal(t *testing.T) {
	srv, tok, f := newSrvFull(t)
	f.PodListErr = errors.New("backend boom")

	resp := authedReq(t, srv, tok, "GET", "/hosts/h1/instances?template=app")
	defer resp.Body.Close()
	assert.Equal(t, http.StatusInternalServerError, resp.StatusCode)
	assert.Contains(t, bodyString(t, resp), `"code":"internal"`)
}

// --- queryBool: non-empty value branch (skip_pull=true) ---------------------

func TestCreateInstance_SkipPullParsesQueryBool(t *testing.T) {
	srv, tok, f := newSrvFull(t)
	// PullErr is set, but skip_pull=true must bypass the pre-pull entirely, so
	// the create still succeeds and ImagePull is never called.
	f.PullErr = map[string]error{"": errors.New("should not be reached")}

	resp := postJSON(t, srv, tok, "POST", "/hosts/h1/instances?skip_pull=true", createBody("sp"))
	defer resp.Body.Close()
	require.Equal(t, http.StatusCreated, resp.StatusCode)
	assert.Empty(t, f.PullCalls, "skip_pull=true must not pull")
}

// --- logsInstance: streaming response bodies --------------------------------

func TestLogsInstance_PlainAndFollow(t *testing.T) {
	srv, tok, f := newSrvFull(t)
	resp := postJSON(t, srv, tok, "POST", "/hosts/h1/instances", createBody("lg"))
	resp.Body.Close()
	require.Equal(t, http.StatusCreated, resp.StatusCode)

	f.LogLines = []podman.LogLine{{Line: "hello"}, {Line: "world"}}

	// Non-follow: newline-delimited text/plain.
	resp = authedReq(t, srv, tok, "GET", "/hosts/h1/instances/app/lg/logs?container=app")
	require.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Equal(t, "text/plain; charset=utf-8", resp.Header.Get("Content-Type"))
	body := bodyString(t, resp)
	resp.Body.Close()
	assert.Equal(t, "hello\nworld\n", body)

	// Follow: server-sent events.
	resp = authedReq(t, srv, tok, "GET", "/hosts/h1/instances/app/lg/logs?container=app&follow=true")
	require.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Equal(t, "text/event-stream", resp.Header.Get("Content-Type"))
	body = bodyString(t, resp)
	resp.Body.Close()
	assert.Contains(t, body, "event: log\ndata: hello\n\n")
	assert.Contains(t, body, "event: log\ndata: world\n\n")
}

func TestLogsInstance_GhostInstanceIsNotFound(t *testing.T) {
	srv, tok, _ := newSrvFull(t)
	resp := authedReq(t, srv, tok, "GET", "/hosts/h1/instances/app/ghost/logs?container=app")
	defer resp.Body.Close()
	assert.Equal(t, http.StatusNotFound, resp.StatusCode)
	assert.Contains(t, bodyString(t, resp), `"code":"instance_not_found"`)
}

func TestLogsInstance_BackendErrorIsInternal(t *testing.T) {
	srv, tok, f := newSrvFull(t)
	resp := postJSON(t, srv, tok, "POST", "/hosts/h1/instances", createBody("le"))
	resp.Body.Close()
	require.Equal(t, http.StatusCreated, resp.StatusCode)

	f.ContainerLogsErr = errors.New("stream broke")
	resp = authedReq(t, srv, tok, "GET", "/hosts/h1/instances/app/le/logs?container=app")
	defer resp.Body.Close()
	assert.Equal(t, http.StatusInternalServerError, resp.StatusCode)
	assert.Contains(t, bodyString(t, resp), `"code":"internal"`)
}

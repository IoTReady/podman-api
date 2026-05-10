package api

import (
	"bytes"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLifecycle_StartStopRestart(t *testing.T) {
	srv, tok, _ := newSrvFull(t)
	body := `{"template":"x","slug":"l","parameters":{"slug":"l","image":"i:1"},"secrets":{"auth_secret":"s"}}`
	req, _ := http.NewRequest("PUT", srv.URL+"/hosts/h1/instances/x/l", bytes.NewBufferString(body))
	req.Header.Set("Authorization", "Bearer "+tok)
	req.Header.Set("Content-Type", "application/json")
	resp, _ := http.DefaultClient.Do(req)
	resp.Body.Close()

	for _, action := range []string{"stop", "start", "restart"} {
		req, _ := http.NewRequest("POST", srv.URL+"/hosts/h1/instances/x/l/"+action, nil)
		req.Header.Set("Authorization", "Bearer "+tok)
		resp, err := http.DefaultClient.Do(req)
		require.NoError(t, err)
		resp.Body.Close()
		assert.Equal(t, http.StatusNoContent, resp.StatusCode, "action %s", action)
	}
}

func TestUpgrade_PullsAndApplies(t *testing.T) {
	srv, tok, _ := newSrvFull(t)
	body := `{"template":"x","slug":"u","parameters":{"slug":"u","image":"i:1"},"secrets":{"auth_secret":"s"}}`
	req, _ := http.NewRequest("PUT", srv.URL+"/hosts/h1/instances/x/u", bytes.NewBufferString(body))
	req.Header.Set("Authorization", "Bearer "+tok)
	req.Header.Set("Content-Type", "application/json")
	resp, _ := http.DefaultClient.Do(req)
	resp.Body.Close()

	upgrade := `{"image":"i:2","parameters":{"slug":"u","image":"i:1"},"secrets":{"auth_secret":"s"}}`
	req, _ = http.NewRequest("POST", srv.URL+"/hosts/h1/instances/x/u/upgrade", bytes.NewBufferString(upgrade))
	req.Header.Set("Authorization", "Bearer "+tok)
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	resp.Body.Close()
	assert.Equal(t, http.StatusOK, resp.StatusCode)
}

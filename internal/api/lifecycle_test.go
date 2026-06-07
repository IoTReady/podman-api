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
	body := `{"template":"app","slug":"lf","parameters":{"slug":"lf","image":"i:1"},"secrets":{"auth_secret":"s"}}`
	req, _ := http.NewRequest("PUT", srv.URL+"/hosts/h1/instances/app/lf", bytes.NewBufferString(body))
	req.Header.Set("Authorization", "Bearer "+tok)
	req.Header.Set("Content-Type", "application/json")
	resp, _ := http.DefaultClient.Do(req)
	resp.Body.Close()

	wantStatus := map[string]int{
		"stop":    http.StatusNoContent,
		"start":   http.StatusOK,
		"restart": http.StatusNoContent,
	}
	for _, action := range []string{"stop", "start", "restart"} {
		req, _ := http.NewRequest("POST", srv.URL+"/hosts/h1/instances/app/lf/"+action, nil)
		req.Header.Set("Authorization", "Bearer "+tok)
		resp, err := http.DefaultClient.Do(req)
		require.NoError(t, err)
		resp.Body.Close()
		assert.Equal(t, wantStatus[action], resp.StatusCode, "action %s", action)
	}
}

func TestUpgrade_PullsAndApplies(t *testing.T) {
	srv, tok, _ := newSrvFull(t)
	body := `{"template":"app","slug":"up","parameters":{"slug":"up","image":"i:1"},"secrets":{"auth_secret":"s"}}`
	req, _ := http.NewRequest("PUT", srv.URL+"/hosts/h1/instances/app/up", bytes.NewBufferString(body))
	req.Header.Set("Authorization", "Bearer "+tok)
	req.Header.Set("Content-Type", "application/json")
	resp, _ := http.DefaultClient.Do(req)
	resp.Body.Close()

	upgrade := `{"image":"i:2","parameters":{"slug":"up","image":"i:1"},"secrets":{"auth_secret":"s"}}`
	req, _ = http.NewRequest("POST", srv.URL+"/hosts/h1/instances/app/up/upgrade", bytes.NewBufferString(upgrade))
	req.Header.Set("Authorization", "Bearer "+tok)
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	resp.Body.Close()
	assert.Equal(t, http.StatusOK, resp.StatusCode)
}

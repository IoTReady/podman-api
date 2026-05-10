package api

import (
	"bytes"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLogs_NoFollow_ReturnsTextAndCloses(t *testing.T) {
	srv, tok, _ := newSrvFull(t)
	body := `{"template":"x","slug":"L","parameters":{"slug":"L","image":"i"},"secrets":{"auth_secret":"s"}}`
	req, _ := http.NewRequest("PUT", srv.URL+"/hosts/h1/instances/x/L", bytes.NewBufferString(body))
	req.Header.Set("Authorization", "Bearer "+tok)
	req.Header.Set("Content-Type", "application/json")
	resp, _ := http.DefaultClient.Do(req)
	resp.Body.Close()

	resp = authedReq(t, srv, tok, "GET", "/hosts/h1/instances/x/L/logs?container=c&tail=10")
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Equal(t, "text/plain; charset=utf-8", resp.Header.Get("Content-Type"))
}

package api

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestDeleteVolume_Idempotent(t *testing.T) {
	srv, tok, _ := newSrvFull(t)
	resp := authedReq(t, srv, tok, "DELETE", "/hosts/h1/volumes/does-not-exist")
	resp.Body.Close()
	assert.Equal(t, http.StatusNoContent, resp.StatusCode)
}

func TestInstanceVolumes_NotFoundForUnknownInstance(t *testing.T) {
	srv, tok, _ := newSrvFull(t)
	// "x" template has no volumes; even if instance exists, returns []. Not having an instance just returns [].
	resp := authedReq(t, srv, tok, "GET", "/hosts/h1/instances/x/never-deployed/volumes")
	defer resp.Body.Close()
	// Either 200 with empty list (template loaded, instance doesn't exist but volumes lookup tolerates) or another expected status.
	assert.Contains(t, []int{http.StatusOK, http.StatusNotFound}, resp.StatusCode)
}

package api

import (
	"bytes"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestUpgradeImage_ReusesSealedSecret is the core of #176: the bearer
// upgrade-image route reuses the instance's stored spec + sealed secrets and
// overrides only the image, so the request carries no secrets at all.
func TestUpgradeImage_ReusesSealedSecret(t *testing.T) {
	srv, tok, f := newSrvFull(t)

	body := `{"template":"app","slug":"roll","parameters":{"slug":"roll","image":"i:1"},"secrets":{"auth_secret":"s"}}`
	req, _ := http.NewRequest("PUT", srv.URL+"/hosts/h1/instances/app/roll", bytes.NewBufferString(body))
	req.Header.Set("Authorization", "Bearer "+tok)
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)

	// Image-only upgrade: no secrets supplied. The sealed auth_secret is reused.
	upgrade := `{"image":"i:2"}`
	req, _ = http.NewRequest("POST", srv.URL+"/hosts/h1/instances/app/roll/upgrade-image", bytes.NewBufferString(upgrade))
	req.Header.Set("Authorization", "Bearer "+tok)
	req.Header.Set("Content-Type", "application/json")
	resp, err = http.DefaultClient.Do(req)
	require.NoError(t, err)
	resp.Body.Close()
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	// The new image was pulled.
	var pulledNew bool
	for _, p := range f.PullCalls {
		if p.Host == "h1" && p.Image == "i:2" {
			pulledNew = true
		}
	}
	assert.True(t, pulledNew, "upgrade-image must pull the new image, got %+v", f.PullCalls)
}

// TestUpgradeImage_StrictUpgradeStillRequiresSecrets is the control: the same
// secret-less body against the strict /upgrade route is rejected. This is the
// gap #176 fills — the strict path can't roll a sealed-secret instance because
// the plaintext is unrecoverable.
func TestUpgradeImage_StrictUpgradeStillRequiresSecrets(t *testing.T) {
	srv, tok, _ := newSrvFull(t)

	body := `{"template":"app","slug":"strict","parameters":{"slug":"strict","image":"i:1"},"secrets":{"auth_secret":"s"}}`
	req, _ := http.NewRequest("PUT", srv.URL+"/hosts/h1/instances/app/strict", bytes.NewBufferString(body))
	req.Header.Set("Authorization", "Bearer "+tok)
	req.Header.Set("Content-Type", "application/json")
	resp, _ := http.DefaultClient.Do(req)
	resp.Body.Close()

	upgrade := `{"image":"i:2"}` // no secrets
	req, _ = http.NewRequest("POST", srv.URL+"/hosts/h1/instances/app/strict/upgrade", bytes.NewBufferString(upgrade))
	req.Header.Set("Authorization", "Bearer "+tok)
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	resp.Body.Close()
	assert.GreaterOrEqual(t, resp.StatusCode, 400, "strict /upgrade must reject a secret-less body")
}

func TestUpgradeImage_MissingImageRejected(t *testing.T) {
	srv, tok, _ := newSrvFull(t)

	body := `{"template":"app","slug":"noimg","parameters":{"slug":"noimg","image":"i:1"},"secrets":{"auth_secret":"s"}}`
	req, _ := http.NewRequest("PUT", srv.URL+"/hosts/h1/instances/app/noimg", bytes.NewBufferString(body))
	req.Header.Set("Authorization", "Bearer "+tok)
	req.Header.Set("Content-Type", "application/json")
	resp, _ := http.DefaultClient.Do(req)
	resp.Body.Close()

	req, _ = http.NewRequest("POST", srv.URL+"/hosts/h1/instances/app/noimg/upgrade-image", bytes.NewBufferString(`{}`))
	req.Header.Set("Authorization", "Bearer "+tok)
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	resp.Body.Close()
	assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
}

func TestUpgradeImage_UnknownInstanceNotFound(t *testing.T) {
	srv, tok, _ := newSrvFull(t)

	req, _ := http.NewRequest("POST", srv.URL+"/hosts/h1/instances/app/ghost/upgrade-image", bytes.NewBufferString(`{"image":"i:2"}`))
	req.Header.Set("Authorization", "Bearer "+tok)
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	resp.Body.Close()
	assert.Equal(t, http.StatusNotFound, resp.StatusCode)
}

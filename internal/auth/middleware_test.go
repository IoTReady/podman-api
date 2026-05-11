package auth

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/iotready/podman-api/internal/config"
)

func newKey(t *testing.T, scopes ...string) (config.APIKey, string) {
	t.Helper()
	tok := "test-secret"
	hash, err := config.HashToken(tok)
	require.NoError(t, err)
	return config.APIKey{ID: "k1", SecretHash: hash, Scopes: scopes}, tok
}

func newReq(token string) *http.Request {
	r := httptest.NewRequest(http.MethodGet, "/x", nil)
	if token != "" {
		r.Header.Set("Authorization", "Bearer "+token)
	}
	return r
}

func TestMiddleware_NoHeader_401(t *testing.T) {
	k, _ := newKey(t, "hosts:read")
	mw := New(NewKeyStore([]config.APIKey{k}), "hosts:read")
	rr := httptest.NewRecorder()
	mw(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		t.Fatal("inner handler should not run")
	})).ServeHTTP(rr, newReq(""))
	assert.Equal(t, http.StatusUnauthorized, rr.Code)
}

func TestMiddleware_ValidToken_AllowsRequest(t *testing.T) {
	k, tok := newKey(t, "hosts:read")
	mw := New(NewKeyStore([]config.APIKey{k}), "hosts:read")
	rr := httptest.NewRecorder()
	called := false
	mw(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		called = true
		w.WriteHeader(http.StatusNoContent)
	})).ServeHTTP(rr, newReq(tok))
	assert.True(t, called)
	assert.Equal(t, http.StatusNoContent, rr.Code)
}

func TestMiddleware_WrongToken_401(t *testing.T) {
	k, _ := newKey(t, "hosts:read")
	mw := New(NewKeyStore([]config.APIKey{k}), "hosts:read")
	rr := httptest.NewRecorder()
	mw(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		t.Fatal("inner should not run")
	})).ServeHTTP(rr, newReq("wrong"))
	assert.Equal(t, http.StatusUnauthorized, rr.Code)
}

func TestMiddleware_MissingScope_403(t *testing.T) {
	k, tok := newKey(t, "hosts:read")
	mw := New(NewKeyStore([]config.APIKey{k}), "instances:write")
	rr := httptest.NewRecorder()
	mw(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		t.Fatal("inner should not run")
	})).ServeHTTP(rr, newReq(tok))
	assert.Equal(t, http.StatusForbidden, rr.Code)
}

func TestKeyIDFromContext(t *testing.T) {
	k, tok := newKey(t, "hosts:read")
	mw := New(NewKeyStore([]config.APIKey{k}), "hosts:read")
	rr := httptest.NewRecorder()
	mw(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "k1", KeyIDFromContext(r.Context()))
		w.WriteHeader(http.StatusOK)
	})).ServeHTTP(rr, newReq(tok))
	assert.Equal(t, http.StatusOK, rr.Code)
}

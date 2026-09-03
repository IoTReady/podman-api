package api

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/iotready/podman-api/internal/auth"
	"github.com/iotready/podman-api/internal/config"
	"github.com/iotready/podman-api/internal/instance"
	"github.com/iotready/podman-api/internal/podman"
	"github.com/iotready/podman-api/internal/podman/fake"
	"github.com/iotready/podman-api/internal/store"
)

func authedReq(t *testing.T, srv *httptest.Server, tok, method, path string) *http.Response {
	t.Helper()
	req, err := http.NewRequest(method, srv.URL+path, nil)
	require.NoError(t, err)
	req.Header.Set("Authorization", "Bearer "+tok)
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	return resp
}

func TestListHosts(t *testing.T) {
	tok := "t"
	hash, _ := config.HashToken(tok)
	keys := []config.APIKey{{ID: "k", SecretHash: hash, Scopes: []string{"hosts:read"}}}
	hosts := []config.Host{
		{ID: "h1", Addr: "unix", Socket: "/x", Labels: map[string]string{"env": "dev"}},
	}
	svc := instance.NewService(fake.New(), hosts)
	svc.SetStore(store.NewMemory())
	srv := httptest.NewServer(NewRouter(svc, nil, auth.NewKeyStore(keys), nil, nil, nil, "", nil))
	defer srv.Close()

	resp := authedReq(t, srv, tok, "GET", "/hosts")
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)

	var got []map[string]any
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&got))
	require.Len(t, got, 1)
	assert.Equal(t, "h1", got[0]["id"])
	assert.Equal(t, "unix", got[0]["addr"])
}

func TestHostHealthz(t *testing.T) {
	tok := "t"
	hash, _ := config.HashToken(tok)
	keys := []config.APIKey{{ID: "k", SecretHash: hash, Scopes: []string{"hosts:read"}}}
	hosts := []config.Host{{ID: "h1", Addr: "unix", Socket: "/x"}}
	svc := instance.NewService(fake.New(), hosts)
	svc.SetStore(store.NewMemory())
	srv := httptest.NewServer(NewRouter(svc, nil, auth.NewKeyStore(keys), nil, nil, nil, "", nil))
	defer srv.Close()

	resp := authedReq(t, srv, tok, "GET", "/hosts/h1/healthz")
	defer resp.Body.Close()
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	resp = authedReq(t, srv, tok, "GET", "/hosts/nope/healthz")
	defer resp.Body.Close()
	assert.Equal(t, http.StatusNotFound, resp.StatusCode)
}

type fakeHostRenamer struct {
	calledOld, calledNew string
	original             []byte
	path                 string
	err                  error
}

func (f *fakeHostRenamer) RenameHostFile(oldID, newID string) (string, []byte, error) {
	f.calledOld, f.calledNew = oldID, newID
	if f.err != nil {
		return "", nil, f.err
	}
	path := f.path
	if path == "" {
		path = "/fake/path.yaml"
	}
	return path, f.original, nil
}

func TestRenameHost(t *testing.T) {
	tok := "t"
	hash, _ := config.HashToken(tok)
	keys := []config.APIKey{{ID: "k", SecretHash: hash, Scopes: []string{"hosts:write", "hosts:read"}}}
	hosts := []config.Host{{ID: "h1", Addr: "unix", Socket: "/x"}}
	svc := instance.NewService(fake.New(), hosts)
	svc.SetStore(store.NewMemory())
	renamer := &fakeHostRenamer{original: []byte("id: h1\n")}
	srv := httptest.NewServer(NewRouter(svc, nil, auth.NewKeyStore(keys), nil, nil, nil, "", nil, WithHostRenamer(renamer)))
	defer srv.Close()

	req, err := http.NewRequest("POST", srv.URL+"/hosts/h1/rename", strings.NewReader(`{"new_id":"h2"}`))
	require.NoError(t, err)
	req.Header.Set("Authorization", "Bearer "+tok)
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)
	require.Equal(t, "h1", renamer.calledOld)
	require.Equal(t, "h2", renamer.calledNew)

	// Old id gone, new id live — no restart between the calls.
	resp2 := authedReq(t, srv, tok, "GET", "/hosts/h1")
	defer resp2.Body.Close()
	require.Equal(t, http.StatusNotFound, resp2.StatusCode)

	resp3 := authedReq(t, srv, tok, "GET", "/hosts/h2")
	defer resp3.Body.Close()
	require.Equal(t, http.StatusOK, resp3.StatusCode)
}

// TestRenameHostUnknownField asserts renameHost rejects an unknown field
// with a 400 rather than silently ignoring it (#254).
func TestRenameHostUnknownField(t *testing.T) {
	tok := "t"
	hash, _ := config.HashToken(tok)
	keys := []config.APIKey{{ID: "k", SecretHash: hash, Scopes: []string{"hosts:write", "hosts:read"}}}
	hosts := []config.Host{{ID: "h1", Addr: "unix", Socket: "/x"}}
	svc := instance.NewService(fake.New(), hosts)
	svc.SetStore(store.NewMemory())
	renamer := &fakeHostRenamer{original: []byte("id: h1\n")}
	srv := httptest.NewServer(NewRouter(svc, nil, auth.NewKeyStore(keys), nil, nil, nil, "", nil, WithHostRenamer(renamer)))
	defer srv.Close()

	req, err := http.NewRequest("POST", srv.URL+"/hosts/h1/rename", strings.NewReader(`{"new_i":"h2"}`))
	require.NoError(t, err)
	req.Header.Set("Authorization", "Bearer "+tok)
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusBadRequest, resp.StatusCode)
	require.Empty(t, renamer.calledOld)
}

func TestRenameHostUnknownHost(t *testing.T) {
	tok := "t"
	hash, _ := config.HashToken(tok)
	keys := []config.APIKey{{ID: "k", SecretHash: hash, Scopes: []string{"hosts:write"}}}
	svc := instance.NewService(fake.New(), nil)
	svc.SetStore(store.NewMemory())
	srv := httptest.NewServer(NewRouter(svc, nil, auth.NewKeyStore(keys), nil, nil, nil, "", nil, WithHostRenamer(&fakeHostRenamer{})))
	defer srv.Close()

	req, _ := http.NewRequest("POST", srv.URL+"/hosts/nope/rename", strings.NewReader(`{"new_id":"h2"}`))
	req.Header.Set("Authorization", "Bearer "+tok)
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusNotFound, resp.StatusCode)
}

func TestRenameHostConflict(t *testing.T) {
	tok := "t"
	hash, _ := config.HashToken(tok)
	keys := []config.APIKey{{ID: "k", SecretHash: hash, Scopes: []string{"hosts:write"}}}
	hosts := []config.Host{{ID: "h1"}, {ID: "h2"}}
	svc := instance.NewService(fake.New(), hosts)
	svc.SetStore(store.NewMemory())
	srv := httptest.NewServer(NewRouter(svc, nil, auth.NewKeyStore(keys), nil, nil, nil, "", nil, WithHostRenamer(&fakeHostRenamer{})))
	defer srv.Close()

	req, _ := http.NewRequest("POST", srv.URL+"/hosts/h1/rename", strings.NewReader(`{"new_id":"h2"}`))
	req.Header.Set("Authorization", "Bearer "+tok)
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusConflict, resp.StatusCode)
}

func TestRenameHostFileRenameFailsLeavesStoreUntouched(t *testing.T) {
	tok := "t"
	hash, _ := config.HashToken(tok)
	keys := []config.APIKey{{ID: "k", SecretHash: hash, Scopes: []string{"hosts:write"}}}
	hosts := []config.Host{{ID: "h1"}}
	svc := instance.NewService(fake.New(), hosts)
	st := store.NewMemory()
	svc.SetStore(st)
	renamer := &fakeHostRenamer{err: errors.New("disk full")}
	srv := httptest.NewServer(NewRouter(svc, nil, auth.NewKeyStore(keys), nil, nil, nil, "", nil, WithHostRenamer(renamer)))
	defer srv.Close()

	req, _ := http.NewRequest("POST", srv.URL+"/hosts/h1/rename", strings.NewReader(`{"new_id":"h2"}`))
	req.Header.Set("Authorization", "Bearer "+tok)
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusInternalServerError, resp.StatusCode)

	got := svc.Hosts()
	require.Len(t, got, 1)
	require.Equal(t, "h1", got[0].ID) // unchanged
}

// A store that accepts the preflight but fails the actual migration, so the
// handler's revert path is still exercised now that every refusal the store
// can predict is caught before the config file is touched.
type renameFailingStore struct {
	*store.Memory
	err error
}

func (s renameFailingStore) RenameHost(_ context.Context, _, _ string) error { return s.err }

// The most common refusal — the host has backups — must never rewrite the
// config file first and revert afterwards (final-review finding #4).
func TestRenameHostWithBackupsNeverTouchesTheFile(t *testing.T) {
	tok := "t"
	hash, _ := config.HashToken(tok)
	keys := []config.APIKey{{ID: "k", SecretHash: hash, Scopes: []string{"hosts:write"}}}
	hosts := []config.Host{{ID: "h1"}}
	svc := instance.NewService(fake.New(), hosts)
	st := store.NewMemory()
	svc.SetStore(st)
	require.NoError(t, st.CreateBackup(context.Background(), store.Backup{
		ID: store.NewBackupID(), Host: "h1", Template: "tmpl", Slug: "slug", State: store.BackupComplete,
	}))

	renamer := &fakeHostRenamer{}
	srv := httptest.NewServer(NewRouter(svc, nil, auth.NewKeyStore(keys), nil, nil, nil, "", nil, WithHostRenamer(renamer)))
	defer srv.Close()

	req, _ := http.NewRequest("POST", srv.URL+"/hosts/h1/rename", strings.NewReader(`{"new_id":"h2"}`))
	req.Header.Set("Authorization", "Bearer "+tok)
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusConflict, resp.StatusCode)

	var body ErrorBody
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&body))
	require.Equal(t, "host_has_backups", body.Code)

	require.Empty(t, renamer.calledOld, "RenameHostFile must not be called for a rename the store will refuse")
	require.Equal(t, "h1", svc.Hosts()[0].ID)
}

func TestRenameHostRevertsFileOnStoreFailure(t *testing.T) {
	tok := "t"
	hash, _ := config.HashToken(tok)
	keys := []config.APIKey{{ID: "k", SecretHash: hash, Scopes: []string{"hosts:write", "hosts:read"}}}
	hosts := []config.Host{{ID: "h1"}}
	svc := instance.NewService(fake.New(), hosts)
	svc.SetStore(renameFailingStore{Memory: store.NewMemory(), err: errors.New("disk on fire")})

	original := []byte("id: h1\n")
	tmpFile := filepath.Join(t.TempDir(), "h1.yaml")
	require.NoError(t, os.WriteFile(tmpFile, original, 0o600))
	// The rewritten file stands in for what RenameHostFile would have left behind.
	require.NoError(t, os.WriteFile(tmpFile, []byte("id: h2\n"), 0o600))
	renamer := &fakeHostRenamer{original: original, path: tmpFile}

	srv := httptest.NewServer(NewRouter(svc, nil, auth.NewKeyStore(keys), nil, nil, nil, "", nil, WithHostRenamer(renamer)))
	defer srv.Close()

	req, _ := http.NewRequest("POST", srv.URL+"/hosts/h1/rename", strings.NewReader(`{"new_id":"h2"}`))
	req.Header.Set("Authorization", "Bearer "+tok)
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusInternalServerError, resp.StatusCode)

	require.Equal(t, "h1", renamer.calledOld)
	require.Equal(t, "h2", renamer.calledNew)

	// The file was reverted to its original bytes, keeping its original mode.
	gotBytes, err := os.ReadFile(tmpFile)
	require.NoError(t, err)
	require.Equal(t, original, gotBytes)
	fi, err := os.Stat(tmpFile)
	require.NoError(t, err)
	require.Equal(t, os.FileMode(0o600), fi.Mode().Perm())

	// The store-level rename never took effect: host still under old id.
	got := svc.Hosts()
	require.Len(t, got, 1)
	require.Equal(t, "h1", got[0].ID)
}

// A successful rename must tell the server to re-read hosts/*.yaml, which is
// what refreshes the podman client's host map and the background loops'
// host list (final-review finding #2).
func TestRenameHostTriggersHostsReload(t *testing.T) {
	tok := "t"
	hash, _ := config.HashToken(tok)
	keys := []config.APIKey{{ID: "k", SecretHash: hash, Scopes: []string{"hosts:write"}}}
	hosts := []config.Host{{ID: "h1"}}
	svc := instance.NewService(fake.New(), hosts)
	svc.SetStore(store.NewMemory())

	reloads := 0
	srv := httptest.NewServer(NewRouter(svc, nil, auth.NewKeyStore(keys), nil, nil, nil, "", nil,
		WithHostRenamer(&fakeHostRenamer{original: []byte("id: h1\n")}),
		WithHostsReloader(func() error { reloads++; return nil })))
	defer srv.Close()

	req, _ := http.NewRequest("POST", srv.URL+"/hosts/h1/rename", strings.NewReader(`{"new_id":"h2"}`))
	req.Header.Set("Authorization", "Bearer "+tok)
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)
	require.Equal(t, 1, reloads)
}

// A reloader that fails must not fail the request: the rename is already
// committed to the config file and the store by then.
func TestRenameHostSucceedsWhenReloadFails(t *testing.T) {
	tok := "t"
	hash, _ := config.HashToken(tok)
	keys := []config.APIKey{{ID: "k", SecretHash: hash, Scopes: []string{"hosts:write"}}}
	hosts := []config.Host{{ID: "h1"}}
	svc := instance.NewService(fake.New(), hosts)
	svc.SetStore(store.NewMemory())

	srv := httptest.NewServer(NewRouter(svc, nil, auth.NewKeyStore(keys), nil, nil, nil, "", nil,
		WithHostRenamer(&fakeHostRenamer{original: []byte("id: h1\n")}),
		WithHostsReloader(func() error { return errors.New("hosts dir vanished") })))
	defer srv.Close()

	req, _ := http.NewRequest("POST", srv.URL+"/hosts/h1/rename", strings.NewReader(`{"new_id":"h2"}`))
	req.Header.Set("Authorization", "Bearer "+tok)
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)
	require.Equal(t, "h2", svc.Hosts()[0].ID)
}

// blockingRenameStore parks the FIRST store-level rename until the test
// releases it, so the second request is guaranteed to arrive while the first
// holds the host lock — the interleaving the split-brain needs, forced rather
// than hoped for.
type blockingRenameStore struct {
	*store.Memory
	once    sync.Once
	entered chan struct{}
	release chan struct{}
}

func (b *blockingRenameStore) RenameHost(ctx context.Context, oldID, newID string) error {
	b.once.Do(func() { close(b.entered) })
	<-b.release
	return b.Memory.RenameHost(ctx, oldID, newID)
}

// Two concurrent renames of the SAME host to DIFFERENT new ids must leave the
// on-disk config agreeing with the store. This runs against a real hosts dir
// and the real config.HostsDir renamer — the whole point is the actual
// file-rewrite code path, which a fake renamer would not exercise.
//
// Before the fix the handler rewrote hosts/*.yaml itself, outside any lock:
// both requests read `id: h1`, one write was silently lost, and then the loser
// — which fails RenameHost's inside-the-lock check with ErrUnknownHost —
// reverted the file to the `id: h1` bytes it had captured, AFTER the winner had
// already committed `h2` to the store and the live host list. The file said h1
// forever while the system said h2. With the rewrite (and its revert) inside
// the same lock, the loser never rewrites anything.
func TestRenameHostConcurrentSameHostKeepsConfigConsistent(t *testing.T) {
	tok := "t"
	hash, _ := config.HashToken(tok)
	keys := []config.APIKey{{ID: "k", SecretHash: hash, Scopes: []string{"hosts:write", "hosts:read"}}}
	hosts := []config.Host{{ID: "h1", Addr: "unix", Socket: "/x"}}
	svc := instance.NewService(fake.New(), hosts)
	st := &blockingRenameStore{
		Memory:  store.NewMemory(),
		entered: make(chan struct{}),
		release: make(chan struct{}),
	}
	svc.SetStore(st)
	// Unpark the store on any exit path: a t.Fatal with a request still parked
	// leaves httptest.Server.Close blocked on the live connection, turning a
	// clear assertion failure into a hang.
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(st.release) }) }

	dir := t.TempDir()
	hostFile := filepath.Join(dir, "h1.yaml")
	require.NoError(t, os.WriteFile(hostFile, []byte("id: h1\naddr: unix\nsocket: /x\n"), 0o600))

	srv := httptest.NewServer(NewRouter(svc, nil, auth.NewKeyStore(keys), nil, nil, nil, "", nil,
		WithHostRenamer(config.HostsDir(dir))))
	defer srv.Close()
	defer release() // LIFO: unpark before Close, or Close waits on the parked request

	rename := func(newID string) <-chan int {
		done := make(chan int, 1)
		go func() {
			req, _ := http.NewRequest("POST", srv.URL+"/hosts/h1/rename", strings.NewReader(`{"new_id":"`+newID+`"}`))
			req.Header.Set("Authorization", "Bearer "+tok)
			resp, err := http.DefaultClient.Do(req)
			if err != nil {
				done <- 0
				return
			}
			defer resp.Body.Close()
			done <- resp.StatusCode
		}()
		return done
	}

	first := rename("h2")
	select {
	case <-st.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("the first rename never reached the store migration")
	}

	// Issued while the first holds the host lock. It must make no progress —
	// in particular it must not rewrite the config file — until the first is
	// done.
	second := rename("h3")
	select {
	case code := <-second:
		t.Fatalf("the second rename completed (HTTP %d) while the first held the lock", code)
	case <-time.After(250 * time.Millisecond):
	}

	release()

	codes := []int{}
	for range 2 {
		select {
		case c := <-first:
			codes = append(codes, c)
			first = nil
		case c := <-second:
			codes = append(codes, c)
			second = nil
		case <-time.After(5 * time.Second):
			t.Fatal("a rename request never returned")
		}
	}

	oks := 0
	for _, c := range codes {
		if c == http.StatusOK {
			oks++
		}
	}
	require.Equal(t, 1, oks, "exactly one of two concurrent renames may succeed, got %v", codes)

	// The winner is h1 -> h2 (it entered the store migration first). Both the
	// live host set and the config file must say so.
	live := svc.Hosts()
	require.Len(t, live, 1)
	require.Equal(t, "h2", live[0].ID)

	got, err := os.ReadFile(hostFile)
	require.NoError(t, err)
	require.Contains(t, string(got), "id: h2\n",
		"the config file must match the rename that won; %q means a lost update or a stale revert", string(got))
	require.NotContains(t, string(got), "id: h1\n")
	require.NotContains(t, string(got), "id: h3\n")

	// And the file is still parseable as the one live host — the definitive
	// check that config and store have not diverged.
	loaded, err := config.LoadHosts(dir)
	require.NoError(t, err)
	require.Len(t, loaded, 1)
	require.Equal(t, live[0].ID, loaded[0].ID)
	require.Equal(t, "/x", loaded[0].Socket, "the rest of the host record must survive the rewrite")
}

func TestRenameHostNoRenamerConfigured(t *testing.T) {
	tok := "t"
	hash, _ := config.HashToken(tok)
	keys := []config.APIKey{{ID: "k", SecretHash: hash, Scopes: []string{"hosts:write"}}}
	hosts := []config.Host{{ID: "h1"}}
	svc := instance.NewService(fake.New(), hosts)
	svc.SetStore(store.NewMemory())
	srv := httptest.NewServer(NewRouter(svc, nil, auth.NewKeyStore(keys), nil, nil, nil, "", nil)) // no WithHostRenamer
	defer srv.Close()

	req, _ := http.NewRequest("POST", srv.URL+"/hosts/h1/rename", strings.NewReader(`{"new_id":"h2"}`))
	req.Header.Set("Authorization", "Bearer "+tok)
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusNotImplemented, resp.StatusCode)
}

// Finding #8: "feature absent" outranks every request-shaped rejection, so an
// unwired server never misreports the reason.
func TestRenameHostNoRenamerConfiguredOutranksValidation(t *testing.T) {
	tok := "t"
	hash, _ := config.HashToken(tok)
	keys := []config.APIKey{{ID: "k", SecretHash: hash, Scopes: []string{"hosts:write"}}}
	hosts := []config.Host{{ID: "h1"}, {ID: "h2"}}
	svc := instance.NewService(fake.New(), hosts)
	svc.SetStore(store.NewMemory())
	srv := httptest.NewServer(NewRouter(svc, nil, auth.NewKeyStore(keys), nil, nil, nil, "", nil)) // no WithHostRenamer
	defer srv.Close()

	for _, tc := range []struct{ name, path, body string }{
		{"unknown host", "/hosts/nope/rename", `{"new_id":"h9"}`},
		{"conflict", "/hosts/h1/rename", `{"new_id":"h2"}`},
		{"invalid body", "/hosts/h1/rename", `not json`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			req, _ := http.NewRequest("POST", srv.URL+tc.path, strings.NewReader(tc.body))
			req.Header.Set("Authorization", "Bearer "+tok)
			resp, err := http.DefaultClient.Do(req)
			require.NoError(t, err)
			defer resp.Body.Close()
			require.Equal(t, http.StatusNotImplemented, resp.StatusCode)
		})
	}
}

// --- GET /hosts: one libpod probe per host (#289) ----------------------------

// captureLog redirects the standard logger for the test's duration.
func captureLog(t *testing.T) func() string {
	t.Helper()
	var mu sync.Mutex
	var buf strings.Builder
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

func countLogLines(s, sub string) int {
	n := 0
	for _, line := range strings.Split(s, "\n") {
		if line != "" && strings.Contains(line, sub) {
			n++
		}
	}
	return n
}

// Ping, Version and HostInfo are all `system.Info` against a real host, so
// rendering one host must issue exactly one of them. Three sequential copies
// inside listHostsPerHostTimeout is what dropped dev's version and its whole
// load object from GET /hosts (#289).
func TestHostView_MakesOneLibpodProbePerHost(t *testing.T) {
	srv, tok, f := newSrvFull(t)

	resp := authedReq(t, srv, tok, "GET", "/hosts")
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)

	assert.Equal(t, 1, f.HostInfoCalls, "exactly one libpod info per rendered host")
	assert.Zero(t, f.PingCalls, "reachability comes from the same info call, not a second probe")
	assert.Zero(t, f.VersionCalls, "the version comes from the same info call, not a third probe")
}

// The version must still be reported, and must come from HostInfo now.
func TestHostView_VersionComesFromHostInfo(t *testing.T) {
	srv, tok, f := newSrvFull(t)
	f.VersionStr = "5.8.4"
	f.HostInfoVal = podman.HostInfo{CPUs: 4}

	resp := authedReq(t, srv, tok, "GET", "/hosts/h1")
	defer resp.Body.Close()
	var got map[string]any
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&got))
	assert.Equal(t, "ok", got["status"])
	assert.Equal(t, "5.8.4", got["podman_version"])
	assert.Zero(t, f.VersionCalls)
}

// A host whose libpod info fails is unreachable and carries neither a version
// nor a load object — and says so in the log, once.
func TestHostView_InfoFailureIsUnreachableAndLoggedOnce(t *testing.T) {
	logs := captureLog(t)
	srv, tok, f := newSrvFull(t)
	f.HostInfoErr = errors.New("context deadline exceeded")

	for i := 0; i < 3; i++ {
		resp := authedReq(t, srv, tok, "GET", "/hosts")
		var body []map[string]any
		require.NoError(t, json.NewDecoder(resp.Body).Decode(&body))
		resp.Body.Close()
		require.Len(t, body, 1)
		assert.Equal(t, "unreachable", body[0]["status"])
		assert.NotContains(t, body[0], "load")
		assert.NotContains(t, body[0], "podman_version")
	}

	assert.Equal(t, 1, countLogLines(logs(), "rendered incomplete"), logs())
	assert.Contains(t, logs(), "[unreachable]")
	assert.Contains(t, logs(), "context deadline exceeded")
}

// The observability hole this closes: a host that reports every other metric
// but never a loadavg used to produce NO log line anywhere, because every
// give-up path in podman.Real.hostLoadAvg returns a bare nil and three of them
// do not log at all (#258 residue).
func TestHostView_MissingLoadAvgIsLoggedOnceThenOnRecovery(t *testing.T) {
	logs := captureLog(t)
	srv, tok, f := newSrvFull(t)
	f.HostInfoVal = podman.HostInfo{CPUs: 8, MemTotal: 100, MemFree: 50} // LoadAvg nil

	for i := 0; i < 3; i++ {
		resp := authedReq(t, srv, tok, "GET", "/hosts")
		resp.Body.Close()
	}
	assert.Equal(t, 1, countLogLines(logs(), "rendered incomplete"), logs())
	assert.Contains(t, logs(), "[loadavg]")
	assert.Contains(t, logs(), "load.loadavg absent")

	la := [3]float64{1, 2, 3}
	f.HostInfoVal = podman.HostInfo{CPUs: 8, MemTotal: 100, MemFree: 50, LoadAvg: &la}
	for i := 0; i < 3; i++ {
		resp := authedReq(t, srv, tok, "GET", "/hosts")
		resp.Body.Close()
	}
	assert.Equal(t, 1, countLogLines(logs(), "renders completely again"), logs())
	assert.Equal(t, 1, countLogLines(logs(), "rendered incomplete"), logs())
}

// A host that has always rendered completely says nothing.
func TestHostView_CompleteRenderIsSilent(t *testing.T) {
	logs := captureLog(t)
	srv, tok, f := newSrvFull(t)
	la := [3]float64{1, 2, 3}
	f.HostInfoVal = podman.HostInfo{CPUs: 8, LoadAvg: &la}

	for i := 0; i < 3; i++ {
		resp := authedReq(t, srv, tok, "GET", "/hosts")
		resp.Body.Close()
	}
	assert.Empty(t, logs())
}

// GET /hosts/{id} runs the same render on a much larger budget, so it must not
// contribute to the transition log — otherwise the two paths' differing
// verdicts flap against each other for a host that is merely slow.
func TestHostView_SingleHostRouteDoesNotLog(t *testing.T) {
	logs := captureLog(t)
	srv, tok, f := newSrvFull(t)
	f.HostInfoErr = errors.New("boom")

	resp := authedReq(t, srv, tok, "GET", "/hosts/h1")
	resp.Body.Close()

	assert.Empty(t, logs())
}

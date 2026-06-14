package instance

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"

	"github.com/iotready/podman-api/extension"
	"github.com/iotready/podman-api/internal/podman"
)

// recordingInjector is a test double for extension.SidecarInjector. It records
// what it was handed and returns either a fixed replacement YAML or an error.
type recordingInjector struct {
	out       string // replacement YAML; "" means echo the input
	secrets   []extension.InjectedSecret
	err       error // non-nil aborts injection
	calls     int   // number of times InjectSidecars was called
	gotYAML   string
	gotMeta   extension.TemplateMeta
	gotParams map[string]any
	gotSlug   string
}

func (r *recordingInjector) InjectSidecars(_ context.Context, yaml string, meta extension.TemplateMeta, params map[string]any, slug string) (extension.SidecarInjection, error) {
	r.calls++
	r.gotYAML, r.gotMeta, r.gotParams, r.gotSlug = yaml, meta, params, slug
	if r.err != nil {
		return extension.SidecarInjection{}, r.err
	}
	out := yaml
	if r.out != "" {
		out = r.out
	}
	return extension.SidecarInjection{YAML: out, Secrets: r.secrets}, nil
}

// injectedPod is a full pod manifest a sidecar injector might return — the
// original db container plus a litestream sidecar with its own image.
const injectedPod = `apiVersion: v1
kind: Pod
metadata:
  name: postgres-demo
spec:
  containers:
    - name: db
      image: docker.io/library/postgres:16
    - name: litestream
      image: docker.io/litestream/litestream:0.3.13
`

// On Apply, a registered injector's output must be the YAML that reaches
// PlayKube, and images it introduces must be pre-pulled (injection runs before
// the pull loop). The injector is handed the projected template meta, the
// resolved params, and the slug.
func TestService_Apply_InjectsSidecar(t *testing.T) {
	svc, f := newSvc(t)
	inj := &recordingInjector{out: injectedPod}
	svc.SetSidecarInjector(inj)

	require.NoError(t, svc.Apply(context.Background(), "h1", pgApply("demo"), ApplyOptions{Replace: true}))

	// Injector saw the rendered YAML and the call context.
	require.Equal(t, 1, inj.calls)
	assert.Contains(t, inj.gotYAML, "name: postgres-demo")
	assert.Equal(t, "demo", inj.gotSlug)
	assert.Equal(t, "docker.io/library/postgres:16", inj.gotParams["image"])

	// Meta is the public projection of render.Meta, carrying ID + backup volumes.
	assert.Equal(t, "postgres", inj.gotMeta.ID)
	assert.Equal(t, []extension.TemplateVolume{{Name: "data", Backup: "none"}}, inj.gotMeta.Volumes)

	// The injected YAML — not the original — is what got played.
	require.Len(t, f.PlayCalls, 1)
	assert.Equal(t, injectedPod, f.PlayCalls[0].YAML)

	// The sidecar image was pulled because injection precedes the pull loop.
	var pulled []string
	for _, p := range f.PullCalls {
		pulled = append(pulled, p.Image)
	}
	assert.Contains(t, pulled, "docker.io/litestream/litestream:0.3.13",
		"sidecar image must be pre-pulled (injection runs before the pull loop)")
}

// An injector error must abort Apply before any host mutation: error wrapped,
// no pod played, no secret written, no spec persisted.
func TestService_Apply_InjectorError_Aborts(t *testing.T) {
	svc, f, mem := newSvcMem(t)
	inj := &recordingInjector{err: errors.New("boom")}
	svc.SetSidecarInjector(inj)
	ctx := context.Background()

	err := svc.Apply(ctx, "h1", pgApply("demo"), ApplyOptions{Replace: true})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "sidecar inject")
	assert.Contains(t, err.Error(), "boom")

	assert.Empty(t, f.PlayCalls, "PlayKube must not be called when injection fails")
	_, secErr := f.SecretInspect(ctx, "h1", "postgres-demo-password")
	assert.ErrorIs(t, secErr, podman.ErrNotFound, "no secret should be written when injection fails")
	_, specErr := mem.GetSpec(ctx, "h1", "postgres", "demo")
	require.Error(t, specErr, "no spec should be persisted when injection fails")
}

// With no injector wired, Apply plays the normally-rendered YAML unchanged.
func TestService_Apply_NilInjector_PassThrough(t *testing.T) {
	svc, f := newSvc(t)
	require.NoError(t, svc.Apply(context.Background(), "h1", pgApply("demo"), ApplyOptions{Replace: true}))
	require.Len(t, f.PlayCalls, 1)
	assert.Contains(t, f.PlayCalls[0].YAML, "name: postgres-demo")
	assert.NotContains(t, f.PlayCalls[0].YAML, "litestream")
}

// On boot converge (reconcileOneSpec), a registered injector's output is what
// reaches PlayKube, and it receives the projected meta + slug.
func TestReconcileOneSpec_InjectsSidecar(t *testing.T) {
	svc, fc, st := specReconcileSvc(t)
	ctx := context.Background()
	seedBootSpec(t, st, "h1", "web", "my-app", nil)

	const reconciledPod = "apiVersion: v1\nkind: Pod\nmetadata:\n  name: web-my-app\nspec:\n  containers:\n    - name: app\n      image: nginx:latest\n    - name: litestream\n      image: docker.io/litestream/litestream:0.3.13\n"
	inj := &recordingInjector{out: reconciledPod}
	svc.SetSidecarInjector(inj)

	svc.ReconcileSpecsOnHost(ctx, "h1")

	require.Equal(t, 1, inj.calls)
	assert.Equal(t, "web", inj.gotMeta.ID)
	assert.Equal(t, "my-app", inj.gotSlug)
	require.Len(t, fc.PlayCalls, 1)
	assert.Equal(t, reconciledPod, fc.PlayCalls[0].YAML)
	assert.True(t, strings.Contains(fc.PlayCalls[0].YAML, "litestream"))
}

// An injector error during boot converge aborts that spec's apply: PlayKube is
// not called for it.
func TestReconcileOneSpec_InjectorError_Aborts(t *testing.T) {
	svc, fc, st := specReconcileSvc(t)
	ctx := context.Background()
	seedBootSpec(t, st, "h1", "web", "my-app", nil)
	svc.SetSidecarInjector(&recordingInjector{err: errors.New("boom")})

	svc.ReconcileSpecsOnHost(ctx, "h1")

	assert.Empty(t, fc.PlayCalls, "PlayKube must not be called when injection fails")
}

// An injector that returns secrets creates podman secrets alongside
// template-declared secrets, and the secrets are tracked for pruning.
func TestService_Apply_InjectorSecrets_Created(t *testing.T) {
	svc, f, mem := newSvcMem(t)
	ctx := context.Background()
	inj := &recordingInjector{
		out: injectedPod,
		secrets: []extension.InjectedSecret{
			{Name: "litestream-s3-key", Key: "access-key-id", Value: "s3-secret-value"},
		},
	}
	svc.SetSidecarInjector(inj)

	require.NoError(t, svc.Apply(ctx, "h1", pgApply("demo"), ApplyOptions{Replace: true}))

	// The injector secret was created as a podman secret.
	_, secErr := f.SecretInspect(ctx, "h1", "postgres-demo-litestream-s3-key")
	require.NoError(t, secErr, "injector secret must exist on the host")

	// Template-declared secrets still work.
	_, tmplSecErr := f.SecretInspect(ctx, "h1", "postgres-demo-password")
	require.NoError(t, tmplSecErr, "template-declared secret must still exist")

	// The spec tracks injector secrets with Name, Key, and Value preserved.
	spec, err := mem.GetSpec(ctx, "h1", "postgres", "demo")
	require.NoError(t, err)
	require.Len(t, spec.InjectorSecrets, 1, "one injector secret must be tracked")
	assert.Equal(t, "litestream-s3-key", spec.InjectorSecrets[0].Name)
	assert.Equal(t, "access-key-id", spec.InjectorSecrets[0].Key)
	assert.Equal(t, "s3-secret-value", spec.InjectorSecrets[0].Value)
}

// The wrapped K8s Secret body carries the correct data key for each secret:
//   - template-declared secrets use the namespaced name as data key
//   - injector-declared secrets use the declared Key as data key
func TestService_Apply_SecretDataKeys(t *testing.T) {
	svc, f, _ := newSvcMem(t)
	ctx := context.Background()
	inj := &recordingInjector{
		out: injectedPod,
		secrets: []extension.InjectedSecret{
			{Name: "litestream-s3-key", Key: "access-key-id", Value: "s3-secret-value"},
		},
	}
	svc.SetSidecarInjector(inj)

	require.NoError(t, svc.Apply(ctx, "h1", pgApply("demo"), ApplyOptions{Replace: true}))

	// Template secret's K8s data key is the namespaced name (backward compat).
	tmplData := f.SecretData["h1"]["postgres-demo-password"]
	require.NotNil(t, tmplData, "template secret body must be recorded")

	var tmplSecret struct {
		Metadata struct {
			Name string `yaml:"name"`
		} `yaml:"metadata"`
		Data map[string]string `yaml:"data"`
	}
	require.NoError(t, yaml.Unmarshal(tmplData, &tmplSecret))
	assert.Equal(t, "postgres-demo-password", tmplSecret.Metadata.Name)
	// Must include key "postgres-demo-password" (namespaced name, not "password").
	_, hasTmplKey := tmplSecret.Data["postgres-demo-password"]
	assert.True(t, hasTmplKey, "template secret data key must be the namespaced name")

	// Injector secret's K8s data key is the declared Key ("access-key-id").
	injData := f.SecretData["h1"]["postgres-demo-litestream-s3-key"]
	require.NotNil(t, injData, "injector secret body must be recorded")

	var injSecret struct {
		Metadata struct {
			Name string `yaml:"name"`
		} `yaml:"metadata"`
		Data map[string]string `yaml:"data"`
	}
	require.NoError(t, yaml.Unmarshal(injData, &injSecret))
	assert.Equal(t, "postgres-demo-litestream-s3-key", injSecret.Metadata.Name)
	// Must include key "access-key-id" (declared Key), not the secret name.
	_, hasInjKey := injSecret.Data["access-key-id"]
	assert.True(t, hasInjKey, "injector secret data key must be the declared Key")
	_, hasWrongKey := injSecret.Data["litestream-s3-key"]
	assert.False(t, hasWrongKey, "injector secret data key must NOT be the secret name")
}

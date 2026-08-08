package instance

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/iotready/podman-api/extension"
	"github.com/iotready/podman-api/internal/config"
	"github.com/iotready/podman-api/internal/podman"
	"github.com/iotready/podman-api/internal/podman/fake"
	"github.com/iotready/podman-api/internal/render"
	"github.com/iotready/podman-api/internal/store"
)

// requiredParams turns a list of names into required ParamDefs of type string —
// the typed equivalent of the old render.Parameters{Required: ...} fixtures.
func requiredParams(names ...string) []render.ParamDef {
	out := make([]render.ParamDef, 0, len(names))
	for _, n := range names {
		out = append(out, render.ParamDef{Name: n, Type: "string", Required: true})
	}
	return out
}

// seedStore returns a Memory store pre-loaded with the given templates, ready to
// hand to svc.SetStore. The instance Service always resolves templates from its
// store, so every test seeds the catalog this way.
func seedStore(t *testing.T, tmpls ...store.Template) *store.Memory {
	t.Helper()
	mem := store.NewMemory()
	for _, tm := range tmpls {
		require.NoError(t, mem.PutTemplate(context.Background(), tm))
	}
	return mem
}

// newSvcWith builds a Service whose catalog is seeded with tmpls and returns it
// alongside the backing Memory store (for tests that also assert on specs).
func newSvcWith(t *testing.T, client podman.Client, hosts []config.Host, tmpls ...store.Template) (*Service, *store.Memory) {
	t.Helper()
	mem := seedStore(t, tmpls...)
	svc := NewService(client, hosts)
	svc.SetStore(mem)
	return svc, mem
}

// pgTemplate is the postgres-shaped fixture used across these tests. It mirrors
// the bundled templates/postgres.yaml but is inlined so the test doesn't depend
// on the embedded FS.
func pgTemplate() store.Template {
	return store.Template{
		Meta: render.Meta{
			ID:         "postgres",
			Parameters: requiredParams("slug", "image", "port", "db", "user"),
			Secrets: render.Secrets{
				PerInstance: []string{"password"},
			},
			Volumes: []render.Volume{{Name: "data", Backup: "none"}},
		},
		Body: `apiVersion: v1
kind: Pod
metadata:
  name: postgres-{{.slug}}
  labels:
    podman-api/template: postgres
    podman-api/slug: {{.slug}}
spec:
  containers:
    - name: db
      image: {{.image}}
      env:
        - name: POSTGRES_DB
          value: {{.db}}
`,
		Origin: "seed",
	}
}

// templateWithHostSecret returns a synthetic template that declares a per-host
// secret reference, used to exercise the ErrHostSecretMissing path.
func templateWithHostSecret() store.Template {
	return store.Template{
		Meta: render.Meta{
			ID:         "needs-host-secret",
			Parameters: requiredParams("slug", "image"),
			Secrets: render.Secrets{
				PerHostReferenced: []string{"shared-pull-token"},
			},
		},
		Body: `apiVersion: v1
kind: Pod
metadata:
  name: needs-host-secret-{{.slug}}
spec:
  containers:
    - name: app
      image: {{.image}}
`,
		Origin: "seed",
	}
}

func newSvc(t *testing.T) (*Service, *fake.Fake) {
	t.Helper()
	svc, f, _ := newSvcMem(t)
	return svc, f
}

// newSvcMem is newSvc but also returns the backing Memory store, already seeded
// with the pg + host-secret templates. Tests that assert on persisted specs use
// this rather than wiring a fresh (template-less) store.
func newSvcMem(t *testing.T) (*Service, *fake.Fake, *store.Memory) {
	t.Helper()
	hosts := []config.Host{{ID: "h1", Addr: "unix", Socket: "/x"}}
	f := fake.New()
	svc, mem := newSvcWith(t, f, hosts, pgTemplate(), templateWithHostSecret())
	return svc, f, mem
}

func pgApply(slug string) ApplyRequest {
	return ApplyRequest{
		Template: "postgres",
		Slug:     slug,
		Parameters: map[string]any{
			"slug": slug, "image": "docker.io/library/postgres:16",
			"port": 5432, "db": "app", "user": "app",
		},
		Secrets: map[string]string{"password": "p"},
	}
}

func TestService_Apply_Then_Get(t *testing.T) {
	svc, _ := newSvc(t)
	ctx := context.Background()

	require.NoError(t, svc.Apply(ctx, "h1", pgApply("demo"), ApplyOptions{Replace: true}))

	obs, err := svc.Get(ctx, "h1", "postgres", "demo")
	require.NoError(t, err)
	assert.Equal(t, "postgres", obs.Template)
	assert.Equal(t, "demo", obs.Slug)
	assert.Equal(t, "Running", obs.Pod.Status)
}

// #201: "slug" is a declared parameter on postgres.yaml, so a parameters.slug
// that disagrees with the request's canonical slug passes render.Validate and
// would render (and --replace) a manifest named for a *different* pod, while
// the lock, the podExists/drain gates, instanceSecretName, validateIngress and
// the persisted spec all keep using the canonical slug. applyLocked pins the
// canonical slug so no caller of any apply path can move the pod name.
//
// Half (a): the pod actually played is named for the canonical slug.
func TestService_Apply_SlugParameterPinnedToCanonicalSlug(t *testing.T) {
	svc, f, mem := newSvcMem(t)
	ctx := context.Background()

	req := pgApply("demo")
	req.Parameters["slug"] = "other" // hand-crafted disagreement

	require.NoError(t, svc.Apply(ctx, "h1", req, ApplyOptions{Replace: true}))

	require.Len(t, f.PlayCalls, 1)
	assert.Contains(t, f.PlayCalls[0].YAML, "name: postgres-demo",
		"the played manifest must name the pod for the request's slug, not the parameter's")
	assert.NotContains(t, f.PlayCalls[0].YAML, "postgres-other")

	_, err := f.PodInspect(ctx, "h1", "postgres-demo")
	require.NoError(t, err, "the pod exists under the canonical slug")
	_, err = f.PodInspect(ctx, "h1", "postgres-other")
	assert.ErrorIs(t, err, podman.ErrNotFound, "no pod was created under the parameter's slug")

	sp, err := mem.GetSpec(ctx, "h1", "postgres", "demo")
	require.NoError(t, err)
	assert.Equal(t, "demo", sp.Parameters["slug"], "the persisted spec renders the pod it names")
}

// #201 half (b): the blast radius. A pre-existing instance living at the
// parameter's slug must be left completely untouched — before the pin, the
// play would have torn its pod down with --replace and stood up a broken
// replacement, under no lock and invisible to validateIngress.
func TestService_Apply_SlugParameterLeavesOtherInstanceUntouched(t *testing.T) {
	svc, f, mem := newSvcMem(t)
	ctx := context.Background()

	// A healthy, unrelated instance at the slug the attacker-parameter names.
	victim := pgApply("other")
	victim.Parameters["image"] = "docker.io/library/postgres:15"
	require.NoError(t, svc.Apply(ctx, "h1", victim, ApplyOptions{Replace: true}))
	before, err := f.PodInspect(ctx, "h1", "postgres-other")
	require.NoError(t, err)

	req := pgApply("demo")
	req.Parameters["slug"] = "other"
	req.Parameters["image"] = "docker.io/library/postgres:17"
	require.NoError(t, svc.Apply(ctx, "h1", req, ApplyOptions{Replace: true}))

	after, err := f.PodInspect(ctx, "h1", "postgres-other")
	require.NoError(t, err, "the victim pod must still be up")
	assert.Equal(t, before.Created, after.Created, "the victim pod must not have been replaced")
	require.NotEmpty(t, after.Containers)
	assert.Equal(t, "docker.io/library/postgres:15", after.Containers[0].ImageTag,
		"the victim pod must still run its own image")

	sp, err := mem.GetSpec(ctx, "h1", "postgres", "other")
	require.NoError(t, err)
	assert.Equal(t, "docker.io/library/postgres:15", sp.Parameters["image"],
		"the victim's stored spec must be unchanged")
}

func TestService_Apply_RequiresHostSecret(t *testing.T) {
	svc, _ := newSvc(t)
	// shared-pull-token is intentionally not seeded on the fake host.
	err := svc.Apply(context.Background(), "h1", ApplyRequest{
		Template:   "needs-host-secret",
		Slug:       "x",
		Parameters: map[string]any{"slug": "x", "image": "x:1"},
	}, ApplyOptions{Replace: false})
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrHostSecretMissing)
}

func TestService_UnknownTemplate(t *testing.T) {
	svc, _ := newSvc(t)
	err := svc.Apply(context.Background(), "h1", ApplyRequest{Template: "nope", Slug: "x"}, ApplyOptions{Replace: false})
	require.ErrorIs(t, err, ErrUnknownTemplate)
}

func TestService_UnknownHost(t *testing.T) {
	svc, _ := newSvc(t)
	err := svc.Apply(context.Background(), "nope", ApplyRequest{Template: "postgres", Slug: "x"}, ApplyOptions{Replace: false})
	require.ErrorIs(t, err, ErrUnknownHost)
}

func TestService_Lifecycle(t *testing.T) {
	svc, _ := newSvc(t)
	ctx := context.Background()
	require.NoError(t, svc.Apply(ctx, "h1", pgApply("demo"), ApplyOptions{Replace: true}))

	require.NoError(t, svc.Stop(ctx, "h1", "postgres", "demo"))
	obs, _ := svc.Get(ctx, "h1", "postgres", "demo")
	assert.Equal(t, "Exited", obs.Pod.Status)

	_, startErr := svc.Start(ctx, "h1", "postgres", "demo")
	require.NoError(t, startErr)
	obs, _ = svc.Get(ctx, "h1", "postgres", "demo")
	assert.Equal(t, "Running", obs.Pod.Status)

	require.NoError(t, svc.Delete(ctx, "h1", "postgres", "demo", DeleteOptions{}))
	_, err := svc.Get(ctx, "h1", "postgres", "demo")
	require.ErrorIs(t, err, ErrInstanceNotFound)
}

func TestService_Delete_PrunesOrphanSecretWhenPodAlreadyGone(t *testing.T) {
	svc, f := newSvc(t)
	ctx := context.Background()
	require.NoError(t, svc.Apply(ctx, "h1", pgApply("orphan"), ApplyOptions{Replace: true}))

	// Applying created the per-instance secret.
	secs, err := f.SecretList(ctx, "h1")
	require.NoError(t, err)
	require.Len(t, secs, 1)

	// A prune-less delete removes the pod but leaves the secret orphaned.
	require.NoError(t, svc.Delete(ctx, "h1", "postgres", "orphan", DeleteOptions{}))
	secs, _ = f.SecretList(ctx, "h1")
	require.Len(t, secs, 1, "secret should be orphaned after a prune-less delete")

	// Deleting again WITH prune must reap the orphan and succeed even though the
	// pod is already gone — delete is an idempotent reconcile, not a 404.
	require.NoError(t, svc.Delete(ctx, "h1", "postgres", "orphan",
		DeleteOptions{PruneSecrets: true, PruneVolumes: true}))
	secs, _ = f.SecretList(ctx, "h1")
	require.Empty(t, secs, "orphaned secret should be pruned")
}

// #214: `podman kube play` resolving a `secret:` volume mount against a
// wrapped K8s Secret creates a named volume — same name as the secret —
// holding the DECODED PLAINTEXT credential on disk. Deleting the pod does not
// remove it, and (pre-fix) prune never looked for it: `prune_secrets=true`
// reported success while the plaintext key stayed on disk. The fake client
// can't reproduce podman's play-kube volume materialization, so this test
// seeds the wrapper volume by hand (AddVolume) the way the real host would
// have left it, and asserts prune reaps it under PruneSecrets alone — no
// PruneVolumes needed, since the wrapper holds credential material, not
// template data.
func TestService_Delete_PrunesSecretWrapperVolume_PerInstanceSecret(t *testing.T) {
	svc, f := newSvc(t)
	ctx := context.Background()
	require.NoError(t, svc.Apply(ctx, "h1", pgApply("wrapvol"), ApplyOptions{Replace: true}))

	wrapperName := instanceSecretName("postgres", "wrapvol", "password")
	f.AddVolume("h1", podman.Volume{Name: wrapperName})

	// PruneSecrets alone (no PruneVolumes) must remove the wrapper volume.
	require.NoError(t, svc.Delete(ctx, "h1", "postgres", "wrapvol", DeleteOptions{PruneSecrets: true}))

	_, err := f.VolumeInspect(ctx, "h1", wrapperName)
	assert.ErrorIs(t, err, podman.ErrNotFound, "the secret's wrapper volume must be pruned by PruneSecrets alone")
}

// Same as above but for a SidecarInjector-added secret (e.g. a VPN PSK),
// which is pruned from the stored spec's InjectorSecrets rather than the
// template's declared secrets — a separate loop in pruneInstanceResources.
func TestService_Delete_PrunesSecretWrapperVolume_InjectorSecret(t *testing.T) {
	svc, f, mem := newSvcMem(t)
	ctx := context.Background()
	require.NoError(t, svc.Apply(ctx, "h1", pgApply("injvol"), ApplyOptions{Replace: true}))

	sp, err := mem.GetSpec(ctx, "h1", "postgres", "injvol")
	require.NoError(t, err)
	sp.InjectorSecrets = append(sp.InjectorSecrets, store.InjectorSecret{
		Name: "vpn-psk", Key: "postgres-injvol-vpn-psk", Value: "secret-psk",
	})
	require.NoError(t, mem.PutSpec(ctx, sp))

	wrapperName := instanceSecretName("postgres", "injvol", "vpn-psk")
	f.AddVolume("h1", podman.Volume{Name: wrapperName})

	require.NoError(t, svc.Delete(ctx, "h1", "postgres", "injvol", DeleteOptions{PruneSecrets: true}))

	_, err = f.VolumeInspect(ctx, "h1", wrapperName)
	assert.ErrorIs(t, err, podman.ErrNotFound, "the injector secret's wrapper volume must be pruned by PruneSecrets alone")
}

// A declared template data volume is data, not credential material, so it
// must stay untouched by PruneSecrets and only go with PruneVolumes.
func TestService_Delete_PruneSecretsAlone_DoesNotTouchDeclaredDataVolume(t *testing.T) {
	svc, f := newSvc(t)
	ctx := context.Background()
	require.NoError(t, svc.Apply(ctx, "h1", pgApply("datavol"), ApplyOptions{Replace: true}))

	dataVol := volumeName("postgres", "datavol", "data")
	f.AddVolume("h1", podman.Volume{Name: dataVol})

	require.NoError(t, svc.Delete(ctx, "h1", "postgres", "datavol", DeleteOptions{PruneSecrets: true}))

	_, err := f.VolumeInspect(ctx, "h1", dataVol)
	assert.NoError(t, err, "a declared data volume must survive PruneSecrets without PruneVolumes")

	// Sanity: PruneVolumes does remove it.
	require.NoError(t, svc.Delete(ctx, "h1", "postgres", "datavol", DeleteOptions{PruneVolumes: true}))
	_, err = f.VolumeInspect(ctx, "h1", dataVol)
	assert.ErrorIs(t, err, podman.ErrNotFound)
}

// If the template has since been removed from the catalog, per-instance
// secrets and declared volumes can no longer be named (they're derived from
// the template), but injector secrets are recorded on the stored spec and
// must still be pruned — including their wrapper volumes. Exercised directly
// against pruneInstanceResources (rather than via Delete/svc.Delete) because
// Delete's own lookup rejects an unknown template before ever reaching prune
// — reconcile.go's migrate-reap path is the caller that hits this case for
// real, calling pruneInstanceResources without that gate.
func TestPruneInstanceResources_TemplateGone_StillPrunesInjectorSecrets(t *testing.T) {
	svc, f, mem := newSvcMem(t)
	ctx := context.Background()
	require.NoError(t, svc.Apply(ctx, "h1", pgApply("tmplgone"), ApplyOptions{Replace: true}))

	sp, err := mem.GetSpec(ctx, "h1", "postgres", "tmplgone")
	require.NoError(t, err)
	sp.InjectorSecrets = append(sp.InjectorSecrets, store.InjectorSecret{
		Name: "vpn-psk", Key: "postgres-tmplgone-vpn-psk", Value: "secret-psk",
	})
	require.NoError(t, mem.PutSpec(ctx, sp))

	injectorSecretName := instanceSecretName("postgres", "tmplgone", "vpn-psk")
	wrapperName := injectorSecretName
	f.AddVolume("h1", podman.Volume{Name: wrapperName})

	// Remove the template from the catalog, simulating an instance whose
	// template was deleted before the instance itself was cleaned up.
	require.NoError(t, mem.DeleteTemplate(ctx, "postgres"))

	svc.pruneInstanceResources(ctx, "h1", "postgres", "tmplgone", true, true, sp.InjectorSecrets)

	_, err = f.SecretInspect(ctx, "h1", injectorSecretName)
	assert.ErrorIs(t, err, podman.ErrNotFound, "injector secret must still be pruned when the template is gone")
	_, err = f.VolumeInspect(ctx, "h1", wrapperName)
	assert.ErrorIs(t, err, podman.ErrNotFound, "injector secret's wrapper volume must still be pruned when the template is gone")
}

func TestService_Delete_AbsentInstanceWithoutPruneIsNotFound(t *testing.T) {
	svc, _ := newSvc(t)
	// Nothing applied; deleting a non-existent instance without prune flags
	// still reports not-found (the idempotent-prune path must not mask this).
	err := svc.Delete(context.Background(), "h1", "postgres", "ghost", DeleteOptions{})
	require.ErrorIs(t, err, ErrInstanceNotFound)
}

func TestService_Apply_ConflictWhenExists(t *testing.T) {
	svc, _ := newSvc(t)
	ctx := context.Background()
	req := pgApply("dup")
	// First apply with replace=true succeeds.
	require.NoError(t, svc.Apply(ctx, "h1", req, ApplyOptions{Replace: true}))

	// Second apply with replace=false must return ErrInstanceExists.
	err := svc.Apply(ctx, "h1", req, ApplyOptions{Replace: false})
	require.ErrorIs(t, err, ErrInstanceExists)
}

func TestService_Upgrade_DoesNotMutateCallerMap(t *testing.T) {
	svc, _ := newSvc(t)
	ctx := context.Background()
	apply := pgApply("up")
	require.NoError(t, svc.Apply(ctx, "h1", apply, ApplyOptions{Replace: true}))

	upgradeReq := pgApply("up")
	originalImage := upgradeReq.Parameters["image"]
	require.NoError(t, svc.Upgrade(ctx, "h1", upgradeReq, "docker.io/library/postgres:17"))

	// Caller's map untouched.
	assert.Equal(t, originalImage, upgradeReq.Parameters["image"], "Upgrade must not mutate caller's Parameters map")

	// Pod actually has the new image (via observed).
	obs, err := svc.Get(ctx, "h1", "postgres", "up")
	require.NoError(t, err)
	require.Len(t, obs.Containers, 1)
	assert.Equal(t, "docker.io/library/postgres:17", obs.Containers[0].Image)
}

func TestService_Apply_PrePullsImages(t *testing.T) {
	svc, f := newSvc(t)
	require.NoError(t, svc.Apply(context.Background(), "h1", pgApply("demo"), ApplyOptions{Replace: true}))
	require.Len(t, f.PullCalls, 1, "Apply must pre-pull every container image")
	assert.Equal(t, "h1", f.PullCalls[0].Host)
	assert.Equal(t, "docker.io/library/postgres:16", f.PullCalls[0].Image)
}

func TestService_Apply_PullBoundedByApplyImagePullTimeout(t *testing.T) {
	// applyLocked must wrap the pull call in a short, fixed deadline
	// (applyImagePullTimeout) regardless of any larger -image-pull-timeout
	// style override, because Apply holds the real instance lock (and
	// hostLock for domain-carrying requests) across the pull — see the
	// applyImagePullTimeout doc comment in service.go (#238 review follow-up).
	svc, f := newSvc(t)
	before := time.Now()
	require.NoError(t, svc.Apply(context.Background(), "h1", pgApply("demo"), ApplyOptions{Replace: true}))
	after := time.Now()

	require.Len(t, f.PullCalls, 1)
	require.True(t, f.PullCalls[0].HasDeadline, "ImagePull must be called with a context carrying a deadline")

	// The deadline must be no later than applyImagePullTimeout past when the
	// call was made, with a little slack for test scheduling jitter — NOT
	// bound by some much larger override (e.g. the 2h -image-pull-timeout
	// default), which would defeat the point of this fix.
	maxDeadline := before.Add(applyImagePullTimeout + 5*time.Second)
	assert.Truef(t, f.PullCalls[0].Deadline.Before(maxDeadline) || f.PullCalls[0].Deadline.Equal(maxDeadline),
		"ImagePull deadline %v exceeds applyImagePullTimeout bound %v", f.PullCalls[0].Deadline, maxDeadline)

	minDeadline := after.Add(applyImagePullTimeout - 5*time.Second)
	assert.Truef(t, f.PullCalls[0].Deadline.After(minDeadline),
		"ImagePull deadline %v is suspiciously short relative to applyImagePullTimeout bound %v", f.PullCalls[0].Deadline, minDeadline)
}

func TestService_Apply_PullSharesOneDeadlineAcrossMultipleImages(t *testing.T) {
	// A pod with a main container plus sidecars (multiple containers AND
	// initContainers) must pull every image under a SINGLE shared
	// applyImagePullTimeout budget for the whole pre-pull phase, not a fresh
	// 10-minute deadline per image — otherwise N images would let the pull
	// phase (and the lock held across it) run for up to N*10min instead of
	// the documented 10-minute worst case (#238 review follow-up).
	multiImgTmpl := store.Template{
		Meta: render.Meta{
			ID:         "multiimg",
			Parameters: requiredParams("slug"),
		},
		Body: `apiVersion: v1
kind: Pod
metadata:
  name: multiimg-{{.slug}}
spec:
  initContainers:
    - name: init
      image: docker.io/library/busybox:init
  containers:
    - name: app
      image: docker.io/library/app:main
    - name: sidecar
      image: docker.io/library/sidecar:vpn
`,
		Origin: "seed",
	}
	hosts := []config.Host{{ID: "h1", Addr: "unix", Socket: "/x"}}
	f := fake.New()
	svc, _ := newSvcWith(t, f, hosts, multiImgTmpl)

	before := time.Now()
	require.NoError(t, svc.Apply(context.Background(), "h1", ApplyRequest{
		Template: "multiimg", Slug: "demo", Parameters: map[string]any{"slug": "demo"},
	}, ApplyOptions{Replace: true}))
	after := time.Now()

	require.Len(t, f.PullCalls, 3, "all three images (init + 2 containers) must be pulled")
	for i, call := range f.PullCalls {
		require.Truef(t, call.HasDeadline, "PullCalls[%d] (%s) must carry a deadline", i, call.Image)
	}

	// Every call's deadline must fall within the applyImagePullTimeout window
	// measured from before/after the whole Apply call, AND all deadlines must
	// be (approximately) the same instant — proving one shared context.WithTimeout
	// wraps the entire loop rather than a fresh one per iteration.
	maxDeadline := before.Add(applyImagePullTimeout + 5*time.Second)
	minDeadline := after.Add(applyImagePullTimeout - 5*time.Second)
	first := f.PullCalls[0].Deadline
	for i, call := range f.PullCalls {
		assert.Truef(t, call.Deadline.Before(maxDeadline) || call.Deadline.Equal(maxDeadline),
			"PullCalls[%d] deadline %v exceeds applyImagePullTimeout bound %v", i, call.Deadline, maxDeadline)
		assert.Truef(t, call.Deadline.After(minDeadline),
			"PullCalls[%d] deadline %v is suspiciously short relative to applyImagePullTimeout bound %v", i, call.Deadline, minDeadline)
		assert.WithinDurationf(t, first, call.Deadline, time.Millisecond,
			"PullCalls[%d] deadline %v must match PullCalls[0] deadline %v — the timeout must be shared across the whole pre-pull loop, not reset per image", i, call.Deadline, first)
	}
}

func TestService_Apply_PullFailureMapsToErrImagePull(t *testing.T) {
	svc, f := newSvc(t)
	f.PullErr = map[string]error{"": errors.New("manifest unknown")}
	err := svc.Apply(context.Background(), "h1", pgApply("demo"), ApplyOptions{Replace: true})
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrImagePull)
	assert.Contains(t, err.Error(), "manifest unknown")
	// Pull failure must abort BEFORE any secret is written so we don't leak orphans.
	_, secretErr := f.SecretInspect(context.Background(), "h1", "postgres-demo-password")
	assert.ErrorIs(t, secretErr, podman.ErrNotFound, "no per-instance secret should be created when pull fails")
}

func TestService_Apply_SkipPull(t *testing.T) {
	svc, f := newSvc(t)
	require.NoError(t, svc.Apply(context.Background(), "h1", pgApply("demo"), ApplyOptions{Replace: true, SkipPull: true}))
	assert.Empty(t, f.PullCalls, "SkipPull must suppress all ImagePull calls")
}

// podman is imported but used only to satisfy reference in tests above.
var _ = podman.ErrNotFound

func TestService_Apply_PersistsSpec(t *testing.T) {
	svc, _, mem := newSvcMem(t)
	ctx := context.Background()

	require.NoError(t, svc.Apply(ctx, "h1", pgApply("demo"), ApplyOptions{Replace: true}))

	sp, err := mem.GetSpec(ctx, "h1", "postgres", "demo")
	require.NoError(t, err)
	assert.Equal(t, "p", sp.Secrets["password"])
	assert.Equal(t, "docker.io/library/postgres:16", sp.Parameters["image"])
}

func TestService_Apply_PlayKubeFail_NoSpec(t *testing.T) {
	svc, f, mem := newSvcMem(t)
	f.PlayKubeErr = errors.New("boom")
	ctx := context.Background()

	require.Error(t, svc.Apply(ctx, "h1", pgApply("demo"), ApplyOptions{Replace: true}))

	_, err := mem.GetSpec(ctx, "h1", "postgres", "demo")
	assert.ErrorIs(t, err, store.ErrNotFound)
}

// keylessStore wraps a Memory store but reports SecretsEnabled()==false, the way
// a SQLite store opened without -spec-key-file behaves.
type keylessStore struct{ *store.Memory }

func (keylessStore) SecretsEnabled() bool { return false }

// On a key-less store, a secret-bearing Apply must be rejected with
// ErrSecretsNeedKey BEFORE any host mutation — no pod played, no secret created —
// so the host is never left with an orphaned pod/secrets the missing spec can't
// account for. (#61)
func TestService_Apply_SecretBearing_KeylessStore_NoMutation(t *testing.T) {
	hosts := []config.Host{{ID: "h1", Addr: "unix", Socket: "/x"}}
	f := fake.New()
	mem := seedStore(t, pgTemplate())
	svc := NewService(f, hosts)
	svc.SetStore(keylessStore{mem})
	ctx := context.Background()

	err := svc.Apply(ctx, "h1", pgApply("demo"), ApplyOptions{Replace: true})
	require.ErrorIs(t, err, store.ErrSecretsNeedKey)

	// No host mutation happened: no pod played, no secret created.
	assert.Empty(t, f.PlayCalls, "PlayKube must not be called")
	secs, lerr := f.SecretList(ctx, "h1")
	require.NoError(t, lerr)
	assert.Empty(t, secs, "no secrets must be created")
	// And of course no spec row.
	_, gerr := mem.GetSpec(ctx, "h1", "postgres", "demo")
	assert.ErrorIs(t, gerr, store.ErrNotFound)
}

func TestService_Apply_StorePutError_Fatal(t *testing.T) {
	svc, _, mem := newSvcMem(t)
	mem.PutErr = errors.New("db down")

	err := svc.Apply(context.Background(), "h1", pgApply("demo"), ApplyOptions{Replace: true})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "persist spec")
}

func TestService_Delete_RemovesSpec(t *testing.T) {
	svc, _, mem := newSvcMem(t)
	ctx := context.Background()
	require.NoError(t, svc.Apply(ctx, "h1", pgApply("demo"), ApplyOptions{Replace: true}))

	require.NoError(t, svc.Delete(ctx, "h1", "postgres", "demo", DeleteOptions{}))

	_, err := mem.GetSpec(ctx, "h1", "postgres", "demo")
	assert.ErrorIs(t, err, store.ErrNotFound)
}

func TestService_Delete_StoreDeleteError_Fatal(t *testing.T) {
	svc, _, mem := newSvcMem(t)
	ctx := context.Background()
	require.NoError(t, svc.Apply(ctx, "h1", pgApply("demo"), ApplyOptions{Replace: true}))

	mem.DeleteErr = errors.New("db down")
	err := svc.Delete(ctx, "h1", "postgres", "demo", DeleteOptions{})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "delete spec")
}

func TestService_UpgradeImage_ReusesStoredSecrets(t *testing.T) {
	svc, _, mem := newSvcMem(t)
	ctx := context.Background()

	// Initial deploy persists params + the per-instance secret.
	require.NoError(t, svc.Apply(ctx, "h1", pgApply("demo"), ApplyOptions{Replace: true}))

	// Image-only upgrade: the operator supplies just the new image.
	require.NoError(t, svc.UpgradeImage(ctx, "h1", "postgres", "demo", "docker.io/library/postgres:17"))

	// Stored spec now carries the new image; the secret is reused unchanged.
	sp, err := mem.GetSpec(ctx, "h1", "postgres", "demo")
	require.NoError(t, err)
	assert.Equal(t, "docker.io/library/postgres:17", sp.Parameters["image"])
	assert.Equal(t, "p", sp.Secrets["password"], "secret should be reused, not wiped")
	// Non-image params are preserved too.
	assert.Equal(t, "app", sp.Parameters["db"])
}

func TestService_UpgradeImage_MissingSpecIsNotFound(t *testing.T) {
	svc, _ := newSvc(t)
	err := svc.UpgradeImage(context.Background(), "h1", "postgres", "ghost", "x:1")
	assert.ErrorIs(t, err, ErrInstanceNotFound)
}

func TestService_UpgradeImage_EmptyImageRejected(t *testing.T) {
	svc, _ := newSvc(t)
	err := svc.UpgradeImage(context.Background(), "h1", "postgres", "demo", "")
	require.Error(t, err)
}

// twoSecretTemplate declares two per-instance secrets so a rotation of one can
// prove the other (a field left blank) is preserved. render.Validate treats the
// PerInstance list as the complete allow-list AND requires every declared name,
// so an undeclared secret can't be seeded — both must be declared and supplied.
func twoSecretTemplate() store.Template {
	return store.Template{
		Meta: render.Meta{
			ID:         "twosec",
			Parameters: requiredParams("slug", "image"),
			Secrets:    render.Secrets{PerInstance: []string{"password", "token"}},
		},
		Body: `apiVersion: v1
kind: Pod
metadata:
  name: twosec-{{.slug}}
spec:
  containers:
    - name: app
      image: {{.image}}
`,
		Origin: "seed",
	}
}

func TestRotateInstanceSecrets_OverlaysAndReapplies(t *testing.T) {
	hosts := []config.Host{{ID: "h1", Addr: "unix", Socket: "/x"}}
	svc, mem := newSvcWith(t, fake.New(), hosts, twoSecretTemplate())
	ctx := context.Background()
	require.NoError(t, svc.Apply(ctx, "h1", ApplyRequest{
		Template:   "twosec",
		Slug:       "demo",
		Parameters: map[string]any{"slug": "demo", "image": "img:1"},
		Secrets:    map[string]string{"password": "p", "token": "keep"},
	}, ApplyOptions{Replace: true}))

	// Rotate only "password"; "token" is left out (the write-only "blank keeps
	// existing value" path).
	require.NoError(t, svc.RotateInstanceSecrets(ctx, "h1", "twosec", "demo",
		map[string]string{"password": "rotated"}))

	got, err := mem.GetSpec(ctx, "h1", "twosec", "demo")
	require.NoError(t, err)
	assert.Equal(t, "rotated", got.Secrets["password"]) // overlaid
	assert.Equal(t, "keep", got.Secrets["token"])       // absent name preserved
	assert.Equal(t, "img:1", got.Parameters["image"])   // params preserved
}

// TestUpgradeImage_AllowsMissingRequiredSecret guards the Upgrade/Rotate
// consistency decision: like rotation, an image-only upgrade must not be blocked
// because the stored spec lacks a per-instance secret the template now requires
// (a secret added after the instance was deployed).
func TestUpgradeImage_AllowsMissingRequiredSecret(t *testing.T) {
	hosts := []config.Host{{ID: "h1", Addr: "unix", Socket: "/x"}}
	svc, mem := newSvcWith(t, fake.New(), hosts, twoSecretTemplate())
	ctx := context.Background()
	require.NoError(t, svc.Apply(ctx, "h1", ApplyRequest{
		Template:   "twosec",
		Slug:       "demo",
		Parameters: map[string]any{"slug": "demo", "image": "img:1"},
		Secrets:    map[string]string{"password": "p", "token": "t"},
	}, ApplyOptions{Replace: true}))
	// Simulate a template that gained "token" after deploy: drop it from the spec.
	sp, err := mem.GetSpec(ctx, "h1", "twosec", "demo")
	require.NoError(t, err)
	delete(sp.Secrets, "token")
	require.NoError(t, mem.PutSpec(ctx, sp))

	require.NoError(t, svc.UpgradeImage(ctx, "h1", "twosec", "demo", "img:2"))

	got, err := mem.GetSpec(ctx, "h1", "twosec", "demo")
	require.NoError(t, err)
	assert.Equal(t, "img:2", got.Parameters["image"])
}

// gatedClient wraps a podman.Client and, once armed, blocks every PlayKube until
// the test closes release — signalling each entry on reached (buffered so a second
// concurrent entry never blocks). It lets the test interleave two rotations of one
// instance deterministically.
type gatedClient struct {
	podman.Client
	armed   atomic.Bool
	reached chan struct{}
	release chan struct{}
}

func (g *gatedClient) PlayKube(ctx context.Context, host, yaml string, replace bool, networks ...string) error {
	if g.armed.Load() {
		g.reached <- struct{}{}
		<-g.release
	}
	return g.Client.PlayKube(ctx, host, yaml, replace, networks...)
}

// TestRotateInstanceSecrets_ConcurrentRotationsDoNotLoseUpdates proves load+apply
// is atomic under one lock: two concurrent rotations of *different* secrets on the
// same instance must both survive. On the pre-fix code (GetSpec outside Apply's
// lock) the second rotation reads the spec before the first commits and re-applies
// a stale value, dropping one update.
func TestRotateInstanceSecrets_ConcurrentRotationsDoNotLoseUpdates(t *testing.T) {
	hosts := []config.Host{{ID: "h1", Addr: "unix", Socket: "/x"}}
	g := &gatedClient{Client: fake.New(), reached: make(chan struct{}, 2), release: make(chan struct{})}
	svc, mem := newSvcWith(t, g, hosts, twoSecretTemplate())
	ctx := context.Background()

	// Deploy with the gate disarmed.
	require.NoError(t, svc.Apply(ctx, "h1", ApplyRequest{
		Template:   "twosec",
		Slug:       "demo",
		Parameters: map[string]any{"slug": "demo", "image": "img:1"},
		Secrets:    map[string]string{"password": "p", "token": "t"},
	}, ApplyOptions{Replace: true}))

	g.armed.Store(true)

	errA := make(chan error, 1)
	errB := make(chan error, 1)
	// A rotates password, parks in PlayKube holding the instance lock.
	go func() {
		errA <- svc.RotateInstanceSecrets(ctx, "h1", "twosec", "demo", map[string]string{"password": "A"})
	}()
	<-g.reached
	// B rotates token. In BOTH worlds B blocks on Apply's instance lock (held by
	// A, parked in PlayKube) and never reaches its own gate — the difference is
	// only WHERE B's GetSpec sits relative to that lock:
	//   pre-fix:  B reads the spec OUTSIDE the lock first (the stale pre-commit
	//             read is what bakes in the lost update), then blocks on the lock;
	//   post-fix: B blocks on the lock BEFORE any read, so it can't read stale.
	go func() {
		errB <- svc.RotateInstanceSecrets(ctx, "h1", "twosec", "demo", map[string]string{"token": "B"})
	}()
	// Give B time to reach that blocking point before releasing A. A fixed wait
	// (not a gate signal) is correct here precisely because B cannot reach the
	// PlayKube gate while A holds the lock — so there is no signal to wait on;
	// longer only slows the test, shorter risks nothing.
	time.Sleep(300 * time.Millisecond)
	close(g.release)
	require.NoError(t, <-errA)
	require.NoError(t, <-errB)

	got, err := mem.GetSpec(ctx, "h1", "twosec", "demo")
	require.NoError(t, err)
	assert.Equal(t, "A", got.Secrets["password"], "password rotation lost (RMW race)")
	assert.Equal(t, "B", got.Secrets["token"], "token rotation lost (RMW race)")
}

func TestRotateInstanceSecrets_EmptyIsRejected(t *testing.T) {
	svc, _, _ := newSvcMem(t)
	ctx := context.Background()
	require.NoError(t, svc.Apply(ctx, "h1", pgApply("demo"), ApplyOptions{Replace: true}))
	require.Error(t, svc.RotateInstanceSecrets(ctx, "h1", "postgres", "demo", map[string]string{}))
}

func TestRotateInstanceSecrets_NoSpecIsNotFound(t *testing.T) {
	svc, _, _ := newSvcMem(t)
	err := svc.RotateInstanceSecrets(context.Background(), "h1", "postgres", "ghost",
		map[string]string{"password": "x"})
	require.ErrorIs(t, err, ErrInstanceNotFound)
}

func TestRotateInstanceSecrets_CorruptSpecPropagates(t *testing.T) {
	svc, _, mem := newSvcMem(t)
	svc.SetStore(&getSpecErrStore{Memory: mem, err: store.ErrSpecCorrupt})
	err := svc.RotateInstanceSecrets(context.Background(), "h1", "postgres", "demo",
		map[string]string{"password": "x"})
	require.ErrorIs(t, err, store.ErrSpecCorrupt)
}

func TestInstanceSecretState_ReportsPresenceNotValues(t *testing.T) {
	svc, _, _ := newSvcMem(t)
	ctx := context.Background()
	require.NoError(t, svc.Apply(ctx, "h1", pgApply("demo"), ApplyOptions{Replace: true}))
	set, err := svc.InstanceSecretState(ctx, "h1", "postgres", "demo")
	require.NoError(t, err)
	assert.True(t, set["password"]) // stored
	assert.False(t, set["token"])   // never set → absent → false
}

func TestInstanceSecretState_NoSpecIsNotFound(t *testing.T) {
	svc, _, _ := newSvcMem(t)
	_, err := svc.InstanceSecretState(context.Background(), "h1", "postgres", "ghost")
	require.ErrorIs(t, err, ErrInstanceNotFound)
}

func TestApplyAndObserve_ReadyOnSuccess(t *testing.T) {
	defer setVerifyKnobs(50*time.Millisecond, 5*time.Millisecond)()
	f := fake.New()
	f.PlayKubeContainerHealth = "healthy"
	hosts := []config.Host{{ID: "h1", Addr: "unix", Socket: "/x"}}
	svc, _ := newSvcWith(t, f, hosts, webTemplate())
	obs, err := svc.ApplyAndObserve(context.Background(), "h1", ApplyRequest{
		Template:   "web",
		Slug:       "s1",
		Parameters: map[string]any{"slug": "s1", "image": "nginx"},
	}, ApplyOptions{})
	require.NoError(t, err)
	assert.True(t, obs.Ready)
	assert.Empty(t, obs.Warnings)
}

func TestApplyAndObserve_WarningOnTimeout(t *testing.T) {
	defer setVerifyKnobs(50*time.Millisecond, 5*time.Millisecond)()
	f := fake.New()
	f.PlayKubeContainerHealth = "starting"
	hosts := []config.Host{{ID: "h1", Addr: "unix", Socket: "/x"}}
	svc, _ := newSvcWith(t, f, hosts, webTemplate())
	obs, err := svc.ApplyAndObserve(context.Background(), "h1", ApplyRequest{
		Template:   "web",
		Slug:       "s1",
		Parameters: map[string]any{"slug": "s1", "image": "nginx"},
	}, ApplyOptions{})
	require.NoError(t, err)
	assert.False(t, obs.Ready)
	require.Len(t, obs.Warnings, 1)
	// A container inside its start period has not failed, so the warning says
	// so rather than reporting a timeout (#196).
	assert.Contains(t, obs.Warnings[0], "still inside its healthcheck start period")
	assert.NotContains(t, obs.Warnings[0], "readiness timeout")
}

// An unhealthy container genuinely failed its check, so it must still be
// reported as a readiness timeout and not excused as "initialising" (#196).
func TestApplyAndObserve_WarningOnUnhealthy(t *testing.T) {
	defer setVerifyKnobs(50*time.Millisecond, 5*time.Millisecond)()
	f := fake.New()
	f.PlayKubeContainerHealth = "unhealthy"
	hosts := []config.Host{{ID: "h1", Addr: "unix", Socket: "/x"}}
	svc, _ := newSvcWith(t, f, hosts, webTemplate())
	obs, err := svc.ApplyAndObserve(context.Background(), "h1", ApplyRequest{
		Template:   "web",
		Slug:       "s1",
		Parameters: map[string]any{"slug": "s1", "image": "nginx"},
	}, ApplyOptions{})
	require.NoError(t, err)
	assert.False(t, obs.Ready)
	require.Len(t, obs.Warnings, 1)
	assert.Contains(t, obs.Warnings[0], "readiness timeout")
}

func TestStart_ReadyOnSuccess(t *testing.T) {
	defer setVerifyKnobs(50*time.Millisecond, 5*time.Millisecond)()
	f := fake.New()
	f.AddPod("h1", podman.Pod{Name: "web-s1", Status: "Running",
		Containers: []podman.Container{{Status: "Running", Health: "healthy"}}})
	hosts := []config.Host{{ID: "h1", Addr: "unix", Socket: "/x"}}
	svc, _ := newSvcWith(t, f, hosts, webTemplate())
	obs, err := svc.Start(context.Background(), "h1", "web", "s1")
	require.NoError(t, err)
	assert.True(t, obs.Ready)
	assert.Empty(t, obs.Warnings)
}

func TestStart_WarningOnTimeout(t *testing.T) {
	defer setVerifyKnobs(50*time.Millisecond, 5*time.Millisecond)()
	f := fake.New()
	f.AddPod("h1", podman.Pod{Name: "web-s1", Status: "Running",
		Containers: []podman.Container{{Status: "Running", Health: "starting"}}})
	hosts := []config.Host{{ID: "h1", Addr: "unix", Socket: "/x"}}
	svc, _ := newSvcWith(t, f, hosts, webTemplate())
	obs, err := svc.Start(context.Background(), "h1", "web", "s1")
	require.NoError(t, err)
	assert.False(t, obs.Ready)
	require.Len(t, obs.Warnings, 1)
	assert.Contains(t, obs.Warnings[0], "still inside its healthcheck start period")
	assert.NotContains(t, obs.Warnings[0], "readiness timeout")
}

func TestStart_WarningOnUnhealthy(t *testing.T) {
	defer setVerifyKnobs(50*time.Millisecond, 5*time.Millisecond)()
	f := fake.New()
	f.AddPod("h1", podman.Pod{Name: "web-s1", Status: "Running",
		Containers: []podman.Container{{Status: "Running", Health: "unhealthy"}}})
	hosts := []config.Host{{ID: "h1", Addr: "unix", Socket: "/x"}}
	svc, _ := newSvcWith(t, f, hosts, webTemplate())
	obs, err := svc.Start(context.Background(), "h1", "web", "s1")
	require.NoError(t, err)
	assert.False(t, obs.Ready)
	require.Len(t, obs.Warnings, 1)
	assert.Contains(t, obs.Warnings[0], "readiness timeout")
}

// #198 regression: a SidecarInjector adds a container whose env comes from a
// secretKeyRef the stored template body never declared. The name pass cannot
// see it; the value pass must.
func TestService_Get_RedactsInjectorAddedSecretEnv(t *testing.T) {
	svc, f, _ := newSvcMem(t)
	ctx := context.Background()

	const psk = "s3cr3t-psk-value"
	svc.SetSidecarInjector(&recordingInjector{
		out: `apiVersion: v1
kind: Pod
metadata:
  name: postgres-demo
spec:
  containers:
    - name: db
      image: docker.io/library/postgres:16
    - name: vpn
      image: docker.io/strongswan/strongswan:6.0.5
      env:
        - name: VPN_PSK
          valueFrom:
            secretKeyRef:
              name: postgres-demo-vpn-psk
              key: postgres-demo-vpn-psk
`,
		secrets: []extension.InjectedSecret{
			{Name: "vpn-psk", Key: "postgres-demo-vpn-psk", Value: psk},
		},
	})

	require.NoError(t, svc.Apply(ctx, "h1", pgApply("demo"), ApplyOptions{Replace: true}))

	// The fake podman client does not resolve secretKeyRef into container env,
	// so stand in for what a real host would report back.
	f.AddPod("h1", podman.Pod{
		Name:   "postgres-demo",
		Status: "Running",
		Labels: map[string]string{"podman-api/template": "postgres", "podman-api/slug": "demo"},
		Containers: []podman.Container{
			{Name: "db", Status: "Running", Env: map[string]string{"POSTGRES_DB": "app"}},
			{Name: "vpn", Status: "Running", Env: map[string]string{"VPN_PSK": psk, "VPN_ENDPOINT": "vpn.example.com"}},
		},
	})

	obs, err := svc.Get(ctx, "h1", "postgres", "demo")
	require.NoError(t, err)

	for k, v := range obs.EnvSummary {
		assert.NotEqual(t, psk, v, "env_summary[%s] leaked the injected secret", k)
	}
	assert.Equal(t, "app", obs.EnvSummary["POSTGRES_DB"], "non-secret env still reported")
	assert.Equal(t, "vpn.example.com", obs.EnvSummary["VPN_ENDPOINT"], "non-secret sidecar env still reported")
	assert.Empty(t, obs.Warnings, "a readable spec produces no warning")
}

// An unreadable spec means the secret values are unknowable, so env_summary is
// withheld entirely and the instance carries a warning — it must not leak, and
// it must not fail the request.
func TestService_Get_UnreadableSpec_WithholdsEnvSummary(t *testing.T) {
	svc, f, mem := newSvcMem(t)
	ctx := context.Background()

	require.NoError(t, svc.Apply(ctx, "h1", pgApply("demo"), ApplyOptions{Replace: true}))
	f.AddPod("h1", podman.Pod{
		Name:   "postgres-demo",
		Status: "Running",
		Labels: map[string]string{"podman-api/template": "postgres", "podman-api/slug": "demo"},
		Containers: []podman.Container{
			{Name: "db", Status: "Running", Env: map[string]string{"POSTGRES_DB": "app"}},
		},
	})
	mem.FailGetSpec(store.ErrSecretsUndecryptable)

	obs, err := svc.Get(ctx, "h1", "postgres", "demo")
	require.NoError(t, err, "an unreadable spec must not fail the request")
	assert.Nil(t, obs.EnvSummary, "env_summary is withheld when secrets are unreadable")
	require.Len(t, obs.Warnings, 1)
	assert.Contains(t, obs.Warnings[0], "env_summary withheld")
	assert.Equal(t, "Running", obs.Pod.Status, "the rest of the observation survives")
	require.Len(t, obs.Containers, 1)
}

// Get reports the parameters the instance was last applied with, so a client
// can read a shared value back before rewriting it (#200).
func TestService_Get_ReportsStoredParameters(t *testing.T) {
	svc, _, _ := newSvcMem(t)
	ctx := context.Background()

	require.NoError(t, svc.Apply(ctx, "h1", pgApply("demo"), ApplyOptions{Replace: true}))

	obs, err := svc.Get(ctx, "h1", "postgres", "demo")
	require.NoError(t, err)
	require.NotNil(t, obs.Parameters)
	assert.Equal(t, "docker.io/library/postgres:16", obs.Parameters["image"])
	assert.Equal(t, "app", obs.Parameters["db"])
	for k, v := range obs.Parameters {
		assert.NotEqual(t, "p", v, "parameters[%s] leaked the instance secret", k)
	}
}

// No stored spec means nothing to report: the field is absent, not null.
func TestService_Get_NoStoredSpec_OmitsParameters(t *testing.T) {
	svc, f, _ := newSvcMem(t)
	ctx := context.Background()

	f.AddPod("h1", podman.Pod{
		Name:   "postgres-orphan",
		Status: "Running",
		Labels: map[string]string{"podman-api/template": "postgres", "podman-api/slug": "orphan"},
		Containers: []podman.Container{
			{Name: "db", Status: "Running"},
		},
	})

	obs, err := svc.Get(ctx, "h1", "postgres", "orphan")
	require.NoError(t, err)
	assert.Nil(t, obs.Parameters)
}

// An unreadable spec withholds the parameters along with env_summary.
func TestService_Get_UnreadableSpec_OmitsParameters(t *testing.T) {
	svc, f, mem := newSvcMem(t)
	ctx := context.Background()

	require.NoError(t, svc.Apply(ctx, "h1", pgApply("demo"), ApplyOptions{Replace: true}))
	f.AddPod("h1", podman.Pod{
		Name:   "postgres-demo",
		Status: "Running",
		Labels: map[string]string{"podman-api/template": "postgres", "podman-api/slug": "demo"},
		Containers: []podman.Container{
			{Name: "db", Status: "Running"},
		},
	})
	mem.FailGetSpec(store.ErrSecretsUndecryptable)

	obs, err := svc.Get(ctx, "h1", "postgres", "demo")
	require.NoError(t, err)
	assert.Nil(t, obs.Parameters, "parameters are withheld when the spec is unreadable")
}

// The list path does not carry parameters — a host sweep stays lean, and #200
// only asks for the single-instance read.
func TestService_List_OmitsParameters(t *testing.T) {
	svc, _, _ := newSvcMem(t)
	ctx := context.Background()

	require.NoError(t, svc.Apply(ctx, "h1", pgApply("demo"), ApplyOptions{Replace: true}))

	list, err := svc.List(ctx, "h1", "postgres")
	require.NoError(t, err)
	require.Len(t, list, 1)
	assert.Nil(t, list[0].Parameters)
}

// A pod with no stored spec has no RECORDED secrets, not unreadable ones. It
// keeps its env_summary (name-pass redaction only) rather than being withheld.
func TestService_Get_NoStoredSpec_KeepsEnvSummary(t *testing.T) {
	svc, f, _ := newSvcMem(t)
	ctx := context.Background()

	f.AddPod("h1", podman.Pod{
		Name:   "postgres-orphan",
		Status: "Running",
		Labels: map[string]string{"podman-api/template": "postgres", "podman-api/slug": "orphan"},
		Containers: []podman.Container{
			{Name: "db", Status: "Running", Env: map[string]string{"POSTGRES_DB": "app"}},
		},
	})

	obs, err := svc.Get(ctx, "h1", "postgres", "orphan")
	require.NoError(t, err)
	assert.Equal(t, "app", obs.EnvSummary["POSTGRES_DB"])
	assert.Empty(t, obs.Warnings)
}

// The list path must redact by value as well — it is the endpoint the UI and
// the inventory poller read most often.
func TestService_List_RedactsInjectorAddedSecretEnv(t *testing.T) {
	svc, f, mem := newSvcMem(t)
	ctx := context.Background()

	const psk = "list-path-psk"
	require.NoError(t, svc.Apply(ctx, "h1", pgApply("demo"), ApplyOptions{Replace: true}))

	sp, err := mem.GetSpec(ctx, "h1", "postgres", "demo")
	require.NoError(t, err)
	sp.InjectorSecrets = append(sp.InjectorSecrets, store.InjectorSecret{
		Name: "vpn-psk", Key: "postgres-demo-vpn-psk", Value: psk,
	})
	require.NoError(t, mem.PutSpec(ctx, sp))

	f.AddPod("h1", podman.Pod{
		Name:   "postgres-demo",
		Status: "Running",
		Labels: map[string]string{"podman-api/template": "postgres", "podman-api/slug": "demo"},
		Containers: []podman.Container{
			{Name: "vpn", Status: "Running", Env: map[string]string{"VPN_PSK": psk, "VPN_ENDPOINT": "vpn.example.com"}},
		},
	})

	list, err := svc.List(ctx, "h1", "postgres")
	require.NoError(t, err)
	require.Len(t, list, 1)
	for k, v := range list[0].EnvSummary {
		assert.NotEqual(t, psk, v, "env_summary[%s] leaked the injected secret on the list path", k)
	}
	assert.Equal(t, "vpn.example.com", list[0].EnvSummary["VPN_ENDPOINT"])
}

// listAllInstancesLive (via ListAllInstances) is a structurally different call
// site from List: its vals lookup happens inside a per-template goroutine and
// is keyed on tmplID rather than the request's template. It is also the path
// that fills the warm inventory cache read by the UI list, the inventory
// poller, and ListAllInstancesWithMeta — the highest-traffic consumer of
// env_summary. A refactor that dropped the value set there would pass the
// rest of the suite while re-opening the leak, so it needs its own coverage.
func TestService_ListAllInstances_RedactsInjectorAddedSecretEnv(t *testing.T) {
	svc, f, mem := newSvcMem(t)
	ctx := context.Background()

	const psk = "list-all-path-psk"
	require.NoError(t, svc.Apply(ctx, "h1", pgApply("demo"), ApplyOptions{Replace: true}))

	sp, err := mem.GetSpec(ctx, "h1", "postgres", "demo")
	require.NoError(t, err)
	sp.InjectorSecrets = append(sp.InjectorSecrets, store.InjectorSecret{
		Name: "vpn-psk", Key: "postgres-demo-vpn-psk", Value: psk,
	})
	require.NoError(t, mem.PutSpec(ctx, sp))

	f.AddPod("h1", podman.Pod{
		Name:   "postgres-demo",
		Status: "Running",
		Labels: map[string]string{"podman-api/template": "postgres", "podman-api/slug": "demo"},
		Containers: []podman.Container{
			{Name: "vpn", Status: "Running", Env: map[string]string{"VPN_PSK": psk, "VPN_ENDPOINT": "vpn.example.com"}},
		},
	})

	list, err := svc.ListAllInstances(ctx, "h1")
	require.NoError(t, err)
	require.NotEmpty(t, list)
	found := false
	for _, obs := range list {
		for k, v := range obs.EnvSummary {
			assert.NotEqual(t, psk, v, "env_summary[%s] leaked the injected secret on the ListAllInstances path", k)
		}
		if obs.Template == "postgres" && obs.Slug == "demo" {
			found = true
			assert.Equal(t, "vpn.example.com", obs.EnvSummary["VPN_ENDPOINT"], "non-secret env still reported")
		}
	}
	assert.True(t, found, "expected postgres/demo in ListAllInstances output")
}

// The inventory sweep must attribute volume-usage metrics to an instance
// without paying for a podman VolumeInspect call per volume, so it derives
// volume names from template meta (the same construction Service.Get uses)
// and leaves SizeBytes at 0 — sizes are joined in later from the usage cache.
func TestListAllInstancesPopulatesVolumeNames(t *testing.T) {
	svc, f, _ := newSvcMem(t)
	ctx := context.Background()

	require.NoError(t, svc.Apply(ctx, "h1", pgApply("demo"), ApplyOptions{Replace: true}))
	f.AddPod("h1", podman.Pod{
		Name:   "postgres-demo",
		Status: "Running",
		Labels: map[string]string{"podman-api/template": "postgres", "podman-api/slug": "demo"},
	})

	got, err := svc.ListAllInstances(ctx, "h1")
	if err != nil {
		t.Fatalf("ListAllInstances: %v", err)
	}
	var found *Observed
	for i := range got {
		if got[i].Template == "postgres" && got[i].Slug == "demo" {
			found = &got[i]
		}
	}
	if found == nil {
		t.Fatalf("expected postgres/demo in ListAllInstances output, got %+v", got)
	}
	if len(found.Volumes) != 1 {
		t.Fatalf("volumes = %+v, want 1", found.Volumes)
	}
	if want := "postgres-demo-data"; found.Volumes[0].Name != want {
		t.Fatalf("volume name = %q, want %q", found.Volumes[0].Name, want)
	}
	if found.Volumes[0].SizeBytes != 0 {
		t.Fatalf("size = %d, want 0 (sweep does not price volumes)", found.Volumes[0].SizeBytes)
	}
}

// A ListSpecKeys failure fails the whole-host sweep, which must withhold
// env_summary for every instance on the host (fail closed) without failing
// the List call itself — a store read failure must never fail an API request.
func TestService_List_SweepFailure_WithholdsEnvSummaryForEveryInstance(t *testing.T) {
	svc, f, mem := newSvcMem(t)
	ctx := context.Background()

	require.NoError(t, svc.Apply(ctx, "h1", pgApply("demo1"), ApplyOptions{Replace: true}))
	require.NoError(t, svc.Apply(ctx, "h1", pgApply("demo2"), ApplyOptions{Replace: true}))
	f.AddPod("h1", podman.Pod{
		Name:   "postgres-demo1",
		Status: "Running",
		Labels: map[string]string{"podman-api/template": "postgres", "podman-api/slug": "demo1"},
		Containers: []podman.Container{
			{Name: "db", Status: "Running", Env: map[string]string{"POSTGRES_DB": "app"}},
		},
	})
	f.AddPod("h1", podman.Pod{
		Name:   "postgres-demo2",
		Status: "Running",
		Labels: map[string]string{"podman-api/template": "postgres", "podman-api/slug": "demo2"},
		Containers: []podman.Container{
			{Name: "db", Status: "Running", Env: map[string]string{"POSTGRES_DB": "app"}},
		},
	})

	mem.ListSpecKeysErr = errors.New("enumeration failed")

	list, err := svc.List(ctx, "h1", "postgres")
	require.NoError(t, err, "a store read failure must not fail the request")
	require.Len(t, list, 2)
	for _, obs := range list {
		assert.Nil(t, obs.EnvSummary, "env_summary must be withheld for %s/%s when the sweep fails", obs.Template, obs.Slug)
		require.Len(t, obs.Warnings, 1)
		assert.Contains(t, obs.Warnings[0], "env_summary withheld")
	}
}

// The core of pro#74: a parameter change must not require the caller to supply
// the instance's sealed secrets, which are write-only and unrecoverable.
func TestService_UpdateInstanceParameters_OverlaysAndKeepsSecrets(t *testing.T) {
	svc, f, mem := newSvcMem(t)
	ctx := context.Background()
	require.NoError(t, svc.Apply(ctx, "h1", pgApply("demo"), ApplyOptions{Replace: true}))

	// No secrets supplied — only the one parameter being changed.
	require.NoError(t, svc.UpdateInstanceParameters(ctx, "h1", "postgres", "demo",
		map[string]any{"db": "newdb"}))

	sp, err := mem.GetSpec(ctx, "h1", "postgres", "demo")
	require.NoError(t, err)
	assert.Equal(t, "newdb", sp.Parameters["db"], "the changed parameter is persisted")
	assert.Equal(t, "app", sp.Parameters["user"], "untouched parameters keep their stored value")
	assert.Equal(t, "docker.io/library/postgres:16", sp.Parameters["image"])
	assert.Equal(t, "p", sp.Secrets["password"], "sealed secrets survive untouched")

	require.NotEmpty(t, f.PlayCalls, "the instance is re-applied")
	assert.Contains(t, f.PlayCalls[len(f.PlayCalls)-1].YAML, "newdb",
		"the changed parameter reaches the played manifest")
}

// Domains are re-applied verbatim from the stored spec. This is what makes the
// missing host lock safe, so it is asserted rather than assumed.
func TestService_UpdateInstanceParameters_PreservesDomains(t *testing.T) {
	svc, _, mem := newSvcMem(t)
	ctx := context.Background()
	req := pgApply("demo")
	require.NoError(t, svc.Apply(ctx, "h1", req, ApplyOptions{Replace: true}))

	before, err := mem.GetSpec(ctx, "h1", "postgres", "demo")
	require.NoError(t, err)

	require.NoError(t, svc.UpdateInstanceParameters(ctx, "h1", "postgres", "demo",
		map[string]any{"db": "newdb"}))

	after, err := mem.GetSpec(ctx, "h1", "postgres", "demo")
	require.NoError(t, err)
	assert.Equal(t, before.Domains, after.Domains, "domains must pass through unchanged")
}

// An unknown parameter is rejected by template validation before any host
// mutation, so nothing is played and the stored spec is untouched.
func TestService_UpdateInstanceParameters_UnknownParameterRejected(t *testing.T) {
	svc, f, mem := newSvcMem(t)
	ctx := context.Background()
	require.NoError(t, svc.Apply(ctx, "h1", pgApply("demo"), ApplyOptions{Replace: true}))
	playsBefore := len(f.PlayCalls)

	err := svc.UpdateInstanceParameters(ctx, "h1", "postgres", "demo",
		map[string]any{"not_a_real_parameter": "x"})
	require.Error(t, err)

	assert.Len(t, f.PlayCalls, playsBefore, "a rejected update must not replay the pod")
	sp, gerr := mem.GetSpec(ctx, "h1", "postgres", "demo")
	require.NoError(t, gerr)
	assert.NotContains(t, sp.Parameters, "not_a_real_parameter",
		"a rejected update must not persist the bad parameter")
	assert.Equal(t, "app", sp.Parameters["db"], "the original value survives a rejected update")
}

// An empty map is rejected: a blank submit must not restart a pod for nothing.
func TestService_UpdateInstanceParameters_EmptyRejected(t *testing.T) {
	svc, f, _ := newSvcMem(t)
	ctx := context.Background()
	require.NoError(t, svc.Apply(ctx, "h1", pgApply("demo"), ApplyOptions{Replace: true}))
	playsBefore := len(f.PlayCalls)

	require.Error(t, svc.UpdateInstanceParameters(ctx, "h1", "postgres", "demo", map[string]any{}))
	require.Error(t, svc.UpdateInstanceParameters(ctx, "h1", "postgres", "demo", nil))
	assert.Len(t, f.PlayCalls, playsBefore, "an empty update must not touch the host")
}

// pro#74 fix 1 (service layer): "slug" is a declared parameter on postgres.yaml
// (and in this test template), so it passes render.Validate — the overlay's
// canonical-slug pin is the only thing stopping it from renaming the pod out
// from under the lock this method holds. A caller other than the HTTP handler
// (which rejects "slug" earlier) must still be safe.
func TestService_UpdateInstanceParameters_SlugParameterIgnored(t *testing.T) {
	svc, f, mem := newSvcMem(t)
	ctx := context.Background()
	require.NoError(t, svc.Apply(ctx, "h1", pgApply("demo"), ApplyOptions{Replace: true}))
	playsBefore := len(f.PlayCalls)

	require.NoError(t, svc.UpdateInstanceParameters(ctx, "h1", "postgres", "demo",
		map[string]any{"slug": "other", "db": "newdb"}))

	require.Len(t, f.PlayCalls, playsBefore+1)
	last := f.PlayCalls[len(f.PlayCalls)-1]
	assert.Contains(t, last.YAML, "name: postgres-demo",
		"the played manifest must still name the original pod, not \"other\"")
	assert.NotContains(t, last.YAML, "postgres-other")

	sp, err := mem.GetSpec(ctx, "h1", "postgres", "demo")
	require.NoError(t, err)
	assert.Equal(t, "demo", sp.Parameters["slug"], "the stored spec's slug must be unchanged")
	assert.Equal(t, "newdb", sp.Parameters["db"], "the legitimate parameter change still applies")

	// And no spec was ever created under the injected slug.
	_, err = mem.GetSpec(ctx, "h1", "postgres", "other")
	assert.ErrorIs(t, err, store.ErrNotFound)
}

// singletonTemplate is a fixture that does NOT declare "slug" as a parameter —
// legal per ParseMeta/validateMeta, which never require it — and hardcodes
// metadata.name rather than templating it from {{.slug}}. Regression fixture
// for pro#74 fix 2: the canonical-slug pin in UpdateInstanceParameters must not
// unconditionally inject "slug" into merged, or render.Validate rejects it as
// an unknown parameter on a template shaped like this one.
func singletonTemplate() store.Template {
	return store.Template{
		Meta: render.Meta{
			ID:         "singleton",
			Parameters: requiredParams("image", "greeting"),
		},
		Body: `apiVersion: v1
kind: Pod
metadata:
  name: singleton-app
spec:
  containers:
    - name: app
      image: {{.image}}
      env:
        - name: GREETING
          value: {{.greeting}}
`,
		Origin: "seed",
	}
}

// pro#74 fix 2 (regression): a template that never declares "slug" as a
// parameter is legal (ParseMeta/validateMeta never require it), and a
// singleton template with a hardcoded metadata.name is a realistic shape for
// that. The overlay's canonical-slug pin must not unconditionally inject
// "slug" into the merged parameters on such a template, or every PATCH fails
// with "unknown parameter \"slug\"" via render.Validate — a regression versus
// UpgradeImage, which injects no such key.
func TestService_UpdateInstanceParameters_SlugNotDeclared(t *testing.T) {
	f := fake.New()
	svc, mem := newSvcWith(t, f, []config.Host{{ID: "h1", Addr: "unix", Socket: "/x"}}, singletonTemplate())
	ctx := context.Background()

	require.NoError(t, svc.Apply(ctx, "h1", ApplyRequest{
		Template: "singleton",
		Slug:     "only",
		Parameters: map[string]any{
			"image": "docker.io/library/app:1", "greeting": "hi",
		},
	}, ApplyOptions{Replace: true}))
	playsBefore := len(f.PlayCalls)

	require.NoError(t, svc.UpdateInstanceParameters(ctx, "h1", "singleton", "only",
		map[string]any{"greeting": "bonjour"}))

	assert.Len(t, f.PlayCalls, playsBefore+1)
	last := f.PlayCalls[len(f.PlayCalls)-1]
	assert.Contains(t, last.YAML, "bonjour")

	sp, err := mem.GetSpec(ctx, "h1", "singleton", "only")
	require.NoError(t, err)
	assert.Equal(t, "bonjour", sp.Parameters["greeting"], "the changed parameter is persisted")
	assert.NotContains(t, sp.Parameters, "slug", "slug must not be injected into a template that never declared it")
}

func TestService_UpdateInstanceParameters_UnknownInstance(t *testing.T) {
	svc, _, _ := newSvcMem(t)
	err := svc.UpdateInstanceParameters(context.Background(), "h1", "postgres", "nope",
		map[string]any{"db": "x"})
	assert.ErrorIs(t, err, ErrInstanceNotFound)
}

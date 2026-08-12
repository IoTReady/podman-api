package instance

import (
	"bytes"
	"context"
	"log"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/iotready/podman-api/internal/config"
	"github.com/iotready/podman-api/internal/podman/fake"
	"github.com/iotready/podman-api/internal/render"
	"github.com/iotready/podman-api/internal/store"
)

// captureLog redirects the standard logger for the duration of fn.
func captureLog(t *testing.T, fn func()) string {
	t.Helper()
	var buf bytes.Buffer
	out, flags := log.Writer(), log.Flags()
	log.SetOutput(&buf)
	log.SetFlags(0)
	t.Cleanup(func() { log.SetOutput(out); log.SetFlags(flags) })
	fn()
	return buf.String()
}

func TestBackupMarkerNoneWriteWarning(t *testing.T) {
	tmpl := func(vols ...render.Volume) store.Template {
		return store.Template{Meta: render.Meta{ID: "t", Volumes: vols}}
	}

	assert.Empty(t, backupMarkerNoneWriteWarning(tmpl()),
		"a template with no volumes must stay silent")
	assert.Empty(t, backupMarkerNoneWriteWarning(tmpl(render.Volume{Name: "data", Backup: "s3; interval=24h"})),
		"a template with no vetoed volume must stay silent")

	got := backupMarkerNoneWriteWarning(tmpl(
		render.Volume{Name: "data", Backup: "s3; interval=24h"},
		render.Volume{Name: "logs", Backup: "none"},
	))
	assert.Contains(t, got, "HARD VETO")
	assert.Contains(t, got, "logs")
	assert.NotContains(t, got, "REJECTED",
		"one vetoed volume among several is not the all-vetoed case")

	// Every declared volume vetoed: categorically worse, since CheckBackupable
	// then rejects every backup of every instance of the template.
	all := backupMarkerNoneWriteWarning(tmpl(render.Volume{Name: "data", Backup: "none"}))
	assert.Contains(t, all, "REJECTED")

	// A near-miss spelling vetoes now (the comparison folds case), so it must be
	// reported here too — that is exactly the reinterpretation to announce.
	assert.Contains(t, backupMarkerNoneWriteWarning(tmpl(render.Volume{Name: "sites", Backup: " None "})), "sites")
}

// TestTemplateWrite_WarnsAboutNoneMarkers (round-3 finding 7): the startup
// audit only ever sees the catalog as it stands at boot, so a template
// registered or edited against a running daemon is never audited — months, on a
// long-lived control plane. Warn where the operator is actually standing.
func TestTemplateWrite_WarnsAboutNoneMarkers(t *testing.T) {
	ctx := context.Background()
	svc, _ := tmplSvc(t)

	tpl := webTemplate()
	tpl.Meta.Volumes = []render.Volume{
		{Name: "data", Backup: "s3; interval=24h"},
		{Name: "logs", Backup: "none"},
	}

	out := captureLog(t, func() { require.NoError(t, svc.CreateTemplate(ctx, tpl)) })
	assert.Contains(t, out, "HARD VETO", "registering a `none`-marked template must warn")
	assert.Contains(t, out, "logs")

	// The same edit made against a template already registered must warn too —
	// that is the case the boot audit is furthest from catching.
	tpl2 := webTemplate()
	tpl2.Meta.Volumes = []render.Volume{{Name: "data", Backup: "none"}}
	out = captureLog(t, func() { require.NoError(t, svc.UpdateTemplate(ctx, tpl2)) })
	assert.Contains(t, out, "HARD VETO", "editing a template to add a `none` marker must warn")
	assert.Contains(t, out, "REJECTED")

	// And a template with no veto stays silent: this warning must not become
	// noise on every ordinary template write.
	tpl3 := webTemplate()
	tpl3.Meta.Volumes = []render.Volume{{Name: "data", Backup: "s3; interval=24h"}}
	out = captureLog(t, func() { require.NoError(t, svc.UpdateTemplate(ctx, tpl3)) })
	assert.NotContains(t, out, "HARD VETO")
}

// A template row stored with a near-miss `None` marker — registered before the
// registration validator existed, which is the whole population that validator
// was written about — must stay EDITABLE. ValidateVolumes runs on
// UpdateTemplate too, so rejecting the near-miss there failed every PUT,
// including one changing an unrelated field, and there is no in-product path to
// the row to fix the marker by hand. Consumption fails closed (the marker still
// vetoes), so the strict rule is belt-and-braces and can yield here.
// (review-4 finding 4)
func TestUpdateTemplate_StoredNearMissMarkerStaysEditable(t *testing.T) {
	ctx := context.Background()
	f := fake.New()
	stored := pgTemplate()
	for i := range stored.Meta.Volumes {
		if stored.Meta.Volumes[i].Name == "logs" {
			stored.Meta.Volumes[i].Backup = "None"
		}
	}
	svc, mem := newSvcWith(t, f, []config.Host{{ID: "h1", Addr: "unix", Socket: "/x"}})
	// Seeded directly, bypassing CreateTemplate: this is a row that predates the
	// validator, which is the only way such a row exists.
	require.NoError(t, mem.PutTemplate(ctx, stored))

	edit := stored
	edit.Meta.Display.Description = "our postgres"
	logs := captureLog(t, func() {
		require.NoError(t, svc.UpdateTemplate(ctx, edit),
			"an unrelated edit must not be held hostage by a marker the row already carried")
	})
	// The write-path warning still names the volume, so the edit is informed.
	assert.Contains(t, logs, "logs")

	got, err := svc.GetTemplate(ctx, edit.Meta.ID)
	require.NoError(t, err)
	assert.Equal(t, "our postgres", got.Meta.Display.Description)

	// A near-miss the update NEWLY introduces is still rejected.
	bad := stored
	bad.Meta.Volumes = append([]render.Volume(nil), stored.Meta.Volumes...)
	for i := range bad.Meta.Volumes {
		if bad.Meta.Volumes[i].Name == "data" {
			bad.Meta.Volumes[i].Backup = "NONE"
		}
	}
	err = svc.UpdateTemplate(ctx, bad)
	require.ErrorIs(t, err, ErrInvalidTemplate)
	assert.Contains(t, err.Error(), `must be exactly "none"`)

	// And create is unchanged: registering the same shape fresh is refused.
	fresh := stored
	fresh.Meta.ID = "pg-copy"
	assert.ErrorIs(t, svc.CreateTemplate(ctx, fresh), ErrInvalidTemplate)
}

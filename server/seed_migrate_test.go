package server

import (
	"context"
	"testing"

	"github.com/iotready/podman-api/internal/render"
	"github.com/iotready/podman-api/internal/store"
	"github.com/iotready/podman-api/templates"
)

// storedLegacySeed returns the `postgres` seed row exactly as an install that
// predates #249 holds it: the currently shipped body and meta, with the `data`
// volume's marker back at the old `none` the seed used to ship.
func storedLegacySeed(t *testing.T) store.Template {
	t.Helper()
	seeds, err := store.ParseSeeds(templates.Files)
	if err != nil {
		t.Fatalf("parse seeds: %v", err)
	}
	for _, s := range seeds {
		if s.Meta.ID != "postgres" {
			continue
		}
		legacy, err := legacySeedMeta(s.Meta, legacySeedVolumeMarkers["postgres"])
		if err != nil {
			t.Fatalf("legacy meta: %v", err)
		}
		s.Meta = legacy
		s.Origin = "seed"
		return s
	}
	t.Fatal("no postgres seed in the bundled catalog")
	return store.Template{}
}

func postgresDataMarker(t *testing.T, tmpl store.Template) string {
	t.Helper()
	for _, v := range tmpl.Meta.Volumes {
		if v.Name == "data" {
			return v.Backup
		}
	}
	t.Fatalf("template %q declares no `data` volume", tmpl.Meta.ID)
	return ""
}

// seedTemplates returns early once the catalog is non-empty, so a fix to a
// bundled template reaches fresh installs and nothing else. An already-deployed
// install kept a `postgres` row declaring `data: backup: none`, which is now a
// HARD VETO — so every volume of that template is vetoed and CheckBackupable
// rejects every backup of every postgres instance with a permanent 400
// `invalid_backup_scope`, including the scheduler's. (review-4 finding 1)
func TestMigrateSeededTemplates_RewritesUntouchedOldSeed(t *testing.T) {
	ctx := context.Background()
	db := store.NewMemory()
	old := storedLegacySeed(t)
	if got := postgresDataMarker(t, old); !render.IsBackupMarkerNone(got) {
		t.Fatalf("fixture is not the old seed: data marker %q", got)
	}
	if err := db.PutTemplate(ctx, old); err != nil {
		t.Fatalf("put: %v", err)
	}

	rw, err := migrateSeededTemplates(ctx, db, templates.Files)
	if err != nil {
		t.Fatalf("migrate: %v", err)
	}
	if len(rw) != 1 || rw[0].Template != "postgres" || rw[0].Volume != "data" || rw[0].From != "none" {
		t.Fatalf("want one postgres.data rewrite from `none`, got %+v", rw)
	}
	got, err := db.GetTemplate(ctx, "postgres")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if m := postgresDataMarker(t, got); render.IsBackupMarkerNone(m) {
		t.Fatalf("stored row still vetoes its only volume: marker %q", m)
	}
	// The rewritten row is stamped as migrated — that stamp IS the applied-once
	// marker (review-5 finding 2), so it must be recorded, not left at "seed".
	if got.Origin != seedMigratedOrigin {
		t.Errorf("origin = %q; want %q, the applied-once marker", got.Origin, seedMigratedOrigin)
	}

	// Idempotent: a second boot has nothing left to rewrite.
	rw2, err := migrateSeededTemplates(ctx, db, templates.Files)
	if err != nil {
		t.Fatalf("migrate again: %v", err)
	}
	if len(rw2) != 0 {
		t.Errorf("second run rewrote %+v; want none", rw2)
	}
}

// TestMigrateSeededTemplates_NeverRevisitsAMigratedRow is the point of the
// applied-once marker (review-5 finding 2). An operator who decides they
// genuinely do not want postgres `data` backed up sets `backup: none` AFTER the
// migration has run — and that row is byte-identical to the untouched legacy
// seed, so the three-part "is this the old seed" guard cannot tell the two
// apart. Without the marker, every subsequent boot silently reverts their veto,
// in the fail-open direction, forever.
func TestMigrateSeededTemplates_NeverRevisitsAMigratedRow(t *testing.T) {
	ctx := context.Background()
	db := store.NewMemory()
	if err := db.PutTemplate(ctx, storedLegacySeed(t)); err != nil {
		t.Fatalf("put: %v", err)
	}
	if rw, err := migrateSeededTemplates(ctx, db, templates.Files); err != nil || len(rw) != 1 {
		t.Fatalf("first migrate: rewrites=%+v err=%v", rw, err)
	}

	// The operator now deliberately vetoes `data`, through the ordinary update
	// path — which preserves the stored Origin. The resulting row is byte-for-
	// byte the legacy seed again, apart from that Origin.
	row, err := db.GetTemplate(ctx, "postgres")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	legacy, err := legacySeedMeta(row.Meta, legacySeedVolumeMarkers["postgres"])
	if err != nil {
		t.Fatalf("legacy meta: %v", err)
	}
	row.Meta = legacy
	if err := db.PutTemplate(ctx, row); err != nil {
		t.Fatalf("put vetoed row: %v", err)
	}

	rw, err := migrateSeededTemplates(ctx, db, templates.Files)
	if err != nil {
		t.Fatalf("second migrate: %v", err)
	}
	if len(rw) != 0 {
		t.Fatalf("rewrote %+v; a deliberate veto set after the migration ran must never be revisited", rw)
	}
	got, err := db.GetTemplate(ctx, "postgres")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if m := postgresDataMarker(t, got); !render.IsBackupMarkerNone(m) {
		t.Fatalf("the operator's `none` was reverted to %q on the next boot", m)
	}
}

// TestMigrateSeededTemplates_CorrectsEditedRowsToo is the review-6 finding 1
// case, and it inverts what an earlier version of this migration did. Requiring
// the whole row to match the old seed byte-for-byte meant an operator who had
// ever touched it — a bumped default image, a tweaked description, an added
// parameter — kept `data: backup: none`. That marker is now a HARD VETO of the
// template's ONLY volume, so CheckBackupable refused every backup of every
// postgres instance on that fleet, permanently. The population most engaged
// with the product got the outage.
//
// The evidence is now the marker itself, not the whole row: this volume still
// carries exactly what our old seed shipped. Everything else the operator wrote
// must survive untouched.
func TestMigrateSeededTemplates_CorrectsEditedRowsToo(t *testing.T) {
	ctx := context.Background()

	cases := map[string]func(*store.Template){
		"edited body": func(tp *store.Template) { tp.Body += "\n# operator note\n" },
		"edited meta": func(tp *store.Template) { tp.Meta.Display.Description = "our postgres" },
		"added a parameter": func(tp *store.Template) {
			tp.Meta.Parameters = append(tp.Meta.Parameters, render.ParamDef{Name: "extra", Type: "string"})
		},
	}

	for name, edit := range cases {
		t.Run(name, func(t *testing.T) {
			db := store.NewMemory()
			row := storedLegacySeed(t)
			edit(&row)
			if err := db.PutTemplate(ctx, row); err != nil {
				t.Fatalf("put: %v", err)
			}
			rw, err := migrateSeededTemplates(ctx, db, templates.Files)
			if err != nil {
				t.Fatalf("migrate: %v", err)
			}
			if len(rw) != 1 {
				t.Fatalf("rewrote %+v; an edited row still carrying our stale default must be corrected", rw)
			}
			got, err := db.GetTemplate(ctx, "postgres")
			if err != nil {
				t.Fatalf("get: %v", err)
			}
			if m := postgresDataMarker(t, got); render.IsBackupMarkerNone(m) {
				t.Fatalf("marker still vetoes the only volume: %q", m)
			}
			// Everything the operator wrote survives: only the one marker moved.
			if got.Body != row.Body {
				t.Errorf("body was rewritten; the migration must touch the marker and nothing else")
			}
			if got.Meta.Display.Description != row.Meta.Display.Description {
				t.Errorf("description = %q; want the operator's %q", got.Meta.Display.Description, row.Meta.Display.Description)
			}
			if len(got.Meta.Parameters) != len(row.Meta.Parameters) {
				t.Errorf("params = %d; want the operator's %d", len(got.Meta.Parameters), len(row.Meta.Parameters))
			}
		})
	}
}

// What is still never touched: a marker the operator chose (it is not the value
// our seed shipped, so it is not ours to correct) and a row that is not ours at
// all. These are the cases where overwriting really would be worse than the bug.
func TestMigrateSeededTemplates_LeavesForeignMarkersAlone(t *testing.T) {
	ctx := context.Background()

	cases := map[string]func(*store.Template){
		"operator chose the marker themselves": func(tp *store.Template) {
			for i := range tp.Meta.Volumes {
				if tp.Meta.Volumes[i].Name == "data" {
					tp.Meta.Volumes[i].Backup = "None"
				}
			}
		},
		"origin is user": func(tp *store.Template) { tp.Origin = "user" },
	}

	for name, edit := range cases {
		t.Run(name, func(t *testing.T) {
			db := store.NewMemory()
			row := storedLegacySeed(t)
			edit(&row)
			if err := db.PutTemplate(ctx, row); err != nil {
				t.Fatalf("put: %v", err)
			}
			rw, err := migrateSeededTemplates(ctx, db, templates.Files)
			if err != nil {
				t.Fatalf("migrate: %v", err)
			}
			if len(rw) != 0 {
				t.Fatalf("rewrote %+v; a marker the operator chose is not ours to correct", rw)
			}
			got, err := db.GetTemplate(ctx, "postgres")
			if err != nil {
				t.Fatalf("get: %v", err)
			}
			if postgresDataMarker(t, got) != postgresDataMarker(t, row) {
				t.Fatalf("marker changed from what the operator stored")
			}
		})
	}
}

// TestMigrateSeededTemplates_StampsEvenWhenNothingChanged: the applied-once
// marker has to land on rows the migration examined but did not need to change,
// not only on rows it rewrote. Those are precisely the rows an operator might
// later set `backup: none` on — and if the stamp were skipped, the next boot
// would see our old default's spelling and revert their deliberate veto.
func TestMigrateSeededTemplates_StampsEvenWhenNothingChanged(t *testing.T) {
	ctx := context.Background()
	db := store.NewMemory()
	row := storedLegacySeed(t)
	for i := range row.Meta.Volumes {
		if row.Meta.Volumes[i].Name == "data" {
			row.Meta.Volumes[i].Backup = "s3; interval=12h" // already fixed by hand
		}
	}
	if err := db.PutTemplate(ctx, row); err != nil {
		t.Fatalf("put: %v", err)
	}
	rw, err := migrateSeededTemplates(ctx, db, templates.Files)
	if err != nil {
		t.Fatalf("migrate: %v", err)
	}
	if len(rw) != 0 {
		t.Fatalf("rewrote %+v; nothing needed correcting", rw)
	}
	got, err := db.GetTemplate(ctx, "postgres")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if got.Origin != seedMigratedOrigin {
		t.Fatalf("origin = %q; want %q so a later deliberate `none` is never revisited", got.Origin, seedMigratedOrigin)
	}
	if m := postgresDataMarker(t, got); m != "s3; interval=12h" {
		t.Fatalf("marker = %q; the operator's own value must survive", m)
	}
}

// A catalog that never held the affected template (or holds nothing at all) is
// a no-op, not an error.
func TestMigrateSeededTemplates_AbsentRowIsNoOp(t *testing.T) {
	ctx := context.Background()
	db := store.NewMemory()
	rw, err := migrateSeededTemplates(ctx, db, templates.Files)
	if err != nil {
		t.Fatalf("migrate: %v", err)
	}
	if len(rw) != 0 {
		t.Fatalf("rewrote %+v against an empty catalog", rw)
	}
}

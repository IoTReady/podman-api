package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"path/filepath"
	"testing"

	"github.com/iotready/podman-api/internal/render"
	"github.com/stretchr/testify/require"
	_ "modernc.org/sqlite"
)

func TestSQLite_TemplateCRUD(t *testing.T) {
	ctx := context.Background()
	db, err := OpenSQLite(filepath.Join(t.TempDir(), "s.db"), NewKeyStore(testKey(0x11)))
	require.NoError(t, err)
	defer db.Close()

	tpl := Template{
		Meta: render.Meta{ID: "web", Display: render.Display{Name: "Web"},
			Parameters: []render.ParamDef{{Name: "image", Type: "string", Required: true}}},
		Body: "kind: Pod", Origin: "seed",
	}
	require.NoError(t, db.PutTemplate(ctx, tpl))

	got, err := db.GetTemplate(ctx, "web")
	require.NoError(t, err)
	require.Equal(t, "Web", got.Meta.Display.Name)
	require.Len(t, got.Meta.Parameters, 1)
	require.Equal(t, "seed", got.Origin)
	require.False(t, got.Created.IsZero())

	n, err := db.CountTemplates(ctx)
	require.NoError(t, err)
	require.Equal(t, 1, n)

	require.NoError(t, db.DeleteTemplate(ctx, "web"))
	_, err = db.GetTemplate(ctx, "web")
	require.ErrorIs(t, err, ErrNotFound)
}

func TestSQLite_TemplateList(t *testing.T) {
	ctx := context.Background()
	db, err := OpenSQLite(filepath.Join(t.TempDir(), "s.db"), NewKeyStore(testKey(0x11)))
	require.NoError(t, err)
	defer db.Close()

	require.NoError(t, db.PutTemplate(ctx, Template{
		Meta: render.Meta{ID: "beta"}, Body: "b", Origin: "seed",
	}))
	require.NoError(t, db.PutTemplate(ctx, Template{
		Meta: render.Meta{ID: "alpha"}, Body: "a", Origin: "seed",
	}))

	list, err := db.ListTemplates(ctx)
	require.NoError(t, err)
	require.Len(t, list, 2)
	// results should be ordered by id
	require.Equal(t, "alpha", list[0].Meta.ID)
	require.Equal(t, "beta", list[1].Meta.ID)
}

func TestSQLite_TemplateUpsert_PreservesCreated(t *testing.T) {
	ctx := context.Background()
	db, err := OpenSQLite(filepath.Join(t.TempDir(), "s.db"), NewKeyStore(testKey(0x11)))
	require.NoError(t, err)
	defer db.Close()

	require.NoError(t, db.PutTemplate(ctx, Template{
		Meta: render.Meta{ID: "pg"}, Body: "v1", Origin: "seed",
	}))
	first, err := db.GetTemplate(ctx, "pg")
	require.NoError(t, err)

	require.NoError(t, db.PutTemplate(ctx, Template{
		Meta: render.Meta{ID: "pg"}, Body: "v2", Origin: "user",
	}))
	second, err := db.GetTemplate(ctx, "pg")
	require.NoError(t, err)

	require.Equal(t, first.Created.Unix(), second.Created.Unix(), "Created must be preserved on upsert")
	require.Equal(t, "v2", second.Body)
	require.Equal(t, "user", second.Origin)
}

func TestSQLite_TemplateGetMissing(t *testing.T) {
	ctx := context.Background()
	db, err := OpenSQLite(filepath.Join(t.TempDir(), "s.db"), NewKeyStore(testKey(0x11)))
	require.NoError(t, err)
	defer db.Close()

	_, err = db.GetTemplate(ctx, "nope")
	require.ErrorIs(t, err, ErrNotFound)
}

// TestMigrateAddsTemplatesTable verifies that a pre-v6 DB (user_version=4,
// no templates table) is upgraded cleanly to v6 by OpenSQLite.
func TestMigrateAddsTemplatesTable(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "old.db")

	// Simulate a v4 DB: full v4 schema but no templates table.
	raw, err := sql.Open("sqlite", "file:"+path)
	require.NoError(t, err)
	_, err = raw.Exec(`CREATE TABLE specs (
  host TEXT NOT NULL, template TEXT NOT NULL, slug TEXT NOT NULL,
  parameters TEXT NOT NULL, secrets BLOB NOT NULL,
  domains TEXT NOT NULL DEFAULT '[]',
  created INTEGER NOT NULL, updated INTEGER NOT NULL,
  PRIMARY KEY (host, template, slug));`)
	require.NoError(t, err)
	_, err = raw.Exec(`PRAGMA user_version = 4`)
	require.NoError(t, err)
	require.NoError(t, raw.Close())

	// OpenSQLite must upgrade the DB and create the templates table.
	s, err := OpenSQLite(path, NewKeyStore(testKey(0x11)))
	require.NoError(t, err)
	t.Cleanup(func() { _ = s.Close() })

	ctx := context.Background()
	// Templates operations must work on the upgraded DB.
	require.NoError(t, s.PutTemplate(ctx, Template{
		Meta: render.Meta{ID: "web"}, Body: "kind: Pod", Origin: "seed",
	}))
	got, err := s.GetTemplate(ctx, "web")
	require.NoError(t, err)
	require.Equal(t, "web", got.Meta.ID)

	// user_version must be 8 (v8 added the injector_secrets column).
	var v int
	require.NoError(t, s.db.QueryRow(`PRAGMA user_version`).Scan(&v))
	require.Equal(t, 8, v)
}

// TestFreshDB_UserVersion8 asserts that a brand-new DB opened via OpenSQLite
// has user_version == 8.
func TestFreshDB_UserVersion8(t *testing.T) {
	s, err := OpenSQLite(filepath.Join(t.TempDir(), "s.db"), NewKeyStore(testKey(0x11)))
	require.NoError(t, err)
	defer s.Close()

	var v int
	require.NoError(t, s.db.QueryRow(`PRAGMA user_version`).Scan(&v))
	require.Equal(t, 8, v)
}

func TestSQLite_KeylessRejectsSecretsButAllowsTemplates(t *testing.T) {
	ctx := context.Background()
	db, err := OpenSQLite(filepath.Join(t.TempDir(), "s.db"), nil) // no keys
	require.NoError(t, err)
	defer db.Close()

	// templates work key-less
	require.NoError(t, db.PutTemplate(ctx, Template{Meta: render.Meta{ID: "x"}, Body: "k", Origin: "user"}))
	// secret op is rejected
	err = db.PutHostSecret(ctx, "h1", "DB_PASS", []byte("p"))
	require.ErrorIs(t, err, ErrSecretsNeedKey)
}

func TestSQLite_KeylessSpecWithoutSecrets(t *testing.T) {
	ctx := context.Background()
	db, err := OpenSQLite(filepath.Join(t.TempDir(), "s.db"), nil)
	require.NoError(t, err)
	defer db.Close()
	require.NoError(t, db.PutSpec(ctx, Spec{Host: "h1", Template: "web", Slug: "demo",
		Parameters: map[string]any{"slug": "demo"}}))
	got, err := db.GetSpec(ctx, "h1", "web", "demo")
	require.NoError(t, err)
	require.Equal(t, "demo", got.Slug)
}

// TestMigrateV6_PreservesExistingSecretsBlob verifies that a spec stored in a
// v5 DB with a real sealed secrets blob is correctly preserved after the v6
// migration (secrets column made nullable).
func TestMigrateV6_PreservesExistingSecretsBlob(t *testing.T) {
	path := filepath.Join(t.TempDir(), "v5.db")
	ks := NewKeyStore(testKey(0x11))

	// Build a v5 DB by hand: specs table with secrets BLOB NOT NULL, one row
	// whose secrets column holds a genuinely sealed blob.
	raw, err := sql.Open("sqlite", "file:"+path)
	require.NoError(t, err)
	_, err = raw.Exec(`CREATE TABLE specs (
  host TEXT NOT NULL, template TEXT NOT NULL, slug TEXT NOT NULL,
  parameters TEXT NOT NULL, secrets BLOB NOT NULL,
  domains TEXT NOT NULL DEFAULT '[]',
  created INTEGER NOT NULL, updated INTEGER NOT NULL,
  PRIMARY KEY (host, template, slug))`)
	require.NoError(t, err)

	secJSON, err := json.Marshal(map[string]string{"password": "s3cr3t"})
	require.NoError(t, err)
	blob, err := seal(ks.Load(), secJSON)
	require.NoError(t, err)

	_, err = raw.Exec(`INSERT INTO specs VALUES (?,?,?,?,?,?,?,?)`,
		"h1", "pg", "demo", `{"user":"app"}`, blob, `[]`, 1000, 1000)
	require.NoError(t, err)
	_, err = raw.Exec(`PRAGMA user_version = 5`)
	require.NoError(t, err)
	require.NoError(t, raw.Close())

	// Reopen via OpenSQLite — this should run the v6 and v7 migrations.
	s, err := OpenSQLite(path, ks)
	require.NoError(t, err)
	t.Cleanup(func() { _ = s.Close() })

	// Confirm all migrations ran.
	var v int
	require.NoError(t, s.db.QueryRow(`PRAGMA user_version`).Scan(&v))
	require.Equal(t, 8, v)

	// The sealed blob must survive the recreate: GetSpec must decrypt it correctly.
	got, err := s.GetSpec(context.Background(), "h1", "pg", "demo")
	require.NoError(t, err)
	require.Equal(t, "s3cr3t", got.Secrets["password"])
}

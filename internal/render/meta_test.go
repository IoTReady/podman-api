package render

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseMeta_Minimal(t *testing.T) {
	src := `# template-meta:
#   id: example
#   parameters:
#     - name: slug
#       type: string
#       required: true
#     - name: image
#       type: string
#       required: true
#   secrets:
#     per_instance: [password]
#     per_host_referenced: [registry-pull-token]
#   volumes:
#     - name: data
#       backup: none
---
apiVersion: v1
kind: Pod
`
	meta, body, err := ParseMeta(src)
	require.NoError(t, err)

	assert.Equal(t, "example", meta.ID)
	require.Len(t, meta.Parameters, 2)
	assert.Equal(t, "slug", meta.Parameters[0].Name)
	assert.True(t, meta.Parameters[0].Required)
	assert.Equal(t, "image", meta.Parameters[1].Name)
	assert.True(t, meta.Parameters[1].Required)
	assert.Equal(t, []string{"password"}, meta.Secrets.PerInstance)
	assert.Equal(t, []string{"registry-pull-token"}, meta.Secrets.PerHostReferenced)
	require.Len(t, meta.Volumes, 1)
	assert.Equal(t, "data", meta.Volumes[0].Name)
	assert.Equal(t, "none", meta.Volumes[0].Backup)

	assert.Contains(t, body, "apiVersion: v1")
	assert.NotContains(t, body, "template-meta")
	assert.True(t, strings.HasPrefix(strings.TrimLeft(body, " \t"), "---"),
		"body must start with --- separator, got: %q", body[:min(40, len(body))])
}

func TestParseMeta_MissingMeta(t *testing.T) {
	src := `apiVersion: v1
kind: Pod
`
	_, _, err := ParseMeta(src)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "template-meta")
}

func TestParseMeta_EmptyBody(t *testing.T) {
	src := `# template-meta:
#   id: x
`
	_, body, err := ParseMeta(src)
	require.NoError(t, err)
	assert.Equal(t, "", body, "body should be empty when meta block runs to EOF")
}

func TestParseMeta_MissingID(t *testing.T) {
	src := `# template-meta:
#   parameters:
#     - name: slug
#       type: string
---
apiVersion: v1
`
	_, _, err := ParseMeta(src)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "id")
}

func TestParseMetaIngress(t *testing.T) {
	src := `# template-meta:
#   id: web
#   ingress:
#     container: web
#     port: 8080
---
apiVersion: v1
kind: Pod
`
	meta, _, err := ParseMeta(src)
	require.NoError(t, err)
	require.NotNil(t, meta.Ingress)
	require.Equal(t, "web", meta.Ingress.Container)
	require.Equal(t, 8080, meta.Ingress.Port)
}

func TestParseMetaNoIngress(t *testing.T) {
	src := `# template-meta:
#   id: postgres
---
apiVersion: v1
kind: Pod
`
	meta, _, err := ParseMeta(src)
	require.NoError(t, err)
	require.Nil(t, meta.Ingress)
}

func TestParseMetaIngressInvalid(t *testing.T) {
	src := `# template-meta:
#   id: web
#   ingress:
#     container: web
#     port: 0
---
apiVersion: v1
kind: Pod
`
	_, _, err := ParseMeta(src)
	require.Error(t, err)
}

func TestParseMeta_TypedParameters(t *testing.T) {
	src := `# template-meta:
#   id: web
#   display:
#     name: Web
#     category: Apps
#   parameters:
#     - name: image
#       type: string
#       required: true
#       default: "nginx:1"
#     - name: port
#       type: int
#       label: HTTP port
#       default: 8080
---
apiVersion: v1
kind: Pod
`
	m, body, err := ParseMeta(src)
	require.NoError(t, err)
	require.Equal(t, "web", m.ID)
	require.Equal(t, "Web", m.Display.Name)
	require.Equal(t, "Apps", m.Display.Category)
	require.Len(t, m.Parameters, 2)
	require.Equal(t, "image", m.Parameters[0].Name)
	require.True(t, m.Parameters[0].Required)
	require.Equal(t, "string", m.Parameters[0].Type)
	require.Equal(t, "port", m.Parameters[1].Name)
	require.False(t, m.Parameters[1].Required)
	require.Contains(t, body, "kind: Pod")
	require.Equal(t, "nginx:1", m.Parameters[0].Default)
	require.EqualValues(t, 8080, m.Parameters[1].Default)
}

func TestParseMeta_RejectsUnknownType(t *testing.T) {
	src := `# template-meta:
#   id: x
#   parameters:
#     - name: foo
#       type: float
---
kind: Pod
`
	_, _, err := ParseMeta(src)
	require.Error(t, err)
	require.Contains(t, err.Error(), "float")
}

func TestParseMeta_RejectsSecretParameter(t *testing.T) {
	src := `# template-meta:
#   id: x
#   parameters:
#     - name: api_token
#       type: string
#       secret: true
---
kind: Pod
`
	_, _, err := ParseMeta(src)
	require.Error(t, err)
	require.Contains(t, err.Error(), "api_token")
	require.Contains(t, err.Error(), "secrets.per_instance")
}

func TestValidateParamDefs(t *testing.T) {
	m := Meta{ID: "x", Parameters: []ParamDef{
		{Name: "image", Type: "string"},
		{Name: "api_token", Type: "string", Secret: true},
	}}
	err := ValidateParamDefs(m)
	require.Error(t, err)
	require.Contains(t, err.Error(), "api_token")

	m.Parameters[1].Secret = false
	require.NoError(t, ValidateParamDefs(m))
}

func TestParseMeta_PreBackup(t *testing.T) {
	src := `# template-meta:
#   id: example
#   parameters:
#     - name: slug
#       type: string
#       required: true
#   pre_backup:
#     container: app
#     command: "bench --site {{.slug}} backup"
---
apiVersion: v1
kind: Pod
`
	meta, _, err := ParseMeta(src)
	require.NoError(t, err)
	require.NotNil(t, meta.PreBackup)
	assert.Equal(t, "app", meta.PreBackup.Container)
	assert.Equal(t, "bench --site {{.slug}} backup", meta.PreBackup.Command)
}

func TestParseMeta_NoPreBackup(t *testing.T) {
	src := `# template-meta:
#   id: example
#   parameters:
#     - name: slug
#       type: string
#       required: true
---
apiVersion: v1
kind: Pod
`
	meta, _, err := ParseMeta(src)
	require.NoError(t, err)
	assert.Nil(t, meta.PreBackup)
}

func TestValidateVolumes(t *testing.T) {
	cases := []struct {
		name    string
		vols    []Volume
		wantErr string
	}{
		{name: "no volumes", vols: nil},
		{name: "no patterns", vols: []Volume{{Name: "data"}}},
		{name: "valid pattern", vols: []Volume{{Name: "sites", Exclude: []string{"*/private/backups/**"}}}},
		{name: "empty pattern", vols: []Volume{{Name: "sites", Exclude: []string{""}}}, wantErr: "must not be empty"},
		{name: "whitespace pattern", vols: []Volume{{Name: "sites", Exclude: []string{"  "}}}, wantErr: "must not be empty"},
		{name: "absolute pattern", vols: []Volume{{Name: "sites", Exclude: []string{"/etc/**"}}}, wantErr: "must be relative"},
		{name: "parent escape", vols: []Volume{{Name: "sites", Exclude: []string{"../other/**"}}}, wantErr: "must not contain"},
		{name: "parent escape mid-path", vols: []Volume{{Name: "sites", Exclude: []string{"a/../../b"}}}, wantErr: "must not contain"},
		{name: "malformed glob", vols: []Volume{{Name: "sites", Exclude: []string{"[a-"}}}, wantErr: "invalid pattern"},
		{name: "leading dot-slash", vols: []Volume{{Name: "sites", Exclude: []string{"./private/**"}}}, wantErr: "not clean"},
		{name: "trailing slash", vols: []Volume{{Name: "sites", Exclude: []string{"private/backups/"}}}, wantErr: "not clean"},
		{name: "internal double slash", vols: []Volume{{Name: "sites", Exclude: []string{"private//backups"}}}, wantErr: "not clean"},
		{name: "leading slash still wins its own message", vols: []Volume{{Name: "sites", Exclude: []string{"/private/backups/"}}}, wantErr: "must be relative"},
		{name: "parent escape still wins its own message", vols: []Volume{{Name: "sites", Exclude: []string{"a/../../b/"}}}, wantErr: "must not contain"},
		// The zero-segment-rescue-swallows-everything class, not just the
		// "consecutive **" spelling of it — pinned against all seven probed
		// shapes plus bare "**".
		{name: "a/** drops something: accept", vols: []Volume{{Name: "sites", Exclude: []string{"a/**"}}}},
		{name: "a/*/** drops something: accept", vols: []Volume{{Name: "sites", Exclude: []string{"a/*/**"}}}},
		{name: "**/b/** drops something: accept", vols: []Volume{{Name: "sites", Exclude: []string{"**/b/**"}}}},
		{name: "a/**/** swallows everything: reject", vols: []Volume{{Name: "sites", Exclude: []string{"a/**/**"}}}, wantErr: "matches everything"},
		{name: "**/** swallows everything: reject", vols: []Volume{{Name: "sites", Exclude: []string{"**/**"}}}, wantErr: "matches everything"},
		{name: "**/*/** swallows everything: reject", vols: []Volume{{Name: "sites", Exclude: []string{"**/*/**"}}}, wantErr: "matches everything"},
		{name: "a/**/*/** swallows everything: reject", vols: []Volume{{Name: "sites", Exclude: []string{"a/**/*/**"}}}, wantErr: "matches everything"},
		{name: "bare ** legitimately drops everything: accept", vols: []Volume{{Name: "sites", Exclude: []string{"**"}}}},
		// Consumption of the `none` veto fails CLOSED — IsBackupMarkerNone folds
		// case and trims, so `None` really does veto. The registration rule on
		// top of that is not the safety mechanism; it is what keeps the stored
		// text saying what it does, so an author who wrote something they
		// believed was a veto is TOLD rather than guessed at, and a reader of the
		// meta and a reader of the code cannot reach different conclusions.
		{name: "exact none: accept", vols: []Volume{{Name: "logs", Backup: "none"}}},
		{name: "no marker at all: accept", vols: []Volume{{Name: "logs"}}},
		{name: "opaque commercial marker: accept", vols: []Volume{{Name: "sites", Backup: "s3; interval=24h"}}},
		{name: "a marker merely containing none: accept", vols: []Volume{{Name: "sites", Backup: "s3; mode=none-ish"}}},
		{name: "capitalised None: reject", vols: []Volume{{Name: "logs", Backup: "None"}}, wantErr: `must be exactly "none"`},
		{name: "upper NONE: reject", vols: []Volume{{Name: "logs", Backup: "NONE"}}, wantErr: `must be exactly "none"`},
		{name: "trailing space: reject", vols: []Volume{{Name: "logs", Backup: "none "}}, wantErr: `must be exactly "none"`},
		{name: "leading space: reject", vols: []Volume{{Name: "logs", Backup: " none"}}, wantErr: `must be exactly "none"`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := ValidateVolumes(Meta{Volumes: tc.vols})
			if tc.wantErr == "" {
				if err != nil {
					t.Fatalf("want nil, got %v", err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("want error containing %q, got %v", tc.wantErr, err)
			}
		})
	}
}

func TestParseMeta_rejectsBadExclude(t *testing.T) {
	src := `# template-meta:
#   id: t1
#   volumes:
#     - { name: data, exclude: ["/abs/**"] }
---
apiVersion: v1
kind: Pod
`
	if _, _, err := ParseMeta(src); err == nil {
		t.Fatal("want ParseMeta to reject an absolute exclude pattern")
	}
}

func TestParseMeta_acceptsExclude(t *testing.T) {
	src := `# template-meta:
#   id: t1
#   volumes:
#     - { name: sites, backup: "s3; interval=24h", exclude: ["*/private/backups/**"] }
---
apiVersion: v1
kind: Pod
`
	m, _, err := ParseMeta(src)
	if err != nil {
		t.Fatal(err)
	}
	if len(m.Volumes) != 1 || len(m.Volumes[0].Exclude) != 1 ||
		m.Volumes[0].Exclude[0] != "*/private/backups/**" {
		t.Fatalf("exclude not parsed: %+v", m.Volumes)
	}
}

// ValidateVolumes runs on UPDATE as well as create, so the near-miss rule made
// a row already stored with `None` — exactly the population the rule was
// written about — permanently uneditable: every PUT failed, including one
// touching an unrelated field, with no in-product path to the row.
// ValidateVolumesUpdate grandfathers a near-miss the stored row already carries
// and nothing else. (review-4 finding 4)
func TestValidateVolumesUpdate_GrandfathersStoredNearMiss(t *testing.T) {
	stored := Meta{Volumes: []Volume{{Name: "sites", Backup: "None"}, {Name: "data"}}}

	t.Run("unrelated edit against a stored near-miss is allowed", func(t *testing.T) {
		next := Meta{Volumes: []Volume{{Name: "sites", Backup: "None", Exclude: []string{"tmp/**"}}, {Name: "data"}}}
		if err := ValidateVolumesUpdate(next, stored); err != nil {
			t.Fatalf("want nil, got %v", err)
		}
		// Create is unchanged: the same meta registered fresh is still rejected.
		if err := ValidateVolumes(next); err == nil {
			t.Fatal("create must still reject a near-miss marker")
		}
	})

	t.Run("a newly introduced near-miss is still rejected", func(t *testing.T) {
		next := Meta{Volumes: []Volume{{Name: "sites", Backup: "None"}, {Name: "data", Backup: "NONE"}}}
		err := ValidateVolumesUpdate(next, stored)
		if err == nil || !strings.Contains(err.Error(), `must be exactly "none"`) {
			t.Fatalf("want a rejection for the newly introduced marker, got %v", err)
		}
	})

	t.Run("changing one near-miss to a different one is rejected", func(t *testing.T) {
		next := Meta{Volumes: []Volume{{Name: "sites", Backup: "NONE"}, {Name: "data"}}}
		if err := ValidateVolumesUpdate(next, stored); err == nil {
			t.Fatal("only the exact stored value is grandfathered")
		}
	})

	t.Run("every other rule still applies on update", func(t *testing.T) {
		next := Meta{Volumes: []Volume{{Name: "sites", Backup: "None", Exclude: []string{"/abs"}}, {Name: "data"}}}
		if err := ValidateVolumesUpdate(next, stored); err == nil {
			t.Fatal("exclude-pattern validation must not be relaxed on update")
		}
	})
}

func TestValidateNetworks(t *testing.T) {
	cases := []struct {
		name     string
		networks []string
		wantErr  string
	}{
		{name: "no networks", networks: nil},
		{name: "single network", networks: []string{"frappe-shared"}},
		{name: "several networks", networks: []string{"frappe-shared", "metrics"}},
		{name: "empty name", networks: []string{""}, wantErr: "must not be empty"},
		{name: "whitespace name", networks: []string{"  "}, wantErr: "must not be empty"},
		{name: "uppercase rejected", networks: []string{"Frappe"}, wantErr: "is not a valid network name"},
		{name: "underscore rejected", networks: []string{"frappe_shared"}, wantErr: "is not a valid network name"},
		{name: "leading dash rejected", networks: []string{"-shared"}, wantErr: "is not a valid network name"},
		{name: "duplicate rejected", networks: []string{"shared", "shared"}, wantErr: "declared twice"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := ValidateNetworks(Meta{Networks: tc.networks})
			if tc.wantErr == "" {
				if err != nil {
					t.Fatalf("want nil, got %v", err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("want error containing %q, got %v", tc.wantErr, err)
			}
		})
	}
}

func TestParseMeta_Networks(t *testing.T) {
	src := `# template-meta:
#   id: frappe-app
#   networks:
#     - frappe-shared
---
apiVersion: v1
kind: Pod
`
	meta, _, err := ParseMeta(src)
	require.NoError(t, err)
	require.Equal(t, []string{"frappe-shared"}, meta.Networks)
}

func TestParseMeta_NoNetworks(t *testing.T) {
	src := `# template-meta:
#   id: postgres
---
apiVersion: v1
kind: Pod
`
	meta, _, err := ParseMeta(src)
	require.NoError(t, err)
	require.Empty(t, meta.Networks)
}

func TestParseMeta_RejectsBadNetwork(t *testing.T) {
	src := `# template-meta:
#   id: frappe-app
#   networks:
#     - Not_Valid
---
apiVersion: v1
kind: Pod
`
	_, _, err := ParseMeta(src)
	require.Error(t, err)
}

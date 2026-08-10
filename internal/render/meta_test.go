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

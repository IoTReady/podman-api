package render

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const podBody = `apiVersion: v1
kind: Pod
metadata:
  name: db-{{.slug}}
spec:
  containers:
    - name: db
      image: {{.image}}
      ports:
        - containerPort: 5432
          hostPort: {{.port}}
`

func namesMeta() Meta {
	return Meta{
		ID: "db",
		Parameters: []ParamDef{
			{Name: "slug", Type: "string"},
			{Name: "image", Type: "string"},
			{Name: "port", Type: "int"},
		},
	}
}

// The body is a Go template and NOT parseable YAML on its own (`hostPort:
// {{.port}}` reads as a flow mapping), so extraction has to go through a render.
func TestContainerNamesLiteral(t *testing.T) {
	lit, tmplted, err := ContainerNames(podBody, namesMeta())
	require.NoError(t, err)
	assert.Equal(t, []string{"db"}, lit)
	assert.Empty(t, tmplted)
}

// A per-instance container name is the fix for a collision, so it must be
// recognised as templated rather than banked as a literal claim.
func TestContainerNamesTemplated(t *testing.T) {
	body := `apiVersion: v1
kind: Pod
metadata:
  name: db-{{.slug}}
spec:
  containers:
    - name: db-{{.slug}}
      image: {{.image}}
`
	lit, tmplted, err := ContainerNames(body, namesMeta())
	require.NoError(t, err)
	assert.Empty(t, lit)
	assert.Equal(t, []string{"db-aaa"}, tmplted)
}

// A DEFAULTED parameter is still per-instance: rendering both probes with the
// default would mislabel {{.name}} as literal.
func TestContainerNamesIgnoresDefaults(t *testing.T) {
	m := Meta{ID: "x", Parameters: []ParamDef{{Name: "name", Type: "string", Default: "db"}}}
	body := "spec:\n  containers:\n    - name: {{.name}}\n"
	lit, tmplted, err := ContainerNames(body, m)
	require.NoError(t, err)
	assert.Empty(t, lit)
	assert.Len(t, tmplted, 1)
}

// Multi-document bodies (a ConfigMap next to the Pod) and the workload shape
// kube play also accepts must both be walked.
func TestContainerNamesMultiDocAndDeployment(t *testing.T) {
	body := `apiVersion: v1
kind: ConfigMap
metadata:
  name: cfg
data:
  a: b
---
apiVersion: apps/v1
kind: Deployment
spec:
  template:
    spec:
      containers:
        - name: web
        - name: sidecar
`
	lit, _, err := ContainerNames(body, Meta{ID: "x"})
	require.NoError(t, err)
	assert.Equal(t, []string{"web", "sidecar"}, lit)
}

// A parameter that changes the SHAPE of the pod cannot be paired positionally.
// Nothing is then provably literal — the check must fail open on enforcement,
// not invent a claim.
func TestContainerNamesConditionalShape(t *testing.T) {
	m := Meta{ID: "x", Parameters: []ParamDef{{Name: "extra", Type: "bool"}}}
	body := "spec:\n  containers:\n    - name: db\n{{if .extra}}    - name: cache\n{{end}}"
	lit, tmplted, err := ContainerNames(body, m)
	require.NoError(t, err)
	assert.Empty(t, lit)
	assert.NotEmpty(t, tmplted)
}

func TestContainerNamesBodyThatCannotRender(t *testing.T) {
	_, _, err := ContainerNames("{{.nope}}", Meta{ID: "x"})
	require.Error(t, err)
}

// An alias equal to one of the template's own container names is redundant and
// misleading: the pod answers to it either way.
func TestValidateAliasesAgainstContainerNames(t *testing.T) {
	m := Meta{ID: "db", Networks: []Network{{Name: "shared", Aliases: []string{"db"}}}}
	err := ValidateAliasesAgainstContainerNames(m, []string{"db"})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "container name")

	require.NoError(t, ValidateAliasesAgainstContainerNames(m, []string{"other"}))
}

// ParseMeta is the file/seed entry point: it must populate the derived field and
// refuse an alias that duplicates a container name.
func TestParseMetaExtractsContainerNames(t *testing.T) {
	src := `# template-meta:
#   id: db
#   parameters:
#     - name: slug
#       type: string
#   networks:
#     - name: shared
#       aliases: [mariadb]
---
apiVersion: v1
kind: Pod
metadata:
  name: db-{{.slug}}
spec:
  containers:
    - name: db
`
	m, _, err := ParseMeta(src)
	require.NoError(t, err)
	assert.Equal(t, []string{"db"}, m.ContainerNames)

	bad := strings.Replace(src, "aliases: [mariadb]", "aliases: [db]", 1)
	_, _, err = ParseMeta(bad)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "container name")
}

// A `select` parameter probed with a value outside its own option set takes the
// same branch in both probes: the real claim ("db") is missed, and "web" — a
// name no instance renders — is banked as a literal claim that would REFUSE a
// legal second instance. Probing with the declared options closes that
// (#285 review item 2).
func TestContainerNamesUsesSelectOptions(t *testing.T) {
	m := Meta{ID: "x", Parameters: []ParamDef{
		{Name: "mode", Type: "select", Options: []string{"postgres", "mysql"}},
	}}
	body := `spec:
  containers:
    - name: {{if eq .mode "postgres"}}db{{else}}web{{end}}
`
	lit, tmplted, err := ContainerNames(body, m)
	require.NoError(t, err)
	assert.Empty(t, lit, "the name depends on the mode, so nothing is literal")
	assert.Equal(t, []string{"db"}, tmplted)
}

// A select with a single option renders the same name for every instance, so
// that name IS a literal claim.
func TestContainerNamesSelectWithOneOption(t *testing.T) {
	m := Meta{ID: "x", Parameters: []ParamDef{
		{Name: "mode", Type: "select", Options: []string{"postgres"}},
	}}
	body := `spec:
  containers:
    - name: {{if eq .mode "postgres"}}db{{else}}web{{end}}
`
	lit, _, err := ContainerNames(body, m)
	require.NoError(t, err)
	assert.Equal(t, []string{"db"}, lit)
}

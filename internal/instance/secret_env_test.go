package instance

import (
	"testing"

	"github.com/iotready/podman-api/internal/store"
	"github.com/stretchr/testify/assert"
)

func TestSecretEnvNames(t *testing.T) {
	body := `apiVersion: v1
kind: Pod
metadata:
  name: postgres-{{.slug}}
spec:
  containers:
    - name: db
      image: {{.image}}
      env:
        - name: POSTGRES_DB
          value: {{.db}}
        - name: POSTGRES_PASSWORD
          valueFrom:
            secretKeyRef:
              name: postgres-{{.slug}}-password
              key: postgres-{{.slug}}-password
`
	got := secretEnvNames(body)
	assert.True(t, got["POSTGRES_PASSWORD"], "POSTGRES_PASSWORD should be detected as secret-sourced")
	assert.False(t, got["POSTGRES_DB"], "POSTGRES_DB has a literal value, not a secret")
}

func TestSecretEnvNames_MultiDoc(t *testing.T) {
	body := `apiVersion: v1
kind: ConfigMap
metadata:
  name: foo-config
data:
  some.yml: hello
---
apiVersion: v1
kind: Pod
metadata:
  name: foo-app
spec:
  containers:
    - name: app
      env:
        - name: AUTH_SECRET
          valueFrom:
            secretKeyRef:
              name: foo-auth
              key: foo-auth
        - name: AWS_ACCESS_KEY_ID
          valueFrom:
            secretKeyRef:
              name: aws
              key: aws
`
	got := secretEnvNames(body)
	assert.True(t, got["AUTH_SECRET"])
	assert.True(t, got["AWS_ACCESS_KEY_ID"])
}

func TestSecretEnvNames_InitContainers(t *testing.T) {
	body := `apiVersion: v1
kind: Pod
metadata:
  name: app-{{.slug}}
spec:
  initContainers:
    - name: restore
      image: {{.image}}
      env:
        - name: RESTORE_TOKEN
          valueFrom:
            secretKeyRef:
              name: app-{{.slug}}-restore
              key: app-{{.slug}}-restore
        - name: RESTORE_MODE
          value: full
  containers:
    - name: app
      image: {{.image}}
      env:
        - name: APP_KEY
          valueFrom:
            secretKeyRef:
              name: app-{{.slug}}-key
              key: app-{{.slug}}-key
`
	got := secretEnvNames(body)
	assert.True(t, got["RESTORE_TOKEN"], "an initContainer secretKeyRef must be detected")
	assert.True(t, got["APP_KEY"], "container secretKeyRefs must still be detected")
	assert.False(t, got["RESTORE_MODE"], "a literal-valued initContainer env is not a secret")
}

func TestSecretValues(t *testing.T) {
	sp := store.Spec{
		Secrets: map[string]string{"password": "p4ssw0rd", "blank": ""},
		InjectorSecrets: []store.InjectorSecret{
			{Name: "vpn-psk", Key: "postgres-demo-vpn-psk", Value: "s3cr3t-psk"},
			{Name: "empty", Key: "k", Value: ""},
		},
	}
	got := secretValues(sp)
	assert.True(t, got["p4ssw0rd"], "template-declared secret values are included")
	assert.True(t, got["s3cr3t-psk"], "injector-declared secret values are included")
	assert.False(t, got[""], "empty values must be excluded — they would blank every empty env var")
	assert.Len(t, got, 2)
}

func TestSecretValues_EmptySpec(t *testing.T) {
	assert.Empty(t, secretValues(store.Spec{}))
}

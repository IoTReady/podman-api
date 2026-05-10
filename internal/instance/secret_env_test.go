package instance

import (
	"testing"

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

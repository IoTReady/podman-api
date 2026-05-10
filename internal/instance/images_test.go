package instance

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestContainerImages(t *testing.T) {
	body := `apiVersion: v1
kind: Pod
metadata:
  name: x
spec:
  initContainers:
    - name: init
      image: busybox:1.36
  containers:
    - name: app
      image: docker.io/library/postgres:16
    - name: dup
      image: docker.io/library/postgres:16
    - name: side
      image: ghcr.io/example/sidecar:v2
---
apiVersion: v1
kind: ConfigMap
metadata: {name: ignored}
data: {x: y}
`
	// Order: regular containers first (postgres, sidecar; postgres dedup'd),
	// then initContainers (busybox).
	got := containerImages(body)
	assert.Equal(t, []string{
		"docker.io/library/postgres:16",
		"ghcr.io/example/sidecar:v2",
		"busybox:1.36",
	}, got)
}

func TestContainerImages_EmptyAndNonPod(t *testing.T) {
	assert.Nil(t, containerImages(""))
	assert.Nil(t, containerImages(`apiVersion: v1
kind: ConfigMap
metadata: {name: x}
`))
}

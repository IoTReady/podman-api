package instance

import (
	"encoding/base64"
	"fmt"
)

// wrapAsKubeSecret encodes a raw secret value as a Kubernetes Secret object
// (Opaque, with a single data key matching key). This is required because
// `podman play kube` resolves both `secretKeyRef` and `volumes.secret` against
// podman secrets whose body is itself K8s Secret YAML; raw byte values do not
// work. The data key matches key, matching the convention used by every bundled
// template's secretKeyRef.key field.
func wrapAsKubeSecret(name, key string, value []byte) []byte {
	body := fmt.Sprintf(`apiVersion: v1
kind: Secret
type: Opaque
metadata:
  name: %s
data:
  %s: %s
`, name, key, base64.StdEncoding.EncodeToString(value))
	return []byte(body)
}

package ingress

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestDisabledControllerIsNoOp(t *testing.T) {
	var c Controller = Disabled{}
	require.NoError(t, c.Reconcile(context.Background(), "h1"))
}

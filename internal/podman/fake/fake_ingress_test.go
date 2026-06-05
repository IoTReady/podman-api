package fake

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/iotready/podman-api/internal/podman"
)

func TestPlayKubeRecordsNetworks(t *testing.T) {
	f := New()
	yaml := "apiVersion: v1\nkind: Pod\nmetadata:\n  name: web-a\nspec:\n  containers:\n    - name: web\n      image: nginx\n"
	// The network must exist first — the fake models podman's "network not found".
	require.NoError(t, f.NetworkEnsure(context.Background(), "h1", "podman-api-ingress"))
	require.NoError(t, f.PlayKube(context.Background(), "h1", yaml, false, "podman-api-ingress"))
	require.Len(t, f.PlayCalls, 1)
	require.Equal(t, []string{"podman-api-ingress"}, f.PlayCalls[0].Networks)
}

func TestPlayKubeRejectsUnknownNetwork(t *testing.T) {
	f := New()
	yaml := "apiVersion: v1\nkind: Pod\nmetadata:\n  name: web-a\nspec:\n  containers:\n    - name: web\n      image: nginx\n"
	err := f.PlayKube(context.Background(), "h1", yaml, false, "not-ensured")
	require.Error(t, err)
	require.Contains(t, err.Error(), "network not found")
}

func TestNetworkEnsureRecords(t *testing.T) {
	f := New()
	require.NoError(t, f.NetworkEnsure(context.Background(), "h1", "podman-api-ingress"))
	require.NoError(t, f.NetworkEnsure(context.Background(), "h1", "podman-api-ingress")) // idempotent
	require.Equal(t, []string{"podman-api-ingress", "podman-api-ingress"}, f.NetworkEnsureCalls["h1"])
}

func TestContainerExecHookAndRecord(t *testing.T) {
	f := New()
	f.ExecFunc = func(host, container string, cmd []string) (podman.ExecResult, error) {
		return podman.ExecResult{ExitCode: 0, Output: "ok"}, nil
	}
	res, err := f.ContainerExec(context.Background(), "h1", "caddy", []string{"caddy", "reload"})
	require.NoError(t, err)
	require.Equal(t, 0, res.ExitCode)
	require.Equal(t, "ok", res.Output)
	require.Len(t, f.ExecCalls, 1)
	require.Equal(t, []string{"caddy", "reload"}, f.ExecCalls[0].Cmd)
}

func TestCopyToContainerRecords(t *testing.T) {
	f := New()
	require.NoError(t, f.CopyToContainer(context.Background(), "h1", "caddy", "/etc/caddy", "Caddyfile", []byte("hello")))
	require.Len(t, f.CopyCalls, 1)
	got := f.CopyCalls[0]
	require.Equal(t, "caddy", got.Container)
	require.Equal(t, "/etc/caddy", got.DestDir)
	require.Equal(t, "Caddyfile", got.Name)
	require.Equal(t, []byte("hello"), got.Content)
}

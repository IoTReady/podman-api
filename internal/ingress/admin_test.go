package ingress

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRoutesToCaddyJSONEmpty(t *testing.T) {
	routes := routesToCaddyJSON(nil)
	require.Empty(t, routes)
}

func TestRoutesToCaddyJSONSingleRoute(t *testing.T) {
	in := []Route{{Domain: "blog.example.com", Backend: "web-blog:8080"}}
	out := routesToCaddyJSON(in)
	require.Len(t, out, 1)
	r := out[0]
	require.Len(t, r.Match, 1)
	require.Equal(t, []string{"blog.example.com"}, r.Match[0].Host)
	require.Len(t, r.Handle, 1)
	require.Equal(t, "reverse_proxy", r.Handle[0].Handler)
	require.Len(t, r.Handle[0].Upstreams, 1)
	require.Equal(t, "web-blog:8080", r.Handle[0].Upstreams[0].Dial)
	require.True(t, r.Terminal)
}

func TestRoutesToCaddyJSONMultipleRoutes(t *testing.T) {
	in := []Route{
		{Domain: "a.example.com", Backend: "svc-a:3000"},
		{Domain: "b.example.com", Backend: "svc-b:4000"},
	}
	out := routesToCaddyJSON(in)
	require.Len(t, out, 2)
}

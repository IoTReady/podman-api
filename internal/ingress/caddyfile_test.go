package ingress

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRenderCaddyfile(t *testing.T) {
	cases := []struct {
		name   string
		email  string
		routes []Route
		want   string
	}{
		{
			name:  "email only, no routes",
			email: "ops@example.com",
			want:  "{\n\temail ops@example.com\n}\n\n",
		},
		{
			name: "routes sorted by domain, no email",
			routes: []Route{
				{Domain: "b.example.com", Backend: "web-b:8080"},
				{Domain: "a.example.com", Backend: "web-a:8080"},
			},
			want: "a.example.com {\n\treverse_proxy web-a:8080\n}\n" +
				"b.example.com {\n\treverse_proxy web-b:8080\n}\n",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := RenderCaddyfile(tc.email, tc.routes)
			require.NoError(t, err)
			require.Equal(t, tc.want, got)
		})
	}
}

func TestRenderCaddyfileRejectsEmptyFields(t *testing.T) {
	_, err := RenderCaddyfile("", []Route{{Domain: "a.example.com"}})
	require.Error(t, err)
}

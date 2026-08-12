package server

import (
	"strings"
	"testing"

	"github.com/iotready/podman-api/internal/render"
	"github.com/iotready/podman-api/internal/store"
)

// `backup: none` changed meaning from opaque ("non-empty == marked") to a hard
// veto (#249). Instances of an affected template keep returning 202 and
// completing green while the blob set quietly loses a volume, with the only
// trace a `skip-volume` step in a job trail nobody reads on a success. The
// reinterpretation has to be visible once, at startup. (re-review H)
func TestBackupMarkerNoneWarning(t *testing.T) {
	tmpl := func(id string, vols ...render.Volume) store.Template {
		return store.Template{Meta: render.Meta{ID: id, Volumes: vols}}
	}

	tests := []struct {
		name  string
		in    []store.Template
		want  []string // substrings that must appear; empty means want ""
		nowat []string // substrings that must NOT appear
	}{
		{
			name: "no none markers is silent",
			in: []store.Template{
				tmpl("web", render.Volume{Name: "data"}),
				tmpl("db", render.Volume{Name: "data", Backup: "s3; interval=24h"}),
			},
		},
		{
			name: "empty catalog is silent",
		},
		{
			name: "names the template and the vetoed volume",
			in: []store.Template{
				tmpl("web", render.Volume{Name: "data"}, render.Volume{Name: "logs", Backup: "none"}),
			},
			want: []string{"HARD VETO", "web[logs]"},
			// The unmarked `data` volume is backed up normally and must not be
			// listed among the vetoed ones.
			nowat: []string{"data]", "[data"},
		},
		{
			// A near-miss spelling stored before the registration validator
			// existed vetoes now too (the comparison folds case), so it is
			// exactly the reinterpretation this warning exists to announce.
			name: "a near-miss spelling is reported as well",
			in: []store.Template{
				tmpl("legacy", render.Volume{Name: "sites", Backup: " None "}),
			},
			want: []string{"legacy[sites]"},
		},
		{
			// Stable across restarts: an operator diffing two boots should see a
			// change only when the catalog changed, not when the store's
			// iteration order did.
			name: "templates and volumes are sorted",
			in: []store.Template{
				tmpl("zeta", render.Volume{Name: "b", Backup: "none"}, render.Volume{Name: "a", Backup: "none"}),
				tmpl("alpha", render.Volume{Name: "x", Backup: "none"}),
			},
			want: []string{"alpha[x], zeta[a b]"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := backupMarkerNoneWarning(tc.in)
			if len(tc.want) == 0 {
				if got != "" {
					t.Fatalf("want no warning, got %q", got)
				}
				return
			}
			for _, sub := range tc.want {
				if !strings.Contains(got, sub) {
					t.Errorf("warning %q missing %q", got, sub)
				}
			}
			for _, sub := range tc.nowat {
				if strings.Contains(got, sub) {
					t.Errorf("warning %q must not mention %q", got, sub)
				}
			}
		})
	}
}

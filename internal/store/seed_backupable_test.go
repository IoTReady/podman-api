package store

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/iotready/podman-api/internal/render"
	"github.com/iotready/podman-api/templates"
)

// TestBundledTemplates_AreBackupable locks the shipped catalog against the
// shape that `backup: none` becoming a HARD VETO (#249) made unusable: a
// template that declares volumes and marks EVERY one of them `none`.
//
// CheckBackupable rejects such an instance outright (`invalid_backup_scope`),
// so a bundled template in that shape ships unbackupable — every POST
// .../backup against it 400s, and any automation that has been backing it up
// starts erroring the moment the daemon is upgraded. The `postgres` seed was
// exactly that on this branch, and the suite stayed green because only the
// TEST fixtures were moved off `none`.
//
// A template declaring NO volumes at all (basic-web) is deliberately fine —
// it is stateless, there is nothing to capture, and CheckBackupable accepts it.
//
// This does NOT require a bundled volume to carry a marker. An EMPTY marker is
// exportable — it is only the `none` veto that removes a volume from every
// backup — and marker GRAMMAR beyond the veto (`s3; interval=…`) is commercial,
// so the OSS catalog deliberately ships bare `- name: data` volumes rather than
// prescribing a cadence a fresh install never asked for.
func TestBundledTemplates_AreBackupable(t *testing.T) {
	seeds, err := ParseSeeds(templates.Files)
	require.NoError(t, err)
	require.NotEmpty(t, seeds)

	for _, s := range seeds {
		if len(s.Meta.Volumes) == 0 {
			continue // stateless template: nothing to back up, and that is valid
		}
		var backupable []string
		for _, v := range s.Meta.Volumes {
			if !render.IsBackupMarkerNone(v.Backup) {
				backupable = append(backupable, v.Name)
			}
		}
		require.NotEmptyf(t, backupable,
			"bundled template %q declares volumes but marks every one of them `backup: none`, "+
				"so CheckBackupable rejects every backup of every instance of it", s.Meta.ID)
	}
}

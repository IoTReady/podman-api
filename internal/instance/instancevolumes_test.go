package instance

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

// A transient (non-NotFound) inspect error must fail InstanceVolumes rather than
// silently dropping the volume — migrate/evacuate reap the source after copying
// this set, so a dropped volume means data loss. A genuine NotFound (volume not
// created yet) is still skipped, not fatal. (#50)
func TestInstanceVolumes_InspectErrorIsFatal(t *testing.T) {
	svc, f := newSvc(t)
	ctx := context.Background()

	f.VolumeInspectErr = errors.New("ssh: connection reset")
	_, err := svc.InstanceVolumes(ctx, "h1", "postgres", "v")
	require.Error(t, err)
	require.NotErrorIs(t, err, ErrUnknownTemplate)

	// Without the injected error, the declared-but-absent volume is skipped.
	f.VolumeInspectErr = nil
	vols, err := svc.InstanceVolumes(ctx, "h1", "postgres", "v")
	require.NoError(t, err)
	require.Empty(t, vols)
}

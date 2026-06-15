package server

import (
	"context"
	"errors"
	"log"

	"github.com/iotready/podman-api/extension"
	"github.com/iotready/podman-api/internal/backupctl"
	"github.com/iotready/podman-api/internal/instance"
)

// *instance.Service must satisfy the controller's Service dependency; this
// static assertion fails the build if the seam and the service drift apart.
var _ backupctl.Service = (*instance.Service)(nil)

// WithBackupScheduler registers a commercial BackupScheduler. When set, the
// server starts it after wiring and runs it until the run context is cancelled,
// handing it a BackupController so it can list backup-eligible instances and
// enqueue backup jobs. The core ships no scheduling behavior of its own.
func WithBackupScheduler(bs extension.BackupScheduler) Option {
	return func(c *cfg) { c.backupScheduler = bs }
}

// runBackupScheduler launches a registered BackupScheduler in its own goroutine,
// running until ctx is cancelled. A nil scheduler is a no-op. Panics are
// recovered and a non-cancellation error is logged, mirroring the prune
// scheduler's defensive launch so a faulty scheduler can't crash the server.
func runBackupScheduler(ctx context.Context, sched extension.BackupScheduler, ctrl extension.BackupController) {
	if sched == nil {
		return
	}
	go func() {
		defer func() {
			if r := recover(); r != nil {
				log.Printf("backup scheduler: panicked: %v", r)
			}
		}()
		if err := sched.Run(ctx, ctrl); err != nil && !errors.Is(err, context.Canceled) {
			log.Printf("backup scheduler: exited: %v", err)
		}
	}()
}

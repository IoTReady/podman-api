package server

import (
	"context"
	"testing"
	"time"

	"github.com/iotready/podman-api/extension"
	"github.com/iotready/podman-api/internal/backupctl"
)

type fakeBackupScheduler struct {
	gotCtrl extension.BackupController
	ran     chan struct{}
}

func (f *fakeBackupScheduler) Run(ctx context.Context, c extension.BackupController) error {
	f.gotCtrl = c
	close(f.ran)
	<-ctx.Done()
	return ctx.Err()
}

func TestWithBackupScheduler_setsConfig(t *testing.T) {
	var c cfg
	sched := &fakeBackupScheduler{}
	WithBackupScheduler(sched)(&c)
	if c.backupScheduler != sched {
		t.Fatalf("WithBackupScheduler did not store the scheduler")
	}
}

func TestRunBackupScheduler_runsUntilCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sched := &fakeBackupScheduler{ran: make(chan struct{})}
	ctrl := &backupctl.Controller{}

	runBackupScheduler(ctx, sched, ctrl)

	select {
	case <-sched.ran:
	case <-time.After(2 * time.Second):
		t.Fatal("scheduler Run was not invoked")
	}
	if sched.gotCtrl != extension.BackupController(ctrl) {
		t.Fatalf("scheduler received the wrong controller")
	}
}

func TestRunBackupScheduler_nilIsNoop(t *testing.T) {
	// A nil scheduler must be a no-op (no goroutine, no panic).
	runBackupScheduler(context.Background(), nil, nil)
}

package fake

import (
	"context"
	"errors"
	"testing"
)

func TestFakeVolumeUsage(t *testing.T) {
	f := New()
	f.VolumeUsageVal = map[string]map[string]int64{"h1": {"engine-valvo-data": 4096}}
	got, err := f.VolumeUsage(context.Background(), "h1")
	if err != nil {
		t.Fatalf("VolumeUsage: %v", err)
	}
	if got["engine-valvo-data"] != 4096 {
		t.Fatalf("size = %d, want 4096", got["engine-valvo-data"])
	}
	if f.VolumeUsageCalls != 1 {
		t.Fatalf("calls = %d, want 1", f.VolumeUsageCalls)
	}

	f.VolumeUsageErr = errors.New("boom")
	if _, err := f.VolumeUsage(context.Background(), "h1"); err == nil {
		t.Fatal("want error when VolumeUsageErr set")
	}
}

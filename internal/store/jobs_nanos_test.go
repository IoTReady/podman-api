package store

import (
	"context"
	"encoding/json"
	"testing"
	"time"
)

func TestJobs_NanosecondTimestamps(t *testing.T) {
	s := openJobStore(t)
	ctx := context.Background()

	before := time.Now()
	j, err := s.Enqueue(ctx, "test", json.RawMessage(`{}`), "")
	if err != nil {
		t.Fatal(err)
	}
	got, err := s.GetJob(ctx, j.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got.Created.Before(before.Truncate(time.Second)) || got.Created.After(time.Now().Add(time.Second)) {
		t.Fatalf("created %v out of expected window", got.Created)
	}
	// A value stored as UnixNano and read with Unix(0, n) keeps nanosecond
	// resolution. Stored as seconds, the sub-second part is always 0.
	if got.Created.Nanosecond() == 0 && before.Nanosecond() != 0 {
		t.Fatalf("created lost sub-second precision: %v (nsec=0)", got.Created)
	}
}

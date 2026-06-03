package store

import (
	"context"
	"errors"
	"testing"
)

func sampleSpec() Spec {
	return Spec{
		Host: "h1", Template: "postgres", Slug: "demo",
		Parameters: map[string]any{"image": "postgres:16", "user": "app"},
		Secrets:    map[string]string{"password": "hunter2"},
	}
}

func TestMemory_PutGetDelete(t *testing.T) {
	ctx := context.Background()
	m := NewMemory()
	if err := m.PutSpec(ctx, sampleSpec()); err != nil {
		t.Fatalf("PutSpec: %v", err)
	}
	got, err := m.GetSpec(ctx, "h1", "postgres", "demo")
	if err != nil {
		t.Fatalf("GetSpec: %v", err)
	}
	if got.Secrets["password"] != "hunter2" || got.Parameters["image"] != "postgres:16" {
		t.Fatalf("round-trip mismatch: %+v", got)
	}
	if err := m.DeleteSpec(ctx, "h1", "postgres", "demo"); err != nil {
		t.Fatalf("DeleteSpec: %v", err)
	}
	if _, err := m.GetSpec(ctx, "h1", "postgres", "demo"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("expected ErrNotFound, got %v", err)
	}
}

func TestMemory_GetMissing(t *testing.T) {
	if _, err := NewMemory().GetSpec(context.Background(), "h1", "x", "y"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("expected ErrNotFound, got %v", err)
	}
}

func TestMemory_DeleteMissing(t *testing.T) {
	if err := NewMemory().DeleteSpec(context.Background(), "h1", "x", "y"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("expected ErrNotFound, got %v", err)
	}
}

func TestMemory_ErrorHooks(t *testing.T) {
	ctx := context.Background()
	m := NewMemory()
	putErr := errors.New("put boom")
	m.PutErr = putErr
	if err := m.PutSpec(ctx, sampleSpec()); !errors.Is(err, putErr) {
		t.Fatalf("expected PutErr, got %v", err)
	}
	m.PutErr = nil
	_ = m.PutSpec(ctx, sampleSpec())
	delErr := errors.New("del boom")
	m.DeleteErr = delErr
	if err := m.DeleteSpec(ctx, "h1", "postgres", "demo"); !errors.Is(err, delErr) {
		t.Fatalf("expected DeleteErr, got %v", err)
	}
}

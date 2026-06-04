# Jobs: bound the runner's `finish()` context (#44)

**Goal:** Ensure a job's terminal state is always recorded, even when the
daemon is shutting down, so a job that succeeds at the moment of shutdown is
never mislabelled `failed` by boot recovery.

## Problem

`jobs.Runner.run` calls `r.finish(ctx, …)` with the runner's lifecycle context
(`runnerCtx`). On shutdown that context is cancelled. If a handler returns
(success *or* failure) at the instant of cancellation, `JobStore.Finish`'s
`ExecContext` fails on the cancelled context, the row stays `running`, and the
next boot's `FailRunning` flips it to `failed` — recording **failure for work
that actually succeeded**.

## Fix

`Runner.finish` derives its own short-lived context from
`context.Background()` instead of riding `runnerCtx`:

```go
const finishTimeout = 5 * time.Second

func (r *Runner) finish(id string, state store.JobState, errMsg string) {
	ctx, cancel := context.WithTimeout(context.Background(), finishTimeout)
	defer cancel()
	if err := r.store.Finish(ctx, id, state, errMsg); err != nil {
		log.Printf("jobs: finish %s failed: %v", id, err)
	}
}
```

The `ctx` parameter is dropped; the three call sites in `run` are updated. This
is a strict improvement: a completed job records its true terminal state
immediately, regardless of `runnerCtx` cancellation. Reap-on-boot remains the
fallback **only** for a true process kill in the narrow window between
handler-return and the finish write.

The existing no-`Wait` shutdown is unchanged: shutdown still does not block on a
mid-flight handler.

## Testing

The in-memory test store ignores its context, so the bug cannot be reproduced
against it. The test introduces a context-honoring double that embeds
`*store.Memory` and overrides `Finish` to return `ctx.Err()` when its context is
already cancelled:

- **TestRunner_FinishSurvivesCancelledRunnerCtx** — build a runner over the
  double, run a job through `r.run(cancelledCtx, job)`, and assert the job is
  recorded `succeeded`. Before the fix this would fail (the double's `Finish`
  sees the cancelled context); after the fix `finish` uses a live background
  context, so the write lands.

Existing runner tests (`TestRunner_RunsHandler_Succeeds`,
`TestRunner_HandlerError_Fails`, `TestRunner_UnknownKind_Fails`) continue to
pass with the signature change.

## Out of scope

- A bounded drain on shutdown (`runner.Wait()` with a deadline) — still
  deliberately omitted; an interrupted in-flight handler stays `running` and is
  reaped on next boot.

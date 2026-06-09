package jobs

import "time"

// Metrics records job lifecycle events. Implementations must be safe for
// concurrent use. Runner.Metrics may be nil; a noop implementation is used in that case.
type Metrics interface {
	JobStarted(kind string)
	JobFinished(kind, result string)
	ObserveDuration(kind string, d time.Duration)
	JobEnqueued(kind string)
	Rollback(kind string)
	ChildFailure(kind string)
}

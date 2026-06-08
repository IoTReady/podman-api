package jobs

import "time"

// Metrics records job lifecycle events. Implementations must be safe for
// concurrent use. A nil *Metrics is valid (all methods are no-ops).
type Metrics interface {
	JobStarted(kind string)
	JobFinished(kind, result string)
	ObserveDuration(kind string, d time.Duration)
	JobEnqueued(kind string)
	Rollback(kind string)
	ChildFailure(kind string)
}

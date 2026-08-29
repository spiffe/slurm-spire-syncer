package metrics

import (
	"context"
	"log/slog"
	"time"
)

// Clock abstracts time so loop tests do not have to sleep. The zero value is
// unusable; use RealClock or supply a fake.
type Clock interface {
	Now() time.Time
	Since(time.Time) time.Duration
}

type realClock struct{}

func (realClock) Now() time.Time                  { return time.Now() }
func (realClock) Since(t time.Time) time.Duration { return time.Since(t) }

// RealClock is the production Clock.
var RealClock Clock = realClock{}

// Runner drives one periodic task.
type Runner struct {
	Name     string
	Interval time.Duration
	Metrics  *LoopMetrics
	Log      *slog.Logger
	Clock    Clock
}

// Run executes fn every Interval until ctx is cancelled.
//
// The first run happens immediately and the wait comes after, so there is no
// cold start: the syncer has a populated snapshot one interval sooner. That
// ordering also guarantees only one invocation is ever in flight — the next run
// cannot begin until the current one returns. A ticker buffers a single tick, so
// ticks missed during a long run collapse into one rather than queueing up.
func (r *Runner) Run(ctx context.Context, fn func(context.Context) error) {
	clock := r.Clock
	if clock == nil {
		clock = RealClock
	}

	ticker := time.NewTicker(r.Interval)
	defer ticker.Stop()

	for {
		r.once(ctx, clock, fn)

		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func (r *Runner) once(ctx context.Context, clock Clock, fn func(context.Context) error) {
	start := clock.Now()
	err := fn(ctx)
	elapsed := clock.Since(start)

	r.Metrics.Duration.Observe(elapsed.Seconds())
	if elapsed > r.Interval {
		r.Metrics.Overrun.Inc()
		r.Log.Warn("loop run took longer than its interval",
			"loop", r.Name, "duration", elapsed, "interval", r.Interval)
	}

	switch {
	case err != nil && ctx.Err() != nil:
		// Shutting down: the error is a consequence of cancellation, not a
		// genuine failure, so it must not flip the failing gauge on the way out.
		r.Log.Debug("loop run cancelled", "loop", r.Name, "error", err)
	case err != nil:
		r.Metrics.Failure.Inc()
		r.Metrics.Failing.Set(1)
		r.Log.Error("loop run failed", "loop", r.Name, "error", err)
	default:
		r.Metrics.Success.Inc()
		r.Metrics.Failing.Set(0)
		r.Metrics.LastSuccess.Set(float64(clock.Now().Unix()))
	}
}

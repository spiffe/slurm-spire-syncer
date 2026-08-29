package metrics

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
)

// fakeClock makes the overrun path testable without actually sleeping past an
// interval.
type fakeClock struct {
	now     time.Time
	elapsed time.Duration
}

func (c *fakeClock) Now() time.Time                { return c.now }
func (c *fakeClock) Since(time.Time) time.Duration { return c.elapsed }

func testRunner(t *testing.T, interval time.Duration, clock Clock) (*Runner, *Metrics) {
	t.Helper()
	m := New()
	return &Runner{
		Name:     LoopSqueue,
		Interval: interval,
		Metrics:  m.Loop(LoopSqueue),
		Log:      slog.New(slog.NewTextHandler(io.Discard, nil)),
		Clock:    clock,
	}, m
}

// Every series is pre-initialized so that "no data" and "healthy, zero events"
// are distinguishable on a dashboard.
func TestMetricsArePreInitialized(t *testing.T) {
	m := New()
	for _, loop := range []string{LoopSqueue, LoopSpireList, LoopReconcile} {
		lm := m.Loop(loop)
		if lm == nil {
			t.Fatalf("Loop(%q) returned nil", loop)
		}
		if got := testutil.ToFloat64(lm.Failing); got != 0 {
			t.Errorf("%s failing = %v, want 0 at startup", loop, got)
		}
		if got := testutil.ToFloat64(lm.Success); got != 0 {
			t.Errorf("%s success_total = %v, want 0 at startup", loop, got)
		}
		if got := testutil.ToFloat64(lm.Overrun); got != 0 {
			t.Errorf("%s overrun_total = %v, want 0 at startup", loop, got)
		}
	}
}

func TestOnceRecordsSuccess(t *testing.T) {
	clock := &fakeClock{now: time.Unix(1_700_000_000, 0), elapsed: time.Millisecond}
	r, m := testRunner(t, time.Second, clock)

	r.once(context.Background(), clock, func(context.Context) error { return nil })

	lm := m.Loop(LoopSqueue)
	if got := testutil.ToFloat64(lm.Success); got != 1 {
		t.Errorf("success_total = %v, want 1", got)
	}
	if got := testutil.ToFloat64(lm.Failure); got != 0 {
		t.Errorf("failure_total = %v, want 0", got)
	}
	if got := testutil.ToFloat64(lm.Failing); got != 0 {
		t.Errorf("failing = %v, want 0", got)
	}
	if got := testutil.ToFloat64(lm.LastSuccess); got != 1_700_000_000 {
		t.Errorf("last_success_timestamp_seconds = %v, want the clock's time", got)
	}
	if got := testutil.ToFloat64(lm.Overrun); got != 0 {
		t.Errorf("overrun_total = %v, want 0 for a fast run", got)
	}
}

func TestOnceRecordsFailureThenRecovery(t *testing.T) {
	clock := &fakeClock{now: time.Unix(1_700_000_000, 0), elapsed: time.Millisecond}
	r, m := testRunner(t, time.Second, clock)
	lm := m.Loop(LoopSqueue)

	r.once(context.Background(), clock, func(context.Context) error { return errors.New("squeue exploded") })

	if got := testutil.ToFloat64(lm.Failure); got != 1 {
		t.Errorf("failure_total = %v, want 1", got)
	}
	if got := testutil.ToFloat64(lm.Failing); got != 1 {
		t.Errorf("failing = %v, want 1 while the last run failed", got)
	}

	r.once(context.Background(), clock, func(context.Context) error { return nil })

	if got := testutil.ToFloat64(lm.Failing); got != 0 {
		t.Errorf("failing = %v, want it cleared once a run succeeds", got)
	}
	if got := testutil.ToFloat64(lm.Failure); got != 1 {
		t.Errorf("failure_total = %v, want the counter to stay at 1", got)
	}
	if got := testutil.ToFloat64(lm.Success); got != 1 {
		t.Errorf("success_total = %v, want 1", got)
	}
}

// A run that outlasts its interval means the loop was still working when the
// next run came due.
func TestOnceRecordsOverrun(t *testing.T) {
	clock := &fakeClock{now: time.Unix(1_700_000_000, 0), elapsed: 5 * time.Second}
	r, m := testRunner(t, time.Second, clock)

	r.once(context.Background(), clock, func(context.Context) error { return nil })

	lm := m.Loop(LoopSqueue)
	if got := testutil.ToFloat64(lm.Overrun); got != 1 {
		t.Errorf("overrun_total = %v, want 1 for a run longer than the interval", got)
	}
	// An overrun is not itself a failure.
	if got := testutil.ToFloat64(lm.Success); got != 1 {
		t.Errorf("success_total = %v, want the run still counted as successful", got)
	}
}

// During shutdown the work function fails because the context was cancelled.
// That must not flip the failing gauge on the way out and page someone.
func TestOnceIgnoresErrorsCausedByCancellation(t *testing.T) {
	clock := &fakeClock{now: time.Unix(1_700_000_000, 0), elapsed: time.Millisecond}
	r, m := testRunner(t, time.Second, clock)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	r.once(ctx, clock, func(context.Context) error { return context.Canceled })

	lm := m.Loop(LoopSqueue)
	if got := testutil.ToFloat64(lm.Failure); got != 0 {
		t.Errorf("failure_total = %v, want cancellation not counted as a failure", got)
	}
	if got := testutil.ToFloat64(lm.Failing); got != 0 {
		t.Errorf("failing = %v, want cancellation not to raise the failing gauge", got)
	}
}

// Work happens first and the wait comes after, so the syncer has a populated
// snapshot one interval sooner than a wait-first loop would give it.
func TestRunExecutesImmediatelyAndRepeats(t *testing.T) {
	r, _ := testRunner(t, 10*time.Millisecond, RealClock)

	var calls atomic.Int64
	ctx, cancel := context.WithCancel(context.Background())

	done := make(chan struct{})
	go func() {
		defer close(done)
		r.Run(ctx, func(context.Context) error {
			if calls.Add(1) >= 3 {
				cancel()
			}
			return nil
		})
	}()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not return after its context was cancelled")
	}

	if got := calls.Load(); got < 3 {
		t.Fatalf("fn was called %d times, want at least 3", got)
	}
}

// Only one invocation may ever be in flight: the next run cannot start until the
// current one returns.
func TestRunNeverOverlaps(t *testing.T) {
	r, _ := testRunner(t, time.Millisecond, RealClock)

	var (
		inFlight atomic.Int64
		overlaps atomic.Int64
		calls    atomic.Int64
	)
	ctx, cancel := context.WithCancel(context.Background())

	done := make(chan struct{})
	go func() {
		defer close(done)
		r.Run(ctx, func(context.Context) error {
			if inFlight.Add(1) > 1 {
				overlaps.Add(1)
			}
			// Deliberately outlast the interval so ticks pile up behind us.
			time.Sleep(5 * time.Millisecond)
			inFlight.Add(-1)
			if calls.Add(1) >= 5 {
				cancel()
			}
			return nil
		})
	}()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not return after its context was cancelled")
	}

	if got := overlaps.Load(); got != 0 {
		t.Fatalf("detected %d overlapping runs, want 0", got)
	}
}

func TestRunStopsOnCancelledContext(t *testing.T) {
	r, _ := testRunner(t, time.Hour, RealClock)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	done := make(chan struct{})
	go func() {
		defer close(done)
		// The first run still happens; the loop then sees the cancelled context
		// instead of waiting an hour for the next tick.
		r.Run(ctx, func(context.Context) error { return nil })
	}()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not return promptly for an already-cancelled context")
	}
}

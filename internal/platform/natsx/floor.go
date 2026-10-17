package natsx

import (
	"context"
	"errors"
	"sync"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/metric"
)

// EventFloor caches the durable sink's safe history boundary. A mutex coalesces
// concurrent refreshes, including failed refreshes, to at most one per second.
// The timeout is independent of an individual HTTP caller's cancellation.
type EventFloor struct {
	mu      sync.Mutex
	read    func(context.Context) (time.Time, uint64, uint64, error)
	next    time.Time
	created time.Time
	value   uint64
	valid   bool
	err     error
	clamped metric.Int64Counter
}

func NewEventFloor(bus *Bus) (*EventFloor, error) {
	counter, err := otel.Meter("github.com/mindreon/orbit-control/internal/platform/natsx").Int64Counter("history_floor_clamped_total")
	if err != nil {
		return nil, err
	}
	return &EventFloor{clamped: counter, read: func(ctx context.Context) (time.Time, uint64, uint64, error) {
		consumer, err := bus.JS.Consumer(ctx, "RUN_EVENTS", "run-events-sink")
		if err != nil {
			return time.Time{}, 0, 0, err
		}
		info, err := consumer.Info(ctx)
		if err != nil {
			return time.Time{}, 0, 0, err
		}
		stream, err := bus.JS.Stream(ctx, "RUN_EVENTS")
		if err != nil {
			return info.Created, 0, 0, err
		}
		streamInfo, err := stream.Info(ctx)
		if err != nil {
			return info.Created, 0, 0, err
		}
		return info.Created, info.AckFloor.Stream, streamInfo.State.FirstSeq, nil
	}}, nil
}

func (f *EventFloor) Floor(ctx context.Context) (uint64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if !time.Now().Before(f.next) {
		probe, cancel := context.WithTimeout(context.WithoutCancel(ctx), 500*time.Millisecond)
		created, floor, first, err := f.read(probe)
		cancel()
		f.next = time.Now().Add(time.Second)
		// Even if the stream-info request failed, a new consumer invalidates
		// the old generation's floor; that generation's AckFloor is not proof.
		if !created.IsZero() && !created.Equal(f.created) {
			f.created, f.value, f.valid = created, 0, false
		}
		f.err = err
		if err == nil {
			if first > 0 && floor < first-1 {
				floor = first - 1
				f.clamped.Add(ctx, 1)
			}
			f.value, f.valid = max(f.value, floor), true
		}
	}
	if !f.valid {
		if f.err != nil {
			return 0, f.err
		}
		return 0, errors.New("event floor unavailable")
	}
	return f.value, nil
}

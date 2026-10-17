package sink

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/mindreon/orbit-control/internal/platform/db"
	"github.com/mindreon/orbit-control/internal/platform/natsx"
)

type Sink struct {
	consumer jetstream.Consumer
	store    eventStore
	logger   *slog.Logger
	metrics  *Metrics
}

func New(d *db.DB, consumer jetstream.Consumer, logger *slog.Logger, metrics *Metrics) *Sink {
	return &Sink{consumer: consumer, store: postgresStore{db: d}, logger: logger, metrics: metrics}
}

func retryDelay(delivered uint64) time.Duration {
	return natsx.SinkBackOff()[min(delivered, 4)]
}

// Run stops fetching on cancellation, then bounds the in-flight batch to 30s.
// Each subject is serial within the batch; at most eight groups transact at once.
func (s *Sink) Run(ctx context.Context) error {
	work, cancel := context.WithCancel(context.WithoutCancel(ctx))
	defer cancel()
	go func() {
		select {
		case <-ctx.Done():
			select {
			case <-time.After(30 * time.Second):
				cancel()
			case <-work.Done():
			}
		case <-work.Done():
		}
	}()
	for ctx.Err() == nil {
		batch, err := s.consumer.Fetch(256, jetstream.FetchMaxWait(2*time.Second))
		if err != nil {
			if !s.waitRetry(ctx, err) {
				break
			}
			continue
		}
		started := time.Now()
		type waiting struct {
			msg  jetstream.Msg
			stop func()
		}
		groups := map[string][]waiting{}
		for msg := range batch.Messages() {
			groups[msg.Subject()] = append(groups[msg.Subject()], waiting{msg: msg, stop: s.keepAlive(work, msg)})
		}
		if err := batch.Error(); err != nil && !errors.Is(err, context.DeadlineExceeded) {
			s.logger.Warn("sink fetch failed", "error_type", fmt.Sprintf("%T", err))
		}
		sem := make(chan struct{}, 8)
		var wg sync.WaitGroup
		for _, group := range groups {

			select {
			case sem <- struct{}{}:
			case <-work.Done():
				wg.Wait()
				for _, pending := range groups {
					for _, item := range pending {
						item.stop()
					}
				}
				return nil
			}
			wg.Go(func() {
				defer func() { <-sem }()
				for _, item := range group {
					if work.Err() != nil {
						return
					}
					item.stop()
					s.process(work, item.msg)
				}
			})
		}
		wg.Wait()
		for _, pending := range groups {
			for _, item := range pending {
				item.stop()
			}
		}
		s.metrics.batch.Record(ctx, time.Since(started).Seconds())
	}
	return nil
}

func (s *Sink) waitRetry(ctx context.Context, err error) bool {
	s.logger.Warn("sink fetch failed", "error_type", fmt.Sprintf("%T", err))
	select {
	case <-ctx.Done():
		return false
	case <-time.After(time.Second):
		return true
	}
}

// SamplePending runs independently so an idle Fetch never hides current backlog.
func (s *Sink) SamplePending(ctx context.Context) {
	for {
		probe, cancel := context.WithTimeout(ctx, time.Second)
		info, err := s.consumer.Info(probe)
		cancel()
		if err != nil && ctx.Err() == nil {
			s.logger.Warn("sink pending sample failed", "error_type", fmt.Sprintf("%T", err))
		} else if err == nil {
			s.metrics.pending.Record(ctx, int64(info.NumPending))
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(30 * time.Second):
		}
	}
}

func (s *Sink) process(ctx context.Context, msg jetstream.Msg) {
	meta, err := msg.Metadata()
	if err != nil {
		s.logger.Error("sink metadata failed", "error_type", fmt.Sprintf("%T", err))
		s.retry(msg, time.Second)
		return
	}
	stop := s.keepAlive(ctx, msg)
	defer stop()
	s.metrics.lag.Record(ctx, max(0, time.Since(meta.Timestamp).Seconds()))
	var result, reason string
	var attempts uint64
	for {
		// Bound each PG attempt, not the outage. InProgress keeps this delivery
		// alive while the batch stays held and Run pauses further Fetch calls.
		attempt, cancel := context.WithTimeout(ctx, 10*time.Second)
		result, reason, err = s.handle(attempt, msg, meta)
		cancel()
		if err == nil {
			break
		}
		if ctx.Err() != nil {
			return
		} // Leave unacked for another replica.
		s.logger.Warn("sink handle failed", "error_type", fmt.Sprintf("%T", err), "stream_seq", meta.Sequence.Stream)
		count(s.metrics.messages, "result", "retried")
		if !infrastructureError(err) {
			s.retry(msg, retryDelay(meta.NumDelivered))
			return
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(retryDelay(attempts + 1)):
		}
		attempts++
	}
	if reason != "" {
		count(s.metrics.rejected, "reason", reason)
		s.logger.Warn("sink.reject", "reason", reason, "subject", msg.Subject(), "stream_seq", meta.Sequence.Stream)
	}
	count(s.metrics.messages, "result", result)
	if err := msg.Ack(); err != nil {
		s.logger.Warn("sink ack failed", "error_type", fmt.Sprintf("%T", err), "stream_seq", meta.Sequence.Stream)
	}
}

func infrastructureError(err error) bool {
	if e, ok := errors.AsType[*pgconn.PgError](err); ok {
		return len(e.Code) >= 2 && (e.Code[:2] == "08" || e.Code[:2] == "40" || e.Code[:2] == "53" || e.Code[:2] == "57")
	}
	if _, ok := errors.AsType[net.Error](err); ok {
		return true
	}
	return errors.Is(err, context.DeadlineExceeded) || errors.Is(err, io.EOF) ||
		errors.Is(err, io.ErrUnexpectedEOF) || errors.Is(err, pgconn.ErrConnClosed) || pgconn.SafeToRetry(err)
}

func (s *Sink) retry(msg jetstream.Msg, delay time.Duration) {
	if err := msg.NakWithDelay(delay); err != nil {
		s.logger.Warn("sink nak failed", "error_type", fmt.Sprintf("%T", err))
	}
}

func (s *Sink) handle(ctx context.Context, msg jetstream.Msg, meta *jetstream.MsgMetadata) (result, reason string, err error) {
	defer func() {
		if recover() != nil {
			result, reason = "rejected", "panic"
			err = s.reject(ctx, msg, meta, "", reason)
		}
	}()
	if natsx.IsEndSubject(msg.Subject()) {
		count(s.metrics.skipped, "type", "end_marker")
		return "skipped", "", nil
	}
	event, reason := classify(msg.Subject(), meta.Sequence.Stream, msg.Data())
	if reason != "" {
		return "rejected", reason, s.reject(ctx, msg, meta, event.Id, reason)
	}
	if !persistent(event.Type) {
		count(s.metrics.skipped, "type", string(event.Type))
		return "skipped", "", nil
	}
	written, err := s.store.write(ctx, event, meta.Sequence.Stream, msg.Data())
	if err != nil {
		if reason := poisonReason(err); reason != "" {
			return "rejected", reason, s.reject(ctx, msg, meta, event.Id, reason)
		}
		if meta.NumDelivered >= 10 && !infrastructureError(err) {
			return "rejected", "max_deliver", s.reject(ctx, msg, meta, event.Id, "max_deliver")
		}
		return "", "", err
	}
	if !written {
		return "duplicate", "", nil
	}
	return "written", "", nil
}

func (s *Sink) reject(ctx context.Context, msg jetstream.Msg, meta *jetstream.MsgMetadata, id, reason string) error {
	tenant := subjectTenant(msg.Subject())
	if tenant == "" {
		// No trustworthy tenant: do not write a cross-tenant rejection record.
		return nil
	}
	return s.store.reject(ctx, rejection{tenant: tenant, subject: msg.Subject(), eventID: id, reason: reason,
		seq: meta.Sequence.Stream, delivered: meta.NumDelivered, payload: msg.Data()})
}

// Start at fetch time, including time spent waiting for the remainder of a batch.
func (s *Sink) keepAlive(ctx context.Context, msg jetstream.Msg) func() {
	ctx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			select {
			case <-ctx.Done():
				return
			case <-time.After(natsx.SinkBackOff()[0] / 3):
				if err := msg.InProgress(); err != nil {
					s.logger.Warn("sink progress failed", "error_type", fmt.Sprintf("%T", err))
				}
			}
		}
	}()
	return func() {
		cancel()
		<-done
	}
}

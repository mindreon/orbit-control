// Package streamgw serves offline-authorized SSE streams without database access.
package streamgw

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json/jsontext"
	"encoding/json/v2"
	"fmt"
	"log/slog"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/nats-io/nats.go/jetstream"

	"github.com/mindreon/orbit-control/internal/auth"
	"github.com/mindreon/orbit-control/internal/contract"
	"github.com/mindreon/orbit-control/internal/platform/natsx"
)

type Config struct {
	MaxConnections, PerTenant, PerSubject          int
	BufferFrames, BufferBytes                      int
	Heartbeat, WriteTimeout, ShutdownGrace, MaxAge time.Duration
	InfoCache                                      time.Duration
}

type Server struct {
	js        eventSource
	connected func() bool
	stream    string
	verifier  *auth.StreamVerifier
	cfg       Config
	logger    *slog.Logger
	limits    limits
	metrics   metrics
	info      streamInfoCache
	draining  atomic.Bool
	mu        sync.Mutex
	wg        sync.WaitGroup
	ctx       context.Context
	cancel    context.CancelFunc
}

func New(bus *natsx.Bus, verifier *auth.StreamVerifier, cfg Config, logger *slog.Logger) *Server {
	ctx, cancel := context.WithCancel(context.Background())
	return &Server{js: bus.JS, connected: bus.Conn.IsConnected, stream: "RUN_EVENTS", verifier: verifier, cfg: cfg, logger: logger,
		ctx: ctx, cancel: cancel, metrics: metrics{values: map[string]uint64{}},
		limits: limits{tenants: map[string]int{}, subjects: map[string]int{}, scopes: map[string]int{}}}
}

func (s *Server) Router() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /v1/runs/{r}/events", s.events)
	mux.HandleFunc("GET /v1/sessions/{s}/events", s.events)
	mux.HandleFunc("GET /healthz", s.health)
	mux.HandleFunc("GET /metrics", s.serveMetrics)
	return mux
}

// BeginShutdown withdraws readiness before the HTTP listener stops accepting.
func (s *Server) BeginShutdown() {
	s.mu.Lock()
	s.draining.Store(true)
	s.mu.Unlock()
}

// Shutdown preserves existing subscriptions for the grace period, then cancels them.
func (s *Server) Shutdown(ctx context.Context) error {
	s.BeginShutdown()
	select {
	case <-time.After(s.cfg.ShutdownGrace):
	case <-ctx.Done():
	}
	s.cancel()
	done := make(chan struct{})
	go func() {
		s.wg.Wait()
		close(done)
	}()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (s *Server) health(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), time.Second)
	defer cancel()
	ok := !s.draining.Load() && s.connected()
	if ok {
		stream, err := s.js.Stream(ctx, s.stream)
		if err != nil {
			ok = false
		} else if _, err = stream.Info(ctx); err != nil {
			ok = false
		}
	}
	if !ok {
		writeError(w, 503, "STREAM_UNAVAILABLE", "Stream unavailable")
		return
	}
	s.limits.mu.Lock()
	n := s.limits.total
	s.limits.mu.Unlock()
	w.Header().Set("Content-Type", "application/json")
	fmt.Fprintf(w, "{\"status\":\"ok\",\"connections\":%d}", n)
}

func (s *Server) events(w http.ResponseWriter, r *http.Request) {
	claims, ok := s.authorize(w, r)
	if !ok {
		return
	}
	filter, err := contract.ParseEventFilter(r.URL.Query())
	if err != nil {
		writeError(w, 422, "INVALID_FILTER", "Invalid event filter")
		return
	}
	cursor, err := contract.ParseEventCursor(r.Header.Get("Last-Event-ID"))
	cfg, cfgErr := consumerConfig(subject(claims), cursor)
	if err != nil || cfgErr != nil {
		writeError(w, 400, "INVALID_CURSOR", "Invalid event cursor")
		return
	}
	s.mu.Lock()
	if s.draining.Load() {
		s.mu.Unlock()
		writeError(w, 503, "STREAM_UNAVAILABLE", "Stream unavailable")
		return
	}
	s.wg.Add(1)
	s.mu.Unlock()
	defer s.wg.Done()
	release, ok := s.acquire(claims.TenantID, subject(claims), claims.Scope)
	if !ok {
		s.metrics.add("streamgw_rejected_total{reason=\"limit\"}", 1)
		w.Header().Set("Retry-After", "5")
		writeError(w, 429, "TOO_MANY_STREAMS", "Too many concurrent streams")
		return
	}
	defer release()
	deadline := minTime(claims.ExpiresAt.Time, time.Now().Add(s.cfg.MaxAge))
	ctx, cancel := context.WithDeadline(r.Context(), deadline)
	stop := context.AfterFunc(s.ctx, cancel)
	defer stop()
	defer cancel()
	setup, setupCancel := context.WithTimeout(ctx, 5*time.Second)
	defer setupCancel()
	if cursor > 0 {
		first, err := s.firstSequence(setup)
		if err != nil {
			s.unavailable(w, err)
			return
		}
		if cursor+1 < first {
			s.metrics.add("streamgw_cursor_expired_total", 1)
			writeError(w, 409, "EVENT_CURSOR_EXPIRED", "Event cursor has expired; rebuild from history")
			return
		}
		s.metrics.add("streamgw_resumes_total", 1)
	}
	consumer, err := s.js.OrderedConsumer(setup, s.stream, cfg)
	if err != nil {
		s.unavailable(w, err)
		return
	}
	finished := false
	if claims.Scope == "run" && cursor > 0 && consumer.CachedInfo().NumPending == 0 {
		finished, err = s.finishedAt(setup, claims, cursor)
		if err != nil {
			s.unavailable(w, err)
			return
		}
	}
	queue := newFrameQueue(s.cfg.BufferFrames, s.cfg.BufferBytes)
	if finished {
		queue.push(ctx, frame{terminal: true})
	}
	last := cursor
	consume, err := consumer.Consume(func(msg jetstream.Msg) {
		meta, err := msg.Metadata()
		if err != nil {
			s.logger.Error("stream metadata failed", "error_type", fmt.Sprintf("%T", err))
			cancel()
			return
		}
		if meta.Sequence.Stream <= last {
			s.metrics.add("streamgw_order_violation_total", 1)
			cancel()
			return
		}
		last = meta.Sequence.Stream
		raw := msg.Data()
		if len(raw) > 64<<10 || contract.Validate("RunEvent", raw) != nil {
			s.metrics.add("streamgw_invalid_frames_total", 1)
			return
		}
		var event contract.RunEvent
		if err := json.Unmarshal(raw, &event); err != nil || !matches(claims, event, msg.Subject()) {
			s.metrics.add("streamgw_invalid_frames_total", 1)
			return
		}
		// Normalize framing whitespace only for pretty-printed JSON; escaped newlines stay untouched.
		if bytes.ContainsAny(raw, "\r\n") {
			value := jsontext.Value(bytes.Clone(raw))
			if err := value.Compact(); err != nil {
				cancel()
				return
			}
			raw = value
		}
		terminal := claims.Scope == "run" && (event.Type == "run.completed" || event.Type == "run.failed")
		f := frame{seq: last, kind: string(event.Type), data: raw, terminal: terminal}
		if !filter.Allows(f.kind) {
			if !terminal {
				return
			}
			f.data = nil
		}
		queue.push(ctx, f)
	}, jetstream.PullMaxMessages(1), jetstream.ConsumeErrHandler(func(_ jetstream.ConsumeContext, err error) {
		s.logger.Warn("stream consumer failed", "error_type", fmt.Sprintf("%T", err))
		cancel()
	}))
	if err != nil {
		s.unavailable(w, err)
		return
	}
	// Cancel the blocked callback before stopping consumption.
	defer func() {
		cancel()
		consume.Stop()
	}()
	started := time.Now()
	hash := sha256.Sum256([]byte(subject(claims)))
	s.logger.Info("stream.open", "scope", claims.Scope, "tenant", claims.TenantID,
		"subject_hash", fmt.Sprintf("%x", hash[:8]), "resume_from", cursor)
	w.Header().Set("Content-Type", "text/event-stream; charset=utf-8")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("X-Accel-Buffering", "no")
	w.Header().Set("Connection", "keep-alive")
	interrupted := make(chan struct{})
	interrupt := context.AfterFunc(ctx, func() {
		defer close(interrupted)
		if err := http.NewResponseController(w).SetWriteDeadline(time.Now()); err != nil {
			s.logger.Warn("stream write cancellation failed", "error_type", fmt.Sprintf("%T", err))
		}
	})
	defer func() {
		if !interrupt() {
			<-interrupted
		}
	}()
	reason, frames := s.writeEvents(ctx, w, queue)
	s.logger.Info("stream.close", "reason", reason, "frames", frames, "duration", time.Since(started))
}

func matches(c auth.StreamClaims, e contract.RunEvent, subj string) bool {
	return c.TenantID == e.TenantId && c.SessionID == e.SessionId && (c.Scope == "session" || c.RunID == e.RunId) &&
		subj == "run."+e.TenantId+"."+e.SessionId+"."+e.RunId
}

func minTime(a, b time.Time) time.Time {
	if a.Before(b) {
		return a
	}
	return b
}

func (s *Server) unavailable(w http.ResponseWriter, err error) {
	s.logger.Warn("stream unavailable", "error_type", fmt.Sprintf("%T", err))
	writeError(w, 503, "STREAM_UNAVAILABLE", "Stream unavailable")
}

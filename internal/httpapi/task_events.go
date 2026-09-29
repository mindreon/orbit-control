package httpapi

import (
	"encoding/json"
	"errors"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/mindreon/orbit-control/internal/app"
	taskruntime "github.com/mindreon/orbit-control/internal/task"
)

func streamTaskEvents(runtime *app.App) principalHandler {
	return func(w http.ResponseWriter, r *http.Request, p app.Principal) {
		taskID := r.PathValue("taskId")
		after := uint64(0)
		if raw := strings.TrimSpace(r.Header.Get("Last-Event-ID")); raw != "" {
			parsed, err := strconv.ParseUint(raw, 10, 64)
			if err != nil {
				writeErr(w, http.StatusBadRequest, "BAD_REQUEST", "Last-Event-ID must be a decimal sequence")
				return
			}
			after = parsed
		}
		sub, err := runtime.Tasks.Subscribe(r.Context(), taskruntime.Principal{TenantID: p.TenantID, UserID: p.UserID}, taskID, after)
		if err != nil {
			if errors.Is(err, taskruntime.ErrNotFound) {
				writeErr(w, http.StatusNotFound, "NOT_FOUND", "task not found")
				return
			}
			if errors.Is(err, taskruntime.ErrCursor) {
				writeErr(w, http.StatusConflict, "EVENT_CURSOR_EXPIRED", "event cursor cannot be resumed")
				return
			}
			writeErr(w, http.StatusInternalServerError, "INTERNAL", "event stream unavailable")
			return
		}
		defer sub.Close()
		flusher, ok := w.(http.Flusher)
		if !ok {
			writeErr(w, http.StatusInternalServerError, "SSE_UNSUPPORTED", "streaming unsupported")
			return
		}
		w.Header().Set("Content-Type", "text/event-stream")
		w.Header().Set("Cache-Control", "no-cache")
		w.Header().Set("Connection", "keep-alive")
		w.Header().Set("X-Accel-Buffering", "no")
		w.WriteHeader(http.StatusOK)
		// Durable events are pushed once, in order. The Projector may run on another replica than the one this client is
		// attached to, so the loop below also reads new rows itself (09 §4.1) instead of trusting only the local fan-out.
		principal := taskruntime.Principal{TenantID: p.TenantID, UserID: p.UserID}
		sender := &eventSender{
			lastSent: after,
			fetch: func(cursor uint64) ([]taskruntime.Event, error) {
				items, _, err := runtime.Tasks.EventsAfter(r.Context(), principal, taskID, cursor)
				return items, err
			},
			emit: func(event taskruntime.Event) error {
				body, err := json.Marshal(event)
				if err != nil {
					return err
				}
				// Only durable events carry an id: the browser resumes from the last one, so an ephemeral event must not move it.
				frame := "data: " + string(body) + "\n\n"
				if event.Seq > 0 {
					frame = "id: " + strconv.FormatUint(event.Seq, 10) + "\n" + frame
				}
				if _, err := w.Write([]byte(frame)); err != nil {
					return err
				}
				flusher.Flush()
				return nil
			},
		}
		heartbeat := time.NewTicker(15 * time.Second)
		defer heartbeat.Stop()
		catchUpTick := time.NewTicker(time.Second)
		defer catchUpTick.Stop()
		for {
			select {
			case <-r.Context().Done():
				return
			case event, ok := <-sub.Events:
				if !ok {
					return
				}
				if err := sender.deliver(event); err != nil {
					return
				}
			case <-sub.Closed:
				return
			case <-catchUpTick.C:
				if err := sender.catchUp(); err != nil {
					return
				}
			case <-heartbeat.C:
				if _, err := w.Write([]byte(": heartbeat\n\n")); err != nil {
					return
				}
				flusher.Flush()
			}
		}
	}
}

// eventSender writes task events to one SSE client. Durable events are pushed once and in order: the Projector may run
// on another replica than the one this client is attached to, so the stream also reads new rows itself (09 §4.1)
// instead of trusting only the local fan-out.
type eventSender struct {
	lastSent uint64
	fetch    func(after uint64) ([]taskruntime.Event, error)
	emit     func(taskruntime.Event) error
}

// write sends one event. A durable event at or before lastSent is a duplicate and is dropped; an ephemeral event
// (Seq 0) is always sent and never moves the cursor.
func (s *eventSender) write(event taskruntime.Event) error {
	if event.Seq > 0 {
		if event.Seq <= s.lastSent {
			return nil
		}
		s.lastSent = event.Seq
	}
	return s.emit(event)
}

// catchUp writes the durable events after the last one sent, in order, from the log. A read error is not fatal: the
// next tick tries again. Ephemeral events in the result are skipped, they are never replayed mid-stream.
func (s *eventSender) catchUp() error {
	items, err := s.fetch(s.lastSent)
	if err != nil {
		return nil
	}
	for _, event := range items {
		if event.Seq == 0 {
			continue
		}
		if err := s.write(event); err != nil {
			return err
		}
	}
	return nil
}

// deliver writes a live event, unless durable events before it were not sent yet: then the log is read first, so a
// client never sees seq 9 without 5 to 8. The live event itself is in the log, so catchUp covers it.
func (s *eventSender) deliver(event taskruntime.Event) error {
	if event.Seq > s.lastSent+1 {
		return s.catchUp()
	}
	return s.write(event)
}

package httpapi

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/mindreon/orbit-control/internal/app"
)

var heartbeatInterval = 15 * time.Second

// Test seams for interleaving publishes with the replay-to-live handoff.
var (
	afterSubscribeHook   func()
	afterReplayFrameHook func(seq uint64)
)

// StreamReset tells the client its resume cursor cannot be honoured and it
// must refetch room state (room, messages, activity) before trusting the stream.
type StreamReset struct {
	Type       string          `json:"type"`
	RoomID     string          `json:"roomId"`
	Reason     app.ResetReason `json:"reason"`
	Sequence   uint64          `json:"sequence"`
	OccurredAt string          `json:"occurredAt"`
}

// authorizeRoomStream is the §17 (C31) hook. Authentication (401) and room
// authorization (404) must be decided here: before the room lookup, before
// any text/event-stream header is written, and before any replay.
// W1 has no user auth, so every caller is allowed.
func authorizeRoomStream(w http.ResponseWriter, r *http.Request, roomID string) bool {
	return true
}

// lastEventID prefers the header: on EventSource auto-reconnect the browser
// sends the newest id there while the URL still carries the original query.
func lastEventID(r *http.Request) (string, bool) {
	if raw := strings.TrimSpace(r.Header.Get("Last-Event-ID")); raw != "" {
		return raw, true
	}
	if raw := strings.TrimSpace(r.URL.Query().Get("lastEventId")); raw != "" {
		return raw, true
	}
	return "", false
}

func streamRoomEvents(runtime *app.App) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		roomID := r.PathValue("roomId")
		if !authorizeRoomStream(w, r, roomID) {
			return
		}
		flusher, ok := w.(http.Flusher)
		if !ok {
			writeErr(w, http.StatusInternalServerError, "SSE_UNSUPPORTED", "streaming unsupported")
			return
		}
		// Subscribe before reading history: anything published from here on is
		// buffered, and the cursor below drops what replay already delivered.
		sub, ok := runtime.SubscribeRoom(roomID)
		if !ok {
			writeErr(w, http.StatusNotFound, "NOT_FOUND", "room not found")
			return
		}
		defer sub.Close()
		if afterSubscribeHook != nil {
			afterSubscribeHook()
		}

		h := w.Header()
		h.Set("Content-Type", "text/event-stream")
		h.Set("Cache-Control", "no-cache")
		h.Set("Connection", "keep-alive")
		h.Set("X-Accel-Buffering", "no")
		w.WriteHeader(http.StatusOK)
		s := &sseStream{w: w, flusher: flusher, runtime: runtime, roomID: roomID, cursor: sub.Head}
		if err := s.comment("connected"); err != nil {
			return
		}

		if raw, ok := lastEventID(r); ok {
			seq, err := app.ParseEventID(roomID, raw)
			if err != nil {
				err = s.resetFor(err, sub.Head)
			} else {
				s.cursor = seq
				err = s.catchUp()
			}
			if err != nil {
				return
			}
		}

		heartbeat := time.NewTicker(heartbeatInterval)
		defer heartbeat.Stop()
		for {
			var err error
			select {
			case <-r.Context().Done():
				return
			case frame := <-sub.Frames:
				err = s.deliver(frame)
			case <-sub.Lagged:
				err = s.catchUp()
			case <-heartbeat.C:
				err = s.comment("heartbeat")
			}
			if err != nil {
				return
			}
		}
	}
}

// sseStream writes one connection's frames. cursor is the sequence of the last
// durable event delivered (or skipped by a reset); every durable frame written
// has sequence cursor+1, which is what makes the stream gap- and dup-free.
type sseStream struct {
	w       io.Writer
	flusher http.Flusher
	runtime *app.App
	roomID  string
	cursor  uint64
}

func (s *sseStream) comment(text string) error {
	if _, err := io.WriteString(s.w, ": "+text+"\n\n"); err != nil {
		return err
	}
	s.flusher.Flush()
	return nil
}

func (s *sseStream) write(seq uint64, data []byte) error {
	if _, err := io.WriteString(s.w, "id: "+app.FormatEventID(s.roomID, seq)+"\ndata: "+string(data)+"\n\n"); err != nil {
		return err
	}
	s.flusher.Flush()
	return nil
}

// catchUp replays retained history after the cursor. It runs on resume and
// whenever the live buffer may have dropped frames.
func (s *sseStream) catchUp() error {
	frames, head, err := s.runtime.EventsAfter(s.roomID, s.cursor)
	if err != nil {
		return s.resetFor(err, head)
	}
	for _, f := range frames {
		if err := s.write(f.Seq, f.Data); err != nil {
			return err
		}
		s.cursor = f.Seq
		if afterReplayFrameHook != nil {
			afterReplayFrameHook(f.Seq)
		}
	}
	return nil
}

func (s *sseStream) deliver(f app.StreamFrame) error {
	if f.Durable {
		switch {
		case f.Seq <= s.cursor:
			return nil
		case f.Seq == s.cursor+1:
			s.cursor = f.Seq
			return s.write(f.Seq, f.Data)
		default:
			// Buffer overflowed earlier; history still holds f and its predecessors.
			return s.catchUp()
		}
	}
	if f.Seq > s.cursor {
		if err := s.catchUp(); err != nil {
			return err
		}
	}
	if f.Seq != s.cursor {
		// Durable events after this draft were already delivered.
		return nil
	}
	return s.write(s.cursor, f.Data)
}

func (s *sseStream) resetFor(err error, head uint64) error {
	var cursorErr *app.CursorError
	if !errors.As(err, &cursorErr) {
		return err
	}
	raw, err := json.Marshal(StreamReset{
		Type:       "reset",
		RoomID:     s.roomID,
		Reason:     cursorErr.Reason,
		Sequence:   head,
		OccurredAt: time.Now().UTC().Format(time.RFC3339Nano),
	})
	if err != nil {
		return err
	}
	s.cursor = head
	return s.write(head, raw)
}

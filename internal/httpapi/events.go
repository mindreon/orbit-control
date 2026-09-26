package httpapi

import (
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/mindreon/orbit-control/internal/app"
)

const heartbeatInterval = 15 * time.Second

// ResetPayload is the payload of a `reset` envelope: the resume cursor cannot
// be honoured and the client must refetch room state (room, messages,
// activity) before trusting the stream, which continues after LastID.
type ResetPayload struct {
	Reason app.ResetReason `json:"reason"`
	LastID uint64          `json:"lastId"`
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

// clientKey identifies a caller for Limits.MaxStreamsPerClient: the peer IP.
// Proxy headers are not trusted; behind a proxy every client shares its IP.
func clientKey(r *http.Request) string {
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		return r.RemoteAddr
	}
	return host
}

// storeRetry is the reconnect delay sent before a stream closes on an event
// store error, so clients back off instead of reconnecting at once.
const storeRetry = 10 * time.Second

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
		// Handoff: subscribe (buffer) → read history → drain buffer, skipping
		// ids <= last replayed. This is only correct within one control
		// instance, because the buffer is this process's in-memory fan-out.
		// P0 supports a single control instance only; for multiple replicas,
		// live fan-out moves to Postgres LISTEN/NOTIFY or NATS, while replay
		// logic stays unchanged.
		sub, err := runtime.SubscribeRoom(roomID, clientKey(r))
		switch {
		case errors.Is(err, app.ErrRoomNotFound):
			writeErr(w, http.StatusNotFound, "NOT_FOUND", "room not found")
			return
		case errors.Is(err, app.ErrRoomStreamLimit):
			writeErr(w, http.StatusTooManyRequests, "STREAM_LIMIT_ROOM", err.Error())
			return
		case errors.Is(err, app.ErrClientStreamLimit):
			writeErr(w, http.StatusTooManyRequests, "STREAM_LIMIT_CLIENT", err.Error())
			return
		case err != nil:
			writeErr(w, http.StatusInternalServerError, "EVENT_STORE_ERROR", "event store unavailable")
			return
		}
		defer sub.Close()
		h := w.Header()
		h.Set("Content-Type", "text/event-stream")
		h.Set("Cache-Control", "no-cache")
		h.Set("Connection", "keep-alive")
		h.Set("X-Accel-Buffering", "no")
		w.WriteHeader(http.StatusOK)
		s := &sseStream{
			w: w, flusher: flusher, rc: http.NewResponseController(w),
			writeTimeout: runtime.Limits.SSEWriteTimeout,
			runtime:      runtime, roomID: roomID, cursor: sub.Head,
		}
		if err := s.comment("connected"); err != nil {
			return
		}

		if raw, ok := lastEventID(r); ok {
			seq, err := app.ParseEventID(raw)
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
		lags := 0
		// onLag re-reads history, or gives up on a reader that keeps falling
		// behind: it sends reset "lagging" and the stream closes.
		onLag := func() error {
			if lags++; lags > runtime.Limits.MaxConsecutiveLags {
				head, err := runtime.RoomHead(roomID)
				if err == nil {
					err = s.resetFor(&app.CursorError{Reason: app.ResetLagging}, head)
				}
				if err == nil {
					err = errStreamDone
				}
				return err
			}
			return s.catchUp()
		}
		for {
			var err error
			select {
			case <-r.Context().Done():
				return
			case frame := <-sub.Frames:
				// A drop is signalled before any later frame is buffered, so
				// checking here keeps a newer frame from jumping a lost one.
				select {
				case <-sub.Lagged:
					err = onLag()
				default:
					lags = 0
				}
				if err == nil {
					err = s.deliver(frame)
				}
			case <-sub.Lagged:
				err = onLag()
			case <-sub.Closed:
				// The room closed: deliver what is buffered, then end the stream.
				for err == nil {
					select {
					case frame := <-sub.Frames:
						err = s.deliver(frame)
					default:
						err = errStreamDone
					}
				}
			case <-heartbeat.C:
				err = s.comment("heartbeat")
			}
			if err != nil {
				return
			}
		}
	}
}

var errStreamDone = errors.New("stream done")

// sseStream writes one connection's frames. cursor is the global id of the
// last durable event of this room delivered (or skipped by a reset). Ids are
// global, so a room's ids are increasing but not contiguous.
type sseStream struct {
	w            io.Writer
	flusher      http.Flusher
	rc           *http.ResponseController
	writeTimeout time.Duration
	runtime      *app.App
	roomID       string
	cursor       uint64
}

func (s *sseStream) send(text string) error {
	if s.writeTimeout > 0 {
		_ = s.rc.SetWriteDeadline(time.Now().Add(s.writeTimeout))
	}
	if _, err := io.WriteString(s.w, text); err != nil {
		return err
	}
	s.flusher.Flush()
	return nil
}

func (s *sseStream) comment(text string) error {
	return s.send(": " + text + "\n\n")
}

func (s *sseStream) write(seq uint64, data []byte) error {
	return s.send("id: " + app.FormatEventID(seq) + "\ndata: " + string(data) + "\n\n")
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
	}
	return nil
}

func (s *sseStream) deliver(f app.StreamFrame) error {
	if f.Durable {
		if f.Seq <= s.cursor {
			return nil
		}
		s.cursor = f.Seq
		return s.write(f.Seq, f.Data)
	}
	if f.Seq < s.cursor {
		// Durable events after this draft were already delivered.
		return nil
	}
	return s.write(s.cursor, f.Data)
}

// resetFor sends a reset for a cursor error. Any other error is an event store
// failure: the stream sends a retry delay and closes, so the client backs off.
func (s *sseStream) resetFor(err error, head uint64) error {
	var cursorErr *app.CursorError
	if !errors.As(err, &cursorErr) {
		_ = s.send("retry: " + strconv.FormatInt(storeRetry.Milliseconds(), 10) + "\n: event store unavailable\n\n")
		return err
	}
	payload, err := json.Marshal(ResetPayload{Reason: cursorErr.Reason, LastID: head})
	if err != nil {
		return err
	}
	raw, err := json.Marshal(app.Envelope{
		Type:    "reset",
		TaskID:  s.roomID,
		TS:      time.Now().UTC().Format(time.RFC3339Nano),
		Source:  "control",
		Payload: payload,
	})
	if err != nil {
		return err
	}
	s.cursor = head
	return s.write(head, raw)
}

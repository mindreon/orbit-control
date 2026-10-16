package streamgw

import (
	"context"
	"fmt"
	"net/http"
	"sync"
	"time"
)

type frame struct {
	seq      uint64
	kind     string
	data     []byte
	terminal bool
}

type frameQueue struct {
	frames chan frame
	mu     sync.Mutex
	bytes  int
	max    int
	space  chan struct{}
}

func newFrameQueue(frames, bytes int) *frameQueue {
	return &frameQueue{frames: make(chan frame, frames), max: bytes, space: make(chan struct{}, 1)}
}

func (q *frameQueue) push(ctx context.Context, f frame) bool {
	for {
		q.mu.Lock()
		if q.bytes+len(f.data) <= q.max {
			q.bytes += len(f.data)
			q.mu.Unlock()
			break
		}
		q.mu.Unlock()
		select {
		case <-ctx.Done():
			return false
		case <-q.space:
		}
	}
	select {
	case q.frames <- f:
		return true
	case <-ctx.Done():
		q.release(f)
		return false
	}
}

func (q *frameQueue) release(f frame) {
	q.mu.Lock()
	q.bytes -= len(f.data)
	q.mu.Unlock()
	select {
	case q.space <- struct{}{}:
	default:
	}
}

func writeFrame(w http.ResponseWriter, timeout time.Duration, raw []byte) error {
	controller := http.NewResponseController(w)
	if err := controller.SetWriteDeadline(time.Now().Add(timeout)); err != nil {
		return err
	}
	if _, err := w.Write(raw); err != nil {
		return err
	}
	return controller.Flush()
}

func encodeFrame(f frame) []byte {
	return fmt.Appendf(nil, "id: %d\nevent: %s\ndata: %s\n\n", f.seq, f.kind, f.data)
}

func (s *Server) writeEvents(ctx context.Context, w http.ResponseWriter, q *frameQueue) (string, int) {
	if err := writeFrame(w, s.cfg.WriteTimeout, []byte("retry: 1000\n\n")); err != nil {
		return s.writeFailure(err), 0
	}
	idle := time.NewTimer(s.cfg.Heartbeat)
	defer idle.Stop()
	frames := 0
	for {
		select {
		case <-ctx.Done():
			return "closed", frames
		case <-idle.C:
			if err := writeFrame(w, s.cfg.WriteTimeout, []byte(": ping\n\n")); err != nil {
				return s.writeFailure(err), frames
			}
			idle.Reset(s.cfg.Heartbeat)
		case f := <-q.frames:
			if len(f.data) > 0 {
				err := writeFrame(w, s.cfg.WriteTimeout, encodeFrame(f))
				q.release(f)
				if err != nil {
					return s.writeFailure(err), frames
				}
				frames++
				s.metrics.add(fmt.Sprintf("streamgw_frames_sent_total{type=%q}", f.kind), 1)
			}
			if f.terminal {
				if err := writeFrame(w, s.cfg.WriteTimeout, []byte("event: end\ndata: {\"reason\":\"run_finished\"}\n\n")); err != nil {
					return s.writeFailure(err), frames
				}
				select {
				case <-ctx.Done():
				case <-time.After(500 * time.Millisecond):
				}
				return "run_finished", frames
			}
			idle.Reset(s.cfg.Heartbeat)
		}
	}
}

func (s *Server) writeFailure(err error) string {
	if e, ok := err.(interface{ Timeout() bool }); ok && e.Timeout() {
		s.metrics.add("streamgw_slow_client_closed_total", 1)
		return "write_timeout"
	}
	return "write_error"
}

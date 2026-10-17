package streamgw

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/require"

	"github.com/mindreon/orbit-control/internal/platform/ids"
)

type e35Msg struct {
	jetstream.Msg
	subject      string
	raw          []byte
	seq, pending uint64
	onData       func()
}

func (m e35Msg) Subject() string { return m.subject }
func (m e35Msg) Data() []byte {
	if m.onData != nil {
		m.onData()
	}
	return m.raw
}
func (m e35Msg) Metadata() (*jetstream.MsgMetadata, error) {
	return &jetstream.MsgMetadata{Sequence: jetstream.SequencePair{Stream: m.seq}, NumPending: m.pending}, nil
}

type e35Batch struct {
	jetstream.MessageBatch
	messages chan jetstream.Msg
}

func (b e35Batch) Messages() <-chan jetstream.Msg { return b.messages }
func (b e35Batch) Error() error                   { return nil }

type e35Consume struct{ jetstream.ConsumeContext }

func (e35Consume) Stop() {}

type e35Consumer struct {
	jetstream.Consumer
	pending  uint64
	messages []jetstream.Msg
	opts     *[]jetstream.PullConsumeOpt
}

func (c e35Consumer) CachedInfo() *jetstream.ConsumerInfo {
	return &jetstream.ConsumerInfo{NumPending: c.pending}
}
func (c e35Consumer) Consume(fn jetstream.MessageHandler, opts ...jetstream.PullConsumeOpt) (jetstream.ConsumeContext, error) {
	if c.opts != nil {
		*c.opts = opts
	}
	for _, m := range c.messages {
		fn(m)
	}
	return e35Consume{}, nil
}
func (c e35Consumer) FetchNoWait(_ int) (jetstream.MessageBatch, error) {
	ch := make(chan jetstream.Msg, len(c.messages))
	for _, m := range c.messages {
		ch <- m
	}
	close(ch)
	return e35Batch{messages: ch}, nil
}

type e35Stream struct{ jetstream.Stream }

func (e35Stream) Info(context.Context, ...jetstream.StreamInfoOpt) (*jetstream.StreamInfo, error) {
	return &jetstream.StreamInfo{State: jetstream.StreamState{FirstSeq: 1}}, nil
}

type e35Source struct {
	main, marker e35Consumer
	markerErr    error
	order        *[]string
}

func (e35Source) Stream(context.Context, string) (jetstream.Stream, error) { return e35Stream{}, nil }
func (s e35Source) OrderedConsumer(_ context.Context, _ string, c jetstream.OrderedConsumerConfig) (jetstream.Consumer, error) {
	which := "main"
	if strings.HasSuffix(c.FilterSubjects[0], ".end") {
		which = "marker"
	}
	if s.order != nil {
		*s.order = append(*s.order, which)
	}
	if which == "marker" {
		return s.marker, s.markerErr
	}
	return s.main, nil
}

func TestE35GatewayEndedCaughtUpFIFO(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		modes := []string{"terminal-late", "filtered-tail", "invalid-tail", "marker-pending", "marker-empty", "not-ended", "not-caught-up"}
		for _, mode := range modes {
			// Each state is deliberately deterministic under synctest.
			func() {

				s, c, key, _ := testServer(t)
				s.cfg.BufferFrames = 16
				s.cfg.MaxAge = 30 * time.Millisecond
				s.cfg.Heartbeat = 5 * time.Millisecond
				msg := func(seq, pending uint64, kind string) jetstream.Msg {
					return e35Msg{subject: subject(c), seq: seq, pending: pending, raw: eventBytes(t, c, int(seq), kind)}
				}
				var afterEnd atomic.Bool
				lateAfterEnd := e35Msg{subject: subject(c), seq: 3, raw: eventBytes(t, c, 3, "run.status"), onData: func() { afterEnd.Store(true) }}
				messages := []jetstream.Msg{msg(1, 1, "run.completed"), msg(2, 0, "run.status"), lateAfterEnd}
				source := e35Source{main: e35Consumer{pending: 2, messages: messages}}
				cursor, query := "0", ""
				switch mode {
				case "filtered-tail":
					query = "?types=run.completed"
				case "invalid-tail":
					source.main.messages[1] = e35Msg{subject: subject(c), seq: 2, raw: []byte(`{}`)}
				case "marker-pending":
					cursor = "1"
					source.marker.messages = []jetstream.Msg{e35Msg{seq: 4}}
					source.main.messages = []jetstream.Msg{msg(2, 0, "run.status")}
				case "marker-empty":
					cursor = "2"
					source.marker.messages = []jetstream.Msg{e35Msg{seq: 4}}
					source.main = e35Consumer{}
				case "not-caught-up":
					source.main.messages = []jetstream.Msg{msg(1, 2, "run.completed")}
				case "not-ended":
					source.main.messages = []jetstream.Msg{msg(1, 0, "run.status")}
				}
				s.js = source
				r := httptest.NewRequest("GET", "/v1/runs/"+c.RunID+"/events"+query, nil)
				r.Header.Set("Authorization", "Bearer "+signed(t, c, key, "test"))
				r.Header.Set("Last-Event-ID", cursor)
				w := &deadlineWriter{headers: http.Header{}}
				s.Router().ServeHTTP(w, r)
				body := w.data.String()
				if mode == "not-ended" || mode == "not-caught-up" {
					require.NotContains(t, body, "event: end")
					return
				}
				require.Equal(t, 1, strings.Count(body, "event: end"), body)
				require.NotContains(t, body, "id: 3\n", "nothing may be queued after end")
				require.False(t, afterEnd.Load(), "callbacks after end are ignored before frame validation or queuing")
				if mode == "marker-pending" {
					require.Contains(t, body, "id: 2\n", "marker presence cannot discard pending late frames")
				}
				if mode == "terminal-late" {
					require.Less(t, strings.Index(body, "id: 1\n"), strings.Index(body, "id: 2\n"))
					require.Less(t, strings.Index(body, "id: 2\n"), strings.Index(body, "event: end"))
				}
			}()
		}
	})
}

func TestE35GatewayMarkerLookupFailure(t *testing.T) {
	s, c, key, _ := testServer(t)
	s.js = e35Source{markerErr: errors.New("marker lookup unavailable")}
	r := httptest.NewRequest("GET", "/v1/runs/"+c.RunID+"/events", nil)
	r.Header.Set("Authorization", "Bearer "+signed(t, c, key, "test"))
	r.Header.Set("Last-Event-ID", "1")
	out := httptest.NewRecorder()
	s.Router().ServeHTTP(out, r)
	require.Equal(t, 503, out.Code)
	require.Contains(t, out.Body.String(), "STREAM_UNAVAILABLE")
}

func TestE35SessionMarkerNotInvalid(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		s, c, key, _ := testServer(t)
		c.Scope, c.RunID, c.Subject = "session", "", "session:"+c.SessionID
		s.cfg.MaxAge = 30 * time.Millisecond
		s.js = e35Source{main: e35Consumer{messages: []jetstream.Msg{e35Msg{
			subject: "run." + c.TenantID + "." + c.SessionID + "." + ids.New() + ".end", seq: 1, raw: []byte(`{}`),
		}}}}
		r := httptest.NewRequest("GET", "/v1/sessions/"+c.SessionID+"/events", nil)
		r.Header.Set("Authorization", "Bearer "+signed(t, c, key, "test"))
		w := &deadlineWriter{headers: http.Header{}}
		s.Router().ServeHTTP(w, r)
		require.NotContains(t, w.data.String(), "event:")
		require.Zero(t, metricCount(t, s, "streamgw_invalid_frames_total"))
	})
}

func TestE35GatewayPullsBufferSizedBatches(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		s, c, key, _ := testServer(t)
		s.cfg.BufferFrames, s.cfg.BufferBytes = 37, 4096
		s.cfg.MaxAge = 30 * time.Millisecond
		var opts []jetstream.PullConsumeOpt
		s.js = e35Source{main: e35Consumer{opts: &opts}}
		r := httptest.NewRequest("GET", "/v1/runs/"+c.RunID+"/events", nil)
		r.Header.Set("Authorization", "Bearer "+signed(t, c, key, "test"))
		s.Router().ServeHTTP(&deadlineWriter{headers: http.Header{}}, r)
		require.Contains(t, opts, jetstream.PullConsumeOpt(jetstream.PullMaxMessagesWithBytesLimit(37, 4096)))
	})
}

func TestE35MarkerLookupPrecedesMainConsumer(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		s, c, key, _ := testServer(t)
		s.cfg.MaxAge = 30 * time.Millisecond
		var order []string
		terminal := e35Msg{subject: subject(c), seq: 6, raw: eventBytes(t, c, 6, "run.completed")}
		s.js = e35Source{order: &order,
			marker: e35Consumer{messages: []jetstream.Msg{e35Msg{seq: 7}}},
			main:   e35Consumer{pending: 1, messages: []jetstream.Msg{terminal}}}
		r := httptest.NewRequest("GET", "/v1/runs/"+c.RunID+"/events", nil)
		r.Header.Set("Authorization", "Bearer "+signed(t, c, key, "test"))
		r.Header.Set("Last-Event-ID", "5")
		w := &deadlineWriter{headers: http.Header{}}
		s.Router().ServeHTTP(w, r)
		body := w.data.String()
		require.Equal(t, []string{"marker", "main"}, order)
		require.Greater(t, strings.Index(body, "id: 6\n"), -1)
		require.Less(t, strings.Index(body, "id: 6\n"), strings.Index(body, "event: end"))
	})
}

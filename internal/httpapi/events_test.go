package httpapi

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mindreon/orbit-control/internal/app"
	"github.com/mindreon/orbit-control/internal/store"
	"github.com/mindreon/orbit-control/internal/worker"
)

type sseFrame struct {
	ID      string
	HasID   bool
	Data    string
	Comment string
}

type sseData struct {
	Type     string `json:"type"`
	RoomID   string `json:"roomId"`
	Sequence uint64 `json:"sequence"`
	Text     string `json:"text"`
	Reason   string `json:"reason"`
	Delta    string `json:"delta"`
}

func (f sseFrame) decode(t *testing.T) sseData {
	t.Helper()
	var d sseData
	if err := json.Unmarshal([]byte(f.Data), &d); err != nil {
		t.Fatalf("frame data %q: %v", f.Data, err)
	}
	return d
}

type sseConn struct {
	resp   *http.Response
	frames chan sseFrame
}

type eventsEnv struct {
	runtime *app.App
	srv     *httptest.Server
}

func newEventsEnv(t *testing.T) *eventsEnv {
	t.Helper()
	var sessions atomic.Int64
	fake := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = fmt.Fprintf(w, `{"sessionId":"sess-%d"}`, sessions.Add(1))
	}))
	t.Cleanup(fake.Close)
	runtime := app.New(worker.New(fake.URL))
	runtime.Store = store.New(t.TempDir())
	srv := httptest.NewServer(HandlerWith(runtime))
	t.Cleanup(srv.Close)
	return &eventsEnv{runtime: runtime, srv: srv}
}

func (e *eventsEnv) createRoom(t *testing.T) *app.Room {
	t.Helper()
	resp, err := http.Post(e.srv.URL+"/v1/rooms", "application/json", strings.NewReader(`{"kind":"solo"}`))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var room app.Room
	if err := json.NewDecoder(resp.Body).Decode(&room); err != nil {
		t.Fatal(err)
	}
	return &room
}

func (e *eventsEnv) publish(roomID, text string) {
	e.runtime.Publish(roomID, app.Event{"type": "room.steered", "roomId": roomID, "text": text})
}

func (e *eventsEnv) head(roomID string) uint64 {
	items := e.runtime.ListActivity(roomID)
	if len(items) == 0 {
		return 0
	}
	return items[len(items)-1].Sequence
}

func (e *eventsEnv) connect(t *testing.T, roomID string, header, query string) *sseConn {
	t.Helper()
	u := e.srv.URL + "/v1/rooms/" + roomID + "/events"
	if query != "" {
		u += "?lastEventId=" + url.QueryEscape(query)
	}
	ctx, cancel := context.WithCancel(context.Background())
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		t.Fatal(err)
	}
	if header != "" {
		req.Header.Set("Last-Event-ID", header)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		cancel()
		t.Fatal(err)
	}
	c := &sseConn{resp: resp, frames: make(chan sseFrame, 1024)}
	go readFrames(resp.Body, c.frames)
	t.Cleanup(func() {
		cancel()
		resp.Body.Close()
	})
	return c
}

func readFrames(body io.Reader, out chan<- sseFrame) {
	defer close(out)
	br := bufio.NewReader(body)
	var f sseFrame
	for {
		line, err := br.ReadString('\n')
		if err != nil {
			return
		}
		line = strings.TrimSuffix(line, "\n")
		switch {
		case line == "":
			out <- f
			f = sseFrame{}
		case strings.HasPrefix(line, ":"):
			f.Comment = strings.TrimSpace(line[1:])
		case strings.HasPrefix(line, "id: "):
			f.ID, f.HasID = line[len("id: "):], true
		case strings.HasPrefix(line, "data: "):
			f.Data = line[len("data: "):]
		}
	}
}

// next returns the next message frame, skipping comment-only frames.
func (c *sseConn) next(t *testing.T) sseFrame {
	t.Helper()
	f, err := c.nextFrame()
	if err != nil {
		t.Fatal(err)
	}
	return f
}

// nextFrame is next without testing.T, for use off the test goroutine.
func (c *sseConn) nextFrame() (sseFrame, error) {
	timeout := time.After(5 * time.Second)
	for {
		select {
		case f, ok := <-c.frames:
			if !ok {
				return sseFrame{}, fmt.Errorf("stream closed")
			}
			if f.Data == "" && f.Comment != "" {
				continue
			}
			return f, nil
		case <-timeout:
			return sseFrame{}, fmt.Errorf("timed out waiting for SSE frame")
		}
	}
}

// expectSeqs asserts the next durable frames are exactly want, in order.
func (c *sseConn) expectSeqs(t *testing.T, roomID string, want ...uint64) {
	t.Helper()
	for _, seq := range want {
		f := c.next(t)
		d := f.decode(t)
		if d.Sequence != seq || d.RoomID != roomID || f.ID != app.FormatEventID(roomID, seq) {
			t.Fatalf("got frame id=%q room=%s seq=%d type=%s, want %s", f.ID, d.RoomID, d.Sequence, d.Type, app.FormatEventID(roomID, seq))
		}
	}
}

func seqRange(from, to uint64) []uint64 {
	var out []uint64
	for s := from; s <= to; s++ {
		out = append(out, s)
	}
	return out
}

func setHooks(t *testing.T, afterSubscribe func(), afterReplayFrame func(uint64)) {
	t.Helper()
	afterSubscribeHook, afterReplayFrameHook = afterSubscribe, afterReplayFrame
	t.Cleanup(func() { afterSubscribeHook, afterReplayFrameHook = nil, nil })
}

// Acceptance 1: replay only this room's events after Last-Event-ID, in order,
// then live, with events emitted during the handoff neither lost nor repeated.
func TestEventsResumeReplaysThenGoesLiveWithoutGapsOrDuplicates(t *testing.T) {
	env := newEventsEnv(t)
	a := env.createRoom(t)
	b := env.createRoom(t)
	for i := 0; i < 5; i++ {
		env.publish(a.ID, "before")
	}
	resumeFrom := env.head(a.ID) - 3

	var once sync.Once
	setHooks(t,
		func() {
			// Lands both in the live buffer and in history: must not repeat.
			env.publish(a.ID, "during-subscribe-1")
			env.publish(a.ID, "during-subscribe-2")
			env.publish(b.ID, "other-room")
		},
		func(uint64) {
			// History is already snapshotted: only the buffer can deliver these.
			once.Do(func() {
				env.publish(a.ID, "during-replay-1")
				env.publish(a.ID, "during-replay-2")
				env.publish(b.ID, "other-room")
			})
		},
	)

	// Header wins over a stale query cursor.
	c := env.connect(t, a.ID, app.FormatEventID(a.ID, resumeFrom), "garbage")
	head := resumeFrom + 3 + 4
	c.expectSeqs(t, a.ID, seqRange(resumeFrom+1, head)...)
	if got := env.head(a.ID); got != head {
		t.Fatalf("head = %d, want %d", got, head)
	}

	env.publish(a.ID, "live")
	c.expectSeqs(t, a.ID, head+1)

	t.Run("concurrent publisher", func(t *testing.T) {
		setHooks(t, nil, nil)
		room := env.createRoom(t)
		const total = 300
		done := make(chan struct{})
		go func() {
			defer close(done)
			for i := 0; i < total; i++ {
				env.publish(room.ID, "burst")
				if i%10 == 0 {
					time.Sleep(time.Millisecond)
				}
			}
		}()
		var wg sync.WaitGroup
		for i := 0; i < 8; i++ {
			time.Sleep(3 * time.Millisecond)
			cursor := env.head(room.ID) / 2
			conn := env.connect(t, room.ID, app.FormatEventID(room.ID, cursor), "")
			wg.Add(1)
			go func() {
				defer wg.Done()
				<-done
				final := env.head(room.ID)
				prev := cursor
				for prev < final {
					f, err := conn.nextFrame()
					if err != nil {
						t.Errorf("client from %d: %v after seq %d", cursor, err, prev)
						return
					}
					var d sseData
					if err := json.Unmarshal([]byte(f.Data), &d); err != nil || d.Sequence != prev+1 || f.ID != app.FormatEventID(room.ID, d.Sequence) {
						t.Errorf("client from %d: got seq %d (id %q) after %d", cursor, d.Sequence, f.ID, prev)
						return
					}
					prev = d.Sequence
				}
			}()
		}
		wg.Wait()
	})
}

// Acceptance 2: every SSE message has an id, ids strictly increase by one per
// durable event, and a client without Last-Event-ID still gets live-only.
func TestEventsEverySSEMessageCarriesMonotonicID(t *testing.T) {
	prev := heartbeatInterval
	heartbeatInterval = 20 * time.Millisecond
	t.Cleanup(func() { heartbeatInterval = prev })

	env := newEventsEnv(t)
	room := env.createRoom(t)
	env.publish(room.ID, "history-not-replayed")
	start := env.head(room.ID)

	c := env.connect(t, room.ID, "", "")
	for k, want := range map[string]string{
		"Content-Type":      "text/event-stream",
		"Cache-Control":     "no-cache",
		"X-Accel-Buffering": "no",
	} {
		if got := c.resp.Header.Get(k); got != want {
			t.Fatalf("%s = %q, want %q", k, got, want)
		}
	}

	sawHeartbeat := false
	deadline := time.After(5 * time.Second)
	for !sawHeartbeat {
		select {
		case f := <-c.frames:
			if f.Data != "" || f.HasID {
				t.Fatalf("unexpected message before any publish: %+v", f)
			}
			sawHeartbeat = f.Comment == "heartbeat"
		case <-deadline:
			t.Fatal("no heartbeat comment")
		}
	}

	for i := 0; i < 5; i++ {
		env.publish(room.ID, "live")
	}
	last := start
	for i := 0; i < 5; i++ {
		f := c.next(t)
		if !f.HasID || f.ID == "" {
			t.Fatalf("message without id: %+v", f)
		}
		seq, err := app.ParseEventID(room.ID, f.ID)
		if err != nil {
			t.Fatalf("id %q: %v", f.ID, err)
		}
		if seq != last+1 || f.decode(t).Sequence != seq {
			t.Fatalf("id %q after seq %d", f.ID, last)
		}
		last = seq
	}
}

// Acceptance 3: malformed, unknown, or expired cursors get an explicit reset,
// then the stream continues live.
func TestEventsInvalidCursorSendsReset(t *testing.T) {
	env := newEventsEnv(t)
	room := env.createRoom(t)
	other := env.createRoom(t)
	for i := 0; i < 3; i++ {
		env.publish(room.ID, "x")
	}
	evicted := env.createRoom(t)
	for i := 0; i < 520; i++ {
		env.publish(evicted.ID, "x")
	}

	cases := []struct {
		name, roomID, header, query, reason string
	}{
		{"garbage", room.ID, "not-an-id", "", "malformed"},
		{"bare sequence", room.ID, "2", "", "malformed"},
		{"non-numeric sequence", room.ID, room.ID + ":abc", "", "malformed"},
		{"non-canonical sequence", room.ID, room.ID + ":02", "", "malformed"},
		{"query fallback", room.ID, "", room.ID + ":-1", "malformed"},
		{"beyond head", room.ID, room.ID + ":999", "", "unknown"},
		{"other room", room.ID, app.FormatEventID(other.ID, 1), "", "unknown"},
		{"evicted", evicted.ID, app.FormatEventID(evicted.ID, 1), "", "expired"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			head := env.head(tc.roomID)
			c := env.connect(t, tc.roomID, tc.header, tc.query)
			f := c.next(t)
			d := f.decode(t)
			if d.Type != "reset" || d.Reason != tc.reason || d.RoomID != tc.roomID || d.Sequence != head {
				t.Fatalf("first message = %s, want reset reason=%s sequence=%d", f.Data, tc.reason, head)
			}
			if f.ID != app.FormatEventID(tc.roomID, head) {
				t.Fatalf("reset id = %q", f.ID)
			}
			env.publish(tc.roomID, "after-reset")
			c.expectSeqs(t, tc.roomID, head+1)
		})
	}
}

// Acceptance 4: assistant.delta is streamed live but never persisted, so a
// resume replays only durable events.
func TestEventsAssistantDeltaIsLiveOnlyAndNeverReplayed(t *testing.T) {
	env := newEventsEnv(t)
	room := env.createRoom(t)
	base := env.head(room.ID)
	delta := func(text string) {
		if err := env.runtime.Ingest(app.Event{
			"type": "assistant.delta", "roomId": room.ID, "sessionId": room.SessionID,
			"turnId": "tn_1", "blockId": "b1", "seq": float64(1), "delta": text, "activityAttempt": float64(1),
		}); err != nil {
			t.Fatal(err)
		}
	}

	live := env.connect(t, room.ID, "", "")
	delta("Hel")
	env.publish(room.ID, "durable")
	delta("lo")

	f := live.next(t)
	if d := f.decode(t); d.Type != "assistant.delta" || d.Delta != "Hel" || f.ID != app.FormatEventID(room.ID, base) {
		t.Fatalf("first live frame = id %q %s", f.ID, f.Data)
	}
	live.expectSeqs(t, room.ID, base+1)
	f = live.next(t)
	if d := f.decode(t); d.Type != "assistant.delta" || d.Delta != "lo" || f.ID != app.FormatEventID(room.ID, base+1) {
		t.Fatalf("third live frame = id %q %s", f.ID, f.Data)
	}

	for _, item := range env.runtime.ListActivity(room.ID) {
		if item.Type == "assistant.delta" {
			t.Fatalf("delta persisted in activity: %+v", item)
		}
	}
	audit, err := os.ReadFile(filepath.Join(env.runtime.Store.Dir, "audit", room.ID+".jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(audit), "assistant.delta") {
		t.Fatal("delta persisted in audit log")
	}

	resumed := env.connect(t, room.ID, app.FormatEventID(room.ID, base), "")
	resumed.expectSeqs(t, room.ID, base+1)
	env.publish(room.ID, "sentinel")
	resumed.expectSeqs(t, room.ID, base+2)
}

// Acceptance 5: cursors are scoped per room; one room's id never yields
// another room's events, in either direction.
func TestEventsCursorIsScopedPerRoom(t *testing.T) {
	env := newEventsEnv(t)
	a := env.createRoom(t)
	b := env.createRoom(t)
	for i := 0; i < 10; i++ {
		env.publish(a.ID, "a")
	}
	for i := 0; i < 3; i++ {
		env.publish(b.ID, "b")
	}

	// A's cursor is numerically within B's range but must not be replayed as B's.
	onB := env.connect(t, b.ID, app.FormatEventID(a.ID, 2), "")
	f := onB.next(t)
	if d := f.decode(t); d.Type != "reset" || d.Reason != "unknown" || d.RoomID != b.ID {
		t.Fatalf("B with A's cursor = %s", f.Data)
	}
	env.publish(a.ID, "a-live")
	env.publish(b.ID, "b-live")
	onB.expectSeqs(t, b.ID, env.head(b.ID))

	setHooks(t, func() { env.publish(b.ID, "b-during-subscribe") }, func(uint64) {})
	aHead := env.head(a.ID)
	onA := env.connect(t, a.ID, app.FormatEventID(a.ID, 2), "")
	onA.expectSeqs(t, a.ID, seqRange(3, aHead)...)
	env.publish(b.ID, "b-live")
	env.publish(a.ID, "a-sentinel")
	onA.expectSeqs(t, a.ID, aHead+1)
}

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

// sseData is an SSE data envelope with the payload fields tests read.
type sseData struct {
	ID      uint64 `json:"id"`
	Type    string `json:"type"`
	TaskID  string `json:"taskId"`
	TS      string `json:"ts"`
	Source  string `json:"source"`
	Payload struct {
		Text   string `json:"text"`
		Delta  string `json:"delta"`
		Reason string `json:"reason"`
		LastID uint64 `json:"lastId"`
	} `json:"payload"`
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

// ingest POSTs body to the internal worker ingest and asserts the status.
func (e *eventsEnv) ingest(t *testing.T, wantStatus int, body any) []byte {
	t.Helper()
	raw, ok := body.([]byte)
	if !ok {
		var err error
		if raw, err = json.Marshal(body); err != nil {
			t.Fatal(err)
		}
	}
	resp, err := http.Post(e.srv.URL+"/internal/events", "application/json", strings.NewReader(string(raw)))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != wantStatus {
		msg, _ := io.ReadAll(resp.Body)
		t.Fatalf("ingest %s: status %d, want %d: %s", raw, resp.StatusCode, wantStatus, msg)
	}
	return raw
}

func (e *eventsEnv) publish(roomID, text string) {
	e.runtime.Publish(roomID, app.Event{"type": "room.steered", "roomId": roomID, "text": text})
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

// expectIDs asserts the next durable frames are exactly want, in order, all
// from roomID, with the SSE id equal to the global event id.
func (c *sseConn) expectIDs(t *testing.T, roomID string, want ...uint64) {
	t.Helper()
	for _, id := range want {
		f := c.next(t)
		d := f.decode(t)
		if d.ID != id || d.TaskID != roomID || f.ID != app.FormatEventID(id) {
			t.Fatalf("got frame id=%q room=%s sequence=%d type=%s, want id %d in %s", f.ID, d.TaskID, d.ID, d.Type, id, roomID)
		}
	}
}

// idsAfter lists the room's retained event ids greater than after.
func (e *eventsEnv) idsAfter(roomID string, after uint64) []uint64 {
	var out []uint64
	for _, item := range e.runtime.ListActivity(roomID) {
		if item.ID > after {
			out = append(out, item.ID)
		}
	}
	return out
}

func setHooks(t *testing.T, afterSubscribe func(), afterReplayFrame func(uint64)) {
	t.Helper()
	testHooks.Store(&streamHooks{afterSubscribe: afterSubscribe, afterReplayFrame: afterReplayFrame})
	t.Cleanup(func() { testHooks.Store(nil) })
}

// Acceptance 1: replay only this room's events after Last-Event-ID, in order,
// then live, with events emitted during the handoff neither lost nor repeated.
func TestEventsResumeReplaysThenGoesLiveWithoutGapsOrDuplicates(t *testing.T) {
	env := newEventsEnv(t)
	a := env.createRoom(t)
	b := env.createRoom(t)
	for i := 0; i < 5; i++ {
		env.publish(a.ID, "before")
		env.publish(b.ID, "other-room")
	}
	history := env.idsAfter(a.ID, 0)
	resumeFrom := history[len(history)-4]

	var once sync.Once
	setHooks(t,
		func() {
			// Lands both in the live buffer and in history: must not repeat.
			env.publish(a.ID, "during-subscribe-1")
			env.publish(b.ID, "other-room")
			env.publish(a.ID, "during-subscribe-2")
		},
		func(uint64) {
			// History is already snapshotted: only the buffer can deliver these.
			once.Do(func() {
				env.publish(a.ID, "during-replay-1")
				env.publish(b.ID, "other-room")
				env.publish(a.ID, "during-replay-2")
			})
		},
	)

	// Header wins over a stale query cursor.
	c := env.connect(t, a.ID, app.FormatEventID(resumeFrom), "garbage")
	const replayed, duringHandoff = 3, 4
	var got []uint64
	for i := 0; i < replayed+duringHandoff; i++ {
		f := c.next(t)
		d := f.decode(t)
		if d.TaskID != a.ID || f.ID != app.FormatEventID(d.ID) {
			t.Fatalf("frame %d: id=%q room=%s", i, f.ID, d.TaskID)
		}
		got = append(got, d.ID)
	}
	if want := env.idsAfter(a.ID, resumeFrom); fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("delivered %v, want exactly %v", got, want)
	}

	env.publish(b.ID, "other-room")
	env.publish(a.ID, "live")
	c.expectIDs(t, a.ID, env.idsAfter(a.ID, got[len(got)-1])...)

	t.Run("concurrent publisher", func(t *testing.T) {
		setHooks(t, nil, nil)
		room := env.createRoom(t)
		noise := env.createRoom(t)
		const total = 300
		done := make(chan struct{})
		go func() {
			defer close(done)
			for i := 0; i < total; i++ {
				env.publish(room.ID, "burst")
				if i%3 == 0 {
					env.publish(noise.ID, "noise")
				}
				if i%10 == 0 {
					time.Sleep(time.Millisecond)
				}
			}
		}()
		var wg sync.WaitGroup
		for i := 0; i < 8; i++ {
			time.Sleep(3 * time.Millisecond)
			ids := env.idsAfter(room.ID, 0)
			cursor := ids[len(ids)/2]
			conn := env.connect(t, room.ID, app.FormatEventID(cursor), "")
			wg.Add(1)
			go func() {
				defer wg.Done()
				<-done
				for _, want := range env.idsAfter(room.ID, cursor) {
					f, err := conn.nextFrame()
					if err != nil {
						t.Errorf("client from %d: %v, want id %d", cursor, err, want)
						return
					}
					var d sseData
					if err := json.Unmarshal([]byte(f.Data), &d); err != nil || d.ID != want || d.TaskID != room.ID || f.ID != app.FormatEventID(want) {
						t.Errorf("client from %d: got id %q (room %s), want %d", cursor, f.ID, d.TaskID, want)
						return
					}
				}
			}()
		}
		wg.Wait()
	})
}

// Acceptance 2: every SSE message has an id; ids come from one global
// sequence, so within a room they strictly increase (with gaps where other
// rooms' events were allocated). A client without Last-Event-ID is live-only.
func TestEventsEverySSEMessageCarriesMonotonicID(t *testing.T) {
	prev := heartbeatInterval
	heartbeatInterval = 20 * time.Millisecond
	t.Cleanup(func() { heartbeatInterval = prev })

	env := newEventsEnv(t)
	room := env.createRoom(t)
	other := env.createRoom(t)
	env.publish(room.ID, "history-not-replayed")
	start := env.idsAfter(room.ID, 0)
	last := start[len(start)-1]

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
		env.publish(other.ID, "interleaved")
	}
	sawGap := false
	for i := 0; i < 5; i++ {
		f := c.next(t)
		if !f.HasID || f.ID == "" {
			t.Fatalf("message without id: %+v", f)
		}
		id, err := app.ParseEventID(f.ID)
		if err != nil {
			t.Fatalf("id %q: %v", f.ID, err)
		}
		if id <= last || f.decode(t).ID != id {
			t.Fatalf("id %q after %d", f.ID, last)
		}
		sawGap = sawGap || id > last+1
		last = id
	}
	if !sawGap {
		t.Fatal("room ids are contiguous; expected a shared global sequence")
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
	firstEvicted := env.idsAfter(evicted.ID, 0)[0]
	for i := 0; i < 520; i++ {
		env.publish(evicted.ID, "x")
	}
	otherID := env.idsAfter(other.ID, 0)[0]

	cases := []struct {
		name, roomID, header, query, reason string
	}{
		{"garbage", room.ID, "not-an-id", "", "malformed"},
		{"old room-prefixed form", room.ID, room.ID + ":2", "", "malformed"},
		{"non-canonical", room.ID, "02", "", "malformed"},
		{"query fallback", room.ID, "", "-1", "malformed"},
		{"beyond global head", room.ID, "999999", "", "unknown"},
		{"other room's event", room.ID, app.FormatEventID(otherID), "", "unknown"},
		{"evicted", evicted.ID, app.FormatEventID(firstEvicted), "", "expired"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ids := env.idsAfter(tc.roomID, 0)
			head := ids[len(ids)-1]
			c := env.connect(t, tc.roomID, tc.header, tc.query)
			f := c.next(t)
			d := f.decode(t)
			if d.Type != "reset" || d.Payload.Reason != tc.reason || d.TaskID != tc.roomID || d.Payload.LastID != head {
				t.Fatalf("first message = %s, want reset reason=%s sequence=%d", f.Data, tc.reason, head)
			}
			if f.ID != app.FormatEventID(head) {
				t.Fatalf("reset id = %q, want room head %d", f.ID, head)
			}
			env.publish(tc.roomID, "after-reset")
			c.expectIDs(t, tc.roomID, env.idsAfter(tc.roomID, head)...)
		})
	}

	// Rooms are in memory too, so after a restart the room itself is gone and
	// the stream is refused before any replay. Ids forgotten by a restart for
	// a room that is known are covered by TestEventIDsAreGlobalAndUnknownAfterRestart.
	t.Run("after restart", func(t *testing.T) {
		restarted := newEventsEnv(t)
		req, _ := http.NewRequest(http.MethodGet, restarted.srv.URL+"/v1/rooms/"+room.ID+"/events", nil)
		req.Header.Set("Last-Event-ID", app.FormatEventID(env.idsAfter(room.ID, 0)[0]))
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusNotFound || resp.Header.Get("Content-Type") == "text/event-stream" {
			t.Fatalf("status %d content-type %q, want 404 JSON", resp.StatusCode, resp.Header.Get("Content-Type"))
		}
	})
}

// Acceptance 4: assistant.delta is streamed live but never persisted, so a
// resume replays only durable events.
func TestEventsAssistantDeltaIsLiveOnlyAndNeverReplayed(t *testing.T) {
	env := newEventsEnv(t)
	room := env.createRoom(t)
	ids := env.idsAfter(room.ID, 0)
	base := ids[len(ids)-1]
	delta := func(text string) {
		env.ingest(t, http.StatusAccepted, map[string]any{
			"type": "assistant.delta", "roomId": room.ID, "sessionId": room.SessionID,
			"turnId": "tn_1", "blockId": "b1", "seq": 1, "delta": text, "activityAttempt": 1,
		})
	}

	live := env.connect(t, room.ID, "", "")
	delta("Hel")
	env.publish(room.ID, "durable")
	delta("lo")
	durable := env.idsAfter(room.ID, base)[0]

	f := live.next(t)
	if d := f.decode(t); d.Type != "assistant.delta" || d.Payload.Delta != "Hel" || f.ID != app.FormatEventID(base) {
		t.Fatalf("first live frame = id %q %s", f.ID, f.Data)
	}
	live.expectIDs(t, room.ID, durable)
	f = live.next(t)
	if d := f.decode(t); d.Type != "assistant.delta" || d.Payload.Delta != "lo" || f.ID != app.FormatEventID(durable) {
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

	resumed := env.connect(t, room.ID, app.FormatEventID(base), "")
	resumed.expectIDs(t, room.ID, durable)
	env.publish(room.ID, "sentinel")
	resumed.expectIDs(t, room.ID, env.idsAfter(room.ID, durable)...)
}

// Acceptance 5: a cursor must be an event id of the requested room; one
// room's id never yields another room's events, in either direction.
func TestEventsCursorIsScopedPerRoom(t *testing.T) {
	env := newEventsEnv(t)
	a := env.createRoom(t)
	b := env.createRoom(t)
	for i := 0; i < 10; i++ {
		env.publish(a.ID, "a")
		env.publish(b.ID, "b")
	}
	aIDs := env.idsAfter(a.ID, 0)
	aCursor := aIDs[2]

	// A's id lies inside B's id range; it must not select B's later events.
	onB := env.connect(t, b.ID, app.FormatEventID(aCursor), "")
	f := onB.next(t)
	if d := f.decode(t); d.Type != "reset" || d.Payload.Reason != "unknown" || d.TaskID != b.ID {
		t.Fatalf("B with A's cursor = %s", f.Data)
	}
	bHead := env.idsAfter(b.ID, 0)
	env.publish(a.ID, "a-live")
	env.publish(b.ID, "b-live")
	onB.expectIDs(t, b.ID, env.idsAfter(b.ID, bHead[len(bHead)-1])...)

	setHooks(t, func() { env.publish(b.ID, "b-during-subscribe") }, func(uint64) {})
	want := env.idsAfter(a.ID, aCursor)
	onA := env.connect(t, a.ID, app.FormatEventID(aCursor), "")
	onA.expectIDs(t, a.ID, want...)
	env.publish(b.ID, "b-live")
	env.publish(a.ID, "a-sentinel")
	onA.expectIDs(t, a.ID, env.idsAfter(a.ID, want[len(want)-1])...)
}

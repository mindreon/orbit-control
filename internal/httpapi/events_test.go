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

// sseData is the part of an SSE data envelope tests read.
type sseData struct {
	ID     uint64 `json:"id"`
	Type   string `json:"type"`
	TaskID string `json:"taskId"`
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
}

package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/mindreon/orbit-control/internal/app"
)

// Tightened limits the E-LE-6 controls run with (controlLimits).
const (
	ele6PerRoom          = 2
	ele6PerClient        = 4
	ele6IngestMax        = 64 << 10
	ele6WriteTimeout     = "200ms"
	ele6ClosedRoomLogTTL = "1s"
	ele6MaxLags          = 1

	// A flood of big events fills the kernel socket buffers of a client that
	// does not read (Linux caps a send buffer at tcp_wmem max, 4 MiB by
	// default), so the server's next write blocks.
	floodBigEvents = 150
	floodBigBytes  = 60 << 10
	// More than the 256-frame live buffer, sent while the write is blocked.
	overflowEvents = 270
	// The slot must be free well before the 10 s default write timeout.
	writeTimeoutWait = 5 * time.Second
)

var limitEnvVars = []string{
	"ORBIT_SSE_MAX_STREAMS_PER_ROOM", "ORBIT_SSE_MAX_STREAMS_PER_CLIENT", "ORBIT_SSE_WRITE_TIMEOUT",
	"ORBIT_SSE_MAX_CONSECUTIVE_LAGS", "ORBIT_INGEST_MAX_BYTES", "ORBIT_CLOSED_ROOM_LOG_TTL",
}

// runLimits resolves env the way the control binary does at startup
// (app.LimitsFromEnv), with only env set among the limit variables.
func runLimits(env []string) app.Limits {
	saved := map[string]string{}
	for _, name := range limitEnvVars {
		if v, ok := os.LookupEnv(name); ok {
			saved[name] = v
		}
		os.Unsetenv(name)
	}
	for _, kv := range env {
		if name, value, ok := strings.Cut(kv, "="); ok {
			os.Setenv(name, value)
		}
	}
	l := app.LimitsFromEnv()
	for _, name := range limitEnvVars {
		os.Unsetenv(name)
		if v, ok := saved[name]; ok {
			os.Setenv(name, v)
		}
	}
	return l
}

// limitsByEnv renders limits keyed by the env var that sets each one.
func limitsByEnv(l app.Limits) map[string]string {
	return map[string]string{
		"ORBIT_SSE_MAX_STREAMS_PER_ROOM":   strconv.Itoa(l.MaxStreamsPerRoom),
		"ORBIT_SSE_MAX_STREAMS_PER_CLIENT": strconv.Itoa(l.MaxStreamsPerClient),
		"ORBIT_SSE_WRITE_TIMEOUT":          l.SSEWriteTimeout.String(),
		"ORBIT_SSE_MAX_CONSECUTIVE_LAGS":   strconv.Itoa(l.MaxConsecutiveLags),
		"ORBIT_INGEST_MAX_BYTES":           strconv.FormatInt(l.IngestMaxBytes, 10),
		"ORBIT_CLOSED_ROOM_LOG_TTL":        l.ClosedRoomLogTTL.String(),
	}
}

func caseELE6(st *stack) caseRow {
	row := caseRow{
		ID:    "E-LE-6",
		Title: "An oversized event returns 413 and is not stored; exceeding a subscription cap rejects the connection; a stalled writer releases its slot; a closed room's log is released after its TTL; repeated lag ends in reset",
		Steps: []string{
			"Real control, started with ORBIT_SSE_MAX_STREAMS_PER_ROOM=" + strconv.Itoa(ele6PerRoom) + ", ORBIT_SSE_MAX_STREAMS_PER_CLIENT=" + strconv.Itoa(ele6PerClient) + ", ORBIT_INGEST_MAX_BYTES=" + strconv.Itoa(ele6IngestMax) + ", ORBIT_SSE_WRITE_TIMEOUT=" + ele6WriteTimeout + ", ORBIT_CLOSED_ROOM_LOG_TTL=" + ele6ClosedRoomLogTTL + ". Mock control, started with ORBIT_SSE_MAX_CONSECUTIVE_LAGS=" + strconv.Itoa(ele6MaxLags) + ".",
			"capsAndIngest (real): open " + strconv.Itoa(ele6PerRoom) + " SSE streams on S1, then one more (per-room cap); " + strconv.Itoa(ele6PerClient-ele6PerRoom) + " on S2, then one on S3 (per-client cap); close one S1 stream and S3 is accepted. POST /internal/events a tool.result for S1 over " + strconv.Itoa(ele6IngestMax) + " bytes, then one just under it. POST /v1/rooms/S1/abort: the open stream delivers a session.status and ends; a new stream on the closed room ends at once.",
			"closedRoomLogReleased (real, F22): after the abort, poll S1's /activity until it is empty; then resume S1 with Last-Event-ID = the last id S1 had before the abort.",
			"writeTimeoutReleasesSlot (real, F20): two clients with a 4 KiB receive buffer open SSE on room W and never read, so W is at its cap; ingest " + strconv.Itoa(floodBigEvents) + " events of " + strconv.Itoa(floodBigBytes) + " bytes into W so their writes block; poll for up to " + writeTimeoutWait.String() + " until a new stream on W is accepted.",
			"lagResetsStream (mock, F21): a client with a 4 KiB receive buffer opens SSE on room G and does not read; ingest " + strconv.Itoa(floodBigEvents) + " events of " + strconv.Itoa(floodBigBytes) + " bytes (the write blocks) and " + strconv.Itoa(overflowEvents) + " small events (the 256-frame buffer overflows); then the client reads while small events keep arriving, until a reset arrives and the stream ends.",
		},
	}
	row.Config = map[string]any{
		"limitsForRun": map[string]any{
			"real": limitsByEnv(runLimits(controlLimits["real"])),
			"mock": limitsByEnv(runLimits(controlLimits["mock"])),
		},
		"runSource":      "app.LimitsFromEnv() over the env each control was started with",
		"limitsDefaults": limitsByEnv(app.DefaultLimits()),
		"defaultsSource": "app.DefaultLimits() in internal/app/limits.go (documented in README 'Resource limits')",
	}
	expected, actual, observed := map[string]any{}, map[string]any{}, map[string]any{}
	s1, err := ele6Caps(st, expected, actual)
	if err != nil {
		actual["errors"] = err.Error()
	}
	ele6ClosedRoomLog(st, s1, expected, actual, observed)
	// The per-client cap counts streams that are still closing server-side.
	time.Sleep(time.Second)
	ele6WriteTimeoutSlot(st, expected, actual, observed)
	ele6LagReset(st, expected, actual, observed)
	expected["errors"] = ""
	if _, ok := actual["errors"]; !ok {
		actual["errors"] = ""
	}
	row.Expected, row.Actual, row.Observed = expected, actual, observed
	return row
}

// ele6Room is S1 after capsAndIngest: aborted at abortedAt, lastID before that.
type ele6Room struct {
	room      room
	lastID    uint64
	abortedAt time.Time
}

func ele6Caps(st *stack, expected, actual map[string]any) (ele6Room, error) {
	c := st.controls["real"]
	// Streams from earlier cases on this control finish closing server-side.
	time.Sleep(time.Second)
	var rooms []room
	for _, label := range []string{"S1", "S2", "S3"} {
		rm, err := st.room("real", label)
		if err != nil {
			return ele6Room{}, err
		}
		rooms = append(rooms, rm)
	}
	s1, s2, s3 := rooms[0], rooms[1], rooms[2]
	var open []*stream
	defer func() {
		for _, s := range open {
			s.close()
		}
	}()
	attempt := func(rm room) string {
		s, err := c.open(rm.ID, "")
		if err == nil {
			open = append(open, s)
		}
		return openResult(err)
	}

	var perRoom, perClient []string
	for i := 0; i <= ele6PerRoom; i++ {
		perRoom = append(perRoom, attempt(s1))
	}
	for i := 0; i < ele6PerClient-ele6PerRoom; i++ {
		perClient = append(perClient, attempt(s2))
	}
	perClient = append(perClient, attempt(s3))

	// Free one S1 slot; the server releases it when it sees the disconnect.
	s1Live := open[1]
	open[0].close()
	open = open[1:]
	released := false
	for deadline := time.Now().Add(5 * time.Second); time.Now().Before(deadline) && !released; {
		if attempt(s3) == "200" {
			released = true
			break
		}
		time.Sleep(100 * time.Millisecond)
	}

	before, _ := c.activity(s1.ID)
	overStatus, overCode := ingestSized(c, s1, ele6IngestMax+1024)
	afterOver, _ := c.activity(s1.ID)
	underStatus, _ := ingestSized(c, s1, ele6IngestMax-1024)
	afterUnder, _ := c.activity(s1.ID)

	s1Last := lastID(afterUnder)
	abortedAt := time.Now()
	abortErr := c.doJSON(http.MethodPost, "/v1/rooms/"+s1.ID+"/abort", map[string]any{}, nil)
	var liveTypes []string
	ended := false
	for deadline := time.Now().Add(10 * time.Second); time.Now().Before(deadline); {
		f, err := s1Live.next(time.Until(deadline))
		if err != nil {
			ended = err.Error() == "stream closed"
			break
		}
		liveTypes = append(liveTypes, f.env.Type)
	}
	toolResultsLive, closingStatus := 0, false
	for _, t := range liveTypes {
		switch t {
		case "tool.result":
			toolResultsLive++
		case "session.status":
			closingStatus = true
		}
	}
	closedRoomStreamEnds := false
	if s, err := c.open(s1.ID, ""); err == nil {
		open = append(open, s)
		_, err := s.next(5 * time.Second)
		closedRoomStreamEnds = err != nil && err.Error() == "stream closed"
	}

	expected["capsAndIngest"] = map[string]any{
		"perRoomCap":                []string{"200", "200", "429 STREAM_LIMIT_ROOM"},
		"perClientCap":              []string{"200", "200", "429 STREAM_LIMIT_CLIENT"},
		"slotReleasedAfterClose":    true,
		"oversizedIngest":           "413 PAYLOAD_TOO_LARGE",
		"oversizedStored":           false,
		"underLimitIngestStatus":    202,
		"underLimitStored":          true,
		"toolResultsOnOpenStream":   1,
		"streamEndedOnRoomClose":    true,
		"closingStatusBeforeEnd":    true,
		"newStreamOnClosedRoomEnds": true,
	}
	actual["capsAndIngest"] = map[string]any{
		"perRoomCap":                perRoom,
		"perClientCap":              perClient,
		"slotReleasedAfterClose":    released,
		"oversizedIngest":           strconv.Itoa(overStatus) + " " + overCode,
		"oversizedStored":           len(afterOver) != len(before),
		"underLimitIngestStatus":    underStatus,
		"underLimitStored":          len(afterUnder) == len(afterOver)+1,
		"toolResultsOnOpenStream":   toolResultsLive,
		"streamEndedOnRoomClose":    ended,
		"closingStatusBeforeEnd":    closingStatus,
		"newStreamOnClosedRoomEnds": closedRoomStreamEnds,
	}
	return ele6Room{room: s1, lastID: s1Last, abortedAt: abortedAt}, abortErr
}

// ele6ClosedRoomLog is F22: the closed room's log is freed after its TTL.
func ele6ClosedRoomLog(st *stack, s1 ele6Room, expected, actual, observed map[string]any) {
	c := st.controls["real"]
	released := false
	var releasedAfter time.Duration
	for deadline := time.Now().Add(10 * time.Second); time.Now().Before(deadline); {
		if items, err := c.activity(s1.room.ID); err == nil && len(items) == 0 {
			released, releasedAfter = true, time.Since(s1.abortedAt)
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	resume := map[string]any{"first": "", "reason": "", "lastId": -1, "streamEndsAfterReset": false}
	if s, err := c.open(s1.room.ID, strconv.FormatUint(s1.lastID, 10)); err != nil {
		resume["first"] = openResult(err)
	} else {
		if f, err := s.next(readTimeout); err == nil {
			var p struct {
				Reason string `json:"reason"`
				LastID uint64 `json:"lastId"`
			}
			_ = json.Unmarshal(f.env.Payload, &p)
			resume["first"], resume["reason"], resume["lastId"] = f.env.Type, p.Reason, p.LastID
			_, err := s.next(5 * time.Second)
			resume["streamEndsAfterReset"] = err != nil && err.Error() == "stream closed"
		}
		s.close()
	}
	expected["closedRoomLogReleased"] = map[string]any{
		"releasedWithin10s": true,
		"resume":            map[string]any{"first": "reset", "reason": "unknown", "lastId": 0, "streamEndsAfterReset": true},
	}
	actual["closedRoomLogReleased"] = map[string]any{"releasedWithin10s": released, "resume": resume}
	observed["closedRoomLogReleasedAfterAbortMs"] = releasedAfter.Milliseconds()
}

// ele6WriteTimeoutSlot is F20: a writer blocked on a reader that never reads
// hits the write deadline, its handler returns, and its slot frees.
func ele6WriteTimeoutSlot(st *stack, expected, actual, observed map[string]any) {
	c := st.controls["real"]
	w, err := st.room("real", "W")
	if err != nil {
		actual["writeTimeoutReleasesSlot"] = err.Error()
		return
	}
	var stalled []string
	var conns []*rawStream
	for i := 0; i < ele6PerRoom; i++ {
		rs, err := openStalled(c, w.ID)
		stalled = append(stalled, rawResult(rs, err))
		if err == nil {
			conns = append(conns, rs)
		}
	}
	defer func() {
		for _, rs := range conns {
			rs.conn.Close()
		}
	}()
	atCap := ""
	if s, err := c.open(w.ID, ""); err == nil {
		atCap = "200"
		s.close()
	} else {
		atCap = openResult(err)
	}
	floodStart := time.Now()
	accepted := ingestMany(c, w, floodBigEvents, floodBigBytes)
	released := false
	var releasedAfter time.Duration
	for deadline := time.Now().Add(writeTimeoutWait); time.Now().Before(deadline); {
		if s, err := c.open(w.ID, ""); err == nil {
			released, releasedAfter = true, time.Since(floodStart)
			s.close()
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	expected["writeTimeoutReleasesSlot"] = map[string]any{
		"stalledStreams":                     []string{"200", "200"},
		"atCapBeforeFlood":                   "429 STREAM_LIMIT_ROOM",
		"floodAccepted":                      floodBigEvents,
		"newStreamAcceptedWithin5s":          true,
		"waitShorterThanDefaultWriteTimeout": true,
	}
	actual["writeTimeoutReleasesSlot"] = map[string]any{
		"stalledStreams":                     stalled,
		"atCapBeforeFlood":                   atCap,
		"floodAccepted":                      accepted,
		"newStreamAcceptedWithin5s":          released,
		"waitShorterThanDefaultWriteTimeout": app.DefaultLimits().SSEWriteTimeout > writeTimeoutWait,
	}
	observed["writeTimeoutSlotFreedAfterFloodStartMs"] = releasedAfter.Milliseconds()
}

// ele6LagReset is F21: a reader that overflows its live buffer
// ORBIT_SSE_MAX_CONSECUTIVE_LAGS+1 times in a row gets reset "lagging".
func ele6LagReset(st *stack, expected, actual, observed map[string]any) {
	c := st.controls["mock"]
	g, err := st.room("mock", "G")
	if err != nil {
		actual["lagResetsStream"] = err.Error()
		return
	}
	rs, err := openStalled(c, g.ID)
	stalled := rawResult(rs, err)
	result := map[string]any{"stalledStream": stalled, "resetReceived": false, "resetReason": "", "streamEndedAfterReset": false}
	expected["lagResetsStream"] = map[string]any{
		"stalledStream": "200", "resetReceived": true, "resetReason": "lagging", "streamEndedAfterReset": true,
	}
	actual["lagResetsStream"] = result
	if err != nil {
		return
	}
	defer rs.conn.Close()
	ingestMany(c, g, floodBigEvents, floodBigBytes)
	ingestMany(c, g, overflowEvents, 200)

	frames := make(chan frame, 8192)
	go readSSE(rs.body, frames)
	stop := make(chan struct{})
	posted := make(chan int, 1)
	go func() {
		n := 0
		defer func() { posted <- n }()
		for n < 400 {
			select {
			case <-stop:
				return
			case <-time.After(10 * time.Millisecond):
			}
			ingestSized(c, g, 200)
			n++
		}
	}()
	var resetReasons []string
	messages := 0
	start := time.Now()
	for deadline := time.Now().Add(30 * time.Second); time.Now().Before(deadline); {
		var f frame
		var ok bool
		select {
		case f, ok = <-frames:
		case <-time.After(time.Until(deadline)):
		}
		if !ok {
			break
		}
		messages++
		if f.env.Type != "reset" {
			continue
		}
		var p struct {
			Reason string `json:"reason"`
		}
		_ = json.Unmarshal(f.env.Payload, &p)
		resetReasons = append(resetReasons, p.Reason)
		if p.Reason == "lagging" {
			result["resetReceived"], result["resetReason"] = true, "lagging"
			select {
			case _, more := <-frames:
				result["streamEndedAfterReset"] = !more
			case <-time.After(5 * time.Second):
			}
			break
		}
	}
	close(stop)
	observed["lagReset"] = map[string]any{
		"messagesBeforeReset":      messages,
		"resetReasonsSeen":         nonNil(resetReasons),
		"eventsPostedWhileReading": <-posted,
		"resetAfterReadStartMs":    time.Since(start).Milliseconds(),
	}
}

// rawStream is an SSE connection whose socket reads are under test control:
// the kernel receive buffer is 4 KiB and nothing is read until body is used.
type rawStream struct {
	conn   net.Conn
	status int
	code   string
	body   io.ReadCloser
}

func openStalled(c *control, roomID string) (*rawStream, error) {
	u, err := url.Parse(c.publicURL)
	if err != nil {
		return nil, err
	}
	d := net.Dialer{Control: func(_, _ string, rc syscall.RawConn) error {
		var serr error
		if err := rc.Control(func(fd uintptr) {
			serr = syscall.SetsockoptInt(int(fd), syscall.SOL_SOCKET, syscall.SO_RCVBUF, 4096)
		}); err != nil {
			return err
		}
		return serr
	}}
	conn, err := d.Dial("tcp", u.Host)
	if err != nil {
		return nil, err
	}
	// HTTP/1.0: the stream body is not chunked, so it can be parsed as-is later.
	if _, err := fmt.Fprintf(conn, "GET /v1/rooms/%s/events HTTP/1.0\r\nHost: %s\r\n\r\n", roomID, u.Host); err != nil {
		conn.Close()
		return nil, err
	}
	resp, err := http.ReadResponse(bufio.NewReaderSize(conn, 4096), nil)
	if err != nil {
		conn.Close()
		return nil, err
	}
	rs := &rawStream{conn: conn, status: resp.StatusCode, body: resp.Body}
	if resp.StatusCode != http.StatusOK {
		raw, _ := io.ReadAll(resp.Body)
		var e struct {
			Code string `json:"code"`
		}
		_ = json.Unmarshal(raw, &e)
		rs.code = e.Code
		conn.Close()
		return rs, &openError{Status: resp.StatusCode, Code: e.Code}
	}
	return rs, nil
}

func rawResult(rs *rawStream, err error) string {
	if err == nil {
		return "200"
	}
	return openResult(err)
}

func openResult(err error) string {
	var oe *openError
	switch {
	case err == nil:
		return "200"
	case errors.As(err, &oe):
		out := strconv.Itoa(oe.Status) + " " + oe.Code
		if strings.HasPrefix(oe.ContentType, "text/event-stream") {
			out += " (stream opened)"
		}
		return out
	default:
		return err.Error()
	}
}

// ingestSized posts one tool.result for rm whose text is textBytes long.
func ingestSized(c *control, rm room, textBytes int) (int, string) {
	body, _ := json.Marshal(map[string]any{
		"type": "tool.result", "roomId": rm.ID, "sessionId": rm.SessionID, "toolName": "bash",
		"callId": "c-size", "toolState": "success", "text": strings.Repeat("x", textBytes),
	})
	req, _ := http.NewRequest(http.MethodPost, c.internalURL+"/internal/events", bytes.NewReader(body))
	req.Header.Set("Authorization", "Bearer "+internalToken)
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return 0, err.Error()
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	var e struct {
		Code string `json:"code"`
	}
	_ = json.Unmarshal(raw, &e)
	return resp.StatusCode, e.Code
}

// ingestMany posts n events of textBytes each and returns how many got 202.
func ingestMany(c *control, rm room, n, textBytes int) int {
	accepted := 0
	for i := 0; i < n; i++ {
		if status, _ := ingestSized(c, rm, textBytes); status == http.StatusAccepted {
			accepted++
		}
	}
	return accepted
}

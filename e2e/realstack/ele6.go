package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/mindreon/orbit-control/internal/app"
)

// Limits the real pair's control runs with (controlLimits).
const (
	ele6PerRoom   = 2
	ele6PerClient = 4
	ele6IngestMax = 64 << 10
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
		Title: "An oversized event returns 413 and is not stored; exceeding a subscription cap rejects the connection",
		Steps: []string{
			"Real control, started with ORBIT_SSE_MAX_STREAMS_PER_ROOM=" + strconv.Itoa(ele6PerRoom) + ", ORBIT_SSE_MAX_STREAMS_PER_CLIENT=" + strconv.Itoa(ele6PerClient) + ", ORBIT_INGEST_MAX_BYTES=" + strconv.Itoa(ele6IngestMax) + ". Create rooms S1, S2, S3.",
			"Open " + strconv.Itoa(ele6PerRoom) + " SSE streams on S1, then one more on S1 (per-room cap).",
			"Open " + strconv.Itoa(ele6PerClient-ele6PerRoom) + " streams on S2 (the client now holds " + strconv.Itoa(ele6PerClient) + "), then one on S3 (per-client cap).",
			"Close one S1 stream; a stream on S3 is then accepted (the slot is released).",
			"POST /internal/events (internal token) a tool.result for S1 whose body is over " + strconv.Itoa(ele6IngestMax) + " bytes, then one just under it; read S1's activity and its open stream.",
			"POST /v1/rooms/S1/abort: S1's open stream delivers a session.status and then ends; a new stream on the closed room ends at once.",
		},
	}
	row.Config = map[string]any{
		"limitsForRun":   limitsByEnv(runLimits(controlLimits["real"])),
		"runSource":      "app.LimitsFromEnv() over the env the E-LE-6 control was started with",
		"limitsDefaults": limitsByEnv(app.DefaultLimits()),
		"defaultsSource": "app.DefaultLimits() in internal/app/limits.go (documented in README 'Resource limits')",
	}
	c := st.controls["real"]
	// Streams from earlier cases on this control finish closing server-side.
	time.Sleep(time.Second)
	var rooms []room
	for _, label := range []string{"S1", "S2", "S3"} {
		rm, err := st.room("real", label)
		if err != nil {
			row.Expected, row.Actual = "rooms created", err.Error()
			return row
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
		var oe *openError
		switch {
		case err == nil:
			open = append(open, s)
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
	post := func(textBytes int) (int, string) {
		body, _ := json.Marshal(map[string]any{
			"type": "tool.result", "roomId": s1.ID, "sessionId": s1.SessionID, "toolName": "bash",
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
	overStatus, overCode := post(ele6IngestMax + 1024)
	afterOver, _ := c.activity(s1.ID)
	underStatus, _ := post(ele6IngestMax - 1024)
	afterUnder, _ := c.activity(s1.ID)

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

	row.Expected = map[string]any{
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
		"errors":                    "",
	}
	row.Actual = map[string]any{
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
		"errors":                    errText(abortErr),
	}
	return row
}

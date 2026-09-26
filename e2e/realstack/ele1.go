package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"
)

// ele1Round is one turn of the E-LE-1 flow and where the client drops the
// connection in it. Every drop point comes before that turn's last event.
type ele1Round struct {
	message    string
	dropOn     string // event type that makes the client disconnect
	decide     bool   // the turn parks for approval; decide after reconnect
	waitTurnOK bool   // reconnect only after the turn has completed
}

func caseELE1(st *stack) caseRow {
	streamParts := func(n int) string {
		part := strings.Repeat("续传测试的流式文本没有任何空格，", 14)
		parts := make([]string, n)
		for i := range parts {
			parts[i] = part
		}
		return "stream:" + strings.Join(parts, chunkSeparator)
	}
	rounds := []ele1Round{
		{message: "echo:round-1", dropOn: "tool.call", decide: true},
		{message: streamParts(5), dropOn: "assistant.delta", waitTurnOK: true},
		{message: streamParts(5), dropOn: "assistant.delta", waitTurnOK: true},
	}
	row := caseRow{
		ID:    "E-LE-1",
		Title: "SSE disconnected mid-stream and resumed with Last-Event-ID: the client-assembled sequence has no gaps, no duplicates, and strictly increasing ids",
		Steps: []string{
			"Mock control: create room L; note its latest event id h0 from /activity; open SSE on L with Last-Event-ID: h0.",
			"Round 1: POST /messages 'echo:round-1' (gated_echo parks for approval). The client disconnects when the round's tool.call arrives; after the turn has parked it reconnects with Last-Event-ID = last stored id received; only then POST /v1/approvals/{id}/decide allow.",
			"Rounds 2 and 3: POST /messages 'stream:' with 5 Chinese parts (5 provider deltas). The client disconnects on the round's first assistant.delta, waits until the turn has completed, then reconnects with Last-Event-ID = last stored id received.",
			"Round 4: one more 'stream:' turn after the last reconnect, delivered live.",
			"Compare the client-assembled stored ids with /activity (ids > h0).",
			"Resume ids that cannot be honoured: reconnect to L with Last-Event-ID 'abc', with the latest id of another room L2, and with 999999999; each first message must be a reset envelope whose lastId and SSE id are L's latest id.",
		},
	}
	c := st.controls["mock"]
	rm, err := st.room("mock", "L")
	if err != nil {
		row.Expected, row.Actual = "room created", err.Error()
		return row
	}
	items, _ := c.activity(rm.ID)
	h0 := lastID(items)

	type signals struct{ turnReturned, reconnected chan struct{} }
	sig := make([]signals, len(rounds))
	for i := range sig {
		sig[i] = signals{make(chan struct{}), make(chan struct{})}
	}
	driveErr := make(chan error, 1)
	driveDone := make(chan struct{})
	go func() {
		defer close(driveDone)
		for i, r := range rounds {
			approvalID, err := c.postMessage(rm.ID, r.message)
			if err == nil && r.decide && approvalID == "" {
				err = errors.New("echo turn did not park for approval")
			}
			close(sig[i].turnReturned)
			if err != nil {
				driveErr <- err
				return
			}
			<-sig[i].reconnected
			if r.decide {
				if err := c.decide(approvalID); err != nil {
					driveErr <- err
					return
				}
			}
		}
		if _, err := c.postMessage(rm.ID, streamParts(3)); err != nil {
			driveErr <- err
			return
		}
		driveErr <- nil
	}()

	var assembled []uint64
	cursor := h0
	missingID, liveOnlyWrongID := 0, 0
	var dropPoints []string
	var replayNonEmpty []bool
	var readErr, driveResult error
	driveFinished := false
	idle := time.Duration(0)
	s, err := c.open(rm.ID, strconv.FormatUint(cursor, 10))
	if err != nil {
		row.Expected, row.Actual = "SSE opened", err.Error()
		return row
	}
	headers := map[string]string{
		"Content-Type":      s.header.Get("Content-Type"),
		"Cache-Control":     s.header.Get("Cache-Control"),
		"X-Accel-Buffering": s.header.Get("X-Accel-Buffering"),
	}
	for {
		if !driveFinished {
			select {
			case <-driveDone:
				driveFinished = true
				driveResult = <-driveErr
			default:
			}
		}
		if driveFinished {
			if truth, _ := c.activity(rm.ID); lastID(truth) == cursor {
				break
			}
		}
		f, err := s.next(500 * time.Millisecond)
		if errors.Is(err, errIdle) {
			if idle += 500 * time.Millisecond; idle < readTimeout {
				continue
			}
		}
		if err != nil {
			readErr = err
			break
		}
		idle = 0
		if !f.hasID || f.id == "" {
			missingID++
		}
		if f.env.ID == 0 {
			if f.id != strconv.FormatUint(cursor, 10) {
				liveOnlyWrongID++
			}
		} else {
			assembled = append(assembled, f.env.ID)
			cursor = f.env.ID
		}
		i := len(dropPoints)
		if i >= len(rounds) || f.env.Type != rounds[i].dropOn {
			continue
		}
		dropPoints = append(dropPoints, f.env.Type)
		s.close()
		<-sig[i].turnReturned
		if s, err = c.open(rm.ID, strconv.FormatUint(cursor, 10)); err != nil {
			readErr = err
			break
		}
		first, ferr := s.next(readTimeout)
		replayNonEmpty = append(replayNonEmpty, ferr == nil && first.env.ID > cursor)
		close(sig[i].reconnected)
		if ferr != nil {
			readErr = ferr
			break
		}
		s = prepend(s, first)
	}
	s.close()
	if !driveFinished {
		<-driveDone
		driveResult = <-driveErr
	}
	truth, _ := c.activity(rm.ID)
	want := idsAfter(truth, h0)
	dups, gaps, nonIncreasing := sequenceStats(assembled, want)
	unusable := resumeWithUnusableIDs(st, rm, lastID(truth))
	row.Expected = map[string]any{
		"sseHeaders": map[string]string{"Content-Type": "text/event-stream", "Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
		"resumeWithUnusableIds": map[string]any{
			"abc":              map[string]any{"first": "reset", "reason": "malformed", "lastIdIsRoomHead": true, "sseIdIsRoomHead": true},
			"otherRoomsLastId": map[string]any{"first": "reset", "reason": "unknown", "lastIdIsRoomHead": true, "sseIdIsRoomHead": true},
			"999999999":        map[string]any{"first": "reset", "reason": "unknown", "lastIdIsRoomHead": true, "sseIdIsRoomHead": true},
		},
		"disconnectsOn":             []string{"tool.call", "assistant.delta", "assistant.delta"},
		"replayNonEmptyOnReconnect": []bool{true, true, true},
		"assembledEqualsActivity":   true,
		"duplicates":                0,
		"gaps":                      0,
		"nonIncreasing":             0,
		"messagesWithoutId":         0,
		"liveOnlyIdNotLastStored":   0,
		"errors":                    "",
	}
	row.Actual = map[string]any{
		"sseHeaders":                headers,
		"resumeWithUnusableIds":     unusable,
		"disconnectsOn":             nonNil(dropPoints),
		"replayNonEmptyOnReconnect": replayNonEmpty,
		"assembledEqualsActivity":   equalIDs(assembled, want),
		"duplicates":                dups,
		"gaps":                      gaps,
		"nonIncreasing":             nonIncreasing,
		"messagesWithoutId":         missingID,
		"liveOnlyIdNotLastStored":   liveOnlyWrongID,
		"errors":                    errText(readErr, driveResult),
	}
	if len(want) == 0 {
		row.Actual.(map[string]any)["errors"] = fmt.Sprint("no events after h0; ", row.Actual.(map[string]any)["errors"])
	}
	return row
}

// resumeWithUnusableIDs reconnects to rm with ids control cannot resume from
// and reports the first message of each stream.
func resumeWithUnusableIDs(st *stack, rm room, head uint64) map[string]any {
	c := st.controls["mock"]
	out := map[string]any{}
	other, err := st.room("mock", "L2")
	if err != nil {
		out["error"] = err.Error()
		return out
	}
	otherItems, _ := c.activity(other.ID)
	cursors := map[string]string{
		"abc":              "abc",
		"otherRoomsLastId": strconv.FormatUint(lastID(otherItems), 10),
		"999999999":        "999999999",
	}
	for label, cursor := range cursors {
		s, err := c.open(rm.ID, cursor)
		if err != nil {
			out[label] = err.Error()
			continue
		}
		f, err := s.next(readTimeout)
		s.close()
		if err != nil {
			out[label] = err.Error()
			continue
		}
		var reset struct {
			Reason string `json:"reason"`
			LastID uint64 `json:"lastId"`
		}
		_ = json.Unmarshal(f.env.Payload, &reset)
		out[label] = map[string]any{
			"first":            f.env.Type,
			"reason":           reset.Reason,
			"lastIdIsRoomHead": reset.LastID == head,
			"sseIdIsRoomHead":  f.id == strconv.FormatUint(head, 10),
		}
	}
	return out
}

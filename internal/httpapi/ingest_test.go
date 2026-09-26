package httpapi

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"reflect"
	"strings"
	"testing"

	"github.com/mindreon/orbit-control/internal/app"
)

// a1Types is OrbitEvent.type in orbit-runtime schema/OrbitEvent.json (A1).
var a1Types = []string{
	"session.status", "assistant.message", "assistant.delta", "tool.call", "tool.result",
	"approval.asked", "approval.resolved", "question.asked", "question.answered", "todo.updated",
	"usage", "agent.started", "agent.finished", "agent.spawn_rejected", "turn.failed",
}

type activityItem struct {
	ID      uint64          `json:"id"`
	Type    string          `json:"type"`
	TaskID  string          `json:"taskId"`
	TS      string          `json:"ts"`
	Source  string          `json:"source"`
	Payload json.RawMessage `json:"payload"`
}

func (e *eventsEnv) activity(t *testing.T, roomID string) []activityItem {
	t.Helper()
	resp, err := http.Get(e.srv.URL + "/v1/rooms/" + roomID + "/activity")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var body struct {
		Items []activityItem `json:"items"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatal(err)
	}
	return body.Items
}

func sameJSON(t *testing.T, a, b []byte) bool {
	t.Helper()
	decode := func(raw []byte) any {
		dec := json.NewDecoder(bytes.NewReader(raw))
		dec.UseNumber()
		var v any
		if err := dec.Decode(&v); err != nil {
			t.Fatalf("decode %s: %v", raw, err)
		}
		return v
	}
	return reflect.DeepEqual(decode(a), decode(b))
}

// A1 compat: every A1 type is accepted; anything else is still a 400.
func TestIngestAcceptsA1EventTypes(t *testing.T) {
	env := newEventsEnv(t)
	room := env.createRoom(t)
	for _, typ := range a1Types {
		env.ingest(t, http.StatusAccepted, map[string]any{"type": typ, "roomId": room.ID, "sessionId": room.SessionID})
	}
	stored := map[string]bool{}
	for _, item := range env.activity(t, room.ID) {
		stored[item.Type] = true
	}
	for _, typ := range a1Types {
		if want := typ != "assistant.delta"; stored[typ] != want {
			t.Errorf("%s stored in activity = %v, want %v", typ, stored[typ], want)
		}
	}

	for _, body := range []string{
		`{"type":"assistant.thinking","roomId":"` + room.ID + `","sessionId":"` + room.SessionID + `"}`,
		`{"type":"session/update","roomId":"` + room.ID + `"}`,
		`{"type":"Tool.Call","roomId":"` + room.ID + `"}`,
		`{"roomId":"` + room.ID + `"}`,
		`[{"type":"tool.call"}]`,
		`{"type":1,"roomId":"` + room.ID + `"}`,
	} {
		env.ingest(t, http.StatusBadRequest, []byte(body))
	}
}

// A1 compat: payloads pass through unchanged — every camelCase field,
// nested objects, and fields control does not know — inside a typed envelope.
func TestIngestPassesA1PayloadThroughUnchanged(t *testing.T) {
	env := newEventsEnv(t)
	room := env.createRoom(t)
	base := map[string]string{"roomId": room.ID, "sessionId": room.SessionID}
	events := []string{
		`{"type":"tool.call","toolName":"bash","callId":"c1","argsPreview":"{\"cmd\":\"ls <dir> && pwd\"}","agentId":"main","agentPath":"main","turnId":"tn_1","modelMode":"mock","occurredAt":"2026-09-26T14:00:00.123456+00:00"}`,
		`{"type":"tool.result","toolName":"bash","callId":"c1","toolState":"success","truncated":true,"text":"README.md","agentId":"ag-x","agentPath":"main/ag-x","parentAgentId":"main","depth":1,"turnId":"tn_1"}`,
		`{"type":"usage","model":"qwen","inputTokens":1200,"outputTokens":80,"cacheInputTokens":1000,"cacheCreationInputTokens":0,"latencyMs":412,"agentPath":"main"}`,
		`{"type":"turn.failed","turnId":"tn_2","failure":{"turnId":"tn_2","agentId":"main","errorCode":"timeout","retryable":true,"message":"模型响应超时，这一轮没跑完。"},"futureField":{"nested":[1,2.5,"x"]}}`,
	}
	live := env.connect(t, room.ID, "", "")

	var sent [][]byte
	for _, raw := range events {
		var body map[string]any
		if err := json.Unmarshal([]byte(raw), &body); err != nil {
			t.Fatal(err)
		}
		for k, v := range base {
			body[k] = v
		}
		// Indented on the wire: control must keep content, not whitespace.
		pretty, _ := json.MarshalIndent(body, "", "  ")
		env.ingest(t, http.StatusAccepted, pretty)
		var compact bytes.Buffer
		_ = json.Compact(&compact, pretty)
		sent = append(sent, compact.Bytes())
	}

	items := env.activity(t, room.ID)
	items = items[len(items)-len(events):]
	for i, item := range items {
		var head struct {
			Type       string `json:"type"`
			OccurredAt string `json:"occurredAt"`
		}
		_ = json.Unmarshal(sent[i], &head)
		if item.ID == 0 || item.Type != head.Type || item.TaskID != room.ID || item.Source != "worker" || item.TS == "" {
			t.Fatalf("envelope %d = %+v", i, item)
		}
		if head.OccurredAt != "" && item.TS != head.OccurredAt {
			t.Fatalf("ts = %q, want payload occurredAt %q", item.TS, head.OccurredAt)
		}
		if !sameJSON(t, item.Payload, sent[i]) {
			t.Fatalf("activity payload changed:\n got %s\nwant %s", item.Payload, sent[i])
		}

		f := live.next(t)
		var frame activityItem
		if err := json.Unmarshal([]byte(f.Data), &frame); err != nil {
			t.Fatal(err)
		}
		if f.ID != app.FormatEventID(item.ID) || frame.ID != item.ID {
			t.Fatalf("SSE id %q / envelope id %d, want %d", f.ID, frame.ID, item.ID)
		}
		if !bytes.Equal(frame.Payload, sent[i]) {
			t.Fatalf("SSE payload bytes changed:\n got %s\nwant %s", frame.Payload, sent[i])
		}
	}

	delta := env.ingest(t, http.StatusAccepted, map[string]any{
		"type": "assistant.delta", "roomId": room.ID, "sessionId": room.SessionID,
		"turnId": "tn_3", "blockId": "b1", "seq": 7, "delta": "Hel<lo>", "activityAttempt": 2, "agentPath": "main",
	})
	f := live.next(t)
	var frame activityItem
	if err := json.Unmarshal([]byte(f.Data), &frame); err != nil {
		t.Fatal(err)
	}
	if frame.ID != 0 || frame.Type != "assistant.delta" || frame.TaskID != room.ID || !bytes.Equal(frame.Payload, delta) {
		t.Fatalf("delta frame = %s, want payload %s and no id", f.Data, delta)
	}
}

// A1 compat: assistant.delta takes no id and no slot in the 500-entry window,
// so a flood of deltas never evicts tool or approval records.
func TestAssistantDeltaNeverStoredOrEvictsHistory(t *testing.T) {
	env := newEventsEnv(t)
	room := env.createRoom(t)
	for _, typ := range []string{"tool.call", "approval.asked", "tool.result"} {
		env.ingest(t, http.StatusAccepted, map[string]any{
			"type": typ, "roomId": room.ID, "sessionId": room.SessionID, "toolName": "gated_echo", "callId": "c1",
		})
	}
	before := env.activity(t, room.ID)

	for i := 0; i < 600; i++ {
		env.ingest(t, http.StatusAccepted, map[string]any{
			"type": "assistant.delta", "roomId": room.ID, "sessionId": room.SessionID,
			"blockId": "b1", "seq": i, "delta": fmt.Sprintf("tok%d ", i),
		})
	}

	after := env.activity(t, room.ID)
	if len(after) != len(before) {
		t.Fatalf("activity grew from %d to %d entries", len(before), len(after))
	}
	for i := range before {
		if after[i].ID != before[i].ID || after[i].Type != before[i].Type {
			t.Fatalf("entry %d changed: %+v -> %+v", i, before[i], after[i])
		}
	}

	env.publish(room.ID, "next")
	now := env.activity(t, room.ID)
	if last, prev := now[len(now)-1].ID, before[len(before)-1].ID; last != prev+1 {
		t.Fatalf("next durable id %d, want %d: deltas consumed ids", last, prev+1)
	}

	// Nothing was evicted, so a cursor of 0 replays the whole room.
	c := env.connect(t, room.ID, "0", "")
	for _, item := range now {
		f := c.next(t)
		if strings.Contains(f.Data, `"type":"reset"`) || f.ID != app.FormatEventID(item.ID) {
			t.Fatalf("replay from 0: got %s, want id %d", f.Data, item.ID)
		}
	}
}

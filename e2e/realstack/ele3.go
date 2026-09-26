package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"net/http"
	"strconv"
	"strings"
	"time"
)

const (
	ele3Parts = 700
	// Just over the worker's 100 ms delta interval, so every streamed part is
	// flushed as its own assistant.delta.
	ele3DeltaLatency = 110 * time.Millisecond
)

func caseELE3(st *stack) caseRow {
	row := caseRow{
		ID:    "E-LE-3",
		Title: "After one turn with 600+ streamed events, tool and approval records remain in activity history; deltas do not count toward the 500 cap",
		Steps: []string{
			"Mock control: create room T; POST /messages 'echo:keep-me' (gated_echo parks: tool.call, approval.asked) and POST /v1/approvals/{id}/decide allow (tool.result). Record T's activity.",
			"Open SSE on T. The recording proxy adds 110 ms to each of T's assistant.delta posts (a slow ingest), so the worker's 100 ms coalescing flushes every streamed part; payloads are forwarded unchanged.",
			"POST /messages 'stream:' with " + strconv.Itoa(ele3Parts) + " parts of 'word ' in one turn (one provider delta per part). The message stays far below the mock model's context limit.",
			"Compare the turn's assistant.delta bodies the worker posted with the deltas streamed over SSE (count and bytes; the first 200 characters are one delta by the worker's size rule, each later part is its own delta), then re-read /activity and /v1/approvals.",
		},
	}
	c := st.controls["mock"]
	t, err := st.room("mock", "T")
	if err != nil {
		row.Expected, row.Actual = "room created", err.Error()
		return row
	}
	approvalID, err := c.postMessage(t.ID, "echo:keep-me")
	if err == nil && approvalID == "" {
		err = errors.New("echo turn did not park for approval")
	}
	if err == nil {
		err = c.decide(approvalID)
	}
	if err != nil {
		row.Expected, row.Actual = "tool and approval turn completes", err.Error()
		return row
	}
	before, _ := c.activity(t.ID)
	recordsBefore := toolAndApprovalRecords(before)

	s, err := c.open(t.ID, "")
	if err != nil {
		row.Expected, row.Actual = "SSE opened", err.Error()
		return row
	}
	defer s.close()
	postsBefore := len(c.postsFor(t.ID))
	c.mu.Lock()
	c.slowDeltas[t.ID] = ele3DeltaLatency
	c.mu.Unlock()
	parts := make([]string, ele3Parts)
	for i := range parts {
		parts[i] = "word "
	}
	_, errTurn := c.postMessage(t.ID, "stream:"+strings.Join(parts, chunkSeparator))
	c.mu.Lock()
	delete(c.slowDeltas, t.ID)
	c.mu.Unlock()
	frames := s.drain(5*time.Second, nil)

	var workerDeltas [][]byte
	for _, p := range c.postsFor(t.ID)[postsBefore:] {
		if p.typ == "assistant.delta" {
			var compact bytes.Buffer
			_ = json.Compact(&compact, p.raw)
			workerDeltas = append(workerDeltas, compact.Bytes())
		}
	}
	var sseDeltas [][]byte
	sseStored := 0
	for _, f := range frames {
		switch {
		case f.env.Type == "assistant.delta":
			sseDeltas = append(sseDeltas, f.env.Payload)
		case f.env.ID != 0:
			sseStored++
		}
	}
	sameDeltas := len(workerDeltas) == len(sseDeltas)
	for i := 0; sameDeltas && i < len(sseDeltas); i++ {
		sameDeltas = bytes.Equal(workerDeltas[i], sseDeltas[i])
	}
	after, _ := c.activity(t.ID)
	deltasInActivity := 0
	for _, it := range after {
		if it.Type == "assistant.delta" {
			deltasInActivity++
		}
	}
	var approvals struct {
		Items []struct {
			ID     string `json:"id"`
			Status string `json:"status"`
		} `json:"items"`
	}
	_ = c.doJSON(http.MethodGet, "/v1/approvals", nil, &approvals)
	approvalStatus := ""
	for _, a := range approvals.Items {
		if a.ID == approvalID {
			approvalStatus = a.Status
		}
	}
	recordsAfter := toolAndApprovalRecords(after)
	counts := func(m map[string][]uint64) map[string]int {
		return map[string]int{"tool.call": len(m["tool.call"]), "tool.result": len(m["tool.result"]), "approval.asked": len(m["approval.asked"])}
	}
	row.Expected = map[string]any{
		"toolAndApprovalRecordsBeforeTurn": map[string]int{"tool.call": 1, "tool.result": 1, "approval.asked": 2},
		"sameRecordIdsAfterTurn":           true,
		"deltasPostedByWorkerAtLeast600":   true,
		"sseDeltasEqualWorkerPosts":        true,
		"streamedEventsInTurnOver600":      true,
		"deltasInActivity":                 0,
		"activityGrowthEqualsStoredEvents": true,
		"oldestActivityIdUnchanged":        true,
		"activityLengthUnder500":           true,
		"approvalRecordStatus":             "decided",
		"errors":                           "",
	}
	row.Actual = map[string]any{
		"toolAndApprovalRecordsBeforeTurn": counts(recordsBefore),
		"sameRecordIdsAfterTurn":           equalRecords(recordsBefore, recordsAfter),
		"deltasPostedByWorkerAtLeast600":   len(workerDeltas) >= 600,
		"sseDeltasEqualWorkerPosts":        sameDeltas,
		"streamedEventsInTurnOver600":      len(sseDeltas)+sseStored > 600,
		"deltasInActivity":                 deltasInActivity,
		"activityGrowthEqualsStoredEvents": len(after) == len(before)+sseStored,
		"oldestActivityIdUnchanged":        len(before) > 0 && len(after) > 0 && before[0].ID == after[0].ID,
		"activityLengthUnder500":           len(after) < 500,
		"approvalRecordStatus":             approvalStatus,
		"errors":                           errText(errTurn),
	}
	return row
}

func toolAndApprovalRecords(items []envelope) map[string][]uint64 {
	out := map[string][]uint64{}
	for _, it := range items {
		switch it.Type {
		case "tool.call", "tool.result", "approval.asked":
			out[it.Type] = append(out[it.Type], it.ID)
		}
	}
	return out
}

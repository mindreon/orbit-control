package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptrace"
	"time"
)

// caseRealDecide is the real-Temporal decide case. It runs only when
// ORBIT_RUNTIME_DECIDE_E2E=1, after the runtime pin is the merge of
// orbit-runtime#11.
func caseRealDecide(st *stack) caseRow {
	row := caseRow{
		ID:    "C3-decide",
		Title: "decide against the real RoomWorkflow is accepted",
		Steps: []string{
			"Create a room on the mock control.",
			"Post a message and read the parked approval.",
			"POST /v1/approvals/{id}/decide allow.",
		},
		Expected: "accepted",
	}
	rm, err := st.room("mock", "C3")
	if err != nil {
		row.Actual = err.Error()
		return row
	}
	approvalID, err := st.controls["mock"].postMessage(rm.ID, "run a command that needs approval")
	if err != nil || approvalID == "" {
		row.Actual = fmt.Sprintf("approval parked: %v id=%t", err, approvalID != "")
		return row
	}
	if err := st.controls["mock"].decide(approvalID); err != nil {
		row.Actual = err.Error()
		return row
	}
	row.Actual = "accepted"
	return row
}

// caseRealFM60 cancels the decide request as soon as it is written and then
// reads the room. It runs only with ORBIT_RUNTIME_DECIDE_E2E=1.
func caseRealFM60(st *stack) caseRow {
	row := caseRow{
		ID:    "C3-fm60",
		Title: "decide then disconnect still leaves a finished room",
		Steps: []string{
			"Create a room and park an approval.",
			"Cancel the decide HTTP request immediately after it is written.",
			"Read the room state from the API.",
		},
		Expected: map[string]any{"client": "canceled", "room": "running"},
	}
	rm, err := st.room("mock", "C3FM60")
	if err != nil {
		row.Actual = err.Error()
		return row
	}
	approvalID, err := st.controls["mock"].postMessage(rm.ID, "run a command that needs approval")
	if err != nil || approvalID == "" {
		row.Actual = fmt.Sprintf("approval parked: %v", err)
		return row
	}
	err = cancelDecide(st.controls["mock"], approvalID)
	client := "ok"
	if err != nil {
		client = "canceled"
	}
	deadline := time.Now().Add(20 * time.Second)
	state := ""
	for time.Now().Before(deadline) {
		var got room
		if gerr := st.controls["mock"].doJSON(http.MethodGet, "/v1/rooms/"+rm.ID, nil, &got); gerr == nil {
			state = got.State
			if state == "running" || state == "failed" || state == "closed" {
				break
			}
		}
		time.Sleep(200 * time.Millisecond)
	}
	row.Actual = map[string]any{"client": client, "room": state}
	return row
}

func cancelDecide(c *control, approvalID string) error {
	body, _ := json.Marshal(map[string]any{"decision": "allow"})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	req, err := http.NewRequestWithContext(httptrace.WithClientTrace(ctx, &httptrace.ClientTrace{
		WroteRequest: func(httptrace.WroteRequestInfo) { cancel() },
	}), http.MethodPost, c.publicURL+"/v1/approvals/"+approvalID+"/decide", bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := api.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, resp.Body)
	return nil
}

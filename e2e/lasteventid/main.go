// Command lasteventid verifies SSE Last-Event-ID resume end to end: it builds
// and starts the real orbit-control binary, drives it only over HTTP, and
// writes one artifact entry per case (case, expected, actual, pass).
//
//	go run ./e2e/lasteventid -out artifacts/e2e-last-event-id.json
//
// Exit status is 1 when any case fails. Room ids are reported as labels
// (A, B, ...) so the artifact is identical across runs of the same commit.
package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const readTimeout = 10 * time.Second

type result struct {
	Case     string `json:"case"`
	Covers   string `json:"covers"`
	Expected any    `json:"expected"`
	Actual   any    `json:"actual"`
	Pass     bool   `json:"pass"`
}

type artifact struct {
	Artifact  string   `json:"artifact"`
	Commit    string   `json:"commit"`
	GoVersion string   `json:"goVersion"`
	Pass      bool     `json:"pass"`
	Cases     []result `json:"cases"`
}

func main() {
	out := flag.String("out", "artifacts/e2e-last-event-id.json", "artifact path")
	bin := flag.String("bin", "", "orbit-control binary (default: go build ./cmd/orbit-control)")
	flag.Parse()

	work, err := os.MkdirTemp("", "e2e-last-event-id-")
	if err != nil {
		fatal(err)
	}
	defer os.RemoveAll(work)

	r := &runner{labels: map[string]string{}}
	c, err := newControl(work, *bin)
	if err != nil {
		r.add("setup", "harness", "control starts", err.Error())
	} else {
		defer c.stop()
		r.c = c
		r.run()
	}

	a := artifact{
		Artifact:  "e2e-last-event-id",
		Commit:    commit(),
		GoVersion: runtime.Version(),
		Pass:      true,
		Cases:     r.results,
	}
	for _, res := range r.results {
		if !res.Pass {
			a.Pass = false
			fmt.Fprintf(os.Stderr, "FAIL %s\n  expected: %s\n  actual:   %s\n", res.Case, mustJSON(res.Expected), mustJSON(res.Actual))
		}
	}
	if err := os.MkdirAll(filepath.Dir(*out), 0o755); err != nil {
		fatal(err)
	}
	raw, _ := json.MarshalIndent(a, "", "  ")
	if err := os.WriteFile(*out, append(raw, '\n'), 0o644); err != nil {
		fatal(err)
	}
	fmt.Printf("%d cases, pass=%v, artifact %s\n", len(a.Cases), a.Pass, *out)
	if !a.Pass {
		if c != nil {
			fmt.Fprintf(os.Stderr, "control log: %s\n", c.logTail())
		}
		os.Exit(1)
	}
}

type runner struct {
	c       *control
	results []result
	labels  map[string]string
	rec     recorder
}

func (r *runner) run() {
	r.replayAfterIDSameTask()
	r.queryFallback()
	r.headerWinsOverQuery()
	r.noCrossTaskReads()
	r.resets()
	r.deltaNotReplayed()
	r.sseHeadersHeartbeatLiveOnly()
	r.liveSwitchNoDupNoGap()
	r.idOnEveryMessage()
	r.restartForgetsIDs()
}

// add records a case; pass means expected and actual are the same JSON value.
func (r *runner) add(name, covers string, expected, actual any) {
	r.results = append(r.results, result{
		Case: name, Covers: covers, Expected: normalize(expected), Actual: normalize(actual),
		Pass: sameJSON(expected, actual),
	})
}

func (r *runner) fail(name, covers string, expected any, err error) {
	r.add(name, covers, expected, map[string]string{"error": err.Error()})
}

func (r *runner) room() (room, error) {
	rm, err := r.c.createRoom()
	if err == nil {
		r.labels[rm.ID] = string(rune('A' + len(r.labels)))
	}
	return rm, err
}

func (r *runner) label(taskID string) string {
	if l, ok := r.labels[taskID]; ok {
		return l
	}
	return "?" + taskID
}

// emit ingests a worker event and returns the id it was stored under.
func (r *runner) emit(rm room, typ string, extra map[string]any) (uint64, error) {
	body := map[string]any{"type": typ, "roomId": rm.ID, "sessionId": rm.SessionID}
	for k, v := range extra {
		body[k] = v
	}
	if err := r.c.ingest(body); err != nil {
		return 0, err
	}
	if typ == "assistant.delta" {
		return 0, nil
	}
	ids, err := r.c.activityIDs(rm.ID)
	if err != nil || len(ids) == 0 {
		return 0, fmt.Errorf("activity after ingest: %v", err)
	}
	return ids[len(ids)-1], nil
}

func (r *runner) emitN(rm room, n int) error {
	for i := 0; i < n; i++ {
		if _, err := r.emit(rm, "tool.call", map[string]any{"callId": fmt.Sprintf("c%d", i), "toolName": "bash"}); err != nil {
			return err
		}
	}
	return nil
}

// readIDs reads n durable frames and returns their ids and task labels.
func (r *runner) readIDs(s *stream, n int) ([]uint64, []string, error) {
	var ids []uint64
	var tasks []string
	for i := 0; i < n; i++ {
		f, err := s.next(&r.rec)
		if err != nil {
			return ids, tasks, err
		}
		ids = append(ids, f.data.ID)
		tasks = append(tasks, r.label(f.data.TaskID))
	}
	return ids, tasks, nil
}

func after(ids []uint64, cursor uint64) []uint64 {
	var out []uint64
	for _, id := range ids {
		if id > cursor {
			out = append(out, id)
		}
	}
	return out
}

func repeat(s string, n int) []string {
	out := make([]string, n)
	for i := range out {
		out[i] = s
	}
	return out
}

func (r *runner) replayAfterIDSameTask() {
	const name, covers = "replay_after_id_same_task", "acceptance #1: replay only this task's events with id > Last-Event-ID, in order, then live"
	a, err := r.room()
	if err != nil {
		r.fail(name, covers, nil, err)
		return
	}
	b, err := r.room()
	if err != nil {
		r.fail(name, covers, nil, err)
		return
	}
	for i := 0; i < 5; i++ {
		if _, err := r.emit(a, "tool.call", map[string]any{"callId": fmt.Sprintf("a%d", i)}); err != nil {
			r.fail(name, covers, nil, err)
			return
		}
		if _, err := r.emit(b, "tool.call", map[string]any{"callId": fmt.Sprintf("b%d", i)}); err != nil {
			r.fail(name, covers, nil, err)
			return
		}
	}
	ids, _ := r.c.activityIDs(a.ID)
	cursor := ids[2]
	want := after(ids, cursor)

	s, err := r.c.open(a.ID, strconv.FormatUint(cursor, 10), "")
	if err != nil {
		r.fail(name, covers, nil, err)
		return
	}
	defer s.close()
	got, tasks, err := r.readIDs(s, len(want))
	live, _ := r.emit(a, "tool.result", nil)
	liveGot, _, err2 := r.readIDs(s, 1)
	r.add(name, covers,
		map[string]any{"cursor": cursor, "replay": want, "replayTasks": repeat(r.label(a.ID), len(want)), "live": []uint64{live}},
		map[string]any{"cursor": cursor, "replay": got, "replayTasks": tasks, "live": liveGot, "error": errText(err, err2)})
}

func (r *runner) queryFallback() {
	const name, covers = "query_fallback", "lastEventId query parameter is honoured when no header is sent"
	a, err := r.room()
	if err == nil {
		err = r.emitN(a, 4)
	}
	if err != nil {
		r.fail(name, covers, nil, err)
		return
	}
	ids, _ := r.c.activityIDs(a.ID)
	cursor := ids[1]
	want := after(ids, cursor)
	s, err := r.c.open(a.ID, "", strconv.FormatUint(cursor, 10))
	if err != nil {
		r.fail(name, covers, nil, err)
		return
	}
	defer s.close()
	got, _, err := r.readIDs(s, len(want))
	r.add(name, covers, map[string]any{"replay": want}, map[string]any{"replay": got, "error": errText(err)})
}

func (r *runner) headerWinsOverQuery() {
	const name, covers = "header_wins_over_query", "Last-Event-ID header takes precedence over a stale lastEventId query value"
	a, err := r.room()
	if err == nil {
		err = r.emitN(a, 3)
	}
	if err != nil {
		r.fail(name, covers, nil, err)
		return
	}
	ids, _ := r.c.activityIDs(a.ID)
	cursor := ids[1]
	want := after(ids, cursor)
	s, err := r.c.open(a.ID, strconv.FormatUint(cursor, 10), "garbage")
	if err != nil {
		r.fail(name, covers, nil, err)
		return
	}
	defer s.close()
	var types []string
	var got []uint64
	for range want {
		f, err := s.next(&r.rec)
		if err != nil {
			break
		}
		types = append(types, f.data.Type)
		got = append(got, f.data.ID)
	}
	r.add(name, covers,
		map[string]any{"replay": want, "types": repeat("tool.call", len(want))},
		map[string]any{"replay": got, "types": types})
}

func (r *runner) noCrossTaskReads() {
	const name, covers = "no_cross_task_reads", "acceptance #5: a task's stream never carries another task's events, even while that task is busy"
	a, err := r.room()
	if err != nil {
		r.fail(name, covers, nil, err)
		return
	}
	b, err := r.room()
	if err != nil {
		r.fail(name, covers, nil, err)
		return
	}
	if err := r.emitN(a, 3); err != nil {
		r.fail(name, covers, nil, err)
		return
	}
	ids, _ := r.c.activityIDs(a.ID)
	cursor := ids[0]
	s, err := r.c.open(a.ID, strconv.FormatUint(cursor, 10), "")
	if err != nil {
		r.fail(name, covers, nil, err)
		return
	}
	defer s.close()
	for i := 0; i < 20; i++ {
		if _, err := r.emit(b, "tool.call", nil); err != nil {
			r.fail(name, covers, nil, err)
			return
		}
		if i%10 == 0 {
			if _, err := r.emit(a, "tool.call", nil); err != nil {
				r.fail(name, covers, nil, err)
				return
			}
		}
	}
	sentinel, _ := r.emit(a, "tool.result", nil)
	aIDs, _ := r.c.activityIDs(a.ID)
	want := after(aIDs, cursor)
	got, tasks, err := r.readIDs(s, len(want))
	r.add(name, covers,
		map[string]any{"ids": want, "tasks": repeat(r.label(a.ID), len(want)), "lastIsSentinel": sentinel},
		map[string]any{"ids": got, "tasks": tasks, "lastIsSentinel": lastOr0(got), "error": errText(err)})
}

func (r *runner) resets() {
	a, err := r.room()
	if err == nil {
		err = r.emitN(a, 3)
	}
	other, err2 := r.room()
	if err == nil {
		err = err2
	}
	if err == nil {
		err = r.emitN(other, 2)
	}
	expired, err3 := r.room()
	if err == nil {
		err = err3
	}
	var firstExpired uint64
	if err == nil {
		ids, _ := r.c.activityIDs(expired.ID)
		firstExpired = ids[0]
		err = r.emitN(expired, 520)
	}
	if err != nil {
		r.fail("reset_setup", "acceptance #3", nil, err)
		return
	}
	otherIDs, _ := r.c.activityIDs(other.ID)

	const malformed = "acceptance #3: malformed Last-Event-ID yields an explicit reset, then live"
	const unknown = "acceptance #3/#5: an id that is not an event of this task yields reset unknown, then live"
	const old = "acceptance #3: an id older than the earliest retained event yields reset expired, then live"
	cases := []struct {
		name, covers  string
		rm            room
		header, query string
		reason        string
	}{
		{"reset_malformed_garbage", malformed, a, "abc", "", "malformed"},
		{"reset_malformed_room_prefixed", malformed, a, a.ID + ":2", "", "malformed"},
		{"reset_malformed_non_canonical", malformed, a, "02", "", "malformed"},
		{"reset_malformed_query", malformed, a, "", "-1", "malformed"},
		{"reset_unknown_other_task", unknown, a, strconv.FormatUint(otherIDs[0], 10), "", "unknown"},
		{"reset_unknown_beyond_head", unknown, a, "999999999", "", "unknown"},
		{"reset_expired_old_id", old, expired, strconv.FormatUint(firstExpired, 10), "", "expired"},
	}
	for _, tc := range cases {
		ids, _ := r.c.activityIDs(tc.rm.ID)
		head := ids[len(ids)-1]
		s, err := r.c.open(tc.rm.ID, tc.header, tc.query)
		if err != nil {
			r.fail(tc.name, tc.covers, nil, err)
			continue
		}
		f, err := s.next(&r.rec)
		var reset struct {
			Reason string `json:"reason"`
			LastID uint64 `json:"lastId"`
		}
		_ = json.Unmarshal(f.data.Payload, &reset)
		next, _ := r.emit(tc.rm, "tool.result", nil)
		liveGot, _, err2 := r.readIDs(s, 1)
		s.close()
		r.add(tc.name, tc.covers,
			map[string]any{"first": "reset", "reason": tc.reason, "lastId": head, "sseId": strconv.FormatUint(head, 10), "task": r.label(tc.rm.ID), "thenLive": []uint64{next}},
			map[string]any{"first": f.data.Type, "reason": reset.Reason, "lastId": reset.LastID, "sseId": f.id, "task": r.label(f.data.TaskID), "thenLive": liveGot, "error": errText(err, err2)})
	}
}

func (r *runner) deltaNotReplayed() {
	const name, covers = "delta_not_replayed", "acceptance #4: assistant.delta is live-only: not stored, never replayed, never evicts durable records"
	d, err := r.room()
	if err != nil {
		r.fail(name, covers, nil, err)
		return
	}
	ids, _ := r.c.activityIDs(d.ID)
	base := ids[len(ids)-1]
	live, err := r.c.open(d.ID, "", "")
	if err != nil {
		r.fail(name, covers, nil, err)
		return
	}
	defer live.close()
	delta := func(text string) error {
		_, err := r.emit(d, "assistant.delta", map[string]any{"turnId": "tn_1", "blockId": "b1", "seq": 1, "delta": text, "activityAttempt": 1})
		return err
	}
	if err := delta("Hel"); err != nil {
		r.fail(name, covers, nil, err)
		return
	}
	durable, _ := r.emit(d, "assistant.message", map[string]any{"text": "Hello"})
	if err := delta("lo"); err != nil {
		r.fail(name, covers, nil, err)
		return
	}
	type seen struct {
		Type  string `json:"type"`
		SSEID string `json:"sseId"`
		ID    uint64 `json:"id"`
	}
	var liveGot []seen
	for i := 0; i < 3; i++ {
		f, err := live.next(&r.rec)
		if err != nil {
			break
		}
		liveGot = append(liveGot, seen{f.data.Type, f.id, f.data.ID})
	}

	before, _ := r.c.activityIDs(d.ID)
	for i := 0; i < 600; i++ {
		if err := delta(fmt.Sprintf("t%d ", i)); err != nil {
			r.fail(name, covers, nil, err)
			return
		}
	}
	afterFlood, _ := r.c.activityIDs(d.ID)
	types, _ := r.c.activityTypes(d.ID)
	audit, _ := os.ReadFile(filepath.Join(r.c.dataDir, "audit", d.ID+".jsonl"))

	resumed, err := r.c.open(d.ID, strconv.FormatUint(base, 10), "")
	if err != nil {
		r.fail(name, covers, nil, err)
		return
	}
	defer resumed.close()
	sentinel, _ := r.emit(d, "tool.result", nil)
	var replayTypes []string
	var replayIDs []uint64
	for i := 0; i < 2; i++ {
		f, err := resumed.next(&r.rec)
		if err != nil {
			break
		}
		replayTypes = append(replayTypes, f.data.Type)
		replayIDs = append(replayIDs, f.data.ID)
	}
	b := strconv.FormatUint(base, 10)
	du := strconv.FormatUint(durable, 10)
	r.add(name, covers,
		map[string]any{
			"live":                  []seen{{"assistant.delta", b, 0}, {"assistant.message", du, durable}, {"assistant.delta", du, 0}},
			"activityHasDelta":      false,
			"auditHasDelta":         false,
			"activityAfter600Delta": before,
			"replayFromBase":        map[string]any{"types": []string{"assistant.message", "tool.result"}, "ids": []uint64{durable, sentinel}},
		},
		map[string]any{
			"live":                  liveGot,
			"activityHasDelta":      contains(types, "assistant.delta"),
			"auditHasDelta":         bytes.Contains(audit, []byte("assistant.delta")),
			"activityAfter600Delta": afterFlood,
			"replayFromBase":        map[string]any{"types": replayTypes, "ids": replayIDs},
		})
}

func (r *runner) sseHeadersHeartbeatLiveOnly() {
	const name, covers = "sse_headers_heartbeat_live_only", "SSE headers, ~15s heartbeat comment, and no history replay without a cursor"
	h, err := r.room()
	if err == nil {
		err = r.emitN(h, 2)
	}
	if err != nil {
		r.fail(name, covers, nil, err)
		return
	}
	s, err := r.c.open(h.ID, "", "")
	if err != nil {
		r.fail(name, covers, nil, err)
		return
	}
	defer s.close()
	heartbeat, messagesBefore := false, 0
	deadline := time.Now().Add(20 * time.Second)
	for !heartbeat && time.Now().Before(deadline) {
		f, err := s.raw(time.Until(deadline))
		if err != nil {
			break
		}
		if f.hasData {
			messagesBefore++
		}
		heartbeat = f.comment == "heartbeat"
	}
	next, _ := r.emit(h, "tool.result", nil)
	liveGot, _, err := r.readIDs(s, 1)
	r.add(name, covers,
		map[string]any{
			"contentType": "text/event-stream", "cacheControl": "no-cache", "xAccelBuffering": "no",
			"heartbeatWithin20s": true, "messagesBeforeFirstPublish": 0, "firstMessage": []uint64{next},
		},
		map[string]any{
			"contentType": s.header.Get("Content-Type"), "cacheControl": s.header.Get("Cache-Control"),
			"xAccelBuffering": s.header.Get("X-Accel-Buffering"), "heartbeatWithin20s": heartbeat,
			"messagesBeforeFirstPublish": messagesBefore, "firstMessage": liveGot, "error": errText(err),
		})
}

func (r *runner) liveSwitchNoDupNoGap() {
	const name, covers = "live_switch_no_dup_no_gap", "acceptance #1: clients resuming while events are published concurrently see every event exactly once, in order"
	const clients, total = 10, 400
	rm, err := r.room()
	if err != nil {
		r.fail(name, covers, nil, err)
		return
	}
	noise, err := r.room()
	if err != nil {
		r.fail(name, covers, nil, err)
		return
	}
	var published atomic.Int64
	pubErr := make(chan error, 1)
	go func() {
		for i := 0; i < total; i++ {
			if err := r.c.ingest(map[string]any{"type": "tool.call", "roomId": rm.ID, "sessionId": rm.SessionID}); err != nil {
				pubErr <- err
				return
			}
			if i%3 == 0 {
				_ = r.c.ingest(map[string]any{"type": "tool.call", "roomId": noise.ID, "sessionId": noise.SessionID})
			}
			published.Add(1)
		}
		pubErr <- nil
	}()

	type client struct {
		cursor uint64
		s      *stream
	}
	var cs []client
	for len(cs) < clients {
		if int(published.Load()) < (len(cs)+1)*total/(clients+1) {
			time.Sleep(time.Millisecond)
			continue
		}
		ids, _ := r.c.activityIDs(rm.ID)
		cursor := ids[len(ids)/2]
		s, err := r.c.open(rm.ID, strconv.FormatUint(cursor, 10), "")
		if err != nil {
			r.fail(name, covers, nil, err)
			return
		}
		defer s.close()
		cs = append(cs, client{cursor, s})
	}
	if err := <-pubErr; err != nil {
		r.fail(name, covers, nil, err)
		return
	}
	final, _ := r.c.activityIDs(rm.ID)

	var mu sync.Mutex
	stats := map[string]int{"clients": 0, "duplicates": 0, "missing": 0, "foreign": 0, "outOfOrder": 0, "readErrors": 0}
	var wg sync.WaitGroup
	for _, cl := range cs {
		wg.Add(1)
		go func(cl client) {
			defer wg.Done()
			want := after(final, cl.cursor)
			var rec recorder
			seen := map[uint64]int{}
			var last uint64
			dup, foreign, order, readErr := 0, 0, 0, 0
			for last < want[len(want)-1] {
				f, err := cl.s.next(&rec)
				if err != nil {
					readErr = 1
					break
				}
				if f.data.TaskID != rm.ID {
					foreign++
					continue
				}
				if seen[f.data.ID]++; seen[f.data.ID] > 1 {
					dup++
				}
				if f.data.ID <= last {
					order++
				}
				last = f.data.ID
			}
			missing := 0
			for _, id := range want {
				if seen[id] == 0 {
					missing++
				}
			}
			mu.Lock()
			defer mu.Unlock()
			stats["clients"]++
			stats["duplicates"] += dup
			stats["missing"] += missing
			stats["foreign"] += foreign
			stats["outOfOrder"] += order
			stats["readErrors"] += readErr
			r.rec.merge(&rec)
		}(cl)
	}
	wg.Wait()
	r.add(name, covers,
		map[string]int{"clients": clients, "duplicates": 0, "missing": 0, "foreign": 0, "outOfOrder": 0, "readErrors": 0},
		stats)
}

func (r *runner) idOnEveryMessage() {
	const name, covers = "id_on_every_message", "acceptance #2: every SSE message of every stream above carries id:, equal to data.id for stored events, strictly increasing per stream"
	r.rec.mu.Lock()
	defer r.rec.mu.Unlock()
	r.add(name, covers,
		map[string]any{"messagesWithoutId": 0, "storedIdMismatch": 0, "nonIncreasingStoredIds": 0, "messagesChecked": ">0"},
		map[string]any{"messagesWithoutId": r.rec.missingID, "storedIdMismatch": r.rec.mismatch, "nonIncreasingStoredIds": r.rec.nonIncreasing, "messagesChecked": positive(r.rec.messages)})
}

func (r *runner) restartForgetsIDs() {
	const name, covers = "restart_forgets_ids", "acceptance #3: after a control restart, pre-restart ids are unknown (room gone: 404 before any stream; known room: reset unknown)"
	x, err := r.room()
	if err == nil {
		err = r.emitN(x, 2)
	}
	if err != nil {
		r.fail(name, covers, nil, err)
		return
	}
	ids, _ := r.c.activityIDs(x.ID)
	preRestart := ids[len(ids)-1]
	if err := r.c.restart(); err != nil {
		r.fail(name, covers, nil, err)
		return
	}
	status, contentType, err := r.c.plainGet(x.ID, strconv.FormatUint(preRestart, 10))
	if err != nil {
		r.fail(name, covers, nil, err)
		return
	}
	y, err := r.room()
	if err != nil {
		r.fail(name, covers, nil, err)
		return
	}
	yIDs, _ := r.c.activityIDs(y.ID)
	s, err := r.c.open(y.ID, strconv.FormatUint(preRestart, 10), "")
	if err != nil {
		r.fail(name, covers, nil, err)
		return
	}
	defer s.close()
	f, err := s.next(&r.rec)
	var reset struct {
		Reason string `json:"reason"`
		LastID uint64 `json:"lastId"`
	}
	_ = json.Unmarshal(f.data.Payload, &reset)
	r.add(name, covers,
		map[string]any{"oldRoomStatus": 404, "oldRoomIsStream": false, "newRoomFirst": "reset", "newRoomReason": "unknown", "newRoomLastId": lastOr0(yIDs)},
		map[string]any{"oldRoomStatus": status, "oldRoomIsStream": strings.HasPrefix(contentType, "text/event-stream"), "newRoomFirst": f.data.Type, "newRoomReason": reset.Reason, "newRoomLastId": reset.LastID, "error": errText(err)})
}

// ---- control process ----

type room struct {
	ID        string `json:"id"`
	SessionID string `json:"sessionId"`
}

type control struct {
	bin, dataDir, logPath, token  string
	base, internalBase, workerURL string
	port, internalPort            int
	cmd                           *exec.Cmd
	worker                        *http.Server
}

func newControl(work, bin string) (*control, error) {
	if bin == "" {
		bin = filepath.Join(work, "orbit-control")
		build := exec.Command("go", "build", "-o", bin, "./cmd/orbit-control")
		build.Stdout, build.Stderr = os.Stderr, os.Stderr
		if err := build.Run(); err != nil {
			return nil, fmt.Errorf("go build ./cmd/orbit-control: %w", err)
		}
	}
	c := &control{bin: bin, dataDir: filepath.Join(work, "data"), logPath: filepath.Join(work, "control.log"), token: "e2e-" + strconv.FormatInt(time.Now().UnixNano(), 36)}
	if err := c.startWorker(); err != nil {
		return nil, err
	}
	port, err := freePort()
	if err != nil {
		return nil, err
	}
	internalPort, err := freePort()
	if err != nil {
		return nil, err
	}
	c.port, c.internalPort = port, internalPort
	c.base = fmt.Sprintf("http://127.0.0.1:%d", port)
	c.internalBase = fmt.Sprintf("http://127.0.0.1:%d", internalPort)
	return c, c.start()
}

// startWorker serves just enough of orbit-worker for control to open sessions.
func (c *control) startWorker() error {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return err
	}
	var n atomic.Int64
	c.worker = &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = fmt.Fprintf(w, `{"sessionId":"sess-%d"}`, n.Add(1))
	})}
	go func() { _ = c.worker.Serve(ln) }()
	c.workerURL = "http://" + ln.Addr().String()
	return nil
}

func (c *control) start() error {
	logFile, err := os.OpenFile(c.logPath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	cmd := exec.Command(c.bin)
	env := []string{}
	for _, kv := range os.Environ() {
		if !strings.HasPrefix(kv, "TEMPORAL_") && !strings.HasPrefix(kv, "ORBIT_") && !strings.HasPrefix(kv, "PORT=") {
			env = append(env, kv)
		}
	}
	cmd.Env = append(env,
		"PORT="+strconv.Itoa(c.port),
		"ORBIT_INTERNAL_ADDR=127.0.0.1:"+strconv.Itoa(c.internalPort),
		"ORBIT_WORKER_URL="+c.workerURL,
		"ORBIT_DATA_DIR="+c.dataDir,
		"ORBIT_INTERNAL_TOKEN="+c.token,
	)
	cmd.Stdout, cmd.Stderr = logFile, logFile
	if err := cmd.Start(); err != nil {
		return err
	}
	c.cmd = cmd
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		resp, err := http.Get(c.base + "/health")
		if err == nil {
			resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				return nil
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
	return errors.New("orbit-control did not become healthy")
}

func (c *control) stop() {
	if c.cmd != nil && c.cmd.Process != nil {
		_ = c.cmd.Process.Kill()
		_ = c.cmd.Wait()
		c.cmd = nil
	}
	if c.worker != nil {
		_ = c.worker.Close()
	}
}

func (c *control) restart() error {
	if c.cmd != nil && c.cmd.Process != nil {
		_ = c.cmd.Process.Kill()
		_ = c.cmd.Wait()
	}
	return c.start()
}

func (c *control) logTail() string {
	raw, _ := os.ReadFile(c.logPath)
	if len(raw) > 4000 {
		raw = raw[len(raw)-4000:]
	}
	return string(raw)
}

func (c *control) createRoom() (room, error) {
	resp, err := http.Post(c.base+"/v1/rooms", "application/json", strings.NewReader(`{"kind":"solo"}`))
	if err != nil {
		return room{}, err
	}
	defer resp.Body.Close()
	var rm room
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return rm, fmt.Errorf("create room: %d %s", resp.StatusCode, body)
	}
	return rm, json.NewDecoder(resp.Body).Decode(&rm)
}

func (c *control) ingest(body map[string]any) error {
	raw, _ := json.Marshal(body)
	req, _ := http.NewRequest(http.MethodPost, c.internalBase+"/internal/events", bytes.NewReader(raw))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+c.token)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusAccepted {
		msg, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("ingest %s: %d %s", body["type"], resp.StatusCode, msg)
	}
	return nil
}

type envelope struct {
	ID      uint64          `json:"id"`
	Type    string          `json:"type"`
	TaskID  string          `json:"taskId"`
	Payload json.RawMessage `json:"payload"`
}

func (c *control) activity(roomID string) ([]envelope, error) {
	resp, err := http.Get(c.base + "/v1/rooms/" + roomID + "/activity")
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	var body struct {
		Items []envelope `json:"items"`
	}
	return body.Items, json.NewDecoder(resp.Body).Decode(&body)
}

func (c *control) activityIDs(roomID string) ([]uint64, error) {
	items, err := c.activity(roomID)
	ids := make([]uint64, 0, len(items))
	for _, it := range items {
		ids = append(ids, it.ID)
	}
	return ids, err
}

func (c *control) activityTypes(roomID string) ([]string, error) {
	items, err := c.activity(roomID)
	types := make([]string, 0, len(items))
	for _, it := range items {
		types = append(types, it.Type)
	}
	return types, err
}

// plainGet requests the SSE endpoint and returns only status and content type.
func (c *control) plainGet(roomID, lastEventID string) (int, string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), readTimeout)
	defer cancel()
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, c.base+"/v1/rooms/"+roomID+"/events", nil)
	req.Header.Set("Last-Event-ID", lastEventID)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return 0, "", err
	}
	resp.Body.Close()
	return resp.StatusCode, resp.Header.Get("Content-Type"), nil
}

// ---- SSE client ----

type frame struct {
	id, comment string
	hasID       bool
	hasData     bool
	data        envelope
}

type stream struct {
	header http.Header
	frames chan frame
	cancel context.CancelFunc
	last   uint64
}

func (c *control) open(roomID, header, query string) (*stream, error) {
	u := c.base + "/v1/rooms/" + roomID + "/events"
	if query != "" {
		u += "?lastEventId=" + query
	}
	ctx, cancel := context.WithCancel(context.Background())
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if header != "" {
		req.Header.Set("Last-Event-ID", header)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		cancel()
		return nil, err
	}
	if resp.StatusCode != http.StatusOK {
		resp.Body.Close()
		cancel()
		return nil, fmt.Errorf("GET %s: status %d", u, resp.StatusCode)
	}
	s := &stream{header: resp.Header, frames: make(chan frame, 4096), cancel: cancel}
	go func() {
		defer close(s.frames)
		defer resp.Body.Close()
		br := bufio.NewReader(resp.Body)
		var f frame
		var data string
		for {
			line, err := br.ReadString('\n')
			if err != nil {
				return
			}
			line = strings.TrimSuffix(line, "\n")
			switch {
			case line == "":
				if f.hasData {
					_ = json.Unmarshal([]byte(data), &f.data)
				}
				s.frames <- f
				f, data = frame{}, ""
			case strings.HasPrefix(line, ":"):
				f.comment = strings.TrimSpace(line[1:])
			case strings.HasPrefix(line, "id: "):
				f.id, f.hasID = line[len("id: "):], true
			case strings.HasPrefix(line, "data: "):
				data, f.hasData = line[len("data: "):], true
			}
		}
	}()
	return s, nil
}

func (s *stream) close() { s.cancel() }

// raw returns the next frame, including comment-only frames.
func (s *stream) raw(timeout time.Duration) (frame, error) {
	select {
	case f, ok := <-s.frames:
		if !ok {
			return frame{}, errors.New("stream closed")
		}
		return f, nil
	case <-time.After(timeout):
		return frame{}, errors.New("timed out waiting for SSE frame")
	}
}

// next returns the next message (skipping comments) and records it for the
// id-on-every-message check.
func (s *stream) next(rec *recorder) (frame, error) {
	deadline := time.Now().Add(readTimeout)
	for {
		f, err := s.raw(time.Until(deadline))
		if err != nil {
			return f, err
		}
		if !f.hasData {
			continue
		}
		rec.observe(s, f)
		return f, nil
	}
}

type recorder struct {
	mu                                           sync.Mutex
	messages, missingID, mismatch, nonIncreasing int
}

func (rec *recorder) observe(s *stream, f frame) {
	rec.mu.Lock()
	defer rec.mu.Unlock()
	rec.messages++
	if !f.hasID || f.id == "" {
		rec.missingID++
	}
	if f.data.ID != 0 {
		if f.id != strconv.FormatUint(f.data.ID, 10) {
			rec.mismatch++
		}
		if f.data.ID <= s.last {
			rec.nonIncreasing++
		}
		s.last = f.data.ID
	}
}

func (rec *recorder) merge(o *recorder) {
	rec.mu.Lock()
	defer rec.mu.Unlock()
	rec.messages += o.messages
	rec.missingID += o.missingID
	rec.mismatch += o.mismatch
	rec.nonIncreasing += o.nonIncreasing
}

// ---- helpers ----

func sameJSON(a, b any) bool {
	return reflect.DeepEqual(normalize(a), normalize(b))
}

func normalize(v any) any {
	raw, _ := json.Marshal(v)
	var out any
	_ = json.Unmarshal(raw, &out)
	if m, ok := out.(map[string]any); ok {
		if e, ok := m["error"]; ok && e == "" {
			delete(m, "error")
		}
	}
	return out
}

func errText(errs ...error) string {
	var parts []string
	for _, err := range errs {
		if err != nil {
			parts = append(parts, err.Error())
		}
	}
	sort.Strings(parts)
	return strings.Join(parts, "; ")
}

func lastOr0(ids []uint64) uint64 {
	if len(ids) == 0 {
		return 0
	}
	return ids[len(ids)-1]
}

func contains(list []string, s string) bool {
	for _, v := range list {
		if v == s {
			return true
		}
	}
	return false
}

func positive(n int) string {
	if n > 0 {
		return ">0"
	}
	return "0"
}

func freePort() (int, error) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return 0, err
	}
	defer ln.Close()
	return ln.Addr().(*net.TCPAddr).Port, nil
}

func commit() string {
	if sha := os.Getenv("GITHUB_SHA"); sha != "" {
		return sha
	}
	out, err := exec.Command("git", "rev-parse", "HEAD").Output()
	if err != nil {
		return ""
	}
	return strings.TrimSpace(string(out))
}

func mustJSON(v any) string {
	raw, _ := json.Marshal(v)
	return string(raw)
}

func fatal(err error) {
	fmt.Fprintln(os.Stderr, err)
	os.Exit(2)
}

//go:build e2e

package persistence

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/mindreon/orbit-control/internal/app"
)

func testSDB07Writers(t *testing.T) {
	const c = "S-DB-7"
	const tenant, u = "t-sdb7", "u-sdb7"
	const extra = 1000
	wk := stubWorker(t, nil)
	srv := startServer(t, serverOpts{tenant: tenant, maxConns: 32, workerURL: wk.URL})
	room := roomID(t, srv.check(t, "S-DB-7/setup/create", c, "create the task that receives 1000 events",
		httpReq{Method: "POST", Path: "/v1/rooms", Headers: user(u), Body: `{"kind":"solo"}`}, httpExp{Status: 200}))
	alias(room, "<room-sdb7>")

	jobs := make(chan int)
	var wg sync.WaitGroup
	var bad atomic.Int32
	for w := 0; w < 32; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range jobs {
				act := sendTo(t, srv.internal, httpReq{Method: "POST", Path: "/internal/events",
					Body: `{"type":"tool.call","roomId":"` + room + `","toolName":"bash","occurredAt":"2026-09-26T00:00:00Z"}`})
				if act.Status != 202 {
					bad.Add(1)
				}
			}
		}()
	}
	for i := 0; i < extra; i++ {
		jobs <- i
	}
	close(jobs)
	wg.Wait()

	ctx := context.Background()
	ownerPool := newPool(t, ownerURL, 2)
	rows, err := ownerPool.Query(ctx, `SELECT seq FROM events WHERE task_id = $1 ORDER BY seq`, room)
	if err != nil {
		t.Fatal(err)
	}
	var seqs []int64
	for rows.Next() {
		var seq int64
		_ = rows.Scan(&seq)
		seqs = append(seqs, seq)
	}
	rows.Close()
	contig := len(seqs) == extra+1
	for i := range seqs {
		if seqs[i] != int64(i+1) {
			contig = false
		}
	}
	mid := int64(500)
	replay := sseIDs(t, srv.base, room, u, strconv.FormatInt(mid, 10))
	replayOK := len(replay) == len(seqs)-int(mid)
	for i, id := range replay {
		if id != mid+1+int64(i) {
			replayOK = false
		}
	}
	record(t, caseInput{ID: "S-DB-7/32-writers-1000-events", Contract: c, Kind: "e2e", FailureModes: []string{"FM-62"},
		Description: "32 writers, 1000 events on one task; a reader reconnects with Last-Event-ID mid-way; seq is contiguous, no loss or duplicate",
		Steps:       []string{"32 concurrent POST /internal/events", "SSE reader reconnect with Last-Event-ID 500", "compare seqs"},
		Request:     map[string]any{"writers": 32, "events": extra, "lastEventId": mid},
		Expected:    map[string]any{"accepted": extra, "contiguousFrom1": true, "replayFromCursor": true},
		Actual:      map[string]any{"rejected": bad.Load(), "count": len(seqs), "contiguousFrom1": contig, "replayCount": len(replay), "replayFromCursor": replayOK},
		Pass:        bad.Load() == 0 && contig && replayOK})
}

var reSSEID = regexp.MustCompile(`(?m)^id: (\d+)$`)

func sseIDs(t *testing.T, base, room, u, last string) []int64 {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, base+"/v1/rooms/"+room+"/events", nil)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set(userHeader, u)
	req.Header.Set("Last-Event-ID", last)
	res, err := http.DefaultClient.Do(req)
	if err != nil && ctx.Err() == nil {
		t.Fatal(err)
	}
	if res == nil {
		return nil
	}
	defer res.Body.Close()
	raw, _ := io.ReadAll(res.Body)
	var ids []int64
	for _, m := range reSSEID.FindAllStringSubmatch(string(raw), -1) {
		n, _ := strconv.ParseInt(m[1], 10, 64)
		ids = append(ids, n)
	}
	return ids
}

func testSDB12Blobs(t *testing.T) {
	const c = "S-DB-12"
	const tenant, u, forged = "t-sdb12", "u-sdb12", "t-forged"
	dir := t.TempDir()
	// 4 MiB is large enough that a streamed reject must not retain the body,
	// and the 1 MiB RSS margin stays above allocator noise in CI.
	const limit = 4 << 20
	wk := stubWorker(t, nil)
	srv := startServer(t, serverOpts{tenant: tenant, maxConns: 4, workerURL: wk.URL, artifactDir: dir, artifactMax: limit})
	room := roomID(t, srv.check(t, "S-DB-12/setup/create", c, "create the task that owns the blob",
		httpReq{Method: "POST", Path: "/v1/rooms", Headers: user(u), Body: `{"kind":"solo"}`}, httpExp{Status: 200}))
	alias(room, "<room-sdb12>")

	body := []byte("orbit-blob")
	sum := sha256.Sum256(body)
	digest := hex.EncodeToString(sum[:])
	postBlob := func(base, digest string, payload io.Reader, extra http.Header) httpAct {
		t.Helper()
		req, err := http.NewRequest(http.MethodPost, base+"/internal/artifact-blobs?taskId="+room+"&tenant="+forged, payload)
		if err != nil {
			t.Fatal(err)
		}
		req.Header.Set("X-Content-Digest", digest)
		req.Header.Set("X-Orbit-Tenant", forged)
		for k, v := range extra {
			req.Header[k] = v
		}
		res, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer res.Body.Close()
		raw, _ := io.ReadAll(res.Body)
		return httpAct{Status: res.StatusCode, Body: string(raw)}
	}
	ok := postBlob(srv.internal, digest, strings.NewReader(string(body)), nil)
	again := postBlob(srv.internal, digest, strings.NewReader(string(body)), nil)
	realPath := filepath.Join(dir, tenant, digest)
	_, realErr := os.Stat(realPath)
	_, forgedErr := os.Stat(filepath.Join(dir, forged, digest))
	record(t, caseInput{ID: "S-DB-12/a-forged-tenant", Contract: c, Kind: "e2e", FailureModes: []string{"FM-63"},
		Description: "a forged tenant in the request is ignored; the file lands under the task's real tenant",
		Steps:       []string{"POST /internal/artifact-blobs with a forged tenant header and query", "stat the real tenant path and the forged path", "POST the same bytes again"},
		Request:     map[string]string{"forgedTenant": forged, "task": room},
		Expected:    map[string]any{"status": 200, "fileUnderRealTenant": true, "forgedPathAbsent": true, "secondStatus": 200},
		Actual:      map[string]any{"status": ok.Status, "body": ok.Body, "fileUnderRealTenant": realErr == nil, "forgedPathAbsent": os.IsNotExist(forgedErr), "secondStatus": again.Status},
		Pass:        ok.Status == 200 && strings.Contains(ok.Body, tenant+"/"+digest) && realErr == nil && os.IsNotExist(forgedErr) && again.Status == 200})

	mismatch := postBlob(srv.internal, strings.Repeat("ab", 32), strings.NewReader(string(body)), nil)
	badName := postBlob(srv.internal, "../x", strings.NewReader(string(body)), nil)
	left := blobTemps(dir)
	record(t, caseInput{ID: "S-DB-12/b-digest-mismatch", Contract: c, Kind: "e2e", FailureModes: []string{"FM-63"},
		Description: "X-Content-Digest mismatch → 422 and no file left; a header that is not 64 hex is 400 and does not affect the path",
		Steps:       []string{"POST with a different 64-hex digest", "POST with X-Content-Digest ../x", "list the artifact directory"},
		Request:     map[string]string{"badDigest": "../x"},
		Expected:    map[string]any{"mismatch": 422, "malformed": 400, "tempsLeft": 0},
		Actual:      map[string]any{"mismatch": mismatch.Status, "malformed": badName.Status, "tempsLeft": left},
		Pass:        mismatch.Status == 422 && badName.Status == 400 && left == 0})

	before := vmRSS()
	tooBig := postBlob(srv.internal, digest, &zeroReader{n: limit + 1}, nil)
	after := vmRSS()
	left = blobTemps(dir)
	rssOK := after-before < 1<<20
	record(t, caseInput{ID: "S-DB-12/c-size-limit", Contract: c, Kind: "e2e", FailureModes: []string{"FM-63"},
		Description: "body of limit+1 bytes → 413, RSS stays far below the body size, no temp file left",
		Steps:       []string{"POST a streaming body of ORBIT_ARTIFACT_MAX_BYTES+1", "read VmRSS", "list temp files"},
		Request:     map[string]any{"limit": limit, "body": limit + 1},
		Expected:    map[string]any{"status": 413, "rssFarBelowBody": true, "tempsLeft": 0},
		Actual:      map[string]any{"status": tooBig.Status, "rssFarBelowBody": rssOK, "tempsLeft": left},
		Pass:        tooBig.Status == 413 && rssOK && left == 0})

	pubBlob := sendTo(t, srv.base, httpReq{Method: "POST", Path: "/internal/artifact-blobs?taskId=" + room, Body: string(body)})
	pubEvents := sendTo(t, srv.base, httpReq{Method: "POST", Path: "/internal/events", Body: `{"type":"tool.call","roomId":"` + room + `"}`})
	intEvents := sendTo(t, srv.internal, httpReq{Method: "POST", Path: "/internal/events",
		Body: `{"type":"tool.call","roomId":"` + room + `","toolName":"bash","occurredAt":"2026-09-26T00:00:02Z"}`})
	record(t, caseInput{ID: "S-DB-12/d-public-listener", Contract: c, Kind: "e2e", FailureModes: []string{"FM-63"},
		Description: "/internal/artifact-blobs and /internal/events on the public listener return 404; the internal listener accepts events",
		Steps:       []string{"POST both paths on the public listener", "POST /internal/events on the internal listener"},
		Request:     map[string]string{"public": "/internal/artifact-blobs", "internal": "/internal/events"},
		Expected:    map[string]any{"publicBlob": 404, "publicEvents": 404, "internalEvents": 202},
		Actual:      map[string]any{"publicBlob": pubBlob.Status, "publicEvents": pubEvents.Status, "internalEvents": intEvents.Status},
		Pass:        pubBlob.Status == 404 && pubEvents.Status == 404 && intEvents.Status == 202})
}

func blobTemps(dir string) int {
	n := 0
	_ = filepath.WalkDir(dir, func(path string, d os.DirEntry, err error) error {
		if err == nil && !d.IsDir() && strings.HasPrefix(d.Name(), ".blob-") {
			n++
		}
		return nil
	})
	return n
}

func vmRSS() int64 {
	raw, err := os.ReadFile("/proc/self/status")
	if err != nil {
		return 0
	}
	for _, line := range strings.Split(string(raw), "\n") {
		if strings.HasPrefix(line, "VmRSS:") {
			fields := strings.Fields(line)
			if len(fields) >= 2 {
				n, _ := strconv.ParseInt(fields[1], 10, 64)
				return n * 1024
			}
		}
	}
	return 0
}

type zeroReader struct{ n int }

func (z *zeroReader) Read(p []byte) (int, error) {
	if z.n <= 0 {
		return 0, io.EOF
	}
	if len(p) > z.n {
		p = p[:z.n]
	}
	for i := range p {
		p[i] = 0
	}
	z.n -= len(p)
	return len(p), nil
}

func TestFM60WriteBack(t *testing.T) {
	const c = "FM-60"
	ctx := context.Background()
	ownerPool := newPool(t, ownerURL, 2)

	// (a) orch: cancel as soon as the Update is received. The resumed turn
	// is still written.
	so := &stubOrch{askApproval: true, completeDelay: 500 * time.Millisecond, resumeText: "resumed-orch"}
	osrv := startServer(t, serverOpts{tenant: "t-fm60-orch", maxConns: 4, orch: so})
	oap, oroom := parkApproval(t, osrv, "FM-60/orch", "t-fm60-orch", "u-fm60-orch")
	oerr := cancelOnDecide(t, osrv, so, oap, "u-fm60-orch")
	owait := waitDecideRow(t, ownerPool, oap, "resumed-orch")
	record(t, caseInput{ID: "FM-60/disconnect-after-approve/orch", Contract: c, Kind: "e2e", FailureModes: []string{"FM-60", "FM-61"},
		Description: "client disconnects immediately after approve on the orch path; the resumed turn is still written once",
		Steps:       []string{"cancel the HTTP request when the decide Update is received", "wait for the background write", "read the room, the approval, and assistant messages"},
		Request:     map[string]string{"decision": "allow", "path": "orch"},
		Expected:    map[string]any{"client": "canceled", "room": "running", "delivery": "delivered", "resultMatchesAttempt": true, "resumedMessages": 1},
		Actual:      map[string]any{"client": errClass(oerr), "room": owait.room, "delivery": owait.delivery, "attempt": owait.attempt, "result": owait.result, "resumedMessages": owait.messages},
		Pass:        errClass(oerr) == "canceled" && owait.room == "running" && owait.delivery == "delivered" && owait.result == owait.attempt && owait.attempt > 0 && owait.messages == 1})

	// (a) worker.
	canceled := make(chan struct{})
	var once sync.Once
	wk := resumeWorker(t, func() { once.Do(func() { close(canceled) }) }, 500*time.Millisecond)
	wsrv := startServer(t, serverOpts{tenant: "t-fm60-worker", maxConns: 4, workerURL: wk.URL})
	wap, _ := parkApproval(t, wsrv, "FM-60/worker", "t-fm60-worker", "u-fm60-worker")
	werr := cancelWhen(t, wsrv, wap, "u-fm60-worker", canceled)
	wwait := waitDecideRow(t, ownerPool, wap, "resumed")
	record(t, caseInput{ID: "FM-60/disconnect-after-approve/worker", Contract: c, Kind: "e2e", FailureModes: []string{"FM-60", "FM-61"},
		Description: "client disconnects immediately after approve on the direct worker path; the resumed turn is still written once",
		Steps:       []string{"cancel the HTTP request when resolveApproval is received", "wait for the background write", "read the room, the approval, and assistant messages"},
		Request:     map[string]string{"decision": "allow", "path": "worker"},
		Expected:    map[string]any{"client": "canceled", "room": "running", "delivery": "delivered", "resultMatchesAttempt": true, "resumedMessages": 1},
		Actual:      map[string]any{"client": errClass(werr), "room": wwait.room, "delivery": wwait.delivery, "attempt": wwait.attempt, "result": wwait.result, "resumedMessages": wwait.messages},
		Pass:        errClass(werr) == "canceled" && wwait.room == "running" && wwait.delivery == "delivered" && wwait.result == wwait.attempt && wwait.attempt > 0 && wwait.messages == 1})

	// (b) duplicate write-back.
	dw := resumeWorker(t, nil, 0)
	dsrv := startServer(t, serverOpts{tenant: "t-fm60-dup", maxConns: 4, workerURL: dw.URL})
	dap, _ := parkApproval(t, dsrv, "FM-60/dup", "t-fm60-dup", "u-fm60-dup")
	dsrv.check(t, "FM-60/duplicate-writeback/decide", c, "the first decide writes the resumed turn",
		httpReq{Method: "POST", Path: "/v1/approvals/" + dap + "/decide", Headers: user("u-fm60-dup"), Body: `{"decision":"allow"}`},
		httpExp{Status: 200, BodyIncludes: []string{`"status":"decided"`, `"deliveryState":"delivered"`}})
	drow := readDecideRow(ctx, ownerPool, dap, "resumed")
	wrote, werr2 := dsrv.runtime.WriteResultAgain(ctx, "t-fm60-dup", dap, drow.attempt)
	drow2 := readDecideRow(ctx, ownerPool, dap, "resumed")
	record(t, caseInput{ID: "FM-60/duplicate-writeback", Contract: c, Kind: "e2e", FailureModes: []string{"FM-60", "FM-61"},
		Description: "a second write-back with the same delivery_attempt inserts nothing",
		Steps:       []string{"decide once", "call WriteResultAgain with that delivery_attempt", "count assistant messages"},
		Request:     map[string]any{"approval": dap, "attempt": drow.attempt},
		Expected:    map[string]any{"secondWrite": false, "messages": 1, "resultMatchesAttempt": true},
		Actual:      map[string]any{"secondWrite": wrote, "writeErr": errClass(werr2), "messages": drow2.messages, "attempt": drow2.attempt, "result": drow2.result},
		Pass:        !wrote && werr2 == nil && drow2.messages == 1 && drow2.result == drow2.attempt})

	// (c) reconciler.
	rw := resumeWorker(t, nil, 0)
	rsrv := startServer(t, serverOpts{tenant: "t-fm60-rec", maxConns: 4, workerURL: rw.URL})
	rsrv.runtime.SetSkipResultWrite(true)
	rap, _ := parkApproval(t, rsrv, "FM-60/rec", "t-fm60-rec", "u-fm60-rec")
	rsrv.check(t, "FM-60/reconcile-unwritten/decide", c, "decide marks the approval delivered and skips the result write",
		httpReq{Method: "POST", Path: "/v1/approvals/" + rap + "/decide", Headers: user("u-fm60-rec"), Body: `{"decision":"allow"}`},
		httpExp{Status: 200, BodyIncludes: []string{`"deliveryState":"delivered"`}})
	before := readDecideRow(ctx, ownerPool, rap, "resumed")
	recErr := rsrv.runtime.ReconcileDeliveredResults(ctx)
	after := readDecideRow(ctx, ownerPool, rap, "resumed")
	record(t, caseInput{ID: "FM-60/reconcile-unwritten", Contract: c, Kind: "e2e", FailureModes: []string{"FM-60", "FM-61"},
		Description: "the reconciler writes a delivered approval whose result_attempt is still null",
		Steps:       []string{"decide with the result write skipped", "read the row", "ReconcileDeliveredResults", "read the row again"},
		Request:     map[string]string{"approval": rap},
		Expected:    map[string]any{"beforeMessages": 0, "beforeResultNull": true, "afterMessages": 1, "afterResultMatches": true},
		Actual:      map[string]any{"beforeMessages": before.messages, "beforeResult": before.result, "reconcileErr": errClass(recErr), "afterMessages": after.messages, "afterAttempt": after.attempt, "afterResult": after.result},
		Pass:        before.messages == 0 && before.result == 0 && recErr == nil && after.messages == 1 && after.result == after.attempt && after.attempt > 0})
	_ = oroom
}

type decideRow struct {
	room, delivery  string
	attempt, result int
	messages        int
}

func readDecideRow(ctx context.Context, owner *pgxpool.Pool, approvalID, text string) decideRow {
	var row decideRow
	var result *int
	_ = owner.QueryRow(ctx, `
		SELECT r.state, COALESCE(a.delivery_state, ''), a.delivery_attempt, a.result_attempt,
		       (SELECT count(*) FROM messages m WHERE m.task_id = a.task_id AND m.text = $2)
		  FROM approvals a JOIN rooms r ON r.id = a.task_id
		 WHERE a.id = $1`, approvalID, text).Scan(&row.room, &row.delivery, &row.attempt, &result, &row.messages)
	if result != nil {
		row.result = *result
	}
	return row
}

func waitDecideRow(t *testing.T, owner *pgxpool.Pool, approvalID, text string) decideRow {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	var row decideRow
	for {
		row = readDecideRow(context.Background(), owner, approvalID, text)
		if row.result == row.attempt && row.attempt > 0 && row.messages == 1 {
			return row
		}
		if time.Now().After(deadline) {
			return row
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func parkApproval(t *testing.T, srv *server, label, tenant, u string) (approvalID, room string) {
	t.Helper()
	room = roomID(t, srv.check(t, label+"/create", "FM-60", "create a task",
		httpReq{Method: "POST", Path: "/v1/rooms", Headers: user(u), Body: `{"kind":"solo"}`}, httpExp{Status: 200}))
	alias(room, "<"+label+"-room>")
	posted := srv.check(t, label+"/park", "FM-60", "a message parks one approval",
		httpReq{Method: "POST", Path: "/v1/rooms/" + room + "/messages", Headers: user(u), Body: `{"message":"list files"}`},
		httpExp{Status: 200, BodyIncludes: []string{`"approval":{`}})
	var body struct {
		Approval *app.Approval `json:"approval"`
	}
	if err := json.Unmarshal([]byte(posted.Body), &body); err != nil || body.Approval == nil {
		t.Fatalf("no approval: %s", posted.Body)
	}
	alias(body.Approval.ID, "<"+label+"-approval>")
	_ = tenant
	return body.Approval.ID, room
}

func cancelOnDecide(t *testing.T, srv *server, so *stubOrch, approvalID, u string) error {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	so.onDecide = cancel
	return postDecide(ctx, srv, approvalID, u)
}

func cancelWhen(t *testing.T, srv *server, approvalID, u string, ready <-chan struct{}) error {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		<-ready
		cancel()
	}()
	return postDecide(ctx, srv, approvalID, u)
}

func postDecide(ctx context.Context, srv *server, approvalID, u string) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, srv.base+"/v1/approvals/"+approvalID+"/decide", strings.NewReader(`{"decision":"allow"}`))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set(userHeader, u)
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer res.Body.Close()
	_, _ = io.Copy(io.Discard, res.Body)
	return nil
}

func errClass(err error) string {
	if err == nil {
		return ""
	}
	if errors.Is(err, context.Canceled) {
		return "canceled"
	}
	return "error"
}

func resumeWorker(t *testing.T, onResolve func(), resumeDelay time.Duration) *httptest.Server {
	t.Helper()
	var n atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		raw, _ := io.ReadAll(r.Body)
		i := n.Add(1)
		switch {
		case strings.HasSuffix(r.URL.Path, "/openSession"):
			fmt.Fprintf(w, `{"sessionId":"sess-fm-%d"}`, i)
		case strings.HasSuffix(r.URL.Path, "/runTurn"):
			if strings.Contains(string(raw), `"resumeAfterApproval":true`) {
				time.Sleep(resumeDelay)
				_, _ = io.WriteString(w, `{"status":"completed","texts":["resumed"]}`)
				return
			}
			_, _ = io.WriteString(w, `{"status":"needs_approval","approval":{"approvalRequestId":"ask-fm","toolName":"bash"},"texts":["parked"]}`)
		case strings.HasSuffix(r.URL.Path, "/resolveApproval"):
			if onResolve != nil {
				onResolve()
			}
			_, _ = io.WriteString(w, `{"applied":true}`)
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(srv.Close)
	return srv
}

func TestLateEventsAfterClose(t *testing.T) {
	const c = "FM-62"
	wk := stubWorker(t, nil)
	srv := startServer(t, serverOpts{tenant: "t-fm62", maxConns: 4, workerURL: wk.URL})
	ownerPool := newPool(t, ownerURL, 2)
	room := roomID(t, srv.check(t, "FM-62/setup/create", c, "create a task",
		httpReq{Method: "POST", Path: "/v1/rooms", Headers: user("u-fm62"), Body: `{"kind":"solo"}`}, httpExp{Status: 200}))
	alias(room, "<room-fm62>")
	srv.check(t, "FM-62/setup/abort", c, "abort closes the task",
		httpReq{Method: "POST", Path: "/v1/rooms/" + room + "/abort", Headers: user("u-fm62")}, httpExp{Status: 200})
	seq := ownerScalar[int64](t, ownerPool, `SELECT last_event_seq FROM rooms WHERE id = $1`, room)
	late := srv.check(t, "FM-62/closed-room-late-event", c, "a worker event after close is 404 and does not move last_event_seq",
		httpReq{Method: "POST", Path: "/internal/events", Body: `{"type":"tool.call","roomId":"` + room + `","toolName":"bash","occurredAt":"2026-09-26T00:00:00Z"}`},
		httpExp{Status: 404})
	seqAfter := ownerScalar[int64](t, ownerPool, `SELECT last_event_seq FROM rooms WHERE id = $1`, room)
	record(t, caseInput{ID: "FM-62/closed-room-seq-unchanged", Contract: c, Kind: "e2e", FailureModes: []string{"FM-62"},
		Description: "last_event_seq does not move when a worker event arrives after close",
		Steps:       []string{"abort the room", "read last_event_seq", "POST /internal/events", "read last_event_seq"},
		Request:     map[string]any{"lateStatus": late.Status},
		Expected:    map[string]any{"unchanged": true},
		Actual:      map[string]any{"before": seq, "after": seqAfter},
		Pass:        late.Status == 404 && seqAfter == seq})

	roomF := roomID(t, srv.check(t, "FM-62/setup/create-failed", c, "create a second task",
		httpReq{Method: "POST", Path: "/v1/rooms", Headers: user("u-fm62"), Body: `{"kind":"solo","title":"will fail"}`}, httpExp{Status: 200}))
	alias(roomF, "<room-fm62-failed>")
	if _, err := ownerPool.Exec(context.Background(), `UPDATE rooms SET state = 'failed' WHERE id = $1`, roomF); err != nil {
		t.Fatal(err)
	}
	seqF := ownerScalar[int64](t, ownerPool, `SELECT last_event_seq FROM rooms WHERE id = $1`, roomF)
	lateF := srv.check(t, "FM-62/failed-room-late-event", c, "a worker event after the room has failed is 404",
		httpReq{Method: "POST", Path: "/internal/events", Body: `{"type":"tool.call","roomId":"` + roomF + `","toolName":"bash","occurredAt":"2026-09-26T00:00:01Z"}`},
		httpExp{Status: 404})
	seqFAfter := ownerScalar[int64](t, ownerPool, `SELECT last_event_seq FROM rooms WHERE id = $1`, roomF)
	record(t, caseInput{ID: "FM-62/failed-room-seq-unchanged", Contract: c, Kind: "e2e", FailureModes: []string{"FM-62"},
		Description: "last_event_seq does not move when a worker event arrives for a failed room",
		Steps:       []string{"mark the room failed", "POST /internal/events", "read last_event_seq"},
		Request:     map[string]any{"lateStatus": lateF.Status},
		Expected:    map[string]any{"unchanged": true},
		Actual:      map[string]any{"before": seqF, "after": seqFAfter},
		Pass:        lateF.Status == 404 && seqFAfter == seqF})
}

func TestC34Transitions(t *testing.T) {
	const c = "C34"
	const tenant = "t-c34"
	ctx := context.Background()
	wk := stubWorker(t, nil)
	srv := startServer(t, serverOpts{tenant: tenant, maxConns: 4, workerURL: wk.URL})
	ownerPool := newPool(t, ownerURL, 2)
	room := roomID(t, srv.check(t, "C34/setup/create", c, "create a task for delivery transitions",
		httpReq{Method: "POST", Path: "/v1/rooms", Headers: user("u-c34"), Body: `{"kind":"solo"}`}, httpExp{Status: 200}))
	alias(room, "<room-c34>")
	ids := []string{"ap_c34_a1", "ap_c34_a2", "ap_c34_a3", "ap_c34_a4", "ap_c34_a5", "ap_c34_a6", "ap_c34_a7"}
	for _, id := range ids {
		if _, err := execAsApp(ctx, srv.appPool, tenant, `
			INSERT INTO approvals (id, tenant_id, task_id, tool_name, status, decision)
			VALUES ($1, $2, $3, 'bash', 'pending', '')`, id, tenant, room); err != nil {
			t.Fatal(err)
		}
	}
	outcomes := map[string]string{}
	pass := true
	note := func(name, got, want string) {
		outcomes[name] = got
		if got != want {
			pass = false
		}
	}
	exec := func(name, want, sql string, args ...any) {
		n, err := execAsApp(ctx, srv.appPool, tenant, sql, args...)
		got := fmt.Sprintf("%d:%s", n, sqlState(err))
		if err != nil {
			got = fmt.Sprintf("%d:%s:%s", n, sqlState(err), pgMessage(err))
		}
		note(name, got, want)
	}
	state := func(id string) string {
		var s string
		_ = ownerPool.QueryRow(ctx, `
			SELECT status || ':' || decision || ':' || COALESCE(delivery_state, '') || ':' || delivery_attempt::text
			  FROM approvals WHERE id = $1`, id).Scan(&s)
		return s
	}
	claim := `
		UPDATE approvals
		   SET status = 'decided', decision = 'allow', decided_at = now(),
		       delivery_state = 'in_flight', delivery_attempt = delivery_attempt + 1,
		       delivery_updated_at = '2000-01-01T00:00:00Z'
		 WHERE tenant_id = $1 AND id = $2 AND status = 'pending' AND delivery_state IS NULL`
	exec("T1", "1:ok", claim, tenant, "ap_c34_a1")
	note("T1-state", state("ap_c34_a1"), "decided:allow:in_flight:1")
	var fresh bool
	_ = ownerPool.QueryRow(ctx, `SELECT delivery_updated_at > '2020-01-01'::timestamptz FROM approvals WHERE id = $1`, "ap_c34_a1").Scan(&fresh)
	note("T1-updated-at-ignored", fmt.Sprintf("%v", fresh), "true")

	exec("T2", "1:ok", `
		UPDATE approvals SET delivery_state = 'delivered', delivery_updated_at = '2000-01-01T00:00:00Z'
		 WHERE tenant_id = $1 AND id = $2 AND delivery_state = 'in_flight' AND delivery_attempt = 1`, tenant, "ap_c34_a1")
	note("T2-state", state("ap_c34_a1"), "decided:allow:delivered:1")
	exec("stale-attempt", "0:ok", `
		UPDATE approvals SET delivery_state = 'unresolved', delivery_updated_at = now()
		 WHERE tenant_id = $1 AND id = $2 AND delivery_state = 'delivered' AND delivery_attempt = 99`, tenant, "ap_c34_a1")
	exec("forbidden", "0:P0001:approval is not pending", `
		UPDATE approvals SET decision = 'reject' WHERE tenant_id = $1 AND id = $2`, tenant, "ap_c34_a1")
	note("forbidden-state", state("ap_c34_a1"), "decided:allow:delivered:1")

	exec("T1-a2", "1:ok", claim, tenant, "ap_c34_a2")
	exec("T3", "1:ok", `
		UPDATE approvals SET delivery_state = 'unknown', delivery_updated_at = now()
		 WHERE tenant_id = $1 AND id = $2 AND delivery_state = 'in_flight' AND delivery_attempt = 1`, tenant, "ap_c34_a2")
	exec("T6", "1:ok", `
		UPDATE approvals SET delivery_state = 'in_flight', delivery_attempt = delivery_attempt + 1, delivery_updated_at = now()
		 WHERE tenant_id = $1 AND id = $2 AND delivery_state = 'unknown' AND delivery_attempt = 1`, tenant, "ap_c34_a2")
	note("T6-state", state("ap_c34_a2"), "decided:allow:in_flight:2")
	exec("T4", "1:ok", `
		UPDATE approvals SET delivery_state = 'not_delivered', delivery_updated_at = now()
		 WHERE tenant_id = $1 AND id = $2 AND delivery_state = 'in_flight' AND delivery_attempt = 2`, tenant, "ap_c34_a2")
	exec("T9", "1:ok", `
		UPDATE approvals
		   SET status = 'pending', decision = '', decided_at = NULL, delivery_state = NULL, delivery_updated_at = now()
		 WHERE tenant_id = $1 AND id = $2 AND status = 'decided' AND delivery_state = 'not_delivered' AND delivery_attempt = 2`, tenant, "ap_c34_a2")
	// status, empty decision, NULL delivery_state (COALESCE ''), attempt 2.
	note("T9-state", state("ap_c34_a2"), "pending:::2")

	exec("T10", "1:ok", `
		UPDATE approvals SET status = 'cancelled', decided_at = now(), delivery_updated_at = now()
		 WHERE tenant_id = $1 AND id = $2 AND status = 'pending' AND delivery_state IS NULL`, tenant, "ap_c34_a4")
	// T10 does not set decision; delivery_state stays NULL; attempt stays 0.
	note("T10-state", state("ap_c34_a4"), "cancelled:::0")

	exec("T1-a7", "1:ok", claim, tenant, "ap_c34_a7")
	exec("T3-a7", "1:ok", `
		UPDATE approvals SET delivery_state = 'unknown', delivery_updated_at = now()
		 WHERE tenant_id = $1 AND id = $2 AND delivery_state = 'in_flight' AND delivery_attempt = 1`, tenant, "ap_c34_a7")
	exec("T7", "1:ok", `
		UPDATE approvals SET delivery_state = 'delivered', delivery_updated_at = now()
		 WHERE tenant_id = $1 AND id = $2 AND delivery_state = 'unknown' AND delivery_attempt = 1`, tenant, "ap_c34_a7")
	note("T7-state", state("ap_c34_a7"), "decided:allow:delivered:1")

	exec("T1-a5", "1:ok", claim, tenant, "ap_c34_a5")
	exec("T3-a5", "1:ok", `
		UPDATE approvals SET delivery_state = 'unknown', delivery_updated_at = now()
		 WHERE tenant_id = $1 AND id = $2 AND delivery_state = 'in_flight' AND delivery_attempt = 1`, tenant, "ap_c34_a5")
	exec("T8", "1:ok", `
		UPDATE approvals SET delivery_state = 'unresolved', delivery_updated_at = now()
		 WHERE tenant_id = $1 AND id = $2 AND delivery_state = 'unknown' AND delivery_attempt = 1`, tenant, "ap_c34_a5")
	exec("T12", "1:ok", `
		UPDATE approvals SET delivery_state = 'delivered', delivery_updated_at = now()
		 WHERE tenant_id = $1 AND id = $2 AND delivery_state = 'unresolved' AND delivery_attempt = 1`, tenant, "ap_c34_a5")
	note("T12-state", state("ap_c34_a5"), "decided:allow:delivered:1")

	exec("T1-a3", "1:ok", claim, tenant, "ap_c34_a3")
	exec("T5", "1:ok", `
		UPDATE approvals SET delivery_state = 'unresolved', delivery_updated_at = now()
		 WHERE tenant_id = $1 AND id = $2 AND delivery_state = 'in_flight' AND delivery_attempt = 1`, tenant, "ap_c34_a3")
	note("T5-state", state("ap_c34_a3"), "decided:allow:unresolved:1")

	exec("T1-a6", "1:ok", claim, tenant, "ap_c34_a6")
	exec("T2-a6", "1:ok", `
		UPDATE approvals SET delivery_state = 'delivered', delivery_updated_at = now()
		 WHERE tenant_id = $1 AND id = $2 AND delivery_state = 'in_flight' AND delivery_attempt = 1`, tenant, "ap_c34_a6")
	exec("T11", "1:ok", `
		UPDATE approvals SET delivery_state = 'unresolved', delivery_updated_at = now()
		 WHERE tenant_id = $1 AND id = $2 AND delivery_state = 'delivered' AND delivery_attempt = 1`, tenant, "ap_c34_a6")
	if _, err := execAsApp(ctx, srv.appPool, tenant, `
		UPDATE rooms SET state = 'failed', failure = '{"code":"DECIDED_APPROVALS_LIMIT","message":"limit"}'::jsonb, updated_at = now()
		 WHERE tenant_id = $1 AND id = $2`, tenant, room); err != nil {
		t.Fatal(err)
	}
	exec("T12-after-fail", "0:P0001:approval is not pending", `
		UPDATE approvals SET delivery_state = 'delivered', delivery_updated_at = now()
		 WHERE tenant_id = $1 AND id = $2 AND delivery_state = 'unresolved' AND delivery_attempt = 1`, tenant, "ap_c34_a6")

	record(t, caseInput{ID: "C34/transitions", Contract: c, Kind: "e2e", FailureModes: []string{"FM-64"},
		Description: "T1–T12 are the only delivery updates; a stale attempt affects 0 rows; a forbidden rewrite raises P0001",
		Steps:       []string{"as orbit_app, apply each C34 transition", "retry T2 with the wrong delivery_attempt", "flip a delivered decision", "fail the room and retry T12"},
		Request:     map[string]string{"role": "orbit_app", "tenant": tenant},
		Expected:    map[string]any{"allMatched": true},
		Actual:      map[string]any{"outcomes": outcomes, "allMatched": pass},
		Pass:        pass})
}

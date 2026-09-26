// Command realstack is the QA sign-off check for SSE Last-Event-ID resume
// (E-LE-1 .. E-LE-4) against the real stack: a Temporal dev server, orbit-orch
// and orbit-worker from the published orbit-runtime image (pinned by digest),
// and the real orbit-control binary. Rooms are driven only through control's
// public API.
//
//	go run ./e2e/realstack run  -runtime-commit <sha> -out artifacts/e2e-real-stack.json -logs e2e-logs
//	go run ./e2e/realstack scan artifacts/e2e-real-stack.json e2e-logs/*.log
//
// Two worker pairs run on their own task queues, each behind its own control:
// "mock" (streaming mock model) and "real" (ORBIT_MODEL_MODE=real against an
// in-process OpenAI-compatible stub). Workers post events to control's
// internal listener through a recording proxy, so SSE payloads can be compared
// with the exact bytes the worker sent.
//
// The report has the commit, component versions, and per case: id, title,
// steps, expected, actual, pass. It has no timestamps, ports, or random ids,
// so two runs on one commit write the same bytes. run exits 1 when a case
// fails; scan exits 1 on any finding.
package main

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
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
	"regexp"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/testsuite"
)

const (
	internalToken   = "e2e-control-internal-token"
	modelKey        = "e2e-stub-model-key-not-real"
	modelName       = "e2e-stub-model"
	nullUsageMarker = "E2E-NULL-USAGE"
	chunkSeparator  = "\x1f"
	caseTimeout     = 240 * time.Second
	readTimeout     = 30 * time.Second
)

var queues = map[string]string{"mock": "orbit-e2e-mock", "real": "orbit-e2e-real"}

// controlLimits are extra control settings per pair. E-LE-6 exercises the
// real pair's tight limits; the mock pair keeps the defaults.
var controlLimits = map[string][]string{
	"real": {
		"ORBIT_SSE_MAX_STREAMS_PER_ROOM=" + strconv.Itoa(ele6PerRoom),
		"ORBIT_SSE_MAX_STREAMS_PER_CLIENT=" + strconv.Itoa(ele6PerClient),
		"ORBIT_INGEST_MAX_BYTES=" + strconv.Itoa(ele6IngestMax),
	},
}

type caseRow struct {
	ID    string   `json:"id"`
	Title string   `json:"title"`
	Steps []string `json:"steps"`
	// Config records settings the case ran with, for comparison across runs.
	Config   any  `json:"config,omitempty"`
	Expected any  `json:"expected"`
	Actual   any  `json:"actual"`
	Pass     bool `json:"pass"`
}

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintln(os.Stderr, "usage: realstack run|scan ...")
		os.Exit(2)
	}
	switch os.Args[1] {
	case "run":
		os.Exit(runCmd(os.Args[2:]))
	case "scan":
		os.Exit(scanCmd(os.Args[2:]))
	default:
		fmt.Fprintln(os.Stderr, "usage: realstack run|scan ...")
		os.Exit(2)
	}
}

func runCmd(args []string) int {
	fs := flag.NewFlagSet("run", flag.ExitOnError)
	out := fs.String("out", "artifacts/e2e-real-stack.json", "report path")
	logs := fs.String("logs", "e2e-logs", "process log directory")
	image := fs.String("runtime-image", "ghcr.io/mindreon/orbit-runtime", "orbit-runtime image repository")
	rtCommit := fs.String("runtime-commit", "", "orbit-runtime commit; its published image tag is pulled and run by digest")
	commit := fs.String("commit", "", "orbit-control commit (default: git rev-parse HEAD)")
	cli := fs.String("temporal-cli", "v1.9.1", "Temporal CLI version for the dev server")
	only := fs.String("only", "", "comma-separated case ids to run (development only)")
	_ = fs.Parse(args)
	if *rtCommit == "" {
		fmt.Fprintln(os.Stderr, "-runtime-commit is required")
		return 2
	}
	if err := os.MkdirAll(*logs, 0o755); err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 2
	}
	st, err := startStack(*image, *rtCommit, *logs, *cli)
	if st != nil {
		defer st.stop()
	}
	var rows []caseRow
	if err != nil {
		rows = append(rows, caseRow{ID: "SETUP", Title: "The real stack starts", Expected: "started", Actual: err.Error()})
	} else {
		all := []struct {
			id string
			fn func(*stack) caseRow
		}{{"E-LE-1", caseELE1}, {"E-LE-2", caseELE2}, {"E-LE-3", caseELE3}, {"E-LE-4", caseELE4}, {"E-LE-6", caseELE6}}
		for _, c := range all {
			if *only != "" && !strings.Contains(","+*only+",", ","+c.id+",") {
				continue
			}
			row := st.guard(c.fn)
			if row.ID == "PANIC" || row.ID == "TIMEOUT" {
				row.ID = c.id
			}
			rows = append(rows, row)
		}
	}
	var failed []string
	for i := range rows {
		rows[i].Expected, rows[i].Actual = normalize(rows[i].Expected), normalize(rows[i].Actual)
		rows[i].Pass = rows[i].Pass || sameJSON(rows[i].Expected, rows[i].Actual)
		if rows[i].ID != "SETUP" && !sameJSON(rows[i].Expected, rows[i].Actual) {
			rows[i].Pass = false
		}
		if !rows[i].Pass {
			failed = append(failed, rows[i].ID)
		}
	}
	report := map[string]any{
		"suite":      "e2e-control-last-event-id",
		"commit":     gitCommit(".", *commit),
		"components": components(st, *rtCommit, *cli),
		"setup": []string{
			"Fresh local Temporal dev server (in-memory), Temporal CLI " + *cli + ".",
			"orbit-orch and orbit-worker as containers of the orbit-runtime image published for runtime commit " + *rtCommit + ", run by digest (components.orbit-runtime-image), host network.",
			"Pair 1 on task queue " + queues["mock"] + ": ORBIT_MODEL_MODE=mock, the streaming mock model.",
			"Pair 2 on task queue " + queues["real"] + ": ORBIT_MODEL_MODE=real against an in-process OpenAI-compatible stub.",
			"Two orbit-control processes built from this commit, one per task queue (TEMPORAL_ADDRESS set), each with a public listener and a separate internal listener (ORBIT_INTERNAL_ADDR).",
			"Workers post events with the internal bearer token to their control's internal listener through a recording proxy.",
			"Rooms are created and driven only through control's public API (POST /v1/rooms, /messages, /v1/approvals/{id}/decide); events are read from GET /v1/rooms/{id}/events and /activity.",
		},
		"cases":  rows,
		"passed": len(rows) - len(failed),
		"failed": nonNil(failed),
	}
	if err := os.MkdirAll(filepath.Dir(*out), 0o755); err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 2
	}
	raw, _ := json.MarshalIndent(report, "", "  ")
	if err := os.WriteFile(*out, append(raw, '\n'), 0o644); err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 2
	}
	for _, r := range rows {
		status := "PASS"
		if !r.Pass {
			status = "FAIL"
			fmt.Fprintf(os.Stderr, "FAIL %s\n  expected: %s\n  actual:   %s\n", r.ID, mustJSON(r.Expected), mustJSON(r.Actual))
		}
		fmt.Printf("%s  %s\n", status, r.ID)
	}
	fmt.Printf("%d/%d passed; report: %s\n", len(rows)-len(failed), len(rows), *out)
	if len(failed) > 0 {
		return 1
	}
	return 0
}

func (st *stack) guard(c func(*stack) caseRow) (row caseRow) {
	done := make(chan caseRow, 1)
	go func() {
		defer func() {
			if p := recover(); p != nil {
				done <- caseRow{ID: "PANIC", Expected: "the case completes", Actual: fmt.Sprint(p)}
			}
		}()
		done <- c(st)
	}()
	select {
	case row = <-done:
	case <-time.After(caseTimeout):
		row = caseRow{ID: "TIMEOUT", Expected: "the case completes", Actual: "timed out"}
	}
	return row
}

// ---- stack ----

type stack struct {
	work, logs     string
	temporal       *testsuite.DevServer
	serverVersion  string
	stub           *http.Server
	stubURL        string
	controls       map[string]*control
	procs          []*exec.Cmd
	containers     []string
	labels         map[string]string
	mu             sync.Mutex
	runtimeImage   string
	pythonVersions map[string]string
}

func startStack(image, rtCommit, logs, cliVersion string) (*stack, error) {
	work, err := os.MkdirTemp("", "e2e-real-stack-")
	if err != nil {
		return nil, err
	}
	st := &stack{work: work, logs: logs, controls: map[string]*control{}, labels: map[string]string{}}

	bin := filepath.Join(work, "orbit-control")
	build := exec.Command("go", "build", "-o", bin, "./cmd/orbit-control")
	build.Stdout, build.Stderr = os.Stderr, os.Stderr
	if err := build.Run(); err != nil {
		return st, fmt.Errorf("go build ./cmd/orbit-control: %w", err)
	}

	tag := image + ":" + rtCommit
	pull := exec.Command("docker", "pull", "-q", tag)
	pull.Stdout, pull.Stderr = os.Stderr, os.Stderr
	if err := pull.Run(); err != nil {
		return st, fmt.Errorf("docker pull %s: %w", tag, err)
	}
	digest, err := exec.Command("docker", "image", "inspect", "--format", "{{index .RepoDigests 0}}", tag).Output()
	if err != nil {
		return st, fmt.Errorf("docker image inspect %s: %w", tag, err)
	}
	st.runtimeImage = strings.TrimSpace(string(digest))
	st.pythonVersions = imageVersions(st.runtimeImage)

	temporalOut, err := os.Create(filepath.Join(logs, "temporal.stdout.log"))
	if err != nil {
		return st, err
	}
	temporalErr, err := os.Create(filepath.Join(logs, "temporal.stderr.log"))
	if err != nil {
		return st, err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	cacheDir := filepath.Join(os.TempDir(), "orbit-e2e-temporal-cli")
	_ = os.MkdirAll(cacheDir, 0o755)
	st.temporal, err = testsuite.StartDevServer(ctx, testsuite.DevServerOptions{
		CachedDownload: testsuite.CachedDownload{Version: cliVersion, DestDir: cacheDir},
		ClientOptions:  &client.Options{Namespace: "default"},
		LogLevel:       "warn",
		Stdout:         temporalOut,
		Stderr:         temporalErr,
	})
	if err != nil {
		return st, fmt.Errorf("temporal dev server: %w", err)
	}
	info, err := st.temporal.Client().WorkflowService().GetSystemInfo(ctx, &workflowservice.GetSystemInfoRequest{})
	if err != nil {
		return st, fmt.Errorf("temporal system info: %w", err)
	}
	st.serverVersion = info.GetServerVersion()
	temporalAddr := st.temporal.FrontendHostPort()

	if err := st.startStub(); err != nil {
		return st, err
	}

	for _, mode := range []string{"mock", "real"} {
		c, err := startControl(bin, work, logs, mode, temporalAddr)
		if err != nil {
			return st, err
		}
		st.controls[mode] = c
		env := []string{
			"PYTHONUNBUFFERED=1",
			"TEMPORAL_ADDRESS=" + temporalAddr,
			"TEMPORAL_NAMESPACE=default",
			"TEMPORAL_TASK_QUEUE=" + queues[mode],
			"ORBIT_EVENT_INGEST_URL=" + c.proxyURL + "/internal/events",
			"ORBIT_INTERNAL_TOKEN=" + internalToken,
			"ORBIT_WORKER_BIND=127.0.0.1",
		}
		if mode == "mock" {
			env = append(env, "ORBIT_MODEL_MODE=mock")
		} else {
			env = append(env,
				"ORBIT_MODEL_MODE=real",
				"ORBIT_MODEL_BASE_URL="+st.stubURL+"/v1",
				"ORBIT_MODEL_API_KEY="+modelKey,
				"ORBIT_MODEL_NAME="+modelName,
				"ORBIT_MODEL_TIMEOUT_SECONDS=10",
			)
		}
		workerPort, err := freePort()
		if err != nil {
			return st, err
		}
		for _, entry := range []string{"orbit-orch", "orbit-worker"} {
			name := fmt.Sprintf("orbit-e2e-%d-%s-%s", os.Getpid(), entry, mode)
			dockerArgs := []string{"run", "--rm", "--name", name, "--network", "host"}
			for _, kv := range append(env, "ORBIT_WORKER_PORT="+strconv.Itoa(workerPort)) {
				dockerArgs = append(dockerArgs, "-e", kv)
			}
			dockerArgs = append(dockerArgs, st.runtimeImage, entry)
			cmd, err := startProcess("docker", dockerArgs, os.Environ(), logs, entry+"-"+mode)
			if err != nil {
				return st, err
			}
			st.procs = append(st.procs, cmd)
			st.containers = append(st.containers, name)
		}
		if err := waitHTTP(fmt.Sprintf("http://127.0.0.1:%d/", workerPort), 60*time.Second); err != nil {
			return st, fmt.Errorf("orbit-worker (%s) health: %w", mode, err)
		}
	}
	return st, nil
}

func (st *stack) stop() {
	var wg sync.WaitGroup
	for _, name := range st.containers {
		wg.Add(1)
		go func(name string) {
			defer wg.Done()
			_ = exec.Command("docker", "stop", "-t", "10", name).Run()
		}(name)
	}
	wg.Wait()
	for _, p := range st.procs {
		done := make(chan struct{})
		go func() { _ = p.Wait(); close(done) }()
		select {
		case <-done:
		case <-time.After(10 * time.Second):
			_ = p.Process.Kill()
		}
	}
	for _, c := range st.controls {
		c.stop()
	}
	if st.stub != nil {
		_ = st.stub.Close()
	}
	if st.temporal != nil {
		_ = st.temporal.Stop()
	}
	_ = os.RemoveAll(st.work)
}

func (st *stack) label(roomID string) string {
	st.mu.Lock()
	defer st.mu.Unlock()
	if l, ok := st.labels[roomID]; ok {
		return l
	}
	return "?"
}

// room creates a room on the given control and gives it a stable label.
func (st *stack) room(mode, label string) (room, error) {
	rm, err := st.controls[mode].createRoom()
	if err == nil {
		st.mu.Lock()
		st.labels[rm.ID] = label
		st.mu.Unlock()
	}
	return rm, err
}

// startStub serves the OpenAI-compatible chat completion endpoint the real
// worker calls. A prompt containing nullUsageMarker gets null token counts.
func (st *stack) startStub() error {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return err
	}
	var mu sync.Mutex
	calls := 0
	mux := http.NewServeMux()
	mux.HandleFunc("POST /v1/chat/completions", func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer "+modelKey {
			w.WriteHeader(http.StatusUnauthorized)
			_, _ = io.WriteString(w, `{"error":{"message":"bad key"}}`)
			return
		}
		raw, _ := io.ReadAll(r.Body)
		mu.Lock()
		calls++
		n := calls
		mu.Unlock()
		usage := map[string]any{"prompt_tokens": 11, "completion_tokens": 7, "total_tokens": 18}
		if bytes.Contains(raw, []byte(nullUsageMarker)) {
			usage = map[string]any{"prompt_tokens": nil, "completion_tokens": nil, "total_tokens": nil}
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"id": fmt.Sprintf("chatcmpl-%d", n), "object": "chat.completion", "created": 1, "model": modelName,
			"choices": []any{map[string]any{
				"index": 0, "finish_reason": "stop",
				"message": map[string]any{"role": "assistant", "content": "stub reply"},
			}},
			"usage": usage,
		})
	})
	st.stub = &http.Server{Handler: mux}
	go func() { _ = st.stub.Serve(ln) }()
	st.stubURL = "http://" + ln.Addr().String()
	return nil
}

// ---- control process + recording proxy ----

type recorded struct {
	raw    []byte
	status int
	roomID string
	typ    string
}

type control struct {
	mode, publicURL, internalURL, proxyURL string
	cmd                                    *exec.Cmd
	proxy                                  *http.Server
	mu                                     sync.Mutex
	posts                                  []recorded
	// slowDeltas delays the proxy's forwarding of a room's assistant.delta posts.
	slowDeltas map[string]time.Duration
}

func startControl(bin, work, logs, mode, temporalAddr string) (*control, error) {
	publicPort, err := freePort()
	if err != nil {
		return nil, err
	}
	internalPort, err := freePort()
	if err != nil {
		return nil, err
	}
	c := &control{
		mode:        mode,
		publicURL:   fmt.Sprintf("http://127.0.0.1:%d", publicPort),
		internalURL: fmt.Sprintf("http://127.0.0.1:%d", internalPort),
	}
	env := append(baseEnv(),
		"PORT="+strconv.Itoa(publicPort),
		"ORBIT_INTERNAL_ADDR=127.0.0.1:"+strconv.Itoa(internalPort),
		"ORBIT_INTERNAL_TOKEN="+internalToken,
		"ORBIT_DATA_DIR="+filepath.Join(work, "control-data-"+mode),
		"TEMPORAL_ADDRESS="+temporalAddr,
		"TEMPORAL_NAMESPACE=default",
		"TEMPORAL_TASK_QUEUE="+queues[mode],
	)
	env = append(env, controlLimits[mode]...)
	c.cmd, err = startProcess(bin, nil, env, logs, "orbit-control-"+mode)
	if err != nil {
		return nil, err
	}
	if err := waitHTTP(c.publicURL+"/health", 60*time.Second); err != nil {
		return nil, fmt.Errorf("orbit-control (%s) public health: %w", mode, err)
	}
	if err := waitHTTP(c.internalURL+"/health", 10*time.Second); err != nil {
		return nil, fmt.Errorf("orbit-control (%s) internal health: %w", mode, err)
	}
	return c, c.startProxy()
}

func (c *control) startProxy() error {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return err
	}
	c.slowDeltas = map[string]time.Duration{}
	c.proxy = &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		raw, _ := io.ReadAll(r.Body)
		var peek struct {
			Type   string `json:"type"`
			RoomID string `json:"roomId"`
		}
		_ = json.Unmarshal(raw, &peek)
		c.mu.Lock()
		delay := c.slowDeltas[peek.RoomID]
		c.mu.Unlock()
		if delay > 0 && peek.Type == "assistant.delta" {
			time.Sleep(delay)
		}
		req, _ := http.NewRequest(r.Method, c.internalURL+r.URL.Path, bytes.NewReader(raw))
		req.Header = r.Header.Clone()
		resp, err := http.DefaultClient.Do(req)
		status := http.StatusBadGateway
		var body []byte
		if err == nil {
			status = resp.StatusCode
			body, _ = io.ReadAll(resp.Body)
			resp.Body.Close()
		}
		var head struct {
			Type   string `json:"type"`
			RoomID string `json:"roomId"`
		}
		_ = json.Unmarshal(raw, &head)
		c.mu.Lock()
		c.posts = append(c.posts, recorded{raw: raw, status: status, roomID: head.RoomID, typ: head.Type})
		c.mu.Unlock()
		w.WriteHeader(status)
		_, _ = w.Write(body)
	})}
	go func() { _ = c.proxy.Serve(ln) }()
	c.proxyURL = "http://" + ln.Addr().String()
	return nil
}

func (c *control) stop() {
	if c.cmd != nil && c.cmd.Process != nil {
		_ = c.cmd.Process.Kill()
		_ = c.cmd.Wait()
	}
	if c.proxy != nil {
		_ = c.proxy.Close()
	}
}

func (c *control) postsFor(roomID string) []recorded {
	c.mu.Lock()
	defer c.mu.Unlock()
	var out []recorded
	for _, p := range c.posts {
		if p.roomID == roomID {
			out = append(out, p)
		}
	}
	return out
}

func (c *control) rejectedWorkerPosts() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	n := 0
	for _, p := range c.posts {
		if p.status != http.StatusAccepted {
			n++
		}
	}
	return n
}

// ---- control public API ----

type room struct {
	ID        string `json:"id"`
	SessionID string `json:"sessionId"`
}

type envelope struct {
	ID      uint64          `json:"id"`
	Type    string          `json:"type"`
	TaskID  string          `json:"taskId"`
	Source  string          `json:"source"`
	Payload json.RawMessage `json:"payload"`
}

var api = &http.Client{Timeout: 2 * time.Minute}

func (c *control) createRoom() (room, error) {
	var rm room
	err := c.doJSON(http.MethodPost, "/v1/rooms", map[string]any{"kind": "solo"}, &rm)
	return rm, err
}

// postMessage runs one turn; it returns the pending approval id, if parked.
func (c *control) postMessage(roomID, message string) (string, error) {
	var out struct {
		Approval *struct {
			ID string `json:"id"`
		} `json:"approval"`
	}
	if err := c.doJSON(http.MethodPost, "/v1/rooms/"+roomID+"/messages", map[string]any{"message": message}, &out); err != nil {
		return "", err
	}
	if out.Approval == nil {
		return "", nil
	}
	return out.Approval.ID, nil
}

func (c *control) decide(approvalID string) error {
	return c.doJSON(http.MethodPost, "/v1/approvals/"+approvalID+"/decide", map[string]any{"decision": "allow"}, nil)
}

func (c *control) activity(roomID string) ([]envelope, error) {
	var out struct {
		Items []envelope `json:"items"`
	}
	err := c.doJSON(http.MethodGet, "/v1/rooms/"+roomID+"/activity", nil, &out)
	return out.Items, err
}

func (c *control) doJSON(method, path string, body, out any) error {
	var rd io.Reader
	if body != nil {
		raw, _ := json.Marshal(body)
		rd = bytes.NewReader(raw)
	}
	req, _ := http.NewRequest(method, c.publicURL+path, rd)
	req.Header.Set("Content-Type", "application/json")
	resp, err := api.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	if resp.StatusCode/100 != 2 {
		return fmt.Errorf("%s %s: %d %s", method, strings.Split(path, "/")[1], resp.StatusCode, firstLine(raw))
	}
	if out != nil {
		return json.Unmarshal(raw, out)
	}
	return nil
}

// ---- SSE client ----

type frame struct {
	id      string
	hasID   bool
	data    []byte
	env     envelope
	comment string
}

type stream struct {
	header http.Header
	frames chan frame
	cancel context.CancelFunc
}

func (c *control) open(roomID, lastEventID string) (*stream, error) {
	ctx, cancel := context.WithCancel(context.Background())
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, c.publicURL+"/v1/rooms/"+roomID+"/events", nil)
	if lastEventID != "" {
		req.Header.Set("Last-Event-ID", lastEventID)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		cancel()
		return nil, err
	}
	if resp.StatusCode != http.StatusOK {
		raw, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		cancel()
		var body struct {
			Code string `json:"code"`
		}
		_ = json.Unmarshal(raw, &body)
		return nil, &openError{Status: resp.StatusCode, Code: body.Code, ContentType: resp.Header.Get("Content-Type")}
	}
	s := &stream{header: resp.Header, frames: make(chan frame, 8192), cancel: cancel}
	go func() {
		defer close(s.frames)
		defer resp.Body.Close()
		br := bufio.NewReaderSize(resp.Body, 1<<20)
		var f frame
		for {
			line, err := br.ReadString('\n')
			if err != nil {
				return
			}
			line = strings.TrimSuffix(line, "\n")
			switch {
			case line == "":
				if f.data != nil {
					_ = json.Unmarshal(f.data, &f.env)
					s.frames <- f
				}
				f = frame{}
			case strings.HasPrefix(line, ":"):
				f.comment = strings.TrimSpace(line[1:])
			case strings.HasPrefix(line, "id: "):
				f.id, f.hasID = line[len("id: "):], true
			case strings.HasPrefix(line, "data: "):
				f.data = []byte(line[len("data: "):])
			}
		}
	}()
	return s, nil
}

func (s *stream) close() { s.cancel() }

// openError is a refused SSE request: the status and ErrorBody code.
type openError struct {
	Status      int
	Code        string
	ContentType string
}

func (e *openError) Error() string { return fmt.Sprintf("SSE status %d %s", e.Status, e.Code) }

var errIdle = errors.New("timed out waiting for an SSE message")

func (s *stream) next(timeout time.Duration) (frame, error) {
	select {
	case f, ok := <-s.frames:
		if !ok {
			return frame{}, errors.New("stream closed")
		}
		return f, nil
	case <-time.After(timeout):
		return frame{}, errIdle
	}
}

// drain reads messages until one satisfies stop, or idle elapses with none.
func (s *stream) drain(idle time.Duration, stop func(frame) bool) []frame {
	var out []frame
	for {
		f, err := s.next(idle)
		if err != nil {
			return out
		}
		out = append(out, f)
		if stop != nil && stop(f) {
			return out
		}
	}
}

// ---- E-LE-1 ----

// prepend returns a stream whose next message is f, followed by s.
func prepend(s *stream, f frame) *stream {
	out := &stream{frames: make(chan frame, 8192), cancel: s.cancel}
	out.frames <- f
	go func() {
		defer close(out.frames)
		for g := range s.frames {
			out.frames <- g
		}
	}()
	return out
}

// ---- E-LE-2 ----

// requiredFields are the A1 fields each type must carry (orbit-runtime README, OrbitEvent schema).
var requiredFields = map[string][]string{
	"assistant.delta": {"type", "roomId", "sessionId", "turnId", "agentId", "agentPath", "blockId", "seq", "delta", "activityAttempt", "modelMode", "modelName", "eventId", "occurredAt"},
	"usage":           {"type", "roomId", "sessionId", "turnId", "agentId", "agentPath", "model", "inputTokens", "outputTokens", "cacheInputTokens", "cacheCreationInputTokens", "latencyMs", "modelMode", "modelName", "eventId", "occurredAt"},
	"turn.failed":     {"type", "roomId", "sessionId", "turnId", "agentId", "agentPath", "failure", "modelMode", "modelName", "eventId", "occurredAt"},
}

var failureFields = []string{"turnId", "agentId", "errorCode", "retryable", "message"}

type typeCheck struct {
	Worker        int      `json:"workerPosted"`
	SSE           int      `json:"sseReceived"`
	Unchanged     bool     `json:"payloadBytesEqualWorkerBody"`
	MissingFields []string `json:"missingRequiredFields"`
}

func checkTypes(posts []recorded, frames []frame, types []string) map[string]typeCheck {
	out := map[string]typeCheck{}
	for _, typ := range types {
		var bodies [][]byte
		for _, p := range posts {
			if p.typ == typ {
				var compact bytes.Buffer
				_ = json.Compact(&compact, p.raw)
				bodies = append(bodies, compact.Bytes())
			}
		}
		var payloads [][]byte
		for _, f := range frames {
			if f.env.Type == typ {
				payloads = append(payloads, f.env.Payload)
			}
		}
		unchanged := len(bodies) > 0 && len(bodies) == len(payloads)
		for i := 0; unchanged && i < len(bodies); i++ {
			unchanged = bytes.Equal(bodies[i], payloads[i])
		}
		missing := map[string]bool{}
		for _, p := range payloads {
			var m map[string]json.RawMessage
			_ = json.Unmarshal(p, &m)
			for _, k := range requiredFields[typ] {
				if _, ok := m[k]; !ok {
					missing[k] = true
				}
			}
			if typ == "turn.failed" {
				var failure map[string]json.RawMessage
				_ = json.Unmarshal(m["failure"], &failure)
				for _, k := range failureFields {
					if _, ok := failure[k]; !ok {
						missing["failure."+k] = true
					}
				}
			}
		}
		out[typ] = typeCheck{Worker: len(bodies), SSE: len(payloads), Unchanged: unchanged, MissingFields: sortedKeys(missing)}
	}
	return out
}

func caseELE2(st *stack) caseRow {
	row := caseRow{
		ID:    "E-LE-2",
		Title: "assistant.delta, turn.failed, and usage arrive over SSE unchanged with all fields; an unknown event type to ingest returns 400",
		Steps: []string{
			"Mock control: create room D, open SSE, POST /messages 'stream:' with 3 Chinese parts of 230 characters (the streaming mock model sends them as 3 provider deltas).",
			"Real control: create room F, open SSE, POST /messages 'E2E-USAGE' (the stub answers with usage), then POST /messages 'E2E-NULL-USAGE please' (HTTP 200 with null token counts: one retryable turn.failed).",
			"For each type, compare every SSE payload with the exact body the worker POSTed (recording proxy, whitespace-compacted), and check the A1 fields for that type.",
			"POST an event of type assistant.thinking with the internal token to each control's internal listener.",
		},
	}
	mock, real := st.controls["mock"], st.controls["real"]
	d, err := st.room("mock", "D")
	if err != nil {
		row.Expected, row.Actual = "room created", err.Error()
		return row
	}
	sd, err := mock.open(d.ID, "")
	if err != nil {
		row.Expected, row.Actual = "SSE opened", err.Error()
		return row
	}
	defer sd.close()
	part := strings.Repeat("模型正在逐字输出一段没有任何空格的中文回答，", 10)
	_, errD := mock.postMessage(d.ID, "stream:"+strings.Join([]string{part, part, part}, chunkSeparator))
	dFrames := sd.drain(3*time.Second, nil)

	f, err := st.room("real", "F")
	if err != nil {
		row.Expected, row.Actual = "room created", err.Error()
		return row
	}
	sf, err := real.open(f.ID, "")
	if err != nil {
		row.Expected, row.Actual = "SSE opened", err.Error()
		return row
	}
	defer sf.close()
	_, errU := real.postMessage(f.ID, "E2E-USAGE")
	_, errF := real.postMessage(f.ID, nullUsageMarker+" please")
	fFrames := sf.drain(3*time.Second, nil)

	mockChecks := checkTypes(mock.postsFor(d.ID), dFrames, []string{"assistant.delta", "usage"})
	realChecks := checkTypes(real.postsFor(f.ID), fFrames, []string{"usage", "turn.failed"})
	var failure map[string]any
	for _, fr := range fFrames {
		if fr.env.Type == "turn.failed" {
			var p struct {
				Failure map[string]any `json:"failure"`
			}
			_ = json.Unmarshal(fr.env.Payload, &p)
			failure = p.Failure
		}
	}

	unknownStatus := map[string]int{}
	for mode, c := range st.controls {
		body, _ := json.Marshal(map[string]any{"type": "assistant.thinking", "roomId": d.ID, "sessionId": d.SessionID})
		req, _ := http.NewRequest(http.MethodPost, c.internalURL+"/internal/events", bytes.NewReader(body))
		req.Header.Set("Authorization", "Bearer "+internalToken)
		req.Header.Set("Content-Type", "application/json")
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			unknownStatus[mode] = 0
			continue
		}
		resp.Body.Close()
		unknownStatus[mode] = resp.StatusCode
	}

	row.Expected = map[string]any{
		"mock": map[string]typeCheck{
			"assistant.delta": {Worker: 3, SSE: 3, Unchanged: true, MissingFields: []string{}},
			"usage":           {Worker: 1, SSE: 1, Unchanged: true, MissingFields: []string{}},
		},
		"real": map[string]typeCheck{
			"usage":       {Worker: 1, SSE: 1, Unchanged: true, MissingFields: []string{}},
			"turn.failed": {Worker: 1, SSE: 1, Unchanged: true, MissingFields: []string{}},
		},
		"turnFailedFailure": map[string]any{
			"turnId": "<control turn id>", "agentId": "main", "errorCode": "provider_error", "retryable": true,
			"message": "模型服务暂时出错，这一轮没跑完。",
		},
		"unknownTypeStatus":   map[string]int{"mock": 400, "real": 400},
		"workerPostsRejected": map[string]int{"mock": 0, "real": 0},
		"errors":              "",
	}
	if failure != nil {
		if tid, ok := failure["turnId"].(string); ok && strings.HasPrefix(tid, "tn_") {
			failure["turnId"] = "<control turn id>"
		}
	}
	row.Actual = map[string]any{
		"mock":                mockChecks,
		"real":                realChecks,
		"turnFailedFailure":   failure,
		"unknownTypeStatus":   unknownStatus,
		"workerPostsRejected": map[string]int{"mock": mock.rejectedWorkerPosts(), "real": real.rejectedWorkerPosts()},
		"errors":              errText(errD, errU, errF),
	}
	return row
}

// ---- E-LE-3 ----

// ---- E-LE-4 ----

func caseELE4(st *stack) caseRow {
	row := caseRow{
		ID:    "E-LE-4",
		Title: "/internal/* returns 404 on the public listener",
		Steps: []string{
			"On each control's public listener: POST /internal/events with the internal token and a valid tool.call body for an existing room; GET /internal/events; GET /internal/anything.",
			"Check the room's activity did not change, and that every event the workers posted to the internal listener was accepted (202).",
		},
	}
	expected := map[string]any{}
	actual := map[string]any{}
	for _, mode := range []string{"mock", "real"} {
		c := st.controls[mode]
		rm, err := st.room(mode, "P-"+mode)
		if err != nil {
			actual[mode] = err.Error()
			continue
		}
		before, _ := c.activity(rm.ID)
		body, _ := json.Marshal(map[string]any{"type": "tool.call", "roomId": rm.ID, "sessionId": rm.SessionID, "toolName": "bash", "callId": "c1"})
		statuses := map[string]string{}
		for _, probe := range []struct{ method, path string }{
			{http.MethodPost, "/internal/events"},
			{http.MethodGet, "/internal/events"},
			{http.MethodGet, "/internal/anything"},
		} {
			var rd io.Reader
			if probe.method == http.MethodPost {
				rd = bytes.NewReader(body)
			}
			req, _ := http.NewRequest(probe.method, c.publicURL+probe.path, rd)
			req.Header.Set("Authorization", "Bearer "+internalToken)
			req.Header.Set("Content-Type", "application/json")
			resp, err := http.DefaultClient.Do(req)
			if err != nil {
				statuses[probe.method+" "+probe.path] = err.Error()
				continue
			}
			raw, _ := io.ReadAll(resp.Body)
			resp.Body.Close()
			var e struct {
				Code string `json:"code"`
			}
			_ = json.Unmarshal(raw, &e)
			statuses[probe.method+" "+probe.path] = strconv.Itoa(resp.StatusCode) + " " + e.Code
		}
		after, _ := c.activity(rm.ID)
		expected[mode] = map[string]any{
			"publicListener": map[string]string{
				"POST /internal/events":  "404 NOT_FOUND",
				"GET /internal/events":   "404 NOT_FOUND",
				"GET /internal/anything": "404 NOT_FOUND",
			},
			"activityUnchanged":             true,
			"workerPostsAcceptedOnInternal": true,
		}
		actual[mode] = map[string]any{
			"publicListener":                statuses,
			"activityUnchanged":             equalIDs(idsAfter(before, 0), idsAfter(after, 0)),
			"workerPostsAcceptedOnInternal": c.rejectedWorkerPosts() == 0 && c.anyPosts(),
		}
	}
	row.Expected, row.Actual = expected, actual
	return row
}

func (c *control) anyPosts() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.posts) > 0
}

// ---- secret scan ----

var scanPatterns = map[string]*regexp.Regexp{
	"database-url":      regexp.MustCompile(`(?i)\b(?:postgres(?:ql)?|mysql|mariadb|mongodb(?:\+srv)?|redis|rediss|amqp|mssql)://`),
	"url-credentials":   regexp.MustCompile(`(?i)\b[a-z][a-z0-9+.-]*://[^/\s:@"]+:[^/\s@"]+@`),
	"openai-key":        regexp.MustCompile(`\bsk-[A-Za-z0-9_-]{6,}`),
	"github-token":      regexp.MustCompile(`\b(?:ghp|gho|ghu|ghs|ghr)_[A-Za-z0-9]{20,}|\bgithub_pat_\w{20,}`),
	"gitlab-token":      regexp.MustCompile(`\bglpat-[A-Za-z0-9_-]{20,}`),
	"aws-access-key":    regexp.MustCompile(`\b(?:AKIA|ASIA)[0-9A-Z]{16}\b`),
	"google-api-key":    regexp.MustCompile(`\bAIza[0-9A-Za-z_-]{35}`),
	"slack-token":       regexp.MustCompile(`\bxox[abprs]-[A-Za-z0-9-]{10,}`),
	"bearer-credential": regexp.MustCompile(`\bBearer\s+[A-Za-z0-9._~+/=-]{8,}`),
	"private-key":       regexp.MustCompile(`-----BEGIN [A-Z ]*PRIVATE KEY-----`),
}

func scanCmd(args []string) int {
	// Flags may follow the file list, so -out is pulled out before parsing.
	var files []string
	outPath := ""
	for i := 0; i < len(args); i++ {
		if args[i] == "-out" && i+1 < len(args) {
			outPath = args[i+1]
			i++
			continue
		}
		files = append(files, args[i])
	}
	fs := flag.NewFlagSet("scan", flag.ExitOnError)
	out := fs.String("out", outPath, "findings file")
	_ = fs.Parse(files)
	planted := []string{internalToken, modelKey}
	var findings []map[string]string
	rules := []string{"planted"}
	for name := range scanPatterns {
		rules = append(rules, name)
	}
	sort.Strings(rules[1:])
	for _, path := range fs.Args() {
		raw, err := os.ReadFile(path)
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			return 2
		}
		for _, value := range planted {
			if bytes.Contains(raw, []byte(value)) {
				findings = append(findings, map[string]string{"file": path, "rule": "planted", "match": fingerprint(value)})
			}
		}
		for _, name := range rules[1:] {
			for _, m := range scanPatterns[name].FindAll(raw, -1) {
				findings = append(findings, map[string]string{"file": path, "rule": name, "match": fingerprint(string(m))})
			}
		}
	}
	result := map[string]any{"scanned": nonNil(fs.Args()), "rules": rules, "findings": nonNilMaps(findings)}
	if *out != "" {
		_ = os.MkdirAll(filepath.Dir(*out), 0o755)
		raw, _ := json.MarshalIndent(result, "", "  ")
		_ = os.WriteFile(*out, append(raw, '\n'), 0o644)
	}
	fmt.Printf("secret scan: %d finding(s) in %d file(s)\n", len(findings), len(fs.Args()))
	if len(findings) > 0 {
		return 1
	}
	return 0
}

// ---- components ----

func components(st *stack, rtCommit, cli string) map[string]string {
	out := map[string]string{
		"orbit-control": gitCommit(".", ""),
		"orbit-runtime": rtCommit,
		"go":            runtime.Version(),
		"temporalCli":   cli,
	}
	if st != nil {
		out["orbit-runtime-image"] = st.runtimeImage
		out["temporalServer"] = st.serverVersion
		for k, v := range st.pythonVersions {
			out[k] = v
		}
	}
	return out
}

// imageVersions reads the Python package versions installed in the runtime image.
func imageVersions(image string) map[string]string {
	script := `import importlib.metadata as m, json, platform
names = ["orbit-contracts", "orbit-orch", "orbit-worker", "agentscope", "temporalio", "pydantic", "openai", "aiohttp"]
print(json.dumps({"python": platform.python_version(), **{n: m.version(n) for n in names}}))`
	raw, err := exec.Command("docker", "run", "--rm", image, "python", "-c", script).Output()
	out := map[string]string{}
	if err != nil {
		out["python"] = "unavailable: " + err.Error()
		return out
	}
	_ = json.Unmarshal(raw, &out)
	return out
}

func gitCommit(dir, explicit string) string {
	if explicit != "" {
		return explicit
	}
	raw, err := exec.Command("git", "-C", dir, "rev-parse", "HEAD").Output()
	if err != nil {
		return ""
	}
	return strings.TrimSpace(string(raw))
}

// ---- helpers ----

func baseEnv() []string {
	var env []string
	for _, kv := range os.Environ() {
		if strings.HasPrefix(kv, "ORBIT_") || strings.HasPrefix(kv, "TEMPORAL_") || strings.HasPrefix(kv, "PORT=") {
			continue
		}
		env = append(env, kv)
	}
	return env
}

// startProcess writes the process's stdout and stderr to <name>.stdout.log and
// <name>.stderr.log under logs.
func startProcess(bin string, args, env []string, logs, name string) (*exec.Cmd, error) {
	stdout, err := os.Create(filepath.Join(logs, name+".stdout.log"))
	if err != nil {
		return nil, err
	}
	stderr, err := os.Create(filepath.Join(logs, name+".stderr.log"))
	if err != nil {
		return nil, err
	}
	cmd := exec.Command(bin, args...)
	cmd.Env = env
	cmd.Stdout, cmd.Stderr = stdout, stderr
	if err := cmd.Start(); err != nil {
		return nil, fmt.Errorf("start %s: %w", filepath.Base(bin), err)
	}
	return cmd, nil
}

func waitHTTP(url string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		resp, err := http.Get(url)
		if err == nil {
			resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				return nil
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
	return errors.New("not ready")
}

func freePort() (int, error) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return 0, err
	}
	defer ln.Close()
	return ln.Addr().(*net.TCPAddr).Port, nil
}

func lastID(items []envelope) uint64 {
	if len(items) == 0 {
		return 0
	}
	return items[len(items)-1].ID
}

func idsAfter(items []envelope, after uint64) []uint64 {
	out := []uint64{}
	for _, it := range items {
		if it.ID > after {
			out = append(out, it.ID)
		}
	}
	return out
}

func equalIDs(a, b []uint64) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func equalRecords(a, b map[string][]uint64) bool {
	if len(a) != len(b) {
		return false
	}
	for k, v := range a {
		if !equalIDs(v, b[k]) {
			return false
		}
	}
	return true
}

// sequenceStats compares an assembled id sequence with the expected one.
func sequenceStats(got, want []uint64) (dups, gaps, nonIncreasing int) {
	seen := map[uint64]int{}
	for i, id := range got {
		if seen[id]++; seen[id] > 1 {
			dups++
		}
		if i > 0 && id <= got[i-1] {
			nonIncreasing++
		}
	}
	for _, id := range want {
		if seen[id] == 0 {
			gaps++
		}
	}
	return dups, gaps, nonIncreasing
}

func sortedKeys(m map[string]bool) []string {
	out := []string{}
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

func sameJSON(a, b any) bool { return mustJSON(normalize(a)) == mustJSON(normalize(b)) }

func normalize(v any) any {
	raw, _ := json.Marshal(v)
	var out any
	_ = json.Unmarshal(raw, &out)
	return out
}

func mustJSON(v any) string {
	raw, _ := json.Marshal(v)
	return string(raw)
}

func errText(errs ...error) string {
	var parts []string
	for _, err := range errs {
		if err != nil {
			parts = append(parts, err.Error())
		}
	}
	return strings.Join(parts, "; ")
}

func firstLine(raw []byte) string {
	s := strings.TrimSpace(string(raw))
	if i := strings.IndexByte(s, '\n'); i >= 0 {
		s = s[:i]
	}
	if len(s) > 200 {
		s = s[:200]
	}
	return s
}

func fingerprint(s string) string {
	sum := sha256.Sum256([]byte(s))
	return "sha256:" + hex.EncodeToString(sum[:])[:12]
}

func nonNil(s []string) []string {
	if s == nil {
		return []string{}
	}
	return s
}

func nonNilMaps(s []map[string]string) []map[string]string {
	if s == nil {
		return []map[string]string{}
	}
	return s
}

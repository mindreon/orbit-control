//go:build e2e

package persistence

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"runtime"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"
)

const contractSHA256 = "053a37bb093e06602a50f6412cd57eb3491161f039b567880ba6650ec6965229"

// Case is one row of the report. Kind is "e2e", "isolated" (must cite FM ids
// from docs/persistence-failure-modes.md), "static" or "process". Status is
// "pass", "fail" or "blocked" (feature outside this PR; see BlockedBy).
type Case struct {
	ID           string          `json:"id"`
	Contract     string          `json:"contract"`
	Kind         string          `json:"kind"`
	FailureModes []string        `json:"failureModes,omitempty"`
	Description  string          `json:"description"`
	Steps        []string        `json:"steps"`
	Request      json.RawMessage `json:"request"`
	Expected     json.RawMessage `json:"expected"`
	Actual       json.RawMessage `json:"actual"`
	Status       string          `json:"status"`
	Pass         bool            `json:"pass"`
	BlockedBy    string          `json:"blockedBy,omitempty"`
}

type caseInput struct {
	ID, Contract, Kind, Description, BlockedBy string
	FailureModes                               []string
	Steps                                      []string
	Request, Expected, Actual                  any
	Pass                                       bool
	Blocked                                    bool
}

var (
	reportMu    sync.Mutex
	cases       []Case
	fatalErrors []string
	anyFailed   bool
	startedAt   = time.Now().UTC()
	aliases     = map[string]string{}
	aliasOrder  []string
	postgresVer string
)

// alias registers a volatile value (generated id, planted secret) that is
// replaced by a stable placeholder everywhere in the report.
func alias(real, placeholder string) {
	if real == "" {
		return
	}
	reportMu.Lock()
	defer reportMu.Unlock()
	if _, ok := aliases[real]; !ok {
		aliasOrder = append(aliasOrder, real)
	}
	aliases[real] = placeholder
}

var volatile = []struct {
	re   *regexp.Regexp
	repl string
}{
	{regexp.MustCompile(`\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d+)?(Z|[+-]\d{2}:\d{2})`), "<ts>"},
	{regexp.MustCompile(`\d{4}/\d{2}/\d{2} \d{2}:\d{2}:\d{2}`), "<logts>"},
	{regexp.MustCompile(`\b(rm|ap|msg|ev|tn|persona|mcp|caj|grant)_[0-9a-f]{16}\b`), "<$1_id>"},
	{regexp.MustCompile(`(listening on |127\.0\.0\.1|localhost):\d+`), "$1:<port>"},
}

// normalize must be called with reportMu held.
func normalize(v any) json.RawMessage {
	raw, err := json.Marshal(v)
	if err != nil {
		raw, _ = json.Marshal(fmt.Sprintf("unmarshalable: %v", err))
	}
	s := string(raw)
	order := append([]string(nil), aliasOrder...)
	sort.SliceStable(order, func(i, j int) bool { return len(order[i]) > len(order[j]) })
	for _, real := range order {
		s = strings.ReplaceAll(s, real, aliases[real])
	}
	for _, v := range volatile {
		s = v.re.ReplaceAllString(s, v.repl)
	}
	return json.RawMessage(s)
}

func deriveSteps(req any) []string {
	switch r := req.(type) {
	case httpReq:
		step := r.Method + " " + r.Path
		if u := r.Headers[userHeader]; u != "" {
			step += " as " + u
		} else {
			step += " without a session"
		}
		var extra []string
		for _, h := range []string{"Origin", "X-Orbit-Request", "Idempotency-Key", "Last-Event-ID"} {
			if v, ok := r.Headers[h]; ok {
				extra = append(extra, h+": "+v)
			}
		}
		if len(extra) > 0 {
			step += " (" + strings.Join(extra, ", ") + ")"
		}
		if r.Body != "" {
			step += " body " + r.Body
		}
		return []string{step, "compare status and body with expected"}
	case sqlReq:
		who := "as " + r.Role
		if r.Tenant != "" {
			who += " with app.tenant_id=" + r.Tenant
		} else if r.Role == "orbit_app" {
			who += " with no app.tenant_id"
		}
		return []string{who + ": " + r.SQL, "compare result with expected"}
	case procReq:
		return []string{"run orbit-control binary with env " + strings.Join(r.Env, " "), r.Probe}
	}
	return []string{"see request"}
}

func recordCase(in caseInput) Case {
	kind := in.Kind
	if kind == "" {
		kind = "e2e"
	}
	steps := in.Steps
	if len(steps) == 0 {
		steps = deriveSteps(in.Request)
	}
	status := "fail"
	switch {
	case in.Blocked:
		status = "blocked"
	case in.Pass:
		status = "pass"
	}
	reportMu.Lock()
	c := Case{
		ID: in.ID, Contract: in.Contract, Kind: kind, FailureModes: in.FailureModes,
		Description: in.Description, BlockedBy: in.BlockedBy,
		Request: normalize(in.Request), Expected: normalize(in.Expected), Actual: normalize(in.Actual),
		Status: status, Pass: status == "pass",
	}
	var stepsNorm []string
	_ = json.Unmarshal(normalize(steps), &stepsNorm)
	c.Steps = stepsNorm
	if status == "fail" {
		anyFailed = true
	}
	cases = append(cases, c)
	reportMu.Unlock()
	return c
}

// record appends a case and fails the test (without stopping it) when the
// case failed, so the report always lists every case.
func record(t *testing.T, in caseInput) bool {
	t.Helper()
	if in.Kind == "isolated" && len(in.FailureModes) == 0 && !in.Blocked {
		t.Fatalf("%s: isolated case without a failure-mode reference", in.ID)
	}
	c := recordCase(in)
	switch c.Status {
	case "fail":
		t.Errorf("%s (%s): expected %s, actual %s", c.ID, c.Contract, c.Expected, c.Actual)
	case "blocked":
		t.Logf("BLOCKED %s (%s): %s", c.ID, c.Contract, c.BlockedBy)
	}
	return c.Pass
}

func blocked(t *testing.T, id, contract, kind, desc, by string, steps []string, expected any) {
	t.Helper()
	record(t, caseInput{ID: id, Contract: contract, Kind: kind, Description: desc, Steps: steps,
		Request: "not run", Expected: expected, Actual: "not implemented in this PR", Blocked: true, BlockedBy: by})
}

func fatalSetup(msg string) {
	reportMu.Lock()
	fatalErrors = append(fatalErrors, msg)
	anyFailed = true
	reportMu.Unlock()
}

func repoRoot() (string, error) {
	dir, err := os.Getwd()
	if err != nil {
		return "", err
	}
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir, nil
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			return "", fmt.Errorf("go.mod not found above %s", dir)
		}
		dir = parent
	}
}

// gitSHA prefers ORBIT_E2E_GIT_SHA: on pull_request runs GITHUB_SHA and the
// checkout are the synthetic merge commit, not the head that gets signed off.
func gitSHA(root string) string {
	for _, env := range []string{"ORBIT_E2E_GIT_SHA", "GITHUB_SHA"} {
		if sha := os.Getenv(env); sha != "" {
			return sha
		}
	}
	out, err := exec.Command("git", "-C", root, "rev-parse", "HEAD").Output()
	if err != nil {
		return "unknown"
	}
	return strings.TrimSpace(string(out))
}

func components() map[string]string {
	out := map[string]string{
		"go":       runtime.Version(),
		"platform": runtime.GOOS + "/" + runtime.GOARCH,
		"postgres": postgresVer,
	}
	if img := os.Getenv("ORBIT_E2E_POSTGRES_IMAGE"); img != "" {
		out["postgresImage"] = img
	}
	if root, err := repoRoot(); err == nil {
		if raw, err := os.ReadFile(filepath.Join(root, "go.mod")); err == nil {
			for _, m := range reModuleVersion.FindAllStringSubmatch(string(raw), -1) {
				out[m[1]] = m[2]
			}
		}
	}
	return out
}

var reModuleVersion = regexp.MustCompile(`(?m)^\s*(?:require\s+)?(github\.com/jackc/pgx/v5|github\.com/pressly/goose/v3|go\.temporal\.io/sdk)\s+(v\S+)`)

// secretNeedles are values that must never appear in the report: DB URLs,
// the DB users' passwords from the environment, and planted secrets.
func secretNeedles() []string {
	needles := []string{"postgres://", "postgresql://", planted, plantedDBPassword, plantedIdemKey, plantedSessionID, "PGPASSWORD", "-----BEGIN"}
	for _, raw := range []string{appURL, ownerURL, opsURL} {
		if u, err := url.Parse(raw); err == nil && u.User != nil {
			if pw, ok := u.User.Password(); ok && pw != "" {
				needles = append(needles, pw)
			}
		}
	}
	return needles
}

var reSecretShapes = regexp.MustCompile(`sk-[A-Za-z0-9-]{12,}|AKIA[0-9A-Z]{16}|ghp_[A-Za-z0-9]{20,}`)

func secretHits(raw []byte) []string {
	var hits []string
	s := string(raw)
	for _, n := range secretNeedles() {
		if n != "" && strings.Contains(s, n) {
			hits = append(hits, "literal of length "+fmt.Sprint(len(n)))
		}
	}
	for _, m := range reSecretShapes.FindAllString(s, -1) {
		hits = append(hits, "pattern match of length "+fmt.Sprint(len(m)))
	}
	return hits
}

// writeReport writes ORBIT_E2E_REPORT (relative to the repo root) or
// artifacts/e2e-persistence-report.json and returns whether the gate passed.
func writeReport() (string, bool, error) {
	root, err := repoRoot()
	if err != nil {
		return "", false, err
	}
	path := os.Getenv("ORBIT_E2E_REPORT")
	if path == "" {
		path = "artifacts/e2e-persistence-report.json"
	}
	if !filepath.IsAbs(path) {
		path = filepath.Join(root, path)
	}

	reportMu.Lock()
	casesJSON, _ := json.Marshal(cases)
	reportMu.Unlock()
	hits := secretHits(casesJSON)
	recordCase(caseInput{
		ID: "report/secret-scan", Contract: "QA gate", Kind: "process",
		Description: "the report contains no DB URL, DB password or planted secret",
		Steps:       []string{"scan the serialized cases for DB URLs, environment DB passwords, planted secrets and key shapes"},
		Request:     "serialized cases", Expected: []string{}, Actual: hits, Pass: len(hits) == 0,
	})

	reportMu.Lock()
	passed, failed, blockedN := 0, 0, 0
	kinds := map[string]int{}
	for _, c := range cases {
		kinds[c.Kind]++
		switch c.Status {
		case "pass":
			passed++
		case "fail":
			failed++
		default:
			blockedN++
		}
	}
	gate := "pass"
	if blockedN > 0 {
		gate = "blocked"
	}
	if failed > 0 || len(fatalErrors) > 0 {
		gate = "fail"
	}
	normCases, _ := json.Marshal(cases)
	sum := sha256.Sum256(normCases)
	report := map[string]any{
		"suite":      "e2e-persistence",
		"contract":   map[string]string{"document": "orbit-contract-draft-v2.md", "section": "§18 (C32 rev2)", "sha256": contractSHA256},
		"gitSha":     gitSHA(root),
		"components": components(),
		"harness": map[string]string{
			"http":          "production httpapi mux + app + pgstore over TCP (httptest.Server)",
			"binary":        "go build ./cmd/orbit-control, run as a separate process (S-DB-3, S-DB-8, S-DB-9)",
			"database":      "real Postgres; the server connects as orbit_app, audit/isolated reads use orbit_owner",
			"authenticator": "in-process HTTP: X-E2E-User stands in for the §17 session; binary: its real local mode",
			"worker":        "stub HTTP server for orbit-worker activities",
			"temporal":      "stub Orchestrator, S-DB-13 (k) only",
		},
		"startedAt":   startedAt,
		"finishedAt":  time.Now().UTC(),
		"gate":        gate,
		"summary":     map[string]any{"total": len(cases), "passed": passed, "failed": failed, "blocked": blockedN, "byKind": kinds, "casesFingerprint": hex.EncodeToString(sum[:])},
		"fatalErrors": fatalErrors,
		"cases":       cases,
	}
	reportMu.Unlock()
	raw, err := json.MarshalIndent(report, "", "  ")
	if err != nil {
		return "", false, err
	}
	if hits := secretHits(raw); len(hits) > 0 {
		return "", false, fmt.Errorf("refusing to write report: secret scan found %v", hits)
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return "", false, err
	}
	if err := os.WriteFile(path, append(raw, '\n'), 0o644); err != nil {
		return "", false, err
	}
	return path, gate != "fail", nil
}

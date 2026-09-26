//go:build e2e

package persistence

import (
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

const contractSHA256 = "053a37bb093e06602a50f6412cd57eb3491161f039b567880ba6650ec6965229"

// Case is one row of the report. Kind is "e2e" (driven through HTTP) or
// "isolated" (docs/persistence-failure-modes.md; FailureModes must be set).
type Case struct {
	ID           string   `json:"id"`
	Contract     string   `json:"contract"`
	Kind         string   `json:"kind"`
	FailureModes []string `json:"failureModes,omitempty"`
	Description  string   `json:"description"`
	Request      any      `json:"request"`
	Expected     any      `json:"expected"`
	Actual       any      `json:"actual"`
	Pass         bool     `json:"pass"`
}

type Report struct {
	Suite       string    `json:"suite"`
	Contract    any       `json:"contract"`
	GitSHA      string    `json:"gitSha"`
	Postgres    string    `json:"postgres"`
	Harness     any       `json:"harness"`
	StartedAt   time.Time `json:"startedAt"`
	FinishedAt  time.Time `json:"finishedAt"`
	Summary     any       `json:"summary"`
	Cases       []Case    `json:"cases"`
	FatalErrors []string  `json:"fatalErrors,omitempty"`
}

var (
	reportMu sync.Mutex
	report   = Report{
		Suite:     "e2e-persistence",
		Contract:  map[string]string{"document": "orbit-contract-draft-v2.md", "section": "§18 (C32 rev2)", "sha256": contractSHA256},
		StartedAt: time.Now().UTC(),
		Harness: map[string]string{
			"http":          "production httpapi mux + app + pgstore served over TCP (httptest.Server)",
			"database":      "real Postgres; server connects as orbit_app, audit/isolated reads as orbit_owner",
			"authenticator": "X-E2E-User header stands in for the §17 session (not implemented yet); tenant fixed per scenario",
			"worker":        "stub HTTP server for orbit-worker activities",
			"temporal":      "stub Orchestrator, S-DB-13 (k) only",
		},
	}
)

// record appends c to the report and fails the test (without stopping it)
// when c did not pass, so the report always lists every case.
func record(t *testing.T, c Case) bool {
	t.Helper()
	if c.Kind == "" {
		c.Kind = "e2e"
	}
	if c.Kind == "isolated" && len(c.FailureModes) == 0 {
		t.Fatalf("%s: isolated case without a failure-mode reference", c.ID)
	}
	reportMu.Lock()
	report.Cases = append(report.Cases, c)
	reportMu.Unlock()
	if !c.Pass {
		exp, _ := json.Marshal(c.Expected)
		act, _ := json.Marshal(c.Actual)
		t.Errorf("%s (%s): expected %s, actual %s", c.ID, c.Contract, exp, act)
	}
	return c.Pass
}

func fatalSetup(msg string) {
	reportMu.Lock()
	report.FatalErrors = append(report.FatalErrors, msg)
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

// writeReport writes the report to ORBIT_E2E_REPORT or
// <repo>/artifacts/e2e-persistence-report.json.
func writeReport() (string, error) {
	root, err := repoRoot()
	if err != nil {
		return "", err
	}
	path := os.Getenv("ORBIT_E2E_REPORT")
	if path == "" {
		path = filepath.Join(root, "artifacts", "e2e-persistence-report.json")
	}
	reportMu.Lock()
	defer reportMu.Unlock()
	report.GitSHA = gitSHA(root)
	report.FinishedAt = time.Now().UTC()
	passed, failed, e2e, isolated := 0, 0, 0, 0
	for _, c := range report.Cases {
		if c.Pass {
			passed++
		} else {
			failed++
		}
		if c.Kind == "isolated" {
			isolated++
		} else {
			e2e++
		}
	}
	report.Summary = map[string]int{"total": len(report.Cases), "passed": passed, "failed": failed, "e2e": e2e, "isolated": isolated}
	raw, err := json.MarshalIndent(report, "", "  ")
	if err != nil {
		return "", err
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return "", err
	}
	return path, os.WriteFile(path, append(raw, '\n'), 0o644)
}

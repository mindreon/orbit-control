//go:build e2e

package persistence

import (
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"testing"
)

var reFMRef = regexp.MustCompile(`"FM-\d+"`)

func git(root string, args ...string) (string, error) {
	out, err := exec.Command("git", append([]string{"-C", root}, args...)...).Output()
	return strings.TrimSpace(string(out)), err
}

// firstCommitAdding returns the oldest commit whose diff adds needle under
// path (git log -S, oldest first).
func firstCommitAdding(root, needle, path string) string {
	out, err := git(root, "log", "--reverse", "--format=%H", "-S", needle, "--", path)
	if err != nil || out == "" {
		return ""
	}
	return strings.SplitN(out, "\n", 2)[0]
}

// Every FM id the checks cite must be introduced in
// docs/persistence-failure-modes.md by a strict ancestor of the commit that
// first cites it under e2e/.
func TestFailureModeDocPrecedesChecks(t *testing.T) {
	root, err := repoRoot()
	if err != nil {
		t.Fatal(err)
	}
	ids := map[string]bool{}
	_ = filepath.WalkDir(filepath.Join(root, "e2e"), func(path string, d os.DirEntry, err error) error {
		if err != nil || d.IsDir() || !strings.HasSuffix(path, ".go") {
			return err
		}
		raw, _ := os.ReadFile(path)
		for _, m := range reFMRef.FindAllString(string(raw), -1) {
			ids[strings.Trim(m, `"`)] = true
		}
		return nil
	})
	sorted := make([]string, 0, len(ids))
	for id := range ids {
		sorted = append(sorted, id)
	}
	sort.Slice(sorted, func(i, j int) bool {
		return len(sorted[i]) < len(sorted[j]) || (len(sorted[i]) == len(sorted[j]) && sorted[i] < sorted[j])
	})
	if _, err := git(root, "rev-parse", "HEAD"); err != nil {
		record(t, caseInput{ID: "process/commit-order", Contract: "QA gate", Kind: "process", Description: "git history is available",
			Request: "git rev-parse HEAD", Expected: "ok", Actual: "no git history", Pass: false})
		return
	}
	for _, id := range sorted {
		doc := firstCommitAdding(root, "**"+id+".**", "docs/persistence-failure-modes.md")
		code := firstCommitAdding(root, `"`+id+`"`, "e2e")
		ordered := false
		if doc != "" && code != "" && doc != code {
			_, ancErr := git(root, "merge-base", "--is-ancestor", doc, code)
			ordered = ancErr == nil
		}
		short := func(s string) string {
			if len(s) >= 12 {
				return s[:12]
			}
			if s == "" {
				return "not found (uncommitted?)"
			}
			return s
		}
		record(t, caseInput{ID: "process/commit-order/" + id, Contract: "QA gate", Kind: "process",
			Description: id + " is documented in a commit that strictly precedes the first check citing it",
			Steps: []string{
				"git log --reverse -S '**" + id + ".**' -- docs/persistence-failure-modes.md",
				"git log --reverse -S '\"" + id + "\"' -- e2e",
				"git merge-base --is-ancestor <doc commit> <code commit>",
			},
			Request:  map[string]string{"failureMode": id},
			Expected: map[string]any{"docCommitStrictlyBeforeCodeCommit": true},
			Actual:   map[string]any{"docCommit": short(doc), "codeCommit": short(code), "docCommitStrictlyBeforeCodeCommit": ordered},
			Pass:     ordered})
	}
}

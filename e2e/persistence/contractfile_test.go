//go:build e2e

package persistence

import (
	"crypto/sha256"
	"encoding/hex"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// contractFile is the contract of record in this repository.
const contractFile = "docs/contracts/orbit-contract-v2.md"

// The shipped contract must be byte-identical to the contract of record:
// sha256 and line count.
func TestContractFileMatchesRevision(t *testing.T) {
	root, err := repoRoot()
	if err != nil {
		t.Fatal(err)
	}
	raw, readErr := os.ReadFile(filepath.Join(root, contractFile))
	sum := sha256.Sum256(raw)
	full := hex.EncodeToString(sum[:])
	lines := strings.Count(string(raw), "\n")
	pinned := contractRevision
	record(t, caseInput{ID: "process/contract-file-sha256", Contract: "QA gate", Kind: "process",
		Description: "the shipped contract file is byte-identical to the C35 blob at " + contractGitRev,
		Steps:       []string{"git show " + contractGitRev + ":" + contractFile, "sha256 that blob", "sha256 the working tree file", "compare"},
		Request:     map[string]string{"file": contractFile, "rev": contractGitRev},
		Expected:    map[string]any{"sha256": contractSHA256, "lines": contractLines, "read": "ok", "revisionNamesC35": true},
		Actual:      map[string]any{"sha256": full, "lines": lines, "read": sqlState(readErr), "revision": pinned},
		Pass:        readErr == nil && contractSHA256 != "" && previousContractSHA256 != "" && full == contractSHA256 && lines == contractLines && strings.Contains(pinned, "C35")})
}

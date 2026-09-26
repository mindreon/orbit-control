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

// The shipped contract must be byte-identical to the Celestial-confirmed
// C32 rev3 file: sha256 and line count.
func TestContractFileMatchesRevision(t *testing.T) {
	root, err := repoRoot()
	if err != nil {
		t.Fatal(err)
	}
	raw, readErr := os.ReadFile(filepath.Join(root, contractFile))
	sum := sha256.Sum256(raw)
	full := hex.EncodeToString(sum[:])
	lines := strings.Count(string(raw), "\n")
	record(t, caseInput{ID: "process/contract-file-sha256", Contract: "QA gate", Kind: "process",
		Description: "the shipped contract file is byte-identical to §18 " + contractRevision,
		Steps:       []string{"read " + contractFile, "sha256 over the raw bytes", "count lines", "compare with the confirmed values"},
		Request:     map[string]string{"file": contractFile},
		Expected:    map[string]any{"sha256": contractSHA256, "lines": 986, "read": "ok"},
		Actual:      map[string]any{"sha256": full, "lines": lines, "read": sqlState(readErr)},
		Pass:        readErr == nil && full == contractSHA256 && lines == 986})
}

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

// contractFile is where the shipped contract lives in the repo.
const contractFile = "docs/contract/orbit-contract-draft-v2.md"

// The relayed rev3 sha256 is abbreviated ("113aebd8…f90b"); the shipped file
// must match its prefix and suffix, and the report carries the full value
// for Sentinel to verify.
func TestContractFileMatchesRevision(t *testing.T) {
	root, err := repoRoot()
	if err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(filepath.Join(root, contractFile))
	if err != nil {
		blocked(t, "process/contract-file-sha256", "QA gate", "process",
			"the shipped contract file matches §18 "+contractRevision+" sha256 "+contractSHA256,
			"the C32 rev3 contract file has not been provided to the implementer; only the rev2 file (sha256 "+previousContractSHA256[:8]+"…) is available",
			[]string{"sha256sum " + contractFile, "compare with the relayed " + contractSHA256}, map[string]string{"sha256": contractSHA256})
		return
	}
	sum := sha256.Sum256(raw)
	full := hex.EncodeToString(sum[:])
	parts := strings.SplitN(contractSHA256, "…", 2)
	match := full == contractSHA256
	if len(parts) == 2 {
		match = strings.HasPrefix(full, parts[0]) && strings.HasSuffix(full, parts[1])
	}
	record(t, caseInput{ID: "process/contract-file-sha256", Contract: "QA gate", Kind: "process",
		Description: "the shipped contract file matches §18 " + contractRevision + " sha256 " + contractSHA256,
		Steps:       []string{"sha256sum " + contractFile, "compare with the relayed " + contractSHA256},
		Request:     map[string]string{"file": contractFile},
		Expected:    map[string]string{"sha256": contractSHA256},
		Actual:      map[string]string{"sha256": full},
		Pass:        match})
}

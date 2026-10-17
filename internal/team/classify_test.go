package team_test

import (
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/mindreon/orbit-control/internal/contract"
	"github.com/mindreon/orbit-control/internal/team"
	"github.com/mindreon/orbit-control/internal/workflows/segloop"
)

func TestAllSegmentCodesClassified(t *testing.T) {
	for _, code := range segloop.Codes {
		require.True(t, team.Classified(code), code)
	}
	for _, code := range []contract.SegmentErrorCode{contract.SegmentErrorCodeAgentError, contract.SegmentErrorCodeLoopDetected,
		contract.SegmentErrorCodeModelError, contract.SegmentErrorCodeSandboxError, contract.SegmentErrorCodeToolPolicyError} {
		require.True(t, team.Classified(strings.ToUpper(string(code))), string(code))
	}
	for _, code := range []string{"TURN_LOST", "TURN_START_CONFLICT", "UNATTENDED_INTERACTION"} {
		require.True(t, team.Classified(code), code)
	}
	files, err := filepath.Glob("../workflows/segloop/*.go")
	require.NoError(t, err)
	pattern := regexp.MustCompile(`"([A-Z_]{4,})"`)
	known := map[string]bool{}
	for _, code := range segloop.Codes {
		known[code] = true
	}
	for _, file := range files {
		raw, err := os.ReadFile(file)
		require.NoError(t, err)
		for _, match := range pattern.FindAllStringSubmatch(string(raw), -1) {
			require.True(t, known[match[1]], file+": "+match[1])
		}
	}
	require.Equal(t, team.Retryable, team.Classify("SEGMENT_FAILED"))
	require.Equal(t, team.Invariant, team.Classify("CHECKPOINT_CONFLICT"))
	require.Equal(t, team.TaskFailure, team.Classify("POLICY_DENIED"))
}

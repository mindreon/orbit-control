package team_test

import (
	"encoding/json/v2"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	contractv2 "github.com/mindreon/orbit-control/contract/v2"
	"github.com/mindreon/orbit-control/internal/team"
	"github.com/mindreon/orbit-control/internal/workflows/segloop"
)

func TestAllSegmentCodesClassified(t *testing.T) {
	for _, code := range segloop.Codes {
		require.True(t, team.Classified(code), code)
	}
	raw, err := contractv2.Schemas.ReadFile("schema/segment_result.json")
	require.NoError(t, err)
	var schema struct {
		Defs map[string]struct {
			Properties map[string]struct {
				Enum []string `json:"enum"`
			} `json:"properties"`
		} `json:"$defs"`
	}
	require.NoError(t, json.Unmarshal(raw, &schema))
	codes := schema.Defs["SegmentError"].Properties["code"].Enum
	require.NotEmpty(t, codes)
	for _, code := range codes {
		require.True(t, team.Classified(strings.ToUpper(code)), code)
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

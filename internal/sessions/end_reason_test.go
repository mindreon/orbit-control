package sessions

import (
	"encoding/json/v2"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/mindreon/orbit-control/internal/contract"
)

func TestEndReasonVectors(t *testing.T) {
	raw, err := os.ReadFile("../../contract/v2/fixtures/end_reason.json")
	require.NoError(t, err)
	var vectors []struct {
		Result    contract.SegmentResult `json:"result"`
		EndReason *string                `json:"end_reason"`
	}
	require.NoError(t, json.Unmarshal(raw, &vectors))
	for _, v := range vectors {
		require.Equal(t, v.EndReason, EndReason(v.Result))
	}
	file, err := parser.ParseFile(token.NewFileSet(), "../contract/contract_gen.go", nil, 0)
	require.NoError(t, err)
	reasons := EndReasons()
	count := 2
	ast.Inspect(file, func(n ast.Node) bool {
		decl, ok := n.(*ast.ValueSpec)
		if !ok || len(decl.Names) != 1 || len(decl.Values) != 1 {
			return true
		}
		name := decl.Names[0].Name
		if !strings.HasPrefix(name, "YieldReason") && !strings.HasPrefix(name, "InteractionKind") &&
			!strings.HasPrefix(name, "SegmentErrorCode") {
			return true
		}
		literal, ok := decl.Values[0].(*ast.BasicLit)
		if !ok {
			return true
		}
		value, err := strconv.Unquote(literal.Value)
		require.NoError(t, err)
		require.Contains(t, reasons, value)
		count++
		return true
	})
	require.Len(t, reasons, count)
	require.Len(t, vectors, count-1+3)
}

func TestRunOpsJSON(t *testing.T) {
	run := Run{TemporalWorkflowID: "workflow/private", TraceID: new("0123456789abcdef0123456789abcdef")}
	raw, err := json.Marshal(run)
	require.NoError(t, err)
	for _, key := range []string{"trace_id", "temporal_workflow_id", "ops"} {
		require.NotContains(t, string(raw), key)
	}
	run.Ops = run.OperatorView("ns/private", "https://example.test/{namespace}/{workflow_id}", "https://trace.test/{trace_id}")
	raw, err = json.Marshal(run)
	require.NoError(t, err)
	require.Contains(t, string(raw), `"ops":{"trace_id":"0123456789abcdef0123456789abcdef"`)
	require.Contains(t, *run.Ops.TemporalURL, "ns%2Fprivate/workflow%2Fprivate")
}

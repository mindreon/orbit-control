package telemetry

import (
	"encoding/json"
	"flag"
	"os"
	"slices"
	"testing"
)

var updateTelemetry = flag.Bool("update", false, "regenerate telemetry golden files")

func TestUpdateGolden(t *testing.T) {
	if !*updateTelemetry {
		return
	}
	keys := slices.Clone(SpanAttributes)
	slices.Sort(keys)
	for file, value := range map[string]any{
		"../../../contract/v2/fixtures/span-allowlist.json": keys,
		"testdata/metrics.golden.json":                      Metrics,
	} {
		raw, err := json.MarshalIndent(value, "", "  ")
		if err != nil {
			t.Fatal(err)
		}
		if err = os.WriteFile(file, append(raw, '\n'), 0644); err != nil {
			t.Fatal(err)
		}
	}
}

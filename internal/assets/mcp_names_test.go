package assets

import (
	"encoding/json/v2"
	"strings"
	"testing"

	"github.com/mindreon/orbit-control/internal/assets/bundle"
)

func TestMCPServerNamesRejectAmbiguousIdentity(t *testing.T) {
	for _, name := range []string{"notify_x", "notify-x", "Notify", "1notify", "", strings.Repeat("a", 64)} {
		raw, err := json.Marshal(map[string]any{"mcpServers": map[string]any{name: map[string]string{"command": "test"}}})
		if err != nil {
			t.Fatal(err)
		}
		_, _, err = ParseManifest([]bundle.File{{Path: "mcp.json", Data: raw}})
		if err == nil || !strings.Contains(err.Error(), "mcp.json") || !strings.Contains(err.Error(), "pattern") {
			t.Fatalf("name %q: expected explicit MCP name validation error, got %v", name, err)
		}
	}
}

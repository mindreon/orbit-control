package log

import (
	"bytes"
	"log/slog"
	"strings"
	"testing"
)

func TestRedaction(t *testing.T) {
	for _, format := range []string{"json", "text"} {
		var b bytes.Buffer
		l := New(&b, format)
		l.Info("test", "authorization", "leak1", slog.Group("nested", "api_key", "leak2", "token", "leak3"), "password", "leak4", "payload", map[string]any{"nested": map[string]any{"secret": "leak5"}}, slog.Group("secrets", "value", "leak6"))
		if strings.Contains(b.String(), "leak") {
			t.Fatal(b.String())
		}
		if !strings.Contains(b.String(), "REDACTED") {
			t.Fatal(b.String())
		}
	}
}

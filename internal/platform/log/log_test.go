package log

import (
	"bytes"
	"log/slog"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/trace"
)

func TestRedaction(t *testing.T) {
	for _, format := range []string{"json", "text"} {
		var b bytes.Buffer
		l := New(&b, format)
		l.Info(
			"test",
			"authorization",
			"leak1",
			slog.Group("nested", "api_key", "leak2", "token", "leak3"),
			"password",
			"leak4",
			"payload",
			map[string]any{"nested": map[string]any{"secret": "leak5"}},
			slog.Group("secrets", "value", "leak6"),
		)
		if strings.Contains(b.String(), "leak") {
			t.Fatal(b.String())
		}
		if !strings.Contains(b.String(), "REDACTED") {
			t.Fatal(b.String())
		}
	}
}

func TestTraceFieldsPreserveRedaction(t *testing.T) {
	for _, format := range []string{"json", "text"} {
		var b bytes.Buffer
		logger := New(&b, format).With("secret", "ORBIT-CANARY").WithGroup("request")
		sc := trace.NewSpanContext(trace.SpanContextConfig{TraceID: trace.TraceID{1}, SpanID: trace.SpanID{2}})
		logger.InfoContext(trace.ContextWithSpanContext(t.Context(), sc), "safe", "token", "ORBIT-CANARY")
		require.Contains(t, b.String(), sc.TraceID().String())
		require.Contains(t, b.String(), sc.SpanID().String())
		require.NotContains(t, b.String(), "ORBIT-CANARY")
		b.Reset()
		logger.InfoContext(t.Context(), "safe")
		require.NotContains(t, b.String(), "trace_id")
		require.NotContains(t, b.String(), "span_id")
	}
}

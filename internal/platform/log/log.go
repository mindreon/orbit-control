package log

import (
	"encoding/json"
	"io"
	"log/slog"
	"strings"
)

func sensitive(k string) bool {
	k = strings.ToLower(k)
	for _, s := range []string{"password", "secret", "token", "authorization", "cookie", "ciphertext", "database_url", "api_key", "request_body"} {
		if strings.Contains(k, s) {
			return true
		}
	}
	return false
}
func redact(v any) any {
	switch x := v.(type) {
	case map[string]any:
		for k, v := range x {
			if sensitive(k) {
				x[k] = "[REDACTED]"
			} else {
				x[k] = redact(v)
			}
		}
	case []any:
		for i, v := range x {
			x[i] = redact(v)
		}
	}
	return v
}
func New(w io.Writer, format string) *slog.Logger {
	opts := &slog.HandlerOptions{ReplaceAttr: func(groups []string, a slog.Attr) slog.Attr {
		for _, g := range groups {
			if sensitive(g) {
				return slog.String(a.Key, "[REDACTED]")
			}
		}
		if sensitive(a.Key) {
			return slog.String(a.Key, "[REDACTED]")
		}
		if a.Value.Kind() == slog.KindAny {
			b, e := json.Marshal(a.Value.Any())
			if e == nil {
				var v any
				if json.Unmarshal(b, &v) == nil {
					a.Value = slog.AnyValue(redact(v))
				}
			}
		}
		return a
	}}
	if format == "text" {
		return slog.New(slog.NewTextHandler(w, opts))
	}
	return slog.New(slog.NewJSONHandler(w, opts))
}

// PanicValue keeps structured diagnostics while redacting sensitive fields.
// Arbitrary strings and errors may contain credentials, so their type is the
// only safe representation; the separately logged stack locates the panic.
func PanicValue(v any) any {
	switch x := v.(type) {
	case map[string]any:
		copy := map[string]any{}
		for k, v := range x {
			copy[k] = v
		}
		return redact(copy)
	default:
		return "[REDACTED]"
	}
}

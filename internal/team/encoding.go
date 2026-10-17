package team

import (
	"unicode/utf8"
)

// EncodedLen measures the default Temporal JSON converter's representation.
// Team state contains only JSON values; an unencodable value must fail closed.
func EncodedLen(v any) int {
	raw, err := Marshal(v)
	if err != nil {
		return int(^uint(0) >> 1)
	}
	return len(raw)
}

// TruncateEncoded retains the longest rune-aligned prefix fitting n encoded bytes.
// A JSON string needs two quote bytes, so n < 2 can only yield an empty string.
func TruncateEncoded(s string, n int) string {
	if EncodedLen(s) <= n {
		return s
	}
	used, end := 2, 0
	for pos, r := range s {
		cost := EncodedLen(string(r)) - 2
		if used+cost > n {
			break
		}
		used += cost
		end = pos + utf8.RuneLen(r)
	}
	return s[:end]
}

func ValidTools(tools []string) bool {
	if len(tools) > NarrowedToolsMax {
		return false
	}
	for _, tool := range tools {
		if len(tool) > ToolNameBytes {
			return false
		}
	}
	return true
}

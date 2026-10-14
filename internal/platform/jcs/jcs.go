// Package jcs implements RFC 8785 JSON Canonicalization Scheme. Numbers use
// IEEE 754 binary64, as required by JCS; encode larger exact integers as strings.
package jcs

import (
	"bytes"
	"cmp"
	"encoding/json/jsontext"
	"encoding/json/v2"
	"fmt"
	"math"
	"slices"
	"strconv"
	"strings"
	"unicode/utf16"
)

// Canonicalize rejects invalid UTF-8, lone surrogates and duplicate object names.
// It preserves array order and sorts object names by UTF-16 code units.
func Canonicalize(input []byte) ([]byte, error) {
	var value any
	if err := json.Unmarshal(input, &value); err != nil {
		return nil, err
	}
	return appendValue(nil, value)
}
func appendValue(dst []byte, v any) ([]byte, error) {
	switch x := v.(type) {
	case nil:
		return append(dst, "null"...), nil
	case bool:
		return strconv.AppendBool(dst, x), nil
	case string:
		b, err := json.Marshal(x, jsontext.EscapeForHTML(false), jsontext.EscapeForJS(false))
		return append(dst, b...), err
	case float64:
		if math.IsInf(x, 0) || math.IsNaN(x) {
			return nil, fmt.Errorf("JCS requires finite numbers")
		}
		if x == 0 {
			return append(dst, '0'), nil
		}
		format := byte('f')
		if math.Abs(x) < 1e-6 || math.Abs(x) >= 1e21 {
			format = 'e'
		}
		s := strconv.FormatFloat(x, format, -1, 64)
		// ECMAScript emits e-7 rather than Go's e-07 (and likewise e+9).
		if mantissa, exp, ok := strings.Cut(s, "e"); ok {
			sign := exp[:1]
			exp = strings.TrimLeft(exp[1:], "0")
			s = mantissa + "e" + sign + exp
		}
		return append(dst, s...), nil
	case []any:
		dst = append(dst, '[')
		for i, item := range x {
			if i > 0 {
				dst = append(dst, ',')
			}
			var err error
			dst, err = appendValue(dst, item)
			if err != nil {
				return nil, err
			}
		}
		return append(dst, ']'), nil
	case map[string]any:
		keys := make([]string, 0, len(x))
		for k := range x {
			keys = append(keys, k)
		}
		slices.SortFunc(keys, func(a, b string) int {
			return slices.CompareFunc(utf16.Encode([]rune(a)), utf16.Encode([]rune(b)), cmp.Compare[uint16])
		})
		dst = append(dst, '{')
		for i, k := range keys {
			if i > 0 {
				dst = append(dst, ',')
			}
			var err error
			dst, err = appendValue(dst, k)
			if err != nil {
				return nil, err
			}
			dst = append(dst, ':')
			dst, err = appendValue(dst, x[k])
			if err != nil {
				return nil, err
			}
		}
		return append(dst, '}'), nil
	default:
		return nil, fmt.Errorf("unsupported JCS value %T", v)
	}
}

// Marshal canonicalizes a Go value after JSON encoding. Raw JSON is validated
// by Canonicalize rather than trusted as pre-canonicalized content.
func Marshal(v any) ([]byte, error) {
	b, err := json.Marshal(v)
	if err != nil {
		return nil, err
	}
	return Canonicalize(bytes.TrimSpace(b))
}

package team

import (
	"encoding/json"
	"strings"
	"testing"
	"unicode/utf8"

	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/converter"
)

func TestEncodedLenMatchesTemporal(t *testing.T) {
	for _, value := range []any{"<>&\x00\n\"\\ 界", map[string]string{"x": "<>&"}, []string{}, nil} {
		if value == nil {
			continue
		} // Temporal handles nil with binary/null before the JSON converter.
		payload, err := converter.GetDefaultDataConverter().ToPayload(value)
		require.NoError(t, err)
		require.Equal(t, len(payload.Data), EncodedLen(value))
	}
	// Unsupported input must never be accepted by a capacity check.
	require.Greater(t, EncodedLen(make(chan int)), BoardBytesMax)
}

func TestTruncateEncoded(t *testing.T) {
	for _, s := range []string{"", "plain", "界🙂", "<>&\x00\n\"\\ ", strings.Repeat("<界", 200)} {
		for n := 2; n <= EncodedLen(s)+1; n++ {
			got := TruncateEncoded(s, n)
			require.True(t, strings.HasPrefix(s, got))
			require.True(t, utf8.ValidString(got))
			require.LessOrEqual(t, EncodedLen(got), n)
			if len(got) < len(s) {
				_, size := utf8.DecodeRuneInString(s[len(got):])
				require.Greater(t, EncodedLen(s[:len(got)+size]), n)
			}
		}
	}
	require.Empty(t, TruncateEncoded("abc", 0))
	require.Empty(t, TruncateEncoded("abc", 1))
}

func TestValidTools(t *testing.T) {
	require.True(t, ValidTools(make([]string, 32)))
	require.False(t, ValidTools(make([]string, 33)))
	require.True(t, ValidTools([]string{strings.Repeat("x", 128)}))
	require.False(t, ValidTools([]string{strings.Repeat("x", 129)}))
	raw, err := json.Marshal("<")
	require.NoError(t, err)
	require.Equal(t, "\"\\u003c\"", string(raw))
}

func TestTruncateEncodedUsesDecodedWidthForInvalidUTF8(t *testing.T) {
	input := "a\xffb"
	got := TruncateEncoded(input, EncodedLen("a\xff"))
	require.Equal(t, "a\xff", got)
	require.LessOrEqual(t, EncodedLen(got), EncodedLen("a\xff"))
}

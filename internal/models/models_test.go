package models

import (
	"math"
	"testing"
)

func TestPrice(t *testing.T) {
	for _, tc := range []struct {
		tokens     int
		rate, want int64
	}{{tokens: 0,
		rate: 1,
		want: 0},
		{tokens: 1,
			rate: 1000,
			want: 1},
		{tokens: 1001,
			rate: 1000,
			want: 2},
		{tokens: 1000000,
			rate: 2000,
			want: 2000},
		{tokens: 1000000,
			rate: math.MaxInt64,
			want: math.MaxInt64}} {
		got, e := Price(tc.tokens, tc.rate)
		if e != nil || got != tc.want {
			t.Fatalf("%+v: %d %v", tc, got, e)
		}
	}
	if _, e := Price(math.MaxInt, math.MaxInt64); e == nil {
		t.Fatal("overflow")
	}
}

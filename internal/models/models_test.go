package models

import (
	"math"
	"testing"
)

func TestPrice(t *testing.T) {
	for _, tc := range []struct {
		tokens     int
		rate, want int64
	}{{0, 1, 0}, {1, 1000, 1}, {1001, 1000, 2}, {1000000, 2000, 2000}, {1000000, math.MaxInt64, math.MaxInt64}} {
		got, e := Price(tc.tokens, tc.rate)
		if e != nil || got != tc.want {
			t.Fatalf("%+v: %d %v", tc, got, e)
		}
	}
	if _, e := Price(math.MaxInt, math.MaxInt64); e == nil {
		t.Fatal("overflow")
	}
}

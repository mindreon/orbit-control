package task

import "testing"

func TestPolicyMaxConcurrencyIsValidated(t *testing.T) {
	two, zero, tooMany := 2, 0, MaxConcurrencyLimit+1
	if err := (Policy{MaxConcurrency: &two}).validate(); err != nil {
		t.Fatalf("2 is valid: %v", err)
	}
	for _, bad := range []*int{&zero, &tooMany} {
		if err := (Policy{MaxConcurrency: bad}).validate(); err == nil {
			t.Fatalf("%d must be refused", *bad)
		}
	}
}

func TestSmallestTakesTheTighterCap(t *testing.T) {
	two, five := 2, 5
	if got := smallest(&five, &two); *got != 2 {
		t.Fatalf("got %d", *got)
	}
	if got := smallest(nil, &five); *got != 5 {
		t.Fatalf("got %d", *got)
	}
	if got := smallest(&two, nil); *got != 2 {
		t.Fatalf("got %d", *got)
	}
	if smallest(nil, nil) != nil {
		t.Fatal("no cap on either side is no cap")
	}
}

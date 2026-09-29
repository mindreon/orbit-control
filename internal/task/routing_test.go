package task

import "testing"

func TestRingRoutesStableAndUnknownWhenEmpty(t *testing.T) {
	ring := NewRing(32)
	if _, ok := ring.Owner("task_01"); ok {
		t.Fatal("empty ring returned an owner")
	}
	ring.SetMembers([]string{"http://control-0", "http://control-1"})
	first, ok := ring.Owner("task_01")
	if !ok || first == "" {
		t.Fatal("member ring did not return an owner")
	}
	second, ok := ring.Owner("task_01")
	if !ok || second != first {
		t.Fatalf("same task moved without membership change: %q -> %q", first, second)
	}
}

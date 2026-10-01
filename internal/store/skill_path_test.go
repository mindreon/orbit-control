package store

import "testing"

func TestSkillPathID(t *testing.T) {
	id, ok := SkillPathID("@indiv-ebandao", "dev-expert")
	if !ok || id != "@indiv-ebandao/dev-expert" {
		t.Fatalf("id=%q ok=%v", id, ok)
	}
	if id, ok := SkillPathID("Tashanworld", "academic-writing"); !ok || id != "Tashanworld/academic-writing" {
		t.Fatalf("a handle without @ is valid: id=%q ok=%v", id, ok)
	}
	if _, ok := SkillPathID("bad/handle", "slug"); ok {
		t.Fatal("a slash in the handle was accepted")
	}
	if _, ok := SkillPathID("handle", ""); ok {
		t.Fatal("an empty slug was accepted")
	}
	if _, ok := SkillPathID("has space", "slug"); ok {
		t.Fatal("a space was accepted")
	}
	if _, ok := SkillPathID("", "slug"); ok {
		t.Fatal("an empty handle was accepted")
	}
}

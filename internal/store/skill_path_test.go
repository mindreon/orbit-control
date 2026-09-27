package store

import "testing"

func TestSkillPathID(t *testing.T) {
	id, ok := SkillPathID("indiv-ebandao", "dev-expert")
	if !ok || id != "indiv-ebandao/dev-expert" {
		t.Fatalf("id=%q ok=%v", id, ok)
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
	slug, ok := SkillSlugID("academic-research-skills")
	if !ok || slug != "academic-research-skills" {
		t.Fatalf("slug=%q ok=%v", slug, ok)
	}
	if _, ok := SkillSlugID("bad/slug"); ok {
		t.Fatal("a slash in the slug was accepted")
	}
}

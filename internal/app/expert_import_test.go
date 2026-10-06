package app

import (
	"reflect"
	"strings"
	"testing"
)

// A catalog agent names the skills and MCP servers it was made with, and only by name. Matching them to what the tenant
// has must not guess.
//
// How it can go wrong, written down before the code:
//   - a blank name (the shipped snapshot has many) is reported as something the caller is missing, or matches an item
//     whose name is blank;
//   - a name differing only in case or surrounding spaces is not matched, or a near name is matched (a guess);
//   - the same name listed twice is matched twice, or reported twice;
//   - what could not be matched is dropped without a word instead of being returned to the caller;
//   - nothing to match yields nil instead of empty lists, which the API would send as null.

func TestMatchNamesDoesNotGuess(t *testing.T) {
	have := map[string]string{"docs": "mcp_1", "search": "mcp_2", "": "mcp_blank"}
	cases := []struct {
		name          string
		wanted        []string
		matched, lost []string
	}{
		{"exact", []string{"docs"}, []string{"mcp_1"}, []string{}},
		{"case and spaces", []string{"  Docs "}, []string{"mcp_1"}, []string{}},
		{"near name is not a match", []string{"docs2", "doc"}, []string{}, []string{"docs2", "doc"}},
		{"blank is neither matched nor reported", []string{"", "  "}, []string{}, []string{}},
		{"duplicates collapse", []string{"docs", "DOCS", "nope", "nope"}, []string{"mcp_1"}, []string{"nope"}},
		{"order of the request is kept", []string{"search", "docs"}, []string{"mcp_2", "mcp_1"}, []string{}},
		{"nothing wanted", nil, []string{}, []string{}},
	}
	for _, c := range cases {
		matched, lost := matchNames(c.wanted, have)
		if !reflect.DeepEqual(matched, c.matched) || !reflect.DeepEqual(lost, c.lost) {
			t.Errorf("%s: matched %#v lost %#v, want %#v %#v", c.name, matched, lost, c.matched, c.lost)
		}
	}
}

func TestAgentInstructionsFromItsPrompts(t *testing.T) {
	long := strings.Repeat("字", maxExpertInstructions+50)
	cases := []struct {
		title       string
		prompts     []string
		name        string
		description string
		want        string
	}{
		{"prompts are joined", []string{" first ", "second"}, "n", "d", "first\n\nsecond"},
		{"empty prompts fall back to the name and description", []string{"  ", ""}, "Helper", "about it", "Helper\n\nabout it"},
		{"nothing but a name", nil, "Helper", "", "Helper"},
	}
	for _, c := range cases {
		if got := agentInstructions(c.prompts, c.name, c.description); got != c.want {
			t.Errorf("%s: %q, want %q", c.title, got, c.want)
		}
	}
	if got := agentInstructions([]string{long}, "n", ""); len([]rune(got)) != maxExpertInstructions {
		t.Errorf("a long prompt is cut to %d characters, got %d", maxExpertInstructions, len([]rune(got)))
	}
}

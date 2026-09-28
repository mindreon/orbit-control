package main

import (
	"strings"
	"testing"
)

func TestGoNameUsesInitialisms(t *testing.T) {
	cases := map[string]string{
		"task_id":             "TaskID",
		"cost_usd_micros":     "CostUSDMicros",
		"from":                "From",
		"output_schema_ref":   "OutputSchemaRef",
		"PAUSED_NEEDS_REVIEW": "PausedNeedsReview",
		"task.status_changed": "TaskStatusChanged",
		"uri":                 "URI",
	}
	for in, want := range cases {
		if got := goName(in); got != want {
			t.Errorf("goName(%q) = %q, want %q", in, got, want)
		}
	}
}

const tinyBundle = `{
  "$defs": {
    "A": {"type": "object", "additionalProperties": false, "required": ["from"],
      "properties": {
        "op": {"const": "a", "default": "a", "type": "string"},
        "from": {"type": "string"},
        "n": {"anyOf": [{"type": "integer"}, {"type": "null"}], "default": null},
        "at": {"type": "string", "format": "date-time"},
        "tags": {"type": "array", "items": {"type": "string"}, "default": []},
        "meta": {"type": "object", "additionalProperties": {"$ref": "#/$defs/JsonValue"}},
        "status": {"enum": ["OPEN", "DONE"], "type": "string"}
      }},
    "B": {"type": "object", "properties": {"op": {"const": "b", "default": "b", "type": "string"}}},
    "JsonValue": {}
  },
  "x-models": ["A"],
  "x-unions": {"Op": {"discriminator": {"propertyName": "op", "mapping": {"a": "#/$defs/A", "b": "#/$defs/B"}},
    "oneOf": [{"$ref": "#/$defs/A"}, {"$ref": "#/$defs/B"}], "x-go-type": "Op"}},
  "x-enums": {"Status": ["OPEN", "DONE"]}
}`

func TestGenerateMapsSchemaToGoTypes(t *testing.T) {
	src, err := generate([]byte(tinyBundle), "tiny")
	if err != nil {
		t.Fatalf("generate: %v", err)
	}
	out := string(src)
	for _, want := range []string{
		"package tiny",
		"From string `json:\"from\"`",
		"N *int64 `json:\"n,omitempty\"`",
		"At *time.Time `json:\"at,omitempty\"`",
		"Tags []string `json:\"tags,omitempty\"`",
		"Meta map[string]json.RawMessage `json:\"meta,omitempty\"`",
		"Status *Status `json:\"status,omitempty\"`",
		"Op string `json:\"op,omitempty\"`",
		"type Status string",
		"StatusOpen Status = \"OPEN\"",
		"type Op struct",
		"func (u Op) MarshalJSON() ([]byte, error)",
		"func (u *Op) UnmarshalJSON(data []byte) error",
		"OpTagA = \"a\"",
	} {
		if !strings.Contains(squash(out), squash(want)) {
			t.Errorf("generated code lacks %q\n%s", want, out)
		}
	}
}

func TestGenerateRejectsUnsupportedSchemas(t *testing.T) {
	bad := `{"$defs": {"A": {"type": "object", "properties": {"x": {"anyOf": [{"type": "integer"}, {"type": "string"}]}}}},
	  "x-models": ["A"], "x-unions": {}, "x-enums": {}}`
	if _, err := generate([]byte(bad), "tiny"); err == nil {
		t.Fatal("expected an error for a non-null anyOf")
	}
}

// squash collapses runs of whitespace so gofmt alignment does not matter.
func squash(s string) string {
	return strings.Join(strings.Fields(s), " ")
}

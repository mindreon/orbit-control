package contractv3

import (
	"encoding/json"
	"os"
	"reflect"
	"sort"
	"testing"
)

// TestExamplesRoundTrip decodes every example exported by orbit-runtime
// (orbit_contracts.v3.examples) into the generated Go type and encodes it
// back. Any field Go drops or renames shows up as a difference.
func TestExamplesRoundTrip(t *testing.T) {
	raw, err := os.ReadFile("testdata/examples.json")
	if err != nil {
		t.Fatal(err)
	}
	var examples map[string][]json.RawMessage
	if err := json.Unmarshal(raw, &examples); err != nil {
		t.Fatal(err)
	}
	names := make([]string, 0, len(examples))
	for name := range examples {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		newValue, ok := contractFactories[name]
		if !ok {
			t.Errorf("no generated type for example %s", name)
			continue
		}
		for i, item := range examples[name] {
			value := newValue()
			if err := json.Unmarshal(item, value); err != nil {
				t.Errorf("%s[%d]: decode: %v", name, i, err)
				continue
			}
			if tagged, ok := value.(interface{ Tag() string }); ok && tagged.Tag() == "" {
				t.Errorf("%s[%d]: decoded union has no member set", name, i)
			}
			again, err := json.Marshal(value)
			if err != nil {
				t.Errorf("%s[%d]: encode: %v", name, i, err)
				continue
			}
			if want, got := normalize(t, item), normalize(t, again); !reflect.DeepEqual(want, got) {
				t.Errorf("%s[%d]: round trip changed the value\nwant %v\ngot  %v", name, i, want, got)
			}
		}
	}
	for name := range contractFactories {
		if _, ok := examples[name]; !ok {
			t.Errorf("generated type %s has no example", name)
		}
	}
}

func TestEventRetentionCoversEveryEventType(t *testing.T) {
	durable, ephemeral := 0, 0
	for tag, retention := range EventRetention {
		switch retention {
		case "durable":
			durable++
		case "ephemeral":
			ephemeral++
		default:
			t.Errorf("event %s has retention %q", tag, retention)
		}
	}
	if durable == 0 || ephemeral == 0 {
		t.Fatalf("durable=%d ephemeral=%d", durable, ephemeral)
	}
}

func TestUnionRejectsUnknownTag(t *testing.T) {
	var op PlanOp
	if err := json.Unmarshal([]byte(`{"op":"drop_table"}`), &op); err == nil {
		t.Fatal("expected an error for an unknown op")
	}
	if _, err := json.Marshal(PlanOp{}); err == nil {
		t.Fatal("expected an error for an empty union")
	}
}

// normalize drops empty arrays and objects: Python writes defaults such as
// [] that Go leaves out with omitempty, and both mean the same thing.
func normalize(t *testing.T, data []byte) any {
	t.Helper()
	var v any
	if err := json.Unmarshal(data, &v); err != nil {
		t.Fatal(err)
	}
	return prune(v)
}

func prune(v any) any {
	switch x := v.(type) {
	case map[string]any:
		out := map[string]any{}
		for k, item := range x {
			item = prune(item)
			if isEmpty(item) {
				continue
			}
			out[k] = item
		}
		return out
	case []any:
		out := make([]any, 0, len(x))
		for _, item := range x {
			out = append(out, prune(item))
		}
		return out
	default:
		return v
	}
}

func isEmpty(v any) bool {
	switch x := v.(type) {
	case map[string]any:
		return len(x) == 0
	case []any:
		return len(x) == 0
	}
	return false
}

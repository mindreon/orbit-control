package contract

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"testing"

	"github.com/santhosh-tekuri/jsonschema/v6"
)

const (
	schemaDir   = "../../contract/v2/schema"
	examplesDir = "../../contract/v2/examples"
)

// registry compiles every schema file and maps each exported type name, as
// listed in the x-exports of its file, to the compiled schema that validates it.
type registry struct {
	docs    map[string]map[string]any // file name -> parsed schema
	schemas map[string]*jsonschema.Schema
	// where records the file each type name comes from.
	where map[string]string
}

func loadRegistry(t *testing.T) *registry {
	t.Helper()
	files, err := filepath.Glob(filepath.Join(schemaDir, "*.json"))
	if err != nil || len(files) == 0 {
		t.Fatalf("no schema files in %s: %v", schemaDir, err)
	}
	c := jsonschema.NewCompiler()
	c.DefaultDraft(jsonschema.Draft2020)
	c.AssertFormat()
	r := &registry{docs: map[string]map[string]any{}, schemas: map[string]*jsonschema.Schema{}, where: map[string]string{}}
	ids := map[string]string{} // file -> $id
	for _, file := range files {
		raw, err := os.ReadFile(file)
		if err != nil {
			t.Fatal(err)
		}
		doc, err := jsonschema.UnmarshalJSON(bytes.NewReader(raw))
		if err != nil {
			t.Fatalf("%s: %v", file, err)
		}
		m := doc.(map[string]any)
		id, _ := m["$id"].(string)
		if id == "" {
			t.Fatalf("%s has no $id", file)
		}
		name := filepath.Base(file)
		if !strings.HasSuffix(id, "/"+name) {
			t.Errorf("%s: $id %q does not end in the file name", name, id)
		}
		if err := c.AddResource(id, doc); err != nil {
			t.Fatalf("%s: %v", file, err)
		}
		r.docs[name] = m
		ids[name] = id
	}
	add := func(typeName, file, pointer string) {
		if prev, dup := r.where[typeName]; dup {
			t.Errorf("type %s is defined in %s and %s", typeName, prev, file)
			return
		}
		sch, err := c.Compile(ids[file] + pointer)
		if err != nil {
			t.Errorf("%s: compile %s: %v", file, typeName, err)
			return
		}
		r.where[typeName] = file
		r.schemas[typeName] = sch
	}
	for name, doc := range r.docs {
		defs, _ := doc["$defs"].(map[string]any)
		for def := range defs {
			if _, dup := r.where[def]; dup && r.where[def] != name {
				t.Errorf("$defs name %s of %s is used by %s too; names share one Go package", def, name, r.where[def])
			}
		}
		exports, _ := doc["x-exports"].([]any)
		for _, e := range exports {
			typeName := e.(string)
			if _, ok := defs[typeName]; !ok {
				t.Errorf("%s: x-exports lists %s, which is not in $defs", name, typeName)
				continue
			}
			add(typeName, name, "#/$defs/"+typeName)
		}
		if len(exports) == 0 && name != "registry.json" {
			t.Errorf("%s: the file exports no type; list the types that need examples in x-exports", name)
		}
	}
	return r
}

// every schema file compiles (this validates it against the 2020-12 meta-schema)
// and follows the house rules.
func TestSchemasFollowTheRules(t *testing.T) {
	r := loadRegistry(t)
	for name, doc := range r.docs {
		if _, ok := doc["description"].(string); !ok {
			t.Errorf("%s: the file has no description", name)
		}
		var walk func(path string, node any)
		walk = func(path string, node any) {
			switch n := node.(type) {
			case map[string]any:
				if n["type"] == "object" {
					if props, ok := n["properties"].(map[string]any); ok {
						if n["additionalProperties"] != false {
							t.Errorf("%s %s: an object with properties must set additionalProperties to false", name, path)
						}
						for prop, sub := range props {
							m, _ := sub.(map[string]any)
							_, hasDesc := m["description"]
							_, isRef := m["$ref"]
							if !hasDesc && !isRef {
								t.Errorf("%s %s.%s: the property has no description", name, path, prop)
							}
						}
					}
				}
				if defs, ok := n["$defs"].(map[string]any); ok {
					for def, sub := range defs {
						if m, _ := sub.(map[string]any); m["description"] == nil {
							t.Errorf("%s %s/$defs/%s: the definition has no description", name, path, def)
						}
					}
				}
				for k, v := range n {
					walk(path+"/"+k, v)
				}
			case []any:
				for i, v := range n {
					walk(fmt.Sprintf("%s/%d", path, i), v)
				}
			}
		}
		walk("", doc)
	}
}

type exampleFile struct {
	typeName string
	name     string // file name without .json
	path     string
}

func listExamples(t *testing.T, dir string) []exampleFile {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	var out []exampleFile
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".json") {
			continue
		}
		name := strings.TrimSuffix(e.Name(), ".json")
		typeName, _, ok := strings.Cut(name, ".")
		if !ok {
			t.Fatalf("example %s is not named <Type>.<case>.json", e.Name())
		}
		out = append(out, exampleFile{typeName: typeName, name: name, path: filepath.Join(dir, e.Name())})
	}
	return out
}

func validate(sch *jsonschema.Schema, raw []byte) error {
	inst, err := jsonschema.UnmarshalJSON(bytes.NewReader(raw))
	if err != nil {
		return err
	}
	return sch.Validate(inst)
}

func TestValidExamplesPass(t *testing.T) {
	r := loadRegistry(t)
	covered := map[string]bool{}
	for _, ex := range listExamples(t, examplesDir) {
		sch, ok := r.schemas[ex.typeName]
		if !ok {
			t.Errorf("%s: %s is not a type of the contract", ex.name, ex.typeName)
			continue
		}
		covered[ex.typeName] = true
		raw, err := os.ReadFile(ex.path)
		if err != nil {
			t.Fatal(err)
		}
		if err := validate(sch, raw); err != nil {
			t.Errorf("%s should be valid: %v", ex.name, err)
		}
	}
	for typeName := range r.schemas {
		if !covered[typeName] {
			t.Errorf("type %s has no valid example", typeName)
		}
	}
}

func TestInvalidExamplesFail(t *testing.T) {
	r := loadRegistry(t)
	count := 0
	for _, ex := range listExamples(t, filepath.Join(examplesDir, "invalid")) {
		_, ok := r.schemas[ex.typeName]
		if !ok {
			t.Errorf("%s: %s is not a type of the contract", ex.name, ex.typeName)
			continue
		}
		raw, err := os.ReadFile(ex.path)
		if err != nil {
			t.Fatal(err)
		}
		if err := ValidateStrict(ex.typeName, raw); err == nil {
			t.Errorf("%s should be rejected", ex.name)
		}
		count++
	}
	if count == 0 {
		t.Fatal("no invalid examples")
	}
}

// Every event type has an example, and nothing else is accepted as a type.
func TestEveryEventTypeHasAnExample(t *testing.T) {
	r := loadRegistry(t)
	defs := r.docs["run_event.json"]["$defs"].(map[string]any)
	types := defs["EventType"].(map[string]any)["enum"].([]any)
	have := map[string]bool{}
	for _, ex := range listExamples(t, examplesDir) {
		if ex.typeName == "RunEvent" {
			for _, typ := range types {
				if strings.HasPrefix(ex.name, "RunEvent."+typ.(string)) {
					have[typ.(string)] = true
				}
			}
		}
	}
	for _, typ := range types {
		if !have[typ.(string)] {
			t.Errorf("event type %s has no example", typ)
		}
	}
}

// eventData maps every event type to the Go type of its data.
var eventData = map[string]func() any{
	"run.started": func() any { return new(RunStarted) }, "run.status": func() any { return new(RunStatus) },
	"run.completed": func() any { return new(RunCompleted) }, "run.failed": func() any { return new(RunFailed) },
	"segment.started": func() any { return new(SegmentStarted) }, "segment.ended": func() any { return new(SegmentEnded) },
	"message.delta": func() any { return new(MessageDelta) }, "message.final": func() any { return new(MessageFinal) },
	"reasoning.delta": func() any { return new(ReasoningDelta) }, "tool.call": func() any { return new(ToolCall) },
	"tool.result": func() any { return new(ToolResult) }, "sandbox.output": func() any { return new(SandboxOutput) }, "sandbox.restored": func() any { return new(SandboxRestored) },
	"artifact.created": func() any { return new(ArtifactCreated) }, "artifact.updated": func() any { return new(ArtifactUpdated) },
	"interaction.requested": func() any { return new(InteractionRequested) }, "interaction.resolved": func() any { return new(InteractionResolved) },
	"team.task.created": func() any { return new(TeamTaskCreated) }, "team.task.updated": func() any { return new(TeamTaskUpdated) },
	"team.member.status": func() any { return new(TeamMemberStatus) }, "team.message": func() any { return new(TeamMessage) },
	"usage.update": func() any { return new(UsageUpdate) }, "error": func() any { return new(ErrorData) },
}

func TestConstructorTablesCoverTheContract(t *testing.T) {
	r := loadRegistry(t)
	var missing []string
	for typeName := range r.schemas {
		if constructors[typeName] == nil {
			missing = append(missing, typeName)
		}
	}
	sort.Strings(missing)
	if len(missing) > 0 {
		t.Errorf("add these types to constructors: %v", missing)
	}
	for typeName := range constructors {
		if r.schemas[typeName] == nil {
			t.Errorf("constructors has %s, which is not a type of the contract", typeName)
		}
	}
	types := r.docs["run_event.json"]["$defs"].(map[string]any)["EventType"].(map[string]any)["enum"].([]any)
	if len(types) != len(eventData) {
		t.Errorf("the contract has %d event types, eventData has %d", len(types), len(eventData))
	}
	for _, typ := range types {
		if eventData[typ.(string)] == nil {
			t.Errorf("add event type %s to eventData", typ)
		}
	}
}

func decodeAny(t *testing.T, raw []byte) any {
	t.Helper()
	var v any
	if err := json.Unmarshal(raw, &v); err != nil {
		t.Fatal(err)
	}
	return v
}

// A valid example decodes into the generated type and encodes back to the same
// JSON value, and the encoding is still valid against the schema.
func TestExamplesRoundTripThroughTheGeneratedTypes(t *testing.T) {
	r := loadRegistry(t)
	for _, ex := range listExamples(t, examplesDir) {
		t.Run(ex.name, func(t *testing.T) {
			raw, err := os.ReadFile(ex.path)
			if err != nil {
				t.Fatal(err)
			}
			v := constructors[ex.typeName]()
			if err := json.Unmarshal(raw, v); err != nil {
				t.Fatalf("decode: %v", err)
			}
			out, err := json.Marshal(v)
			if err != nil {
				t.Fatalf("encode: %v", err)
			}
			if !reflect.DeepEqual(decodeAny(t, raw), decodeAny(t, out)) {
				t.Errorf("round trip changed the value:\n in: %s\nout: %s", raw, out)
			}
			if err := validate(r.schemas[ex.typeName], out); err != nil {
				t.Errorf("encoded value is not valid: %v", err)
			}
		})
	}
}

// The data of an event decodes into the type its event type selects.
func TestEventDataRoundTripsThroughItsType(t *testing.T) {
	for _, ex := range listExamples(t, examplesDir) {
		if ex.typeName != "RunEvent" {
			continue
		}
		t.Run(ex.name, func(t *testing.T) {
			raw, err := os.ReadFile(ex.path)
			if err != nil {
				t.Fatal(err)
			}
			var ev RunEvent
			if err := json.Unmarshal(raw, &ev); err != nil {
				t.Fatal(err)
			}
			data, err := json.Marshal(ev.Data)
			if err != nil {
				t.Fatal(err)
			}
			v := eventData[string(ev.Type)]()
			if err := json.Unmarshal(data, v); err != nil {
				t.Fatalf("decode data as %T: %v", v, err)
			}
			out, err := json.Marshal(v)
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(decodeAny(t, data), decodeAny(t, out)) {
				t.Errorf("data round trip changed the value:\n in: %s\nout: %s", data, out)
			}
		})
	}
}

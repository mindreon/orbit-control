package task

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"
)

func steps(t *testing.T, raw string) []SOPStep {
	t.Helper()
	var out []SOPStep
	if err := json.Unmarshal([]byte(raw), &out); err != nil {
		t.Fatalf("decode %s: %v", raw, err)
	}
	return out
}

func TestV1DefinitionIsALinearV2One(t *testing.T) {
	sop, err := ValidateSOP(SOP{SOPID: "v1", Steps: steps(t, `["draft", {"subject":"review","description":"check","max_attempts":2}]`)})
	if err != nil {
		t.Fatal(err)
	}
	if sop.Name != "v1" || sop.Steps[0].ID != "s1" || sop.Steps[1].ID != "s2" {
		t.Fatalf("defaults: %+v", sop)
	}
	if len(sop.Steps[0].DependsOn) != 0 || sop.Steps[0].DependsOn == nil || strings.Join(sop.Steps[1].DependsOn, ",") != "s1" {
		t.Fatalf("a step depends on the one before it, the first on nothing: %+v", sop.Steps)
	}
	if sop.Steps[0].Description != "draft" || sop.Steps[0].MaxAttempts != DefaultStepAttempts || sop.Steps[1].MaxAttempts != 2 {
		t.Fatalf("description and attempts: %+v", sop.Steps)
	}
}

func TestAnEmptyDependsOnStartsAtOnceAndAbsentIsThePreviousStep(t *testing.T) {
	sop, err := ValidateSOP(SOP{SOPID: "x", Steps: steps(t, `[{"id":"a","subject":"a"},{"id":"b","subject":"b","depends_on":[]},{"id":"c","subject":"c"}]`)})
	if err != nil {
		t.Fatal(err)
	}
	if len(sop.Steps[1].DependsOn) != 0 || strings.Join(sop.Steps[2].DependsOn, ",") != "b" {
		t.Fatalf("depends_on: %+v", sop.Steps)
	}
	out, _ := json.Marshal(sop.Steps[1])
	if !strings.Contains(string(out), `"depends_on":[]`) {
		t.Fatalf("a root step stores its empty list, or it would read as the step before it: %s", out)
	}
}

func TestInvalidDefinitionsAreRefusedWithAReason(t *testing.T) {
	cases := map[string]string{
		"cycle":     `[{"id":"a","subject":"a","depends_on":["b"]},{"id":"b","subject":"b","depends_on":["a"]}]`,
		"itself":    `[{"id":"a","subject":"a","depends_on":["a"]}]`,
		"unknown":   `[{"id":"a","subject":"a","depends_on":["z"]}]`,
		"duplicate": `[{"id":"a","subject":"a"},{"id":"a","subject":"b"}]`,
		"taken":     `[{"id":"s2","subject":"a"},{"subject":"b"}]`,
		"approval":  `[{"subject":"a","human_approval":"x"}]`,
		"executor":  `[{"subject":"a","executor":"writer"}]`,
		"schema":    `[{"subject":"a","output_schema_ref":"nope"}]`,
		"attempts":  `[{"subject":"a","max_attempts":99}]`,
		"subject":   `[{"subject":" "}]`,
		"artifact":  `[{"subject":"a","required_artifacts":[{"name":"x"}]}]`,
		"verifier":  `[{"subject":"a","verifier":{"expert":"auditor"}}]`,
	}
	for name, raw := range cases {
		_, err := ValidateSOP(SOP{SOPID: "x", Steps: steps(t, raw)})
		var invalid *InvalidSOPError
		if err == nil || !errors.As(err, &invalid) {
			t.Errorf("%s: want an InvalidSOPError, got %v", name, err)
		}
	}
	tooMany := make([]SOPStep, maxSOPSteps+1)
	for i := range tooMany {
		tooMany[i] = SOPStep{Subject: "s"}
	}
	if _, err := ValidateSOP(SOP{SOPID: "x", Steps: tooMany}); err == nil {
		t.Error("more than 50 steps must be refused")
	}
	if _, err := ValidateSOP(SOP{SOPID: "x"}); err == nil {
		t.Error("no steps must be refused")
	}
}

func TestUnknownStepFieldsAreRefused(t *testing.T) {
	var out []SOPStep
	if err := json.Unmarshal([]byte(`[{"subject":"a","retries":3}]`), &out); err == nil {
		t.Fatal("the runtime's contract refuses unknown fields, so a definition holding one must not be stored")
	}
}

func TestSameSOPComparesNormalizedDefinitions(t *testing.T) {
	short := SOP{SOPID: "x", Steps: steps(t, `["a","b"]`)}
	long := SOP{SOPID: "x", Name: "x", Steps: steps(t, `[{"id":"s1","subject":"a","description":"a","max_attempts":3,"depends_on":[]},{"id":"s2","subject":"b","description":"b","max_attempts":3,"depends_on":["s1"]}]`)}
	if !SameSOP(short, long) {
		t.Fatal("a definition and itself with its defaults spelled out are the same version")
	}
	other := SOP{SOPID: "x", Steps: steps(t, `["a","b"]`), Description: "changed"}
	if SameSOP(short, other) {
		t.Fatal("another description is another definition")
	}
}

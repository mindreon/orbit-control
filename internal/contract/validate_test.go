package contract

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestConsumerAndStrictValidation(t *testing.T) {
	r := loadRegistry(t)
	for _, ex := range listExamples(t, examplesDir) {
		raw, err := os.ReadFile(ex.path)
		if err != nil {
			t.Fatal(err)
		}
		payload := decodeAny(t, raw).(map[string]any)
		payload["future_optional"] = "new"
		raw, err = json.Marshal(payload)
		if err != nil {
			t.Fatal(err)
		}
		definition := r.docs[r.where[ex.typeName]]["$defs"].(map[string]any)[ex.typeName].(map[string]any)
		if err := Validate(ex.typeName, raw); (err == nil) != (definition["x-extra"] == "ignore") {
			t.Errorf("%s consumer: %v", ex.name, err)
		}
		if err := ValidateStrict(ex.typeName, raw); err == nil {
			t.Errorf("%s strict accepted future field", ex.name)
		}
	}
	// Nested future fields must also be accepted, including event data selected by if/then.
	for _, name := range []string{"SegmentInput.unknown_field", "RunEvent.data_extra_field"} {
		raw, err := os.ReadFile(filepath.Join(examplesDir, "invalid", name+".json"))
		if err != nil {
			t.Fatal(err)
		}
		if err := Validate(strings.Split(name, ".")[0], raw); err != nil {
			t.Errorf("%s: %v", name, err)
		}
	}
}

func TestBoundaryErrorsNeverRetainValues(t *testing.T) {
	const sentinel = "synthetic-sensitive-sentinel"
	r := loadRegistry(t)
	for _, ex := range listExamples(t, examplesDir) {
		definition := r.docs[r.where[ex.typeName]]["$defs"].(map[string]any)[ex.typeName].(map[string]any)
		props, _ := definition["properties"].(map[string]any)
		for field, property := range props {
			schema, _ := property.(map[string]any)
			if schema["x-sensitive"] != true {
				continue
			}
			raw, err := os.ReadFile(ex.path)
			if err != nil {
				t.Fatal(err)
			}
			for _, scenario := range []string{"missing", "wrong_type"} {
				payload := decodeAny(t, raw).(map[string]any)
				payload[field] = map[string]any{"secret": sentinel}
				if scenario == "missing" {
					delete(payload, field)
					payload["future_optional"] = sentinel
				}
				raw, err := json.Marshal(payload)
				if err != nil {
					t.Fatal(err)
				}
				for _, check := range []func(string, []byte) error{Validate, ValidateStrict} {
					err := check(ex.typeName, raw)
					if err == nil || strings.Contains(err.Error(), sentinel) {
						t.Fatalf("%s %s %s: %v", ex.name, field, scenario, err)
					}
				}
			}
		}
	}
	raw, err := os.ReadFile(filepath.Join(examplesDir, "ModelCredentials.profile.json"))
	if err != nil {
		t.Fatal(err)
	}
	payload := decodeAny(t, raw).(map[string]any)
	payload["api_key"] = sentinel
	delete(payload, "pricing")
	encoded, _ := json.Marshal(payload)
	if err := Validate("ModelCredentials", encoded); err == nil || err.Error() != "/: required" {
		t.Fatalf("required error: %v", err)
	}
	payload["base_url"] = sentinel
	encoded, _ = json.Marshal(payload)
	if err := Validate("ModelCredentials", encoded); err == nil || strings.Contains(err.Error(), sentinel) || !strings.Contains(err.Error(), "/base_url: pattern") {
		t.Fatalf("pattern error: %v", err)
	}
	// blob_url is a nested sensitive field whose pattern error previously echoed its value.
	raw, err = os.ReadFile(filepath.Join(examplesDir, "GetLockResponse.text_and_blob.json"))
	if err != nil {
		t.Fatal(err)
	}
	payload = decodeAny(t, raw).(map[string]any)
	content := payload["contents"].([]any)[0].(map[string]any)
	delete(content, "text")
	content["blob_url"] = sentinel
	encoded, _ = json.Marshal(payload)
	if err := Validate("GetLockResponse", encoded); err == nil || strings.Contains(err.Error(), sentinel) || !strings.Contains(err.Error(), "/contents/0/blob_url: pattern") {
		t.Fatalf("sensitive pattern error: %v", err)
	}
	for _, raw := range []string{`{"api_key":"` + sentinel + `",}`, `"` + sentinel + `"`} {
		if err := Validate("ModelCredentials", []byte(raw)); err == nil || strings.Contains(err.Error(), sentinel) {
			t.Fatalf("JSON/type error: %v", err)
		}
	}
}

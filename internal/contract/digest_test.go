package contract

import (
	"encoding/json"
	"os"
	"strings"
	"testing"
)

func TestRunLockExampleDigests(t *testing.T) {
	for _, ex := range listExamples(t, examplesDir) {
		if ex.typeName != "RunLock" && ex.typeName != "GetLockResponse" {
			continue
		}
		raw, err := os.ReadFile(ex.path)
		if err != nil {
			t.Fatal(err)
		}
		value := decodeAny(t, raw).(map[string]any)
		if ex.typeName == "GetLockResponse" {
			value = value["lock"].(map[string]any)
		}
		raw, _ = json.Marshal(value)
		digest, err := RunLockDigest(raw)
		if err != nil || digest != value["digest"] {
			t.Fatalf("%s lock digest: %s %v", ex.name, digest, err)
		}
		for _, entry := range value["entries"].([]any) {
			e := entry.(map[string]any)
			files := map[string]string{}
			for _, file := range e["files"].([]any) {
				f := file.(map[string]any)
				files[f["path"].(string)] = f["sha256"].(string)
			}
			got, err := BundleDigest(files)
			if err != nil || got != e["digest"] {
				t.Fatalf("%s entry digest: %s %v", ex.name, got, err)
			}
		}
		value["lock_id"] = "another-lock"
		value["digest"] = "sha256:" + strings.Repeat("f", 64)
		raw, _ = json.Marshal(value)
		got, err := RunLockDigest(raw)
		if err != nil || got != digest {
			t.Fatal("identity changed digest", err)
		}
		// Nested entry digests are content, not identities to omit.
		value["entries"].([]any)[0].(map[string]any)["digest"] = "sha256:" + strings.Repeat("e", 64)
		raw, _ = json.Marshal(value)
		if got, err := RunLockDigest(raw); err != nil || got == digest {
			t.Fatal("nested content was omitted", err)
		}
	}
	for _, raw := range []string{`null`, `[]`, `{"v":1,"v":2}`} {
		if _, err := RunLockDigest([]byte(raw)); err == nil {
			t.Fatal("accepted invalid lock JSON")
		}
	}
}

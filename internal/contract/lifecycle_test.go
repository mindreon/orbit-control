package contract

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

func TestPurposeSpecificTokenRequests(t *testing.T) {
	for _, purpose := range []string{"segment", "sandbox"} {
		name := "TokenRequest.sandbox.json"
		if purpose == "segment" {
			name = "TokenRequest.minimal.json"
		}
		raw, err := os.ReadFile(filepath.Join(examplesDir, name))
		if err != nil {
			t.Fatal(err)
		}
		fields := []string{"member", "seq", "attempt"}
		if purpose == "sandbox" {
			fields = []string{"lease_id"}
		}
		for _, field := range fields {
			value := decodeAny(t, raw).(map[string]any)
			delete(value, field)
			encoded, _ := json.Marshal(value)
			if err := Validate("TokenRequest", encoded); err == nil {
				t.Fatalf("%s accepted missing %s", purpose, field)
			}
		}
	}
	raw, err := os.ReadFile(filepath.Join(examplesDir, "SandboxConfigureInput.widen_egress.json"))
	if err != nil {
		t.Fatal(err)
	}
	value := decodeAny(t, raw).(map[string]any)
	// Configuration has enough identity to request a sandbox token without segment metadata.
	token, _ := json.Marshal(
		map[string]any{
			"tenant_id": value["tenant_id"],
			"run_id":    value["run_id"],
			"purpose":   "sandbox",
			"lease_id":  value["isolation_key"],
		},
	)
	if err := ValidateStrict("TokenRequest", token); err != nil {
		t.Fatal(err)
	}
	delete(value, "run_id")
	encoded, _ := json.Marshal(value)
	if err := Validate("SandboxConfigureInput", encoded); err == nil {
		t.Fatal("configuration accepted missing run_id")
	}
}

func TestSandboxRenewRequiresPositiveTTL(t *testing.T) {
	raw, err := os.ReadFile(filepath.Join(examplesDir, "SandboxRenewInput.renew.json"))
	if err != nil {
		t.Fatal(err)
	}
	for _, ttl := range []any{nil, 0, -1, 1} {
		value := decodeAny(t, raw).(map[string]any)
		value["ttl_seconds"] = ttl
		if ttl == nil {
			delete(value, "ttl_seconds")
		}
		encoded, _ := json.Marshal(value)
		if err := Validate("SandboxRenewInput", encoded); (err == nil) != (ttl == 1) {
			t.Fatalf("TTL %v: %v", ttl, err)
		}
	}
}

func TestIsolationKeysShareConstraint(t *testing.T) {
	r := loadRegistry(t)
	for file, doc := range r.docs {
		defs, _ := doc["$defs"].(map[string]any)
		for kind, raw := range defs {
			definition, _ := raw.(map[string]any)
			props, _ := definition["properties"].(map[string]any)
			if value, ok := props["isolation_key"]; ok {
				if value.(map[string]any)["$ref"] != "common.json#/$defs/IsolationKey" {
					t.Fatalf("%s %s isolation_key diverged", file, kind)
				}
			}
		}
	}
	for _, value := range []string{"isolation-1", "", "bad/key", "white space"} {
		raw, _ := json.Marshal(value)
		if err := Validate("IsolationKey", raw); (err == nil) != (value == "isolation-1") {
			t.Fatalf("isolation key: %v", err)
		}
	}
}

func TestSingleAgentLockRole(t *testing.T) {
	raw, err := os.ReadFile(filepath.Join(examplesDir, "RunLock.agent.json"))
	if err != nil {
		t.Fatal(err)
	}
	value := decodeAny(t, raw).(map[string]any)
	member := value["members"].([]any)[0].(map[string]any)
	for _, role := range []string{"agent", "leader", "teammate", "reviewer"} {
		member["role"] = role
		encoded, _ := json.Marshal(value)
		if err := Validate("RunLock", encoded); (err == nil) != (role == "agent") {
			t.Fatalf("single-agent role %s: %v", role, err)
		}
	}
}

func TestModelCredentialsMayOmitParams(t *testing.T) {
	raw, err := os.ReadFile(filepath.Join(examplesDir, "ModelCredentials.profile.json"))
	if err != nil {
		t.Fatal(err)
	}
	value := new(ModelCredentials)
	if err := json.Unmarshal(raw, value); err != nil {
		t.Fatal(err)
	}
	value.Params = nil
	encoded, _ := json.Marshal(value)
	if err := ValidateStrict("ModelCredentials", encoded); err != nil {
		t.Fatal(err)
	}
}

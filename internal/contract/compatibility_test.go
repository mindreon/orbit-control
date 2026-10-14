package contract

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"encoding/json" // Exercise the v1 converter used by generated contracts and Temporal, including nil slices.
	"fmt"
	"log/slog"
	"math"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"testing"

	"github.com/mindreon/orbit-control/internal/platform/jcs"
)

const fixturesDir = "../../contract/v2/fixtures"

func readFixture(t *testing.T, name string, target any) {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join(fixturesDir, name))
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(raw, target); err != nil {
		t.Fatal(err)
	}
}

func TestSharedDigestVectors(t *testing.T) {
	var vectors struct {
		Objects []struct{ Input, Canonical, Digest string }
		Numbers []struct{ Bits, Canonical string }
		Bundles []struct {
			Files  map[string]string
			Digest string
		}
		Invalid []string
	}
	readFixture(t, "digest.json", &vectors)
	for _, v := range vectors.Objects {
		canonical, err := jcs.Canonicalize([]byte(v.Input))
		if err != nil || string(canonical) != v.Canonical {
			t.Fatalf("canonical mismatch: %s: %v", canonical, err)
		}
		got, err := ObjectDigest([]byte(v.Input))
		if err != nil || got != v.Digest {
			t.Fatalf("object digest mismatch: %s: %v", got, err)
		}
	}
	for _, v := range vectors.Numbers {
		bits, _ := hex.DecodeString(v.Bits)
		number := math.Float64frombits(binary.BigEndian.Uint64(bits))
		got, err := jcs.Canonicalize([]byte(strconv.FormatFloat(number, 'g', -1, 64)))
		if err != nil || string(got) != v.Canonical {
			t.Fatalf("RFC 8785 Appendix B %s: %s: %v", v.Bits, got, err)
		}
	}
	for _, v := range vectors.Bundles {
		got, err := BundleDigest(v.Files)
		if err != nil || got != v.Digest {
			t.Fatalf("bundle mismatch: %s: %v", got, err)
		}
	}
	for _, raw := range vectors.Invalid {
		if _, err := ObjectDigest([]byte(raw)); err == nil {
			t.Fatalf("accepted invalid JCS %q", raw)
		}
	}
	for _, path := range []string{"..", "../a", "a/../b", "/a", "a\nb", "a\x00b", "a\\b", "a//b"} {
		if _, err := BundleDigest(map[string]string{path: "sha256:" + strings.Repeat("a", 64)}); err == nil {
			t.Fatalf("accepted bundle path %q", path)
		}
	}
}

func TestSharedGlobVectors(t *testing.T) {
	var vectors []struct {
		Pattern, Value string
		Match, Error   bool
	}
	readFixture(t, "glob.json", &vectors)
	for _, v := range vectors {
		got, err := MatchGlob(v.Pattern, v.Value)
		if (err != nil) != v.Error || got != v.Match {
			t.Errorf("%q %q: %v %v", v.Pattern, v.Value, got, err)
		}
	}
}

func TestSecretFormattingAndWireValues(t *testing.T) {
	const sentinel = "synthetic-sensitive-sentinel"
	secret := Secret(sentinel)
	value := ModelCredentials{ApiKey: secret}
	for _, output := range []string{fmt.Sprint(secret), fmt.Sprintf("%#v", secret), fmt.Sprintf("%+v", value), fmt.Sprintf("%#v", value)} {
		if strings.Contains(output, sentinel) || !strings.Contains(output, "[REDACTED]") {
			t.Fatalf("secret formatting leaked: %s", output)
		}
	}
	for _, handler := range []func(*bytes.Buffer) slog.Handler{
		func(b *bytes.Buffer) slog.Handler { return slog.NewJSONHandler(b, nil) },
		func(b *bytes.Buffer) slog.Handler { return slog.NewTextHandler(b, nil) },
	} {
		var buf bytes.Buffer
		slog.New(handler(&buf)).Info("test", "secret", secret, "credentials", value, "grants", SecretGrantResponse{Grants: []SecretGrant{{Value: secret}}})
		if strings.Contains(buf.String(), sentinel) {
			t.Fatalf("secret log leaked: %s", buf.String())
		}
	}
	raw, err := json.Marshal(secret)
	if err != nil || string(raw) != `"`+sentinel+`"` {
		t.Fatal("secret changed wire value", string(raw), err)
	}
	var decoded Secret
	if err := json.Unmarshal(raw, &decoded); err != nil || string(decoded) != sentinel {
		t.Fatal("secret decode", err)
	}
}

type wireFixture struct {
	Kind    string          `json:"kind"`
	Case    string          `json:"case"`
	Valid   bool            `json:"valid"`
	Payload json.RawMessage `json:"payload"`
}

// Minimize an actual valid example, validating each deletion. Required fields,
// nonempty arrays and if/then requirements survive; optional empty arrays do not.
func minimalValue(t *testing.T, kind string, root any) any {
	t.Helper()
	valid := func() bool { raw, err := json.Marshal(root); return err == nil && Validate(kind, raw) == nil }
	var prune func(any)
	prune = func(value any) {
		switch v := value.(type) {
		case map[string]any:
			keys := make([]string, 0, len(v))
			for key := range v {
				keys = append(keys, key)
			}
			slices.Sort(keys)
			for _, key := range keys {
				saved := v[key]
				delete(v, key)
				if !valid() {
					v[key] = saved
					if items, ok := saved.([]any); ok {
						for len(items) > 0 {
							v[key] = items[:len(items)-1]
							if !valid() {
								v[key] = items
								break
							}
							items = items[:len(items)-1]
						}
						saved = v[key]
					}
					prune(saved)
				}
			}
		case []any:
			for _, item := range v {
				prune(item)
			}
		}
	}
	prune(root)
	return root
}

// Regenerate deliberately with ORBIT_UPDATE_CONTRACT_FIXTURES=1. Ordinary tests
// compare the bytes so a stale/missing cross-language fixture fails CI.
func TestGoWireFixtures(t *testing.T) {
	var fixtures []wireFixture
	examples := listExamples(t, examplesDir)
	first := map[string]exampleFile{}
	appendFixture := func(kind, name string, value any) {
		t.Helper()
		raw, err := json.Marshal(value)
		if err != nil {
			t.Fatal(err)
		}
		valid := Validate(kind, raw) == nil
		if name != "zero" && !valid {
			t.Fatalf("%s %s: %v", kind, name, Validate(kind, raw))
		}
		fixtures = append(fixtures, wireFixture{kind, name, valid, raw})
	}
	for _, ex := range examples {
		raw, err := os.ReadFile(ex.path)
		if err != nil {
			t.Fatal(err)
		}
		value := constructors[ex.typeName]()
		if err := json.Unmarshal(raw, value); err != nil {
			t.Fatal(ex.name, err)
		}
		appendFixture(ex.typeName, "example:"+ex.name, value)
		if _, ok := first[ex.typeName]; !ok {
			first[ex.typeName] = ex
		}
	}
	kinds := make([]string, 0, len(constructors))
	for kind := range constructors {
		kinds = append(kinds, kind)
	}
	slices.Sort(kinds)
	for _, kind := range kinds {
		appendFixture(kind, "zero", constructors[kind]())
		raw, err := os.ReadFile(first[kind].path)
		if err != nil {
			t.Fatal(err)
		}
		root := minimalValue(t, kind, decodeAny(t, raw))
		raw, err = json.Marshal(root)
		if err != nil {
			t.Fatal(err)
		}
		value := constructors[kind]()
		if err := json.Unmarshal(raw, value); err != nil {
			t.Fatal(kind, err)
		}
		appendFixture(kind, "minimal", value)
	}
	raw, err := json.MarshalIndent(fixtures, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	raw = append(raw, '\n')
	path := filepath.Join(fixturesDir, "go-wire.json")
	if os.Getenv("ORBIT_UPDATE_CONTRACT_FIXTURES") == "1" {
		if err := os.WriteFile(path, raw, 0644); err != nil {
			t.Fatal(err)
		}
	}
	stored, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(raw, stored) {
		t.Fatal("Go wire fixtures are stale: ORBIT_UPDATE_CONTRACT_FIXTURES=1 go test ./internal/contract -run TestGoWireFixtures")
	}
}

func TestRegistryReferencesAndErrorNames(t *testing.T) {
	r := loadRegistry(t)
	doc := r.docs["registry.json"]
	queues := doc["x-task-queues"].(map[string]any)
	for _, table := range []string{"x-activities", "x-updates", "x-signals", "x-queries"} {
		for name, raw := range doc[table].(map[string]any) {
			binding := raw.(map[string]any)
			for _, direction := range []string{"input", "output"} {
				kind := binding[direction].(string)
				if r.schemas[kind] == nil {
					t.Errorf("%s %s: unknown %s %s", table, name, direction, kind)
				}
			}
			if table == "x-activities" && queues[binding["queue"].(string)] == nil {
				t.Errorf("%s: unknown task queue", name)
			}
		}
	}
	errors := doc["x-errors"].(map[string]any)
	for _, name := range NonRetryableErrors {
		if errors[name] == nil {
			t.Errorf("unknown nonretryable error %s", name)
		}
	}
}

func TestArtifactLimitAtBoundary(t *testing.T) {
	raw, err := os.ReadFile(filepath.Join(examplesDir, "SegmentResult.completed.json"))
	if err != nil {
		t.Fatal(err)
	}
	value := decodeAny(t, raw).(map[string]any)
	artifact := value["artifacts"].([]any)[0]
	for _, count := range []int{200, 201} {
		artifacts := make([]any, count)
		for i := range artifacts {
			artifacts[i] = artifact
		}
		value["artifacts"] = artifacts
		raw, err = json.Marshal(value)
		if err != nil {
			t.Fatal(err)
		}
		if err := Validate("SegmentResult", raw); (err == nil) != (count == 200) {
			t.Fatalf("artifact limit %d: %v", count, err)
		}
	}
	if err := Validate("not-a-kind", []byte(`{}`)); err == nil {
		t.Fatal("unknown kind accepted")
	}
	if err := Validate("SegmentInput", []byte(`null`)); err == nil {
		t.Fatal("null payload accepted")
	}
}

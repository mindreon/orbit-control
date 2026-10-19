package httpapi

import (
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"

	"github.com/mindreon/orbit-control/internal/sessions"
	"github.com/mindreon/orbit-control/internal/usage"
)

func TestRoutesExactlyMatchOpenAPI(t *testing.T) {
	b, e := os.ReadFile("../../docs/openapi.yaml")
	if e != nil {
		t.Fatal(e)
	}
	var spec struct {
		OpenAPI string                    `yaml:"openapi"`
		Paths   map[string]map[string]any `yaml:"paths"`
	}
	if e = yaml.Unmarshal(b, &spec); e != nil {
		t.Fatal(e)
	}
	if spec.OpenAPI != "3.1.0" {
		t.Fatal(spec.OpenAPI)
	}
	expected := []string{}
	for path, ops := range spec.Paths {
		for method := range ops {
			if ops["x-served-by"] == "stream-gateway" {
				continue
			}
			if slices.Contains([]string{"get", "post", "patch", "delete", "put", "options", "head"}, method) {
				expected = append(expected, strings.ToUpper(method)+" "+path)
			}
		}
	}
	actual := Routes()
	slices.Sort(actual)
	slices.Sort(expected)
	if !slices.Equal(actual, expected) {
		t.Fatalf("route mismatch\nactual: %v\nOpenAPI: %v", actual, expected)
	}
}

func TestObservabilityEnums(t *testing.T) {
	raw, err := os.ReadFile("../../docs/openapi.yaml")
	if err != nil {
		t.Fatal(err)
	}
	var doc struct {
		Components struct {
			Schemas map[string]struct {
				Properties map[string]struct {
					Enum []any `yaml:"enum"`
				} `yaml:"properties"`
			} `yaml:"schemas"`
		} `yaml:"components"`
	}
	if err = yaml.Unmarshal(raw, &doc); err != nil {
		t.Fatal(err)
	}
	var reasons []string
	for _, value := range doc.Components.Schemas["Segment"].Properties["end_reason"].Enum {
		if value != nil {
			reasons = append(reasons, value.(string))
		}
	}
	slices.Sort(reasons)
	if !slices.Equal(reasons, sessions.EndReasons()) {
		t.Fatalf("end reason drift: %v", reasons)
	}
	var generic map[string]any
	if err := yaml.Unmarshal(raw, &generic); err != nil {
		t.Fatal(err)
	}
	schemas := generic["components"].(map[string]any)["schemas"].(map[string]any)
	for _, name := range []string{"RunCost", "UsageReport"} {
		properties := schemas[name].(map[string]any)["properties"].(map[string]any)
		items := properties["items"].(map[string]any)["items"].(map[string]any)
		enum := items["properties"].(map[string]any)["meter"].(map[string]any)["enum"].([]any)
		var got, want []string
		for _, value := range enum {
			if value != nil {
				got = append(got, value.(string))
			}
		}
		for _, value := range usage.Meters() {
			want = append(want, string(value))
		}
		slices.Sort(got)
		slices.Sort(want)
		if !slices.Equal(got, want) {
			t.Fatalf("%s meter drift: %v", name, got)
		}
	}
}

func TestOpenAPIStrictYAMLAndErrorCodes(t *testing.T) {
	raw, err := os.ReadFile("../../docs/openapi.yaml")
	if err != nil {
		t.Fatal(err)
	}
	var node yaml.Node
	if err = yaml.Unmarshal(raw, &node); err != nil {
		t.Fatal(err)
	}
	anchors := map[string]bool{}
	var visit func(*yaml.Node)
	visit = func(n *yaml.Node) {
		if n.Anchor != "" {
			if anchors[n.Anchor] {
				t.Errorf("duplicate YAML anchor %s", n.Anchor)
			}
			anchors[n.Anchor] = true
		}
		for _, child := range n.Content {
			visit(child)
		}
	}
	visit(&node)
	var doc map[string]any
	if err = yaml.Unmarshal(raw, &doc); err != nil {
		t.Fatal(err)
	}
	schema := doc["components"].(map[string]any)["schemas"].(map[string]any)["Error"].(map[string]any)
	code := schema["properties"].(map[string]any)["error"].(map[string]any)["properties"].(map[string]any)["code"].(map[string]any)
	enum := map[string]bool{}
	for _, value := range code["enum"].([]any) {
		enum[value.(string)] = true
	}
	pattern := regexp.MustCompile(`httpx\.E\(\s*\d+\s*,\s*"([A-Z][A-Z_0-9]+)"`)
	err = filepath.WalkDir("../..", func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() && strings.HasPrefix(entry.Name(), ".") && entry.Name() != ".." {
			return filepath.SkipDir
		}
		if entry.IsDir() || !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		source, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		for _, match := range pattern.FindAllSubmatch(source, -1) {
			if !enum[string(match[1])] {
				t.Errorf("%s: undocumented error code %s", path, match[1])
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

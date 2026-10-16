package httpapi

import (
	"os"
	"slices"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
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

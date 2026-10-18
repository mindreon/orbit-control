package archtest

import (
	"bytes"
	"encoding/json/jsontext"
	"encoding/json/v2"
	"os/exec"
	"strings"
	"testing"
)

func TestPackageBoundaries(t *testing.T) {
	cmd := exec.CommandContext(
		t.Context(),
		"go",
		"list",
		"-json",
		"-deps",
		"./internal/workflows/...",
		"./internal/httpapi",
		"./internal/kernelapi",
		"./internal/streamgw",
		"./internal/platform/telemetry",
	)
	cmd.Dir = "../.."
	raw, err := cmd.Output()
	if err != nil {
		t.Fatal(err)
	}
	// go list emits a sequence of independent JSON objects.
	var packages []struct {
		ImportPath string
		Imports    []string
		Standard   bool
	}
	for len(strings.TrimSpace(string(raw))) > 0 {
		var item struct {
			ImportPath string
			Imports    []string
			Standard   bool
		}
		dec := jsontext.NewDecoder(bytes.NewReader(raw))
		value, err := dec.ReadValue()
		if err != nil {
			t.Fatal(err)
		}
		if err = json.Unmarshal(value, &item); err != nil {
			t.Fatal(err)
		}
		raw = raw[dec.InputOffset():]
		packages = append(packages, item)
	}
	const module = "github.com/mindreon/orbit-control/internal/"
	standard := map[string]bool{}
	for _, pkg := range packages {
		standard[pkg.ImportPath] = pkg.Standard
	}
	for _, pkg := range packages {
		if strings.HasPrefix(pkg.ImportPath, module+"workflows") {
			for _, dep := range pkg.Imports {
				allowed := standard[dep] || dep == module+"platform/telemetry/wfctx" || dep == module+"contract" || dep == module+"team" ||
					dep == module+"workflows" ||
					strings.HasPrefix(dep, module+"workflows/") || strings.HasPrefix(dep, "go.temporal.io/sdk/") ||
					strings.HasPrefix(dep, "go.temporal.io/api/")
				if !allowed {
					t.Errorf("%s imports forbidden %s", pkg.ImportPath, dep)
				}
			}
		}
		if pkg.ImportPath == module+"platform/telemetry" {
			for _, dep := range pkg.Imports {
				if strings.HasPrefix(dep, "go.temporal.io/") || strings.HasPrefix(dep, "github.com/jackc/") || dep == module+"platform/db" {
					t.Errorf("telemetry imports forbidden orchestration dependency %s", dep)
				}
			}
		}
		if pkg.ImportPath == module+"platform/telemetry/wfctx" {
			for _, dep := range pkg.Imports {
				if dep != "go.temporal.io/sdk/workflow" {
					t.Errorf("wfctx imports forbidden %s", dep)
				}
			}
		}
		if pkg.ImportPath == module+"team" {
			for _, dep := range pkg.Imports {
				if dep != "go.temporal.io/sdk/workflow" && strings.HasPrefix(dep, "go.temporal.io/") ||
					strings.HasPrefix(dep, module+"platform/db") || strings.HasPrefix(dep, module+"httpapi") {
					t.Errorf("%s imports forbidden %s", pkg.ImportPath, dep)
				}
			}
		}
		if pkg.ImportPath == module+"streamgw" {
			for _, dep := range pkg.Imports {
				allowed := standard[dep] || dep == module+"auth" || dep == module+"contract" ||
					dep == module+"platform/log" || dep == module+"platform/natsx" ||
					strings.HasPrefix(dep, "github.com/nats-io/nats.go") || strings.HasPrefix(dep, "go.opentelemetry.io/otel")
				if !allowed {
					t.Errorf("%s imports forbidden %s", pkg.ImportPath, dep)
				}
			}
		}

		if pkg.ImportPath == module+"httpapi" || pkg.ImportPath == module+"kernelapi" {
			for _, dep := range pkg.Imports {
				if strings.HasPrefix(dep, module+"workflows/") {
					t.Errorf("%s imports Workflow implementation %s", pkg.ImportPath, dep)
				}
			}
		}
	}
}

//go:build e2e

package persistence

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"testing"
)

// S-DB-1 allowlist: methods that may skip the tenantID parameter. Changes
// need Sentinel review (docs/persistence-failure-modes.md ISO-9, FM-25).
var tenantParamAllowlist = map[string]bool{
	"Close": true,
}

var reTenantTableSQL = regexp.MustCompile(`(?i)\b(FROM|INTO|UPDATE|JOIN)\s+(public\.)?(users|rooms|turns|events|messages|approvals|approval_rules|idempotency_keys|artifacts|artifact_versions|personas|mcp_connectors|cloud_agent_jobs)\b`)

type methodFinding struct {
	Method string `json:"method"`
	Issue  string `json:"issue"`
}

func parseDir(t *testing.T, dir string) (*token.FileSet, []*ast.File) {
	t.Helper()
	fset := token.NewFileSet()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	var files []*ast.File
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".go") || strings.HasSuffix(e.Name(), "_test.go") {
			continue
		}
		f, err := parser.ParseFile(fset, filepath.Join(dir, e.Name()), nil, parser.SkipObjectResolution)
		if err != nil {
			t.Fatal(err)
		}
		files = append(files, f)
	}
	return fset, files
}

func hasTenantParam(ft *ast.FuncType) bool {
	for _, field := range ft.Params.List {
		for _, n := range field.Names {
			if n.Name == "tenantID" {
				return true
			}
		}
	}
	return false
}

func usesIdent(body *ast.BlockStmt, name string) bool {
	found := false
	ast.Inspect(body, func(n ast.Node) bool {
		if id, ok := n.(*ast.Ident); ok && id.Name == name {
			found = true
		}
		return !found
	})
	return found
}

// storeMethods checks exported methods on the named receiver type.
func storeMethods(t *testing.T, dir, recv string) (checked []string, findings []methodFinding) {
	_, files := parseDir(t, dir)
	for _, f := range files {
		for _, d := range f.Decls {
			fn, ok := d.(*ast.FuncDecl)
			if !ok || fn.Recv == nil || !fn.Name.IsExported() {
				continue
			}
			star, ok := fn.Recv.List[0].Type.(*ast.StarExpr)
			if !ok {
				continue
			}
			if id, ok := star.X.(*ast.Ident); !ok || id.Name != recv {
				continue
			}
			name := filepath.Base(dir) + "." + fn.Name.Name
			checked = append(checked, name)
			if tenantParamAllowlist[fn.Name.Name] {
				continue
			}
			if !hasTenantParam(fn.Type) {
				findings = append(findings, methodFinding{name, "FM-25: no tenantID parameter"})
			} else if !usesIdent(fn.Body, "tenantID") {
				findings = append(findings, methodFinding{name, "FM-26: tenantID is never used"})
			}
		}
	}
	sort.Strings(checked)
	return checked, findings
}

func TestSDB01StaticTenantScopedRepository(t *testing.T) {
	const c = "S-DB-1"
	root, err := repoRoot()
	if err != nil {
		t.Fatal(err)
	}

	// Interface.
	_, files := parseDir(t, filepath.Join(root, "internal/store"))
	var ifaceChecked []string
	var ifaceFindings []methodFinding
	for _, f := range files {
		ast.Inspect(f, func(n ast.Node) bool {
			ts, ok := n.(*ast.TypeSpec)
			if !ok || ts.Name.Name != "Repository" {
				return true
			}
			it, ok := ts.Type.(*ast.InterfaceType)
			if !ok {
				return true
			}
			for _, m := range it.Methods.List {
				ft, ok := m.Type.(*ast.FuncType)
				if !ok || len(m.Names) == 0 {
					continue
				}
				name := "Repository." + m.Names[0].Name
				ifaceChecked = append(ifaceChecked, name)
				if !tenantParamAllowlist[m.Names[0].Name] && !hasTenantParam(ft) {
					ifaceFindings = append(ifaceFindings, methodFinding{name, "FM-25: no tenantID parameter"})
				}
			}
			return false
		})
	}
	sort.Strings(ifaceChecked)
	if ifaceFindings == nil {
		ifaceFindings = []methodFinding{}
	}
	record(t, caseInput{ID: "S-DB-1/interface-methods", Contract: c, Kind: "static", FailureModes: []string{"FM-25"},
		Description: "every store.Repository method takes tenantID (allowlist: Close)",
		Steps:       []string{"parse internal/store with go/ast", "inspect the Repository interface's method parameters"},
		Request:     map[string]any{"package": "internal/store", "allowlist": []string{"Close"}},
		Expected:    map[string]any{"findings": []methodFinding{}, "methodsAtLeast": 10},
		Actual:      map[string]any{"findings": ifaceFindings, "methods": ifaceChecked},
		Pass:        len(ifaceFindings) == 0 && len(ifaceChecked) >= 10})

	for _, impl := range []struct{ dir, recv string }{{"internal/store/pgstore", "Store"}, {"internal/store/memstore", "Store"}} {
		checked, findings := storeMethods(t, filepath.Join(root, impl.dir), impl.recv)
		if findings == nil {
			findings = []methodFinding{}
		}
		record(t, caseInput{ID: "S-DB-1/methods/" + filepath.Base(impl.dir), Contract: c, Kind: "static", FailureModes: []string{"FM-25", "FM-26"},
			Description: "every exported " + filepath.Base(impl.dir) + " method takes and uses tenantID",
			Steps:       []string{"parse " + impl.dir + " with go/ast", "for each exported *Store method: tenantID parameter present and referenced in the body"},
			Request:     map[string]any{"package": impl.dir, "allowlist": []string{"Close"}},
			Expected:    map[string]any{"findings": []methodFinding{}},
			Actual:      map[string]any{"findings": findings, "methods": checked},
			Pass:        len(findings) == 0 && len(checked) >= 10})
	}

	// SQL literals in pgstore touching [T] tables must carry tenant_id.
	fset, pgFiles := parseDir(t, filepath.Join(root, "internal/store/pgstore"))
	var sqlChecked int
	sqlFindings := []finding{}
	for _, f := range pgFiles {
		ast.Inspect(f, func(n ast.Node) bool {
			b, ok := n.(*ast.BasicLit)
			if !ok || b.Kind != token.STRING {
				return true
			}
			s, err := strconv.Unquote(b.Value)
			if err != nil || !reTenantTableSQL.MatchString(s) {
				return true
			}
			sqlChecked++
			if !strings.Contains(s, "tenant_id") {
				pos := fset.Position(b.Pos())
				rel, _ := filepath.Rel(root, pos.Filename)
				sqlFindings = append(sqlFindings, finding{filepath.ToSlash(rel) + ":" + strconv.Itoa(pos.Line), "FM-27: [T] table statement without tenant_id"})
			}
			return true
		})
	}
	record(t, caseInput{ID: "S-DB-1/sql-tenant-predicate", Contract: c, Kind: "static", FailureModes: []string{"FM-27"},
		Description: "every pgstore SQL literal that reads or writes a [T] table contains tenant_id",
		Steps:       []string{"parse internal/store/pgstore string literals", "for statements naming a [T] table after FROM/INTO/UPDATE/JOIN: require tenant_id"},
		Request:     map[string]any{"package": "internal/store/pgstore"},
		Expected:    map[string]any{"findings": []finding{}, "statementsAtLeast": 10},
		Actual:      map[string]any{"findings": sqlFindings, "statements": sqlChecked},
		Pass:        len(sqlFindings) == 0 && sqlChecked >= 10})
}

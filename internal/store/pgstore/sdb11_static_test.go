package pgstore_test

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"testing"
)

// S-DB-11 (a) static check, plus the soft-delete rules from §18.5.
//
// Changes to sdb11SecurityDefinerAllowlist need Sentinel review.
var sdb11SecurityDefinerAllowlist = map[string]string{
	"orbit_soft_delete_room": "internal/store/migrations/sql/00003_soft_delete_room.sql",
}

var (
	reSetTenantGUC   = regexp.MustCompile(`(?i)\bSET\s+(LOCAL\s+|SESSION\s+)?app\.tenant_id\b`)
	reSetConfigCall  = regexp.MustCompile(`(?i)\bset_config\s*\(`)
	reSetConfigOK    = regexp.MustCompile(`(?i)^set_config\(\s*'app\.tenant_id'\s*,\s*\$1\s*,\s*true\s*\)`)
	reSecDefiner     = regexp.MustCompile(`(?i)\bSECURITY\s+DEFINER\b`)
	reSoftDeleteSet  = regexp.MustCompile(`(?i)\bdeleted_(at|by)\s*=`)
	reSQLFunction    = regexp.MustCompile(`(?is)CREATE\s+(?:OR\s+REPLACE\s+)?FUNCTION\s+(?:public\.)?([a-z_][a-z0-9_]*)\s*\(.*?\$\$.*?\$\$`)
	reSQLLineComment = regexp.MustCompile(`--[^\n]*`)
	reStartsWithSET  = regexp.MustCompile(`(?i)^\s*SET\s`)
)

type sdb11Finding struct {
	where string
	rule  string
}

type span struct {
	start, end int
	name       string
}

// checkSQLText applies the text rules. fns are allowlisted function spans in
// which SECURITY DEFINER and deleted_at/deleted_by assignments are allowed.
func checkSQLText(where, text string, fns []span) []sdb11Finding {
	var out []sdb11Finding
	inAllowed := func(pos int) bool {
		for _, f := range fns {
			if pos >= f.start && pos < f.end {
				return true
			}
		}
		return false
	}
	for range reSetTenantGUC.FindAllStringIndex(text, -1) {
		out = append(out, sdb11Finding{where, "session or LOCAL SET of app.tenant_id; use the bound transaction-local form"})
	}
	for _, loc := range reSetConfigCall.FindAllStringIndex(text, -1) {
		if !reSetConfigOK.MatchString(text[loc[0]:]) {
			out = append(out, sdb11Finding{where, "set_config must be exactly set_config('app.tenant_id', $1, true)"})
		}
	}
	for _, loc := range reSecDefiner.FindAllStringIndex(text, -1) {
		if !inAllowed(loc[0]) {
			out = append(out, sdb11Finding{where, "security-definer function outside the allowlist"})
		}
	}
	for _, loc := range reSoftDeleteSet.FindAllStringIndex(text, -1) {
		if !inAllowed(loc[0]) {
			out = append(out, sdb11Finding{where, "deleted_at/deleted_by written outside orbit_soft_delete_room"})
		}
	}
	return out
}

func checkSQLFile(rel, content string) []sdb11Finding {
	text := reSQLLineComment.ReplaceAllString(content, "")
	var fns []span
	for _, m := range reSQLFunction.FindAllStringSubmatchIndex(text, -1) {
		name := strings.ToLower(text[m[2]:m[3]])
		if sdb11SecurityDefinerAllowlist[name] == rel {
			fns = append(fns, span{start: m[0], end: m[1], name: name})
		}
	}
	return checkSQLText(rel, text, fns)
}

// checkGoFile scans string literals only (comments may discuss the rules),
// and rejects SET statements assembled by concatenation or Sprintf.
func checkGoFile(rel string, src []byte) []sdb11Finding {
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, rel, src, parser.SkipObjectResolution)
	if err != nil {
		return []sdb11Finding{{rel, "unparseable Go file: " + err.Error()}}
	}
	var out []sdb11Finding
	lit := func(e ast.Expr) (string, bool) {
		b, ok := e.(*ast.BasicLit)
		if !ok || b.Kind != token.STRING {
			return "", false
		}
		s, err := strconv.Unquote(b.Value)
		return s, err == nil
	}
	ast.Inspect(file, func(n ast.Node) bool {
		switch x := n.(type) {
		case *ast.BasicLit:
			if s, ok := lit(x); ok {
				where := rel + ":" + strconv.Itoa(fset.Position(x.Pos()).Line)
				out = append(out, checkSQLText(where, s, nil)...)
			}
		case *ast.BinaryExpr:
			if x.Op != token.ADD {
				return true
			}
			left := x.X
			for {
				inner, ok := left.(*ast.BinaryExpr)
				if !ok || inner.Op != token.ADD {
					break
				}
				left = inner.X
			}
			if s, ok := lit(left); ok && reStartsWithSET.MatchString(s) {
				out = append(out, sdb11Finding{rel + ":" + strconv.Itoa(fset.Position(x.Pos()).Line), "SET statement built by string concatenation"})
			}
		case *ast.CallExpr:
			sel, ok := x.Fun.(*ast.SelectorExpr)
			if !ok || len(x.Args) == 0 || !strings.HasSuffix(sel.Sel.Name, "printf") {
				return true
			}
			if s, ok := lit(x.Args[0]); ok && reStartsWithSET.MatchString(s) {
				out = append(out, sdb11Finding{rel + ":" + strconv.Itoa(fset.Position(x.Pos()).Line), "SET statement built with Sprintf"})
			}
		}
		return true
	})
	return out
}

func repoRoot(t *testing.T) string {
	t.Helper()
	dir, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			t.Fatal("go.mod not found")
		}
		dir = parent
	}
}

func TestSDB11aStaticTenantGUCAndSoftDeleteRules(t *testing.T) {
	root := repoRoot(t)
	var findings []sdb11Finding
	scanned := 0
	err := filepath.WalkDir(root, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			switch d.Name() {
			case ".git", "vendor", "node_modules":
				return filepath.SkipDir
			}
			return nil
		}
		ext := filepath.Ext(path)
		if ext != ".go" && ext != ".sql" {
			return nil
		}
		rel, _ := filepath.Rel(root, path)
		rel = filepath.ToSlash(rel)
		raw, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		scanned++
		if ext == ".sql" {
			findings = append(findings, checkSQLFile(rel, string(raw))...)
		} else {
			findings = append(findings, checkGoFile(rel, raw)...)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if scanned == 0 {
		t.Fatal("static check scanned no files")
	}
	for _, f := range findings {
		t.Errorf("%s: %s", f.where, f.rule)
	}
	for name, rel := range sdb11SecurityDefinerAllowlist {
		raw, err := os.ReadFile(filepath.Join(root, rel))
		if err != nil {
			t.Fatalf("allowlisted %s: %v", name, err)
		}
		if !regexp.MustCompile(`(?i)FUNCTION\s+` + name + `\s*\(`).Match(raw) {
			t.Fatalf("allowlisted function %s is not defined in %s", name, rel)
		}
	}
}

// The checker itself must reject the forbidden shapes. Samples are joined at
// runtime so this file does not trip the repo scan.
func TestSDB11aCheckerRejectsForbiddenShapes(t *testing.T) {
	j := func(parts ...string) string { return strings.Join(parts, "") }
	sqlBad := []string{
		j("SE", "T app.tenant_id = 't1'"),
		j("SE", "T LOCAL app.tenant_id = 't1'"),
		j("SE", "T SESSION app.tenant_id TO 't1'"),
		j("SELECT set_", "config('app.tenant_id', $1, false)"),
		j("SELECT set_", "config('app.tenant_id', $1, $2)"),
		j("SELECT set_", "config('app.tenant_id', 't1', true)"),
		j("UPDATE rooms SET deleted_", "at = now() WHERE id = $1"),
	}
	for _, s := range sqlBad {
		if len(checkSQLText("sample", s, nil)) == 0 {
			t.Errorf("checker accepted %q", s)
		}
	}
	if got := checkSQLText("sample", j("SELECT set_", "config('app.tenant_id', $1, true)"), nil); len(got) != 0 {
		t.Errorf("checker rejected the allowed form: %+v", got)
	}
	fn := j("CREATE FUNCTION other_fn() RETURNS int LANGUAGE sql SECURITY", " DEFINER AS $$ SELECT 1 $$;")
	if len(checkSQLFile("internal/store/migrations/sql/99999_x.sql", fn)) == 0 {
		t.Error("checker accepted a security-definer function outside the allowlist")
	}
	goBad := []string{
		j("package x\nvar tenant string\nvar q = \"SE", "T app.tenant_id = '\" + tenant + \"'\"\n"),
		j("package x\nimport \"fmt\"\nvar q = fmt.Sprintf(\"SE", "T LOCAL app.tenant_id = '%s'\", \"t\")\n"),
		j("package x\nvar tenant string\nvar q = \"SE", "T search_path = \" + tenant\n"),
	}
	for _, src := range goBad {
		if len(checkGoFile("sample.go", []byte(src))) == 0 {
			t.Errorf("checker accepted Go source:\n%s", src)
		}
	}
}

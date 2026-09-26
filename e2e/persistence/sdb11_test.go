//go:build e2e

package persistence

import (
	"context"
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

// S-DB-11 (a) allowlist of SECURITY DEFINER functions. Changes need Sentinel
// review (docs/persistence-failure-modes.md ISO-1, FM-4).
var securityDefinerAllowlist = map[string]string{
	"orbit_soft_delete_room": "internal/store/migrations/sql/00003_soft_delete_room.sql",
}

const (
	ruleSetGUC     = "FM-1/FM-2: SET, SET LOCAL or SET SESSION of app.tenant_id"
	ruleSetConfig  = "FM-3: set_config other than the bound transaction-local form"
	ruleConcatSET  = "FM-2: SET statement built by concatenation or printf"
	ruleSecDefiner = "FM-4: security-definer function outside the allowlist"
	ruleSoftDelete = "FM-5: deleted_at/deleted_by written outside orbit_soft_delete_room"
)

var staticRules = []string{ruleSetGUC, ruleSetConfig, ruleConcatSET, ruleSecDefiner, ruleSoftDelete}

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

type finding struct {
	Where string `json:"where"`
	Rule  string `json:"rule"`
}

type span struct{ start, end int }

func scanText(where, text string, allowed []span) []finding {
	var out []finding
	inAllowed := func(pos int) bool {
		for _, s := range allowed {
			if pos >= s.start && pos < s.end {
				return true
			}
		}
		return false
	}
	for range reSetTenantGUC.FindAllStringIndex(text, -1) {
		out = append(out, finding{where, ruleSetGUC})
	}
	for _, loc := range reSetConfigCall.FindAllStringIndex(text, -1) {
		if !reSetConfigOK.MatchString(text[loc[0]:]) {
			out = append(out, finding{where, ruleSetConfig})
		}
	}
	for _, loc := range reSecDefiner.FindAllStringIndex(text, -1) {
		if !inAllowed(loc[0]) {
			out = append(out, finding{where, ruleSecDefiner})
		}
	}
	for _, loc := range reSoftDeleteSet.FindAllStringIndex(text, -1) {
		if !inAllowed(loc[0]) {
			out = append(out, finding{where, ruleSoftDelete})
		}
	}
	return out
}

func scanSQL(rel, content string) []finding {
	text := reSQLLineComment.ReplaceAllString(content, "")
	var allowed []span
	for _, m := range reSQLFunction.FindAllStringSubmatchIndex(text, -1) {
		if securityDefinerAllowlist[strings.ToLower(text[m[2]:m[3]])] == rel {
			allowed = append(allowed, span{m[0], m[1]})
		}
	}
	return scanText(rel, text, allowed)
}

// scanGo checks string literals only, so comments may discuss the rules.
func scanGo(rel string, src []byte) []finding {
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, rel, src, parser.SkipObjectResolution)
	if err != nil {
		return []finding{{rel, "unparseable Go file: " + err.Error()}}
	}
	at := func(n ast.Node) string { return rel + ":" + strconv.Itoa(fset.Position(n.Pos()).Line) }
	lit := func(e ast.Expr) (string, bool) {
		b, ok := e.(*ast.BasicLit)
		if !ok || b.Kind != token.STRING {
			return "", false
		}
		s, err := strconv.Unquote(b.Value)
		return s, err == nil
	}
	var out []finding
	ast.Inspect(file, func(n ast.Node) bool {
		switch x := n.(type) {
		case *ast.BasicLit:
			if s, ok := lit(x); ok {
				out = append(out, scanText(at(x), s, nil)...)
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
				out = append(out, finding{at(x), ruleConcatSET})
			}
		case *ast.CallExpr:
			sel, ok := x.Fun.(*ast.SelectorExpr)
			if !ok || len(x.Args) == 0 || !strings.HasSuffix(sel.Sel.Name, "printf") {
				return true
			}
			if s, ok := lit(x.Args[0]); ok && reStartsWithSET.MatchString(s) {
				out = append(out, finding{at(x), ruleConcatSET})
			}
		}
		return true
	})
	return out
}

func TestSDB11aStaticCheck(t *testing.T) {
	const c = "S-DB-11 (a)"
	fms := []string{"FM-1", "FM-2", "FM-3", "FM-4", "FM-5"}

	// The checker must reject each forbidden shape before its scan counts.
	// Samples are joined at runtime so this file does not trip the scan.
	j := func(parts ...string) string { return strings.Join(parts, "") }
	for i, s := range []struct{ kind, src, rule string }{
		{"sql", j("SE", "T app.tenant_id = 't1'"), ruleSetGUC},
		{"sql", j("SE", "T LOCAL app.tenant_id = 't1'"), ruleSetGUC},
		{"sql", j("SE", "T SESSION app.tenant_id TO 't1'"), ruleSetGUC},
		{"sql", j("SELECT set_", "config('app.tenant_id', $1, false)"), ruleSetConfig},
		{"sql", j("SELECT set_", "config('app.tenant_id', $1, $2)"), ruleSetConfig},
		{"sql", j("SELECT set_", "config('app.tenant_id', 't1', true)"), ruleSetConfig},
		{"sql", j("UPDATE rooms SET deleted_", "at = now() WHERE id = $1"), ruleSoftDelete},
		{"sqlfile", j("CREATE FUNCTION other_fn() RETURNS int LANGUAGE sql SECURITY", " DEFINER AS $$ SELECT 1 $$;"), ruleSecDefiner},
		{"go", j("package x\nvar tenant string\nvar q = \"SE", "T app.tenant_id = '\" + tenant + \"'\"\n"), ruleConcatSET},
		{"go", j("package x\nimport \"fmt\"\nvar q = fmt.Sprintf(\"SE", "T LOCAL app.tenant_id = '%s'\", \"t\")\n"), ruleConcatSET},
		{"go", j("package x\nvar tenant string\nvar q = \"SE", "T search_path = \" + tenant\n"), ruleConcatSET},
		{"sql-allowed", j("SELECT set_", "config('app.tenant_id', $1, true)"), ""},
	} {
		var got []finding
		switch s.kind {
		case "go":
			got = scanGo("sample.go", []byte(s.src))
		case "sqlfile":
			got = scanSQL("internal/store/migrations/sql/99999_sample.sql", s.src)
		default:
			got = scanText("sample", s.src, nil)
		}
		rules := map[string]bool{}
		for _, f := range got {
			rules[f.Rule] = true
		}
		pass := rules[s.rule] || (s.rule == "" && len(got) == 0)
		expected := "rejected by " + s.rule
		if s.rule == "" {
			expected = "accepted"
		}
		iso(t, "S-DB-11(a)/checker-sample-"+strconv.Itoa(i+1), c, fms, "checker self-test on a "+s.kind+" sample",
			map[string]string{"sample": s.src}, expected, got, pass)
	}

	root, err := repoRoot()
	if err != nil {
		t.Fatal(err)
	}
	byRule := map[string][]finding{}
	var scanned []string
	err = filepath.WalkDir(root, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			switch d.Name() {
			case ".git", "vendor", "node_modules", "artifacts":
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
		scanned = append(scanned, rel)
		var fs []finding
		if ext == ".sql" {
			fs = scanSQL(rel, string(raw))
		} else {
			fs = scanGo(rel, raw)
		}
		for _, f := range fs {
			byRule[f.Rule] = append(byRule[f.Rule], f)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	sort.Strings(scanned)
	for _, rule := range staticRules {
		got := byRule[rule]
		if got == nil {
			got = []finding{}
		}
		iso(t, "S-DB-11(a)/scan/"+strings.SplitN(rule, ":", 2)[0], c, fms, "repo scan: "+rule,
			map[string]any{"filesScanned": len(scanned), "extensions": []string{".go", ".sql"}}, []finding{}, got, len(got) == 0)
	}
	for name, rel := range securityDefinerAllowlist {
		raw, err := os.ReadFile(filepath.Join(root, rel))
		defined := err == nil && regexp.MustCompile(`(?i)FUNCTION\s+(public\.)?`+name+`\s*\(`).Match(raw)
		iso(t, "S-DB-11(a)/allowlist-"+name, c, []string{"FM-4"}, "every allowlisted function is defined where the allowlist says",
			map[string]string{"file": rel}, "defined", map[string]bool{"defined": defined}, defined)
	}
}

// S-DB-11 (b): the server runs with a single pooled connection. Request A is
// served over HTTP; the same backend then shows no tenant and no rows.
func TestSDB11bPooledConnectionDoesNotLeakTenant(t *testing.T) {
	const c = "S-DB-11 (b)"
	ctx := context.Background()
	const tenant, u = "t-sdb11", "u-sdb11"
	wk := stubWorker(t, nil)
	srv := startServer(t, serverOpts{tenant: tenant, maxConns: 1, workerURL: wk.URL})

	var pidBefore int
	if err := srv.appPool.QueryRow(ctx, `SELECT pg_backend_pid()`).Scan(&pidBefore); err != nil {
		t.Fatal(err)
	}
	srv.check(t, "S-DB-11(b)/create", c, "create a room so the tenant has data",
		httpReq{Method: "POST", Path: "/v1/rooms", Headers: user(u), Body: `{"kind":"solo"}`}, httpExp{Status: 200})
	srv.check(t, "S-DB-11(b)/request-A-commit", c, "request A: a committed tenant transaction that sees the room",
		httpReq{Method: "GET", Path: "/v1/rooms", Headers: user(u)}, httpExp{Status: 200, BodyIncludes: []string{`"id":"rm_`}})
	srv.check(t, "S-DB-11(b)/request-A-rollback", c, "request A': a tenant transaction that rolls back (404)",
		httpReq{Method: "GET", Path: "/v1/rooms/rm_missing", Headers: user(u)}, httpExp{Status: 404})

	var pid int
	var guc *string
	if err := srv.appPool.QueryRow(ctx, `SELECT pg_backend_pid(), current_setting('app.tenant_id', true)`).Scan(&pid, &guc); err != nil {
		t.Fatal(err)
	}
	counts := map[string]int{}
	for _, table := range []string{"rooms", "users", "messages", "idempotency_keys"} {
		var n int
		if err := srv.appPool.QueryRow(ctx, `SELECT count(*) FROM `+table).Scan(&n); err != nil {
			t.Fatal(err)
		}
		counts[table] = n
	}
	gucValue := ""
	if guc != nil {
		gucValue = *guc
	}
	zero := true
	for _, n := range counts {
		zero = zero && n == 0
	}
	iso(t, "S-DB-11(b)/request-B", c, []string{"FM-6", "FM-7"}, "request B on the same pooled backend: no tenant GUC, 0 rows in [T] tables",
		sqlReq{Role: "orbit_app", SQL: "SELECT pg_backend_pid(), current_setting('app.tenant_id', true); SELECT count(*) FROM <table>"},
		map[string]any{"samePid": true, "poolConns": 1, "appTenantID": "", "rows": "0 in every table"},
		map[string]any{"samePid": pid == pidBefore, "poolConns": srv.appPool.Stat().TotalConns(), "appTenantID": gucValue, "rows": counts},
		pid == pidBefore && srv.appPool.Stat().TotalConns() == 1 && gucValue == "" && zero)
}

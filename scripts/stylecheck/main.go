// Command stylecheck checks source rules that gofmt does not enforce.
package main

import (
	"bytes"
	"flag"
	"fmt"
	"go/ast"
	"go/format"
	"go/parser"
	"go/scanner"
	"go/token"
	"go/types"
	"os"
	"path/filepath"
	"slices"
	"strings"

	"golang.org/x/tools/go/packages"
)

type edit struct {
	start, end int
	text       string
}

var tags = flag.String("tags", "", "additional build tags to check")

var fix = flag.Bool("fix", false, "apply keyed literal and statement layout fixes")

func main() {
	flag.Parse()
	cfg := &packages.Config{Mode: packages.NeedName | packages.NeedFiles | packages.NeedCompiledGoFiles |
		packages.NeedSyntax | packages.NeedTypes | packages.NeedTypesInfo | packages.NeedImports | packages.NeedDeps, Tests: true}
	if *tags != "" {
		cfg.BuildFlags = []string{"-tags=" + *tags}
	}
	pkgs, err := packages.Load(cfg, "./...")
	if err != nil {
		panic(err)
	}
	if packages.PrintErrors(pkgs) > 0 {
		os.Exit(1)
	}
	seen := map[string]bool{}
	failures := 0
	for _, pkg := range pkgs {
		for _, file := range pkg.Syntax {
			name := pkg.Fset.Position(file.Pos()).Filename
			if seen[name] || !strings.HasPrefix(name, cwd()+string(filepath.Separator)) {
				continue
			}
			seen[name] = true
			raw, err := os.ReadFile(name)
			if err != nil {
				panic(err)
			}
			if ast.IsGenerated(file) {
				continue
			}
			edits := literalEdits(pkg, file)
			edits = append(edits, semicolonEdits(raw)...)
			if len(edits) == 0 {
				continue
			}
			if !*fix {
				fmt.Printf("%s: %d unkeyed literals or packed statements\n", name, len(edits))
				failures++
				continue
			}
			slices.SortFunc(edits, func(a, b edit) int { return b.start - a.start })
			for _, e := range edits {
				raw = append(append(append([]byte{}, raw[:e.start]...), e.text...), raw[e.end:]...)
			}
			raw, err = format.Source(raw)
			if err != nil {
				panic(fmt.Errorf("%s: %w", name, err))
			}
			if err = os.WriteFile(name, raw, 0644); err != nil {
				panic(err)
			}
		}
	}
	if failures > 0 {
		os.Exit(1)
	}
}
func cwd() string {
	dir, err := os.Getwd()
	if err != nil {
		panic(err)
	}
	return dir
}
func literalEdits(pkg *packages.Package, file *ast.File) []edit {
	var edits []edit
	ast.Inspect(file, func(node ast.Node) bool {
		lit, ok := node.(*ast.CompositeLit)
		if !ok || len(lit.Elts) == 0 {
			return true
		}
		typ := pkg.TypesInfo.TypeOf(lit)
		if typ == nil {
			return true
		}
		st, ok := typ.Underlying().(*types.Struct)
		if !ok {
			return true
		}
		for i, elt := range lit.Elts {
			if _, keyed := elt.(*ast.KeyValueExpr); keyed {
				continue
			}
			pos := pkg.Fset.Position(elt.Pos()).Offset
			edits = append(edits, edit{start: pos, end: pos, text: st.Field(i).Name() + ": "})
		}
		return true
	})
	return edits
}
func semicolonEdits(raw []byte) []edit {
	fs := token.NewFileSet()
	f, err := parser.ParseFile(fs, "source.go", raw, 0)
	if err != nil {
		panic(err)
	}
	var headers [][2]int
	ast.Inspect(f, func(node ast.Node) bool {
		var body *ast.BlockStmt
		switch n := node.(type) {
		case *ast.ForStmt:
			body = n.Body
		case *ast.IfStmt:
			body = n.Body
		case *ast.SwitchStmt:
			body = n.Body
		case *ast.TypeSwitchStmt:
			body = n.Body
		}
		if body != nil {
			headers = append(headers, [2]int{fs.Position(node.Pos()).Offset, fs.Position(body.Pos()).Offset})
		}

		return true
	})
	var scan scanner.Scanner
	file := fs.AddFile("tokens.go", -1, len(raw))
	scan.Init(file, raw, nil, 0)
	var edits []edit
	for {
		pos, tok, lit := scan.Scan()
		if tok == token.EOF {
			break
		}
		if tok != token.SEMICOLON || lit != ";" {
			continue
		}
		off := file.Offset(pos)
		if slices.ContainsFunc(headers, func(h [2]int) bool { return off >= h[0] && off < h[1] }) {
			continue
		}
		// A trailing semicolon is redundant; a separator becomes a new source line.
		text := "\n"
		if bytes.HasPrefix(bytes.TrimLeft(raw[off+1:], " \t"), []byte("\n")) {
			text = ""
		}
		edits = append(edits, edit{start: off, end: off + 1, text: text})
	}
	return edits
}

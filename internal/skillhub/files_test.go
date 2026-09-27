package skillhub

import (
	"archive/zip"
	"bytes"
	"strings"
	"testing"

	"github.com/mindreon/orbit-control/internal/store"
)

func TestTextFilesFromZipKeepsMarkdownAndDropsUnsafePaths(t *testing.T) {
	var buf bytes.Buffer
	zw := zip.NewWriter(&buf)
	write := func(name, body string) {
		w, err := zw.Create(name)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := w.Write([]byte(body)); err != nil {
			t.Fatal(err)
		}
	}
	write("SKILL.md", "# 周报\n只阅读")
	write("templates/01.md", "模板")
	write("../secret.md", "nope")
	write(".env", "TOKEN=value")
	write("notes.bin", "abc")
	if err := zw.Close(); err != nil {
		t.Fatal(err)
	}
	files, err := textFilesFromZip(buf.Bytes())
	if err != nil {
		t.Fatal(err)
	}
	if len(files) != 2 || files[0].Path != "SKILL.md" || !strings.Contains(files[0].Body, "周报") {
		t.Fatalf("files = %+v", files)
	}
	if files[1].Path != "templates/01.md" {
		t.Fatalf("second = %+v", files)
	}
}

func TestTextFilesKeepsOverviewAheadOfTheRuneBudget(t *testing.T) {
	var buf bytes.Buffer
	zw := zip.NewWriter(&buf)
	write := func(name, body string) {
		w, err := zw.Create(name)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := w.Write([]byte(body)); err != nil {
			t.Fatal(err)
		}
	}
	big := strings.Repeat("x", 50000)
	for _, name := range []string{"a.md", "b.md", "c.md", "d.md", "e.md"} {
		write(name, big)
	}
	write("SKILL.md", "# 技能说明")
	if err := zw.Close(); err != nil {
		t.Fatal(err)
	}
	files, err := textFilesFromZip(buf.Bytes())
	if err != nil {
		t.Fatal(err)
	}
	for _, file := range files {
		if file.Path == "SKILL.md" && strings.Contains(file.Body, "技能说明") {
			return
		}
	}
	t.Fatalf("overview missing from %+v", pathsOf(files))
}

func pathsOf(files []store.SkillFile) []string {
	out := make([]string, len(files))
	for i, file := range files {
		out[i] = file.Path
	}
	return out
}

func TestSkillHandleFromCanonicalName(t *testing.T) {
	if got := skillHandle("", "@indiv-ebandao/dev-expert"); got != "indiv-ebandao" {
		t.Fatalf("handle = %q", got)
	}
	if got := skillHandle("user_1fb43088", ""); got != "user_1fb43088" {
		t.Fatalf("explicit handle = %q", got)
	}
	if got := skillHandle("", ""); got != "" {
		t.Fatalf("empty handle = %q", got)
	}
}

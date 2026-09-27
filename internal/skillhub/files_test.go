package skillhub

import (
	"archive/zip"
	"bytes"
	"strings"
	"testing"
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

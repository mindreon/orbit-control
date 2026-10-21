package market

import (
	"os"
	"path/filepath"
	"testing"
)

func TestSkillFilesReadsTextBesideTheSkill(t *testing.T) {
	root := t.TempDir()
	dir := filepath.Join(root, "ebandao", "dev-expert", "references")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "ebandao", "dev-expert", "SKILL.md"), []byte("---\nname: x\n---\n# 你好\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "notes.md"), []byte("note"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "blob.bin"), []byte("a\x00b"), 0o644); err != nil {
		t.Fatal(err)
	}
	SetSkillsRoot(root)
	t.Cleanup(func() { SetSkillsRoot("") })

	files, err := SkillFiles("ebandao", "dev-expert")
	if err != nil {
		t.Fatal(err)
	}
	if len(files) != 2 || files[0].Path != "SKILL.md" || files[1].Path != "references/notes.md" {
		t.Fatalf("files = %+v", files)
	}
	if files, err := SkillFiles("../ebandao", "dev-expert"); err != nil || len(files) != 0 {
		t.Fatalf("escaped path files=%v err=%v", files, err)
	}
}

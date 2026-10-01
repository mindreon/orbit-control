package app

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/mindreon/orbit-control/internal/skillstore"
	"github.com/mindreon/orbit-control/internal/store"
	"github.com/mindreon/orbit-control/internal/store/memstore"
)

// Where the app reads a skill's files from: the library on disk first, then the text the catalog holds.
//
// How it can go wrong, written down before the code:
//   - the catalog's older text wins over the file an operator just updated on disk;
//   - a skill on disk with no SKILL.md, or with an unsafe name, is accepted because the catalog would have been;
//   - a skill that is too big for the library is quietly served from the catalog instead, which is another version;
//   - with no library configured, or a skill the library does not have, the catalog stops working.

const tenant = "t"

func appWith(t *testing.T, library map[string]string) (*App, *memstore.Store) {
	t.Helper()
	root := t.TempDir()
	for rel, body := range library {
		path := filepath.Join(root, filepath.FromSlash(rel))
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, []byte(body), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	skills, err := skillstore.Open(root)
	if err != nil {
		t.Fatal(err)
	}
	repo := memstore.New()
	return NewWithOptions(Options{Repo: repo, Skills: skills}), repo
}

func catalogSkill(t *testing.T, repo *memstore.Store, id, name, text string) {
	t.Helper()
	ctx := context.Background()
	if err := repo.ReplaceSkills(ctx, tenant, []store.SkillRecord{{ID: id, Handle: strings.Split(id, "/")[0], Slug: strings.Split(id, "/")[1], Name: name, Description: name + " d"}}, nil); err != nil {
		t.Fatal(err)
	}
	if text != "" {
		if err := repo.SaveSkillTextFilesBatch(ctx, tenant, []store.SkillTextFilesRow{{ID: id, Files: []store.SkillFile{{Path: "SKILL.md", Body: text}}}}); err != nil {
			t.Fatal(err)
		}
	}
}

func TestTheLibraryOnDiskWinsOverTheCatalogText(t *testing.T) {
	a, repo := appWith(t, map[string]string{"h/s/SKILL.md": "from disk", "h/s/refs/a.md": "ref"})
	catalogSkill(t, repo, "h/s", "S", "from the catalog")
	bundle, err := a.SkillBundleForWorker(context.Background(), tenant, "h", "s")
	if err != nil {
		t.Fatal(err)
	}
	if len(bundle.Files) != 2 || bundle.Files[0].Body != "from disk" || bundle.Name != "S" {
		t.Fatalf("the disk's files with the catalog's name: %+v", bundle)
	}
}

func TestASkillTheLibraryDoesNotHaveStillComesFromTheCatalog(t *testing.T) {
	a, repo := appWith(t, map[string]string{"other/x/SKILL.md": "x"})
	catalogSkill(t, repo, "h/s", "S", "from the catalog")
	bundle, err := a.SkillBundleForWorker(context.Background(), tenant, "h", "s")
	if err != nil || bundle.Files[0].Body != "from the catalog" {
		t.Fatalf("catalog fallback: %v %+v", err, bundle)
	}
}

func TestAnUnusableSkillOnDiskIsRefusedEvenIfTheCatalogHasOne(t *testing.T) {
	a, repo := appWith(t, map[string]string{"h/nomd/README.md": "no SKILL.md here"})
	catalogSkill(t, repo, "h/nomd", "N", "catalog text with a SKILL.md")
	if _, err := a.SkillBundleForWorker(context.Background(), tenant, "h", "nomd"); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("no SKILL.md on disk: want not found, got %v", err)
	}
	if err := a.checkSkill(context.Background(), tenant, "h/nomd"); err == nil {
		t.Fatal("choosing it is refused too")
	}
}

func TestASkillTooBigForTheLibraryIsNotServedFromTheCatalogInstead(t *testing.T) {
	files := map[string]string{"h/big/SKILL.md": "x"}
	for i := 0; i <= skillstore.MaxBytes/skillstore.MaxFileBytes; i++ {
		files["h/big/p"+string(rune('a'+i))+".md"] = strings.Repeat("y", skillstore.MaxFileBytes)
	}
	a, repo := appWith(t, files)
	catalogSkill(t, repo, "h/big", "B", "a different, older version")
	if _, err := a.SkillBundleForWorker(context.Background(), tenant, "h", "big"); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("too big: want not found, got %v", err)
	}
}

func TestWithoutALibraryTheCatalogWorksAsBefore(t *testing.T) {
	repo := memstore.New()
	a := NewWithOptions(Options{Repo: repo})
	catalogSkill(t, repo, "h/s", "S", "from the catalog")
	if err := a.checkSkill(context.Background(), tenant, "h/s"); err != nil {
		t.Fatal(err)
	}
	files, err := a.SkillTextFiles(context.Background(), tenant, "h", "s")
	if err != nil || len(files) != 1 {
		t.Fatalf("%v %v", err, files)
	}
}

func TestThePublicFileListReadsTheLibraryToo(t *testing.T) {
	a, repo := appWith(t, map[string]string{"h/s/SKILL.md": "from disk"})
	catalogSkill(t, repo, "h/s", "S", "")
	files, err := a.SkillTextFiles(context.Background(), tenant, "h", "s")
	if err != nil || len(files) != 1 || files[0].Body != "from disk" {
		t.Fatalf("%v %+v", err, files)
	}
}

// A skill is usable only if AgentScope can load it: a file named exactly SKILL.md at the skill's root. Control decides what
// may be chosen, the worker decides what is loaded; they must agree, or a chosen skill silently does nothing.
func TestOnlyASkillMdAtTheRootMakesASkillUsable(t *testing.T) {
	cases := []struct {
		name  string
		files []store.SkillFile
		want  bool
	}{
		{"exact at the root", []store.SkillFile{{Path: "SKILL.md", Body: "x"}}, true},
		{"in a subdirectory only", []store.SkillFile{{Path: "sub/SKILL.md", Body: "x"}}, false},
		{"another case", []store.SkillFile{{Path: "skill.md", Body: "x"}}, false},
		{"another case in a subdirectory", []store.SkillFile{{Path: "sub/Skill.MD", Body: "x"}}, false},
		{"beside others", []store.SkillFile{{Path: "README.md", Body: "x"}, {Path: "SKILL.md", Body: "x"}}, true},
		{"none", nil, false},
	}
	for _, c := range cases {
		if got := skillUsable(c.files); got != c.want {
			t.Errorf("%s: usable=%v, want %v", c.name, got, c.want)
		}
	}
}

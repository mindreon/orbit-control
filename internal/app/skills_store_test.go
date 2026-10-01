package app

import (
	"context"
	"testing"

	"github.com/mindreon/orbit-control/internal/store"
	"github.com/mindreon/orbit-control/internal/store/memstore"
)

func seedSkill(t *testing.T, repo *memstore.Store) {
	t.Helper()
	err := repo.ReplaceSkills(context.Background(), "default", []store.SkillRecord{{
		ID: "@demo/weekly", Handle: "@demo", Slug: "weekly", Name: "周报", Category: "writing",
		TextFiles: []store.SkillFile{{Path: "SKILL.md", Body: "# 技能说明"}}, FilesKnown: true,
	}}, []store.SkillCategoryRecord{{Key: "writing", Name: "写作"}})
	if err != nil {
		t.Fatal(err)
	}
}

func TestSkillTextFilesReadsStoredCopy(t *testing.T) {
	repo := memstore.New()
	seedSkill(t, repo)
	runtime := &App{Repo: repo}
	files, err := runtime.SkillTextFiles(context.Background(), "default", "@demo", "weekly")
	if err != nil {
		t.Fatal(err)
	}
	if len(files) != 1 || files[0].Path != "SKILL.md" {
		t.Fatalf("files = %+v", files)
	}
}

func TestSkillTextFilesEmptyWhenTextNotKnown(t *testing.T) {
	repo := memstore.New()
	ctx := context.Background()
	if err := repo.ReplaceSkills(ctx, "default", []store.SkillRecord{{
		ID: "@demo/bare", Handle: "@demo", Slug: "bare", Name: "bare",
	}}, nil); err != nil {
		t.Fatal(err)
	}
	runtime := &App{Repo: repo}
	files, err := runtime.SkillTextFiles(ctx, "default", "@demo", "bare")
	if err != nil {
		t.Fatal(err)
	}
	if len(files) != 0 {
		t.Fatalf("files = %+v", files)
	}
}

func TestSkillTextFilesMissingSkill(t *testing.T) {
	runtime := &App{Repo: memstore.New()}
	if _, err := runtime.SkillTextFiles(context.Background(), "default", "@demo", "nope"); err != store.ErrNotFound {
		t.Fatalf("err = %v", err)
	}
	if _, err := runtime.SkillTextFiles(context.Background(), "default", "bad/space", "nope"); err != store.ErrNotFound {
		t.Fatalf("malformed path err = %v", err)
	}
}

func TestListSkillsCarriesCategoryName(t *testing.T) {
	repo := memstore.New()
	seedSkill(t, repo)
	runtime := &App{Repo: repo}
	list, err := runtime.ListSkills(context.Background(), "default", store.SkillCatalogQuery{})
	if err != nil {
		t.Fatal(err)
	}
	if list.Total != 1 || list.Items[0].CategoryName != "写作" || list.Items[0].Source != "" {
		t.Fatalf("list = %+v", list.Items)
	}
}

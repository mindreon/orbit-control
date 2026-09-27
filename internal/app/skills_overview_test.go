package app

import (
	"context"
	"strings"
	"testing"

	"github.com/mindreon/orbit-control/internal/store"
	"github.com/mindreon/orbit-control/internal/store/memstore"
)

func TestSkillPageCopiesMissingOverviewOnce(t *testing.T) {
	repo := memstore.New()
	ctx := context.Background()
	if err := repo.UpsertSkillCatalog(ctx, "default", []store.SkillRecord{{
		ID: "demo/weekly", Slug: "weekly", Handle: "demo", Name: "周报",
	}}); err != nil {
		t.Fatal(err)
	}
	if err := repo.SaveSkillTextFiles(ctx, "default", "demo/weekly", []store.SkillFile{{Path: "README.md", Body: "说明"}}); err != nil {
		t.Fatal(err)
	}
	if err := repo.SaveSkillDetail(ctx, "default", "demo/weekly", []byte(`{"fileIndex":[{"path":"SKILL.md","size":12}],"score":4.7}`)); err != nil {
		t.Fatal(err)
	}
	fetches := 0
	runtime := &App{Repo: repo}
	fetch := func(context.Context, string, string) ([]store.SkillFile, error) {
		fetches++
		return []store.SkillFile{{Path: "SKILL.md", Body: "# 技能说明"}}, nil
	}
	files, meta, err := runtime.SkillPage(ctx, "default", "demo", "weekly", fetch, nil)
	if err != nil {
		t.Fatal(err)
	}
	if fetches != 1 || len(files) != 1 || files[0].Path != "SKILL.md" {
		t.Fatalf("fetches=%d files=%+v", fetches, files)
	}
	if strings.Contains(string(meta), "overviewTried") || strings.Contains(string(meta), "filesTried") || !strings.Contains(string(meta), `"score":4.7`) {
		t.Fatalf("meta = %s", meta)
	}
	if _, _, err := runtime.SkillPage(ctx, "default", "demo", "weekly", fetch, nil); err != nil {
		t.Fatal(err)
	}
	if fetches != 1 {
		t.Fatalf("second view fetched again: %d", fetches)
	}
}

func TestSkillPageCopiesMissingNestedFileOnce(t *testing.T) {
	repo := memstore.New()
	ctx := context.Background()
	if err := repo.UpsertSkillCatalog(ctx, "default", []store.SkillRecord{{
		ID: "demo/weekly", Slug: "weekly", Handle: "demo", Name: "周报",
	}}); err != nil {
		t.Fatal(err)
	}
	if err := repo.SaveSkillTextFiles(ctx, "default", "demo/weekly", []store.SkillFile{{Path: "SKILL.md", Body: "# 技能说明"}}); err != nil {
		t.Fatal(err)
	}
	detail := []byte(`{"fileIndex":[{"path":"SKILL.md","size":12},{"path":"references/ai-coding-governance.md","size":20}],"score":4.7,"overviewTried":true}`)
	if err := repo.SaveSkillDetail(ctx, "default", "demo/weekly", detail); err != nil {
		t.Fatal(err)
	}
	fetches := 0
	runtime := &App{Repo: repo}
	fetch := func(context.Context, string, string) ([]store.SkillFile, error) {
		fetches++
		return []store.SkillFile{
			{Path: "SKILL.md", Body: "# 技能说明"},
			{Path: "references/ai-coding-governance.md", Body: "# 治理"},
		}, nil
	}
	files, meta, err := runtime.SkillPage(ctx, "default", "demo", "weekly", fetch, nil)
	if err != nil {
		t.Fatal(err)
	}
	if fetches != 1 || len(files) != 2 || files[1].Path != "references/ai-coding-governance.md" {
		t.Fatalf("fetches=%d files=%+v", fetches, files)
	}
	if strings.Contains(string(meta), "filesTried") || strings.Contains(string(meta), "overviewTried") || !strings.Contains(string(meta), `"score":4.7`) {
		t.Fatalf("meta = %s", meta)
	}
	if _, _, err := runtime.SkillPage(ctx, "default", "demo", "weekly", fetch, nil); err != nil {
		t.Fatal(err)
	}
	if fetches != 1 {
		t.Fatalf("second view fetched again: %d", fetches)
	}
}

func TestSkillPageRetriesWhenTheCopyFails(t *testing.T) {
	repo := memstore.New()
	ctx := context.Background()
	if err := repo.UpsertSkillCatalog(ctx, "default", []store.SkillRecord{{
		ID: "demo/weekly", Slug: "weekly", Handle: "demo", Name: "周报",
	}}); err != nil {
		t.Fatal(err)
	}
	if err := repo.SaveSkillTextFiles(ctx, "default", "demo/weekly", []store.SkillFile{{Path: "SKILL.md", Body: "# 技能说明"}}); err != nil {
		t.Fatal(err)
	}
	detail := []byte(`{"fileIndex":[{"path":"scripts/build_graph.py","size":20}],"score":4.7}`)
	if err := repo.SaveSkillDetail(ctx, "default", "demo/weekly", detail); err != nil {
		t.Fatal(err)
	}
	fetches := 0
	runtime := &App{Repo: repo}
	fetch := func(context.Context, string, string) ([]store.SkillFile, error) {
		fetches++
		if fetches == 1 {
			return nil, context.DeadlineExceeded
		}
		return []store.SkillFile{
			{Path: "SKILL.md", Body: "# 技能说明"},
			{Path: "scripts/build_graph.py", Body: "print('graph')"},
		}, nil
	}
	files, _, err := runtime.SkillPage(ctx, "default", "demo", "weekly", fetch, nil)
	if err != nil {
		t.Fatal(err)
	}
	if fetches != 1 || len(files) != 1 {
		t.Fatalf("first fetches=%d files=%+v", fetches, files)
	}
	files, meta, err := runtime.SkillPage(ctx, "default", "demo", "weekly", fetch, nil)
	if err != nil {
		t.Fatal(err)
	}
	if fetches != 2 || len(files) != 2 || files[1].Path != "scripts/build_graph.py" {
		t.Fatalf("second fetches=%d files=%+v", fetches, files)
	}
	if strings.Contains(string(meta), "filesTried") {
		t.Fatalf("meta = %s", meta)
	}
}

//go:build e2e

package persistence

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/mindreon/orbit-control/internal/store"
	"github.com/mindreon/orbit-control/internal/store/pgstore"
)

// The shared marketplace tables through the gorm store: filters, sort orders, paging, upserts that keep a renamed row,
// the trending refresh, and the text a card keeps beside it.
func TestMarketplaceStoreSkillCatalog(t *testing.T) {
	const tenant = "t-market-store"
	opsEnsureTenant(t, tenant)
	repo := pgstore.New(newPool(t, appURL, 2))
	ctx := context.Background()
	when := time.Now().UTC().Truncate(time.Microsecond)
	// The tables are shared and outlive a run, so this run's rows carry a category of their own.
	run := fmt.Sprintf("%d", when.UnixNano())
	dev, ops := "dev-"+run, "ops-"+run

	if err := repo.UpsertSkillCategories(ctx, tenant, []store.SkillCategoryRecord{{Key: dev, Name: "开发", NameEn: "Dev", SortOrder: 1}, {Key: ops, Name: "运维", NameEn: "Ops", SortOrder: 2}}); err != nil {
		t.Fatalf("categories: %v", err)
	}
	skills := []store.SkillRecord{
		{ID: "acme/lint", Slug: "lint", Handle: "acme", Name: "Lint", Description: "find 100% of issues", Category: dev, Downloads: 10, Stars: 5, Source: "clawhub", Version: "1", Score: 0.9, UpdatedAt: when},
		{ID: "acme/deploy", Slug: "deploy", Handle: "acme", Name: "Deploy", Description: "ship it", Category: ops, Downloads: 99, Stars: 1, Source: "clawhub", Version: "2", RequiresAPIKey: true, Paid: true, Score: 0.5, UpdatedAt: when.Add(time.Minute)},
		{ID: "rename-me", Slug: "rename-me", Name: "Renamed", Category: dev, Source: "community", Score: 0.1, UpdatedAt: when},
	}
	if err := repo.UpsertSkillCatalog(ctx, tenant, skills); err != nil {
		t.Fatalf("upsert: %v", err)
	}
	page, err := repo.ListSkillCatalog(ctx, tenant, store.SkillCatalogQuery{})
	if err != nil || page.Total < 3 {
		t.Fatalf("list: %v total=%d", err, page.Total)
	}
	want := func(name string, q store.SkillCatalogQuery, ids ...string) {
		t.Helper()
		got, err := repo.ListSkillCatalog(ctx, tenant, q)
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		var have []string
		for _, item := range got.Items {
			for _, id := range ids {
				if item.ID == id {
					have = append(have, item.ID)
				}
			}
		}
		if len(have) != len(ids) {
			t.Fatalf("%s: want %v among %+v", name, ids, got.Items)
		}
	}
	want("category", store.SkillCatalogQuery{Category: ops}, "acme/deploy")
	want("paid", store.SkillCatalogQuery{Paid: "true"}, "acme/deploy")
	want("needs key", store.SkillCatalogQuery{RequiresAPIKey: "true"}, "acme/deploy")
	want("keyword escapes %", store.SkillCatalogQuery{Keyword: "100%"}, "acme/lint")
	sorted, _ := repo.ListSkillCatalog(ctx, tenant, store.SkillCatalogQuery{Sort: "downloads", Category: dev})
	if len(sorted.Items) != 2 || sorted.Items[0].ID != "acme/lint" || sorted.Items[0].CategoryName != "开发" {
		t.Fatalf("downloads order with the category label: %+v", sorted.Items)
	}

	// The same skill arrives again with its handle: the row keeps its data and moves to the new id.
	if err := repo.UpsertSkillCatalog(ctx, tenant, []store.SkillRecord{{ID: "acme/rename-me", Slug: "rename-me", Handle: "acme", Name: "Renamed", Category: dev, Source: "community", Score: 0.2, UpdatedAt: when}}); err != nil {
		t.Fatalf("rename upsert: %v", err)
	}
	if _, err := repo.GetSkill(ctx, tenant, "rename-me"); err != store.ErrNotFound {
		t.Fatalf("the old id is gone, got %v", err)
	}
	if got, err := repo.GetSkill(ctx, tenant, "acme/rename-me"); err != nil || got.Handle != "acme" {
		t.Fatalf("the row moved: %v %+v", err, got)
	}

	// Trending: a refresh clears the ranks it does not name and sets the ones it does.
	if err := repo.ReplaceSkillTrending(ctx, tenant, []store.SkillRecord{{ID: "acme/lint", Slug: "lint", Handle: "acme", Name: "Lint", Category: dev, Source: "clawhub", UpdatedAt: when, TrendingRank: 1}}); err != nil {
		t.Fatalf("trending: %v", err)
	}
	trending, _ := repo.ListSkillCatalog(ctx, tenant, store.SkillCatalogQuery{Sort: "trending"})
	if len(trending.Items) != 1 || trending.Items[0].ID != "acme/lint" || trending.Items[0].TrendingRank != 1 {
		t.Fatalf("trending: %+v", trending.Items)
	}

	// The text kept beside a skill: unknown before it is saved, and saving to a skill that does not exist is ErrNotFound.
	if _, known, err := repo.GetSkillTextFiles(ctx, tenant, "acme/lint"); err != nil || known {
		t.Fatalf("text files before saving: known=%v err=%v", known, err)
	}
	if err := repo.SaveSkillTextFiles(ctx, tenant, "acme/lint", []store.SkillFile{{Path: "SKILL.md", Body: "hello"}}); err != nil {
		t.Fatalf("save text: %v", err)
	}
	if files, known, err := repo.GetSkillTextFiles(ctx, tenant, "acme/lint"); err != nil || !known || len(files) != 1 || files[0].Body != "hello" {
		t.Fatalf("text files: %v %v %+v", err, known, files)
	}
	if err := repo.SaveSkillTextFiles(ctx, tenant, "no/such", nil); err != store.ErrNotFound {
		t.Fatalf("saving to a missing skill: %v", err)
	}
	if err := repo.SaveSkillDetail(ctx, tenant, "acme/lint", []byte(`{"readme":"x"}`)); err != nil {
		t.Fatalf("save detail: %v", err)
	}
	if raw, known, err := repo.GetSkillDetail(ctx, tenant, "acme/lint"); err != nil || !known || len(raw) == 0 {
		t.Fatalf("detail: %v %v %s", err, known, raw)
	}
}

func TestMarketplaceStoreMcpPlaza(t *testing.T) {
	const tenant = "t-plaza-store"
	opsEnsureTenant(t, tenant)
	repo := pgstore.New(newPool(t, appURL, 2))
	ctx := context.Background()
	servers := []store.McpMarketRecord{
		{ID: "fs", Name: "Files", Summary: "read files", Author: "acme", Category: "dev", Hosted: false, NeedsOnline: false, Rank: 1},
		{ID: "web", Name: "Web", Summary: "fetch pages", Author: "globex", Category: "dev", Hosted: true, NeedsOnline: true, Rank: 2},
		{ID: "db", Name: "Database", Summary: "query sql", Author: "acme", Category: "data", Hosted: true, Rank: 3},
	}
	categories := []store.McpMarketCategoryRecord{{Key: "dev", Name: "开发", SortOrder: 1}, {Key: "data", Name: "数据", SortOrder: 2}, {Key: "empty", Name: "空", SortOrder: 3}}
	details := []store.McpMarketDetailRecord{
		{ID: "fs", License: "MIT", Readme: "# fs", Tools: []store.McpMarketTool{{Name: "read", Description: "read a file", Params: []store.McpMarketToolParam{{Name: "path", Type: "string", Required: true}}}}},
		{ID: "ghost", License: "none"}, // no such server: dropped
	}
	if err := repo.ReplaceMcpMarket(ctx, tenant, servers, categories, details); err != nil {
		t.Fatalf("replace: %v", err)
	}

	page, err := repo.ListMcpMarket(ctx, tenant, store.McpMarketQuery{})
	if err != nil || page.Total != 3 || page.Stored != 3 || page.Items[0].ID != "fs" || page.Items[0].CategoryName != "开发" {
		t.Fatalf("list: %v %+v", err, page)
	}
	if page, _ = repo.ListMcpMarket(ctx, tenant, store.McpMarketQuery{ServiceType: "hosted"}); page.Total != 2 {
		t.Fatalf("hosted: %+v", page)
	}
	if page, _ = repo.ListMcpMarket(ctx, tenant, store.McpMarketQuery{Category: "data"}); page.Total != 1 || page.Items[0].ID != "db" {
		t.Fatalf("category: %+v", page)
	}
	// A keyword matches name, author, summary and the category label; a name or author match ranks first.
	if page, _ = repo.ListMcpMarket(ctx, tenant, store.McpMarketQuery{Keyword: "acme"}); page.Total != 2 || page.Stored != 3 {
		t.Fatalf("keyword on author: %+v", page)
	}
	if page, _ = repo.ListMcpMarket(ctx, tenant, store.McpMarketQuery{Keyword: "数据"}); page.Total != 1 || page.Items[0].ID != "db" {
		t.Fatalf("keyword on the category label: %+v", page)
	}
	if page, _ = repo.ListMcpMarket(ctx, tenant, store.McpMarketQuery{NeedsOnline: "true"}); page.Total != 1 || page.Stored != 1 {
		t.Fatalf("needs online: %+v", page)
	}

	counts, err := repo.ListMcpMarketCategories(ctx, tenant, "")
	if err != nil || len(counts) != 2 || counts[0].Key != "dev" || counts[0].Count != 2 || counts[1].Count != 1 {
		t.Fatalf("categories keep only the ones with servers: %v %+v", err, counts)
	}

	detail, err := repo.GetMcpMarket(ctx, tenant, "fs")
	if err != nil || detail.License != "MIT" || len(detail.Tools) != 1 || detail.Tools[0].Params[0].Name != "path" {
		t.Fatalf("detail: %v %+v", err, detail)
	}
	if noDetail, err := repo.GetMcpMarket(ctx, tenant, "web"); err != nil || noDetail.Tools == nil || len(noDetail.Tools) != 0 || noDetail.License != "" {
		t.Fatalf("a card without stored text has no tools, not null: %v %+v", err, noDetail)
	}
	if _, err := repo.GetMcpMarket(ctx, tenant, "ghost"); err != store.ErrNotFound {
		t.Fatalf("a dropped detail is not a card: %v", err)
	}

	// Replacing swaps the plaza: what the new one does not name is gone.
	if err := repo.ReplaceMcpMarket(ctx, tenant, servers[:1], nil, nil); err != nil {
		t.Fatalf("second replace: %v", err)
	}
	if page, _ = repo.ListMcpMarket(ctx, tenant, store.McpMarketQuery{}); page.Total != 1 {
		t.Fatalf("after replace: %+v", page)
	}
}

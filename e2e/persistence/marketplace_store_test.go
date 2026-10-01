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

// The shared marketplace tables through the gorm store: filters, sort orders,
// paging, snapshot replacement, the stored skill text, and the agents catalog.
func TestMarketplaceStoreSkillCatalog(t *testing.T) {
	const tenant = "t-market-store"
	opsEnsureTenant(t, tenant)
	repo := pgstore.New(newPool(t, appURL, 2))
	ctx := context.Background()
	when := time.Now().UTC().Truncate(time.Microsecond)
	// The tables are shared and outlive a run, so this run's rows carry a category of their own.
	run := fmt.Sprintf("%d", when.UnixNano())
	dev, ops := "dev-"+run, "ops-"+run

	skills := []store.SkillRecord{
		{ID: "@acme/lint-" + run, Handle: "@acme", Slug: "lint-" + run, Name: "Lint", Description: "find 100% of issues", Category: dev, Downloads: 10, Likes: 5, UpdatedAt: when},
		{ID: "@acme/deploy-" + run, Handle: "@acme", Slug: "deploy-" + run, Name: "Deploy", Description: "ship it", Category: ops, Downloads: 99, Likes: 1, UpdatedAt: when.Add(time.Minute)},
		{ID: "@acme/nexa-" + run, Handle: "@acme", Slug: "nexa-" + run, Name: "Nexa only", Category: dev, Downloads: 5, Source: "nexa", UpdatedAt: when},
	}
	categories := []store.SkillCategoryRecord{{Key: dev, Name: "开发", NameEn: "Dev", SortOrder: 1}, {Key: ops, Name: "运维", NameEn: "Ops", SortOrder: 2}}
	if err := repo.ReplaceSkills(ctx, tenant, skills, categories); err != nil {
		t.Fatalf("replace: %v", err)
	}
	page, err := repo.ListSkillCatalog(ctx, tenant, store.SkillCatalogQuery{})
	if err != nil || page.Total != 3 {
		t.Fatalf("list: %v total=%d", err, page.Total)
	}
	if page.InstalledAt.IsZero() {
		t.Fatal("installed time is empty")
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
	want("category", store.SkillCatalogQuery{Category: ops}, skills[1].ID)
	want("source", store.SkillCatalogQuery{Source: "nexa"}, skills[2].ID)
	want("keyword escapes %", store.SkillCatalogQuery{Keyword: "100%"}, skills[0].ID)
	sorted, _ := repo.ListSkillCatalog(ctx, tenant, store.SkillCatalogQuery{Sort: "downloads", Category: dev})
	if len(sorted.Items) != 2 || sorted.Items[0].ID != skills[0].ID || sorted.Items[0].CategoryName != "开发" {
		t.Fatalf("downloads order with the category label: %+v", sorted.Items)
	}
	// The text pass fills text for the ids it knows and ignores the rest.
	if err := repo.SaveSkillTextFilesBatch(ctx, tenant, []store.SkillTextFilesRow{
		{ID: skills[0].ID, Files: []store.SkillFile{{Path: "SKILL.md", Body: "hello"}}},
		{ID: "no/such-row", Files: []store.SkillFile{{Path: "SKILL.md", Body: "ghost"}}},
	}); err != nil {
		t.Fatalf("save text: %v", err)
	}
	if files, known, err := repo.GetSkillTextFiles(ctx, tenant, skills[0].ID); err != nil || !known || len(files) != 1 || files[0].Body != "hello" {
		t.Fatalf("text files: %v %v %+v", err, known, files)
	}
	if _, known, err := repo.GetSkillTextFiles(ctx, tenant, skills[1].ID); err != nil || known {
		t.Fatalf("text of a row without text: known=%v err=%v", known, err)
	}
	// Replacing swaps the snapshot: what the new one does not name is gone.
	if err := repo.ReplaceSkills(ctx, tenant, skills[:1], categories[:1]); err != nil {
		t.Fatalf("second replace: %v", err)
	}
	if page, _ = repo.ListSkillCatalog(ctx, tenant, store.SkillCatalogQuery{}); page.Total != 1 {
		t.Fatalf("after replace: %+v", page)
	}

	// Agents: replace, list, get.
	agents := []store.AgentRecord{
		{ID: "@acme/bot-" + run, Handle: "@acme", Slug: "bot-" + run, Name: "Bot", Description: "a helper", Catalogues: []string{"tools", "ops"}, Files: []store.SkillFile{{Path: "AGENTS.md", Body: "# bot"}}, Downloads: 7, UpdatedAt: when},
		{ID: "@acme/calc-" + run, Handle: "@acme", Slug: "calc-" + run, Name: "Calc", Description: "math", Catalogues: []string{"tools"}, Downloads: 9, Stars: 2, UpdatedAt: when},
	}
	if err := repo.ReplaceAgents(ctx, tenant, agents); err != nil {
		t.Fatalf("replace agents: %v", err)
	}
	agentPage, err := repo.ListAgentCatalog(ctx, tenant, store.AgentCatalogQuery{Catalogue: "tools"})
	if err != nil || agentPage.Total != 2 || agentPage.Items[0].ID != agents[1].ID {
		t.Fatalf("agent list: %v %+v", err, agentPage)
	}
	got, err := repo.GetAgent(ctx, tenant, agents[0].ID)
	if err != nil || got.Catalogues[0] != "tools" {
		t.Fatalf("agent get: %v %+v", err, got)
	}
	if len(got.Files) != 1 || got.Files[0].Body != "# bot" {
		t.Fatalf("agent files: %+v", got.Files)
	}
	if _, err := repo.GetAgent(ctx, tenant, "@acme/none"); err != store.ErrNotFound {
		t.Fatalf("a missing agent: %v", err)
	}

	// Snapshot bookkeeping: the hash is unknown until it is stored.
	if sha, known, err := repo.CatalogSnapshot(ctx, tenant, "msmarket"); err != nil || known || sha != "" {
		t.Fatalf("snapshot before storing: %v %v %q", err, known, sha)
	}
	if err := repo.SetCatalogSnapshot(ctx, tenant, "msmarket", "abc"); err != nil {
		t.Fatalf("set snapshot: %v", err)
	}
	if sha, known, _ := repo.CatalogSnapshot(ctx, tenant, "msmarket"); !known || sha != "abc" {
		t.Fatalf("snapshot after storing: known=%v sha=%q", known, sha)
	}
}

func TestMarketplaceStoreMcpPlaza(t *testing.T) {
	const tenant = "t-plaza-store"
	opsEnsureTenant(t, tenant)
	repo := pgstore.New(newPool(t, appURL, 2))
	ctx := context.Background()
	servers := []store.McpMarketRecord{
		{ID: "fs", Name: "Files", Summary: "read files", Author: "acme", Category: "dev", Hosted: false, NeedsOnline: false, Source: "common", Rank: 1},
		{ID: "web", Name: "Web", Summary: "fetch pages", Author: "globex", Category: "dev", Hosted: true, NeedsOnline: true, Source: "nexa", Rank: 2},
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
	if page, _ = repo.ListMcpMarket(ctx, tenant, store.McpMarketQuery{Source: "nexa"}); page.Total != 1 || page.Items[0].ID != "web" {
		t.Fatalf("source: %+v", page)
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

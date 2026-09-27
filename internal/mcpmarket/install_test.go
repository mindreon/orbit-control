package mcpmarket

import (
	"context"
	"testing"

	"github.com/mindreon/orbit-control/internal/store"
	"github.com/mindreon/orbit-control/internal/store/memstore"
)

func TestInstallStoresSnapshot(t *testing.T) {
	repo := memstore.New()
	if err := Install(context.Background(), repo, "default"); err != nil {
		t.Fatal(err)
	}
	page, err := repo.ListMcpMarket(context.Background(), "default", store.McpMarketQuery{})
	if err != nil {
		t.Fatal(err)
	}
	if page.Stored != 5206 || page.Total != 5206 {
		t.Fatalf("stored %d total %d", page.Stored, page.Total)
	}
	if page.PageSize != 30 || len(page.Items) != 30 {
		t.Fatalf("page %d items %d", page.PageSize, len(page.Items))
	}
	offline, err := repo.ListMcpMarket(context.Background(), "default", store.McpMarketQuery{NeedsOnline: "false"})
	if err != nil {
		t.Fatal(err)
	}
	if offline.Total != 86 {
		t.Fatalf("offline %d", offline.Total)
	}
	files, err := repo.ListMcpMarket(context.Background(), "default", store.McpMarketQuery{Keyword: "文件系统"})
	if err != nil {
		t.Fatal(err)
	}
	if len(files.Items) == 0 || files.Items[0].Name != "文件系统" || files.Items[0].NeedsOnline {
		t.Fatalf("filesystem %+v", files.Items)
	}
	if files.Items[0].CategoryName != "文件系统" {
		t.Fatalf("category name %q", files.Items[0].CategoryName)
	}
	fetch, err := repo.ListMcpMarket(context.Background(), "default", store.McpMarketQuery{Keyword: "Fetch网页内容抓取"})
	if err != nil {
		t.Fatal(err)
	}
	if len(fetch.Items) == 0 || fetch.Items[0].Name != "Fetch网页内容抓取" || !fetch.Items[0].NeedsOnline {
		t.Fatalf("fetch %+v", fetch.Items)
	}
	cats, err := repo.ListMcpMarketCategories(context.Background(), "default", "")
	if err != nil {
		t.Fatal(err)
	}
	if len(cats) == 0 || cats[0].Key != "browser-automation" || cats[0].Count == 0 {
		t.Fatalf("categories %+v", cats)
	}
}

func TestInstallRejectsMissingTenant(t *testing.T) {
	if err := Install(context.Background(), memstore.New(), ""); err == nil {
		t.Fatal("expected error")
	}
}

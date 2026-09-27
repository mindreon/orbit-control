package skillhub

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/mindreon/orbit-control/internal/store"
	"github.com/mindreon/orbit-control/internal/store/memstore"
)

func TestSyncOnceStoresLocallyAndListDoesNotCallUpstream(t *testing.T) {
	var hits int
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits++
		if r.Header.Get("User-Agent") != userAgent {
			t.Errorf("user agent %q", r.Header.Get("User-Agent"))
		}
		w.Header().Set("Content-Type", "application/json")
		switch {
		case r.URL.Path == "/api/v1/categories":
			_, _ = w.Write([]byte(`{"count":1,"items":[{"key":"office-efficiency","name":"办公效率","nameEn":"Office","sortOrder":10,"active":true}]}`))
		case r.URL.Path == "/api/skills":
			if r.URL.Query().Get("page") == "2" {
				_, _ = w.Write([]byte(`{"code":0,"data":{"skills":[],"total":150},"message":"success"}`))
				return
			}
			_, _ = w.Write([]byte(`{"code":0,"data":{"total":150,"skills":[
				{"category":"office-efficiency","description":"en","description_zh":"把一周进展收成周报","downloads":12000,"iconUrl":"https://evil.example/a.png","name":"周报汇总","slug":"weekly","source":"community","stars":80,"score":90,"version":"1.2.0","updated_at":1790477786310,"labels":{"requires_api_key":"false"},"namespace":{"handle":"demo"}},
				{"category":"dev-programming","description":"review","description_zh":"","downloads":3000,"iconUrl":"http://cloudcache.tencent-cloud.com/a.png","name":"代码审查","slug":"code","source":"clawhub","stars":10,"score":40,"version":"0.2.0","updated_at":1780000000000,"labels":{"requires_api_key":"true","pricing_type":"paid"},"namespace":{"handle":"demo"}}
			]},"message":"success"}`))
		case r.URL.Path == "/api/v1/showcase/trending":
			_, _ = w.Write([]byte(`{"section":"trending","total":1,"skills":[
				{"category":"office-efficiency","description_zh":"把一周进展收成周报","downloads":12000,"name":"周报汇总","slug":"weekly","source":"community","stars":80,"score":90,"version":"1.2.0","updated_at":1790477786310,"namespace":{"handle":"demo"}}
			]}`))
		default:
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()

	repo := memstore.New()
	ctx := context.Background()
	if err := SyncOnce(ctx, repo, "default", NewClient(srv.URL), nil); err != nil {
		t.Fatal(err)
	}
	if hits != 4 {
		t.Fatalf("upstream hits during sync = %d", hits)
	}
	before := hits
	page, err := repo.ListSkillCatalog(ctx, "default", store.SkillCatalogQuery{Sort: "score"})
	if err != nil {
		t.Fatal(err)
	}
	if hits != before {
		t.Fatal("listing the catalog called upstream")
	}
	if page.Total != 2 || len(page.Items) == 0 || page.Items[0].Name != "周报汇总" || page.Items[0].IconURL != "" {
		t.Fatalf("score page = %+v", page.Items)
	}
	if page.Items[0].Description != "把一周进展收成周报" || page.Items[0].CategoryName != "办公效率" {
		t.Fatalf("description/category = %+v", page.Items[0])
	}
	if len(page.Items) < 2 || !page.Items[1].RequiresAPIKey || !page.Items[1].Paid || page.Items[1].IconURL != "" {
		t.Fatalf("second skill = %+v", page.Items)
	}
	other, err := repo.ListSkillCatalog(ctx, "other-tenant", store.SkillCatalogQuery{Sort: "score"})
	if err != nil || other.Total != 2 {
		t.Fatalf("shared catalog other tenant total=%d err=%v", other.Total, err)
	}
	trending, err := repo.ListSkillCatalog(ctx, "default", store.SkillCatalogQuery{Sort: "trending"})
	if err != nil || trending.Total != 1 || trending.Items[0].Slug != "weekly" || trending.Items[0].TrendingRank != 1 {
		t.Fatalf("trending = %+v err=%v", trending, err)
	}
	found, err := repo.ListSkillCatalog(ctx, "default", store.SkillCatalogQuery{Sort: "score", Keyword: "周报"})
	if err != nil || found.Total != 1 || found.Items[0].Slug != "weekly" {
		t.Fatalf("keyword = %+v err=%v", found, err)
	}
	if _, err := repo.ListSkillCatalog(ctx, "", store.SkillCatalogQuery{}); err == nil {
		t.Fatal("empty tenant was accepted")
	}
}

func TestRedirectStaysOnAPIHost(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, "https://example.com/api/skills", http.StatusFound)
	}))
	defer srv.Close()
	_, _, err := NewClient(srv.URL).ListSkills(context.Background(), 1, 1, "score")
	if err == nil || !strings.Contains(err.Error(), "unreachable") {
		t.Fatalf("err = %v", err)
	}
}

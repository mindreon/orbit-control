package app

import (
	"context"

	"github.com/mindreon/orbit-control/internal/mcpmarket"
	"github.com/mindreon/orbit-control/internal/store"
)

// McpMarketServer is one stored plaza card. This API does not connect to it.
type McpMarketServer struct {
	ID           string `json:"id"`
	Name         string `json:"name"`
	Summary      string `json:"summary"`
	Author       string `json:"author"`
	Category     string `json:"category"`
	CategoryName string `json:"categoryName"`
	CategoryMore int    `json:"categoryMore"`
	Calls        int64  `json:"calls"`
	Views        int64  `json:"views"`
	Stars        int64  `json:"stars"`
	Verified     bool   `json:"verified"`
	Hosted       bool   `json:"hosted"`
	NeedsOnline  bool   `json:"needsOnline"`
}

// McpMarketList is one page of the stored plaza.
type McpMarketList struct {
	Items      []McpMarketServer `json:"items"`
	Total      int               `json:"total"`
	Stored     int               `json:"stored"`
	PlazaTotal int               `json:"plazaTotal"`
	Page       int               `json:"page"`
	PageSize   int               `json:"pageSize"`
}

// McpMarketCategory is one sidebar label and its stored count.
type McpMarketCategory struct {
	Key       string `json:"key"`
	Name      string `json:"name"`
	SortOrder int    `json:"sortOrder"`
	Count     int    `json:"count"`
}

// ListMcpMarket reads the stored snapshot. It does not call ModelScope.
func (a *App) ListMcpMarket(ctx context.Context, tenantID string, q store.McpMarketQuery) (McpMarketList, error) {
	page, err := a.Repo.ListMcpMarket(ctx, tenantID, q)
	if err != nil {
		return McpMarketList{}, err
	}
	items := make([]McpMarketServer, 0, len(page.Items))
	for _, rec := range page.Items {
		items = append(items, McpMarketServer{
			ID:           rec.ID,
			Name:         rec.Name,
			Summary:      rec.Summary,
			Author:       rec.Author,
			Category:     rec.Category,
			CategoryName: rec.CategoryName,
			CategoryMore: rec.CategoryMore,
			Calls:        rec.Calls,
			Views:        rec.Views,
			Stars:        rec.Stars,
			Verified:     rec.Verified,
			Hosted:       rec.Hosted,
			NeedsOnline:  rec.NeedsOnline,
		})
	}
	return McpMarketList{
		Items:      items,
		Total:      page.Total,
		Stored:     page.Stored,
		PlazaTotal: mcpmarket.PlazaTotal,
		Page:       page.Page,
		PageSize:   page.PageSize,
	}, nil
}

// McpMarketDetail is one server page. It has no discussion thread and no hosted URL.
type McpMarketDetail struct {
	McpMarketServer
	License   string                `json:"license"`
	UpdatedOn string                `json:"updatedOn"`
	Readme    string                `json:"readme"`
	Tools     []store.McpMarketTool `json:"tools"`
}

// GetMcpMarket reads one stored server. It does not call ModelScope.
func (a *App) GetMcpMarket(ctx context.Context, tenantID, id string) (McpMarketDetail, error) {
	rec, err := a.Repo.GetMcpMarket(ctx, tenantID, id)
	if err != nil {
		return McpMarketDetail{}, err
	}
	tools := rec.Tools
	if tools == nil {
		tools = []store.McpMarketTool{}
	}
	return McpMarketDetail{
		McpMarketServer: McpMarketServer{
			ID:           rec.ID,
			Name:         rec.Name,
			Summary:      rec.Summary,
			Author:       rec.Author,
			Category:     rec.Category,
			CategoryName: rec.CategoryName,
			CategoryMore: rec.CategoryMore,
			Calls:        rec.Calls,
			Views:        rec.Views,
			Stars:        rec.Stars,
			Verified:     rec.Verified,
			Hosted:       rec.Hosted,
			NeedsOnline:  rec.NeedsOnline,
		},
		License:   rec.License,
		UpdatedOn: rec.UpdatedOn,
		Readme:    rec.Readme,
		Tools:     tools,
	}, nil
}

// ListMcpMarketCategories reads stored labels. It does not call ModelScope.
func (a *App) ListMcpMarketCategories(ctx context.Context, tenantID, needsOnline string) ([]McpMarketCategory, error) {
	rows, err := a.Repo.ListMcpMarketCategories(ctx, tenantID, needsOnline)
	if err != nil {
		return nil, err
	}
	out := make([]McpMarketCategory, 0, len(rows))
	for _, row := range rows {
		out = append(out, McpMarketCategory{
			Key: row.Key, Name: row.Name, SortOrder: row.SortOrder, Count: row.Count,
		})
	}
	return out, nil
}

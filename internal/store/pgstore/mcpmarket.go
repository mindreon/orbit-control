package pgstore

import (
	"context"
	"encoding/json"
	"errors"
	"strings"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"

	"github.com/mindreon/orbit-control/internal/store"
)

// mcp_market_servers, mcp_market_categories and mcp_market_details are shared marketplace metadata. They are not
// tenant tables: statements do not filter by tenant_id. tenantID is still required so a caller without a tenant cannot
// use the repository.

type mcpMarketServerRow struct {
	ID           string `gorm:"primaryKey"`
	Name         string
	Summary      string
	Author       string
	Category     string
	CategoryMore int
	Calls        int64
	Views        int64
	Stars        int64
	Verified     bool
	Hosted       bool
	NeedsOnline  bool
	Rank         int
}

func (mcpMarketServerRow) TableName() string { return "mcp_market_servers" }

type mcpMarketCategoryRow struct {
	Key       string `gorm:"primaryKey"`
	Name      string
	SortOrder int
}

func (mcpMarketCategoryRow) TableName() string { return "mcp_market_categories" }

type mcpMarketDetailRow struct {
	ID        string `gorm:"primaryKey"`
	License   string
	UpdatedOn string
	Readme    string
	Tools     []byte `gorm:"type:jsonb"`
}

func (mcpMarketDetailRow) TableName() string { return "mcp_market_details" }

// mcpMarketListRow is a server with the label of its category, and the fields of its detail when there are any: flat
// (gorm skips embedded structs of unexported type).
type mcpMarketListRow struct {
	ID           string
	Name         string
	Summary      string
	Author       string
	Category     string
	CategoryName string
	CategoryMore int
	Calls        int64
	Views        int64
	Stars        int64
	Verified     bool
	Hosted       bool
	NeedsOnline  bool
	Rank         int
	License      string
	UpdatedOn    string
	Readme       string
	Tools        []byte
}

func (r mcpMarketListRow) record() store.McpMarketRecord {
	return store.McpMarketRecord{
		ID: r.ID, Name: r.Name, Summary: r.Summary, Author: r.Author, Category: r.Category, CategoryName: r.CategoryName,
		CategoryMore: r.CategoryMore, Calls: r.Calls, Views: r.Views, Stars: r.Stars, Verified: r.Verified, Hosted: r.Hosted,
		NeedsOnline: r.NeedsOnline, Rank: r.Rank,
	}
}

const mcpMarketSelect = `s.id, s.name, s.summary, s.author, s.category, COALESCE(c.name, '') AS category_name,
	s.category_more, s.calls, s.views, s.stars, s.verified, s.hosted, s.needs_online, s.rank`

func mcpServers(tx *gorm.DB) *gorm.DB {
	return tx.Table("mcp_market_servers AS s").Joins("LEFT JOIN mcp_market_categories c ON c.key = s.category")
}

func (s *Store) ListMcpMarket(ctx context.Context, tenantID string, q store.McpMarketQuery) (store.McpMarketPage, error) {
	if tenantID == "" {
		return store.McpMarketPage{}, store.ErrNotFound
	}
	q = store.NormalizeMcpMarketQuery(q)
	page := store.McpMarketPage{Page: q.Page, PageSize: q.PageSize, Items: []store.McpMarketRecord{}}
	err := s.inShared(ctx, func(tx *gorm.DB) error {
		var stored, total int64
		if err := mcpServers(tx).Scopes(mcpMarketFilter(store.McpMarketQuery{NeedsOnline: q.NeedsOnline}, false)).Count(&stored).Error; err != nil {
			return storageErr("count mcp market stored", err)
		}
		if err := mcpServers(tx).Scopes(mcpMarketFilter(q, true)).Count(&total).Error; err != nil {
			return storageErr("count mcp market", err)
		}
		page.Stored, page.Total = int(stored), int(total)
		var rows []mcpMarketListRow
		list := mcpServers(tx).Select(mcpMarketSelect).Scopes(mcpMarketFilter(q, true))
		if q.Keyword == "" {
			list = list.Order("s.rank ASC, s.id ASC")
		} else {
			// Cards whose name or author match come before those that only match elsewhere.
			list = list.Order(clause.Expr{
				SQL:  `CASE WHEN lower(s.name) LIKE ? ESCAPE '\' OR lower(s.author) LIKE ? ESCAPE '\' THEN 0 ELSE 1 END, s.rank ASC, s.id ASC`,
				Vars: []any{likeContains(q.Keyword), likeContains(q.Keyword)},
			})
		}
		if err := list.Limit(q.PageSize).Offset((q.Page - 1) * q.PageSize).Find(&rows).Error; err != nil {
			return storageErr("list mcp market", err)
		}
		for _, row := range rows {
			page.Items = append(page.Items, row.record())
		}
		return nil
	})
	return page, err
}

func (s *Store) ListMcpMarketCategories(ctx context.Context, tenantID string, needsOnline string) ([]store.McpMarketCategoryCount, error) {
	if tenantID == "" {
		return nil, store.ErrNotFound
	}
	filter := "TRUE"
	switch store.NormalizeMcpMarketQuery(store.McpMarketQuery{NeedsOnline: needsOnline}).NeedsOnline {
	case "true":
		filter = "s.needs_online"
	case "false":
		filter = "NOT s.needs_online"
	}
	// filter is one of three constants above, never caller input.
	counted := "count(s.id) FILTER (WHERE " + filter + ")"
	var rows []struct {
		Key       string
		Name      string
		SortOrder int
		Count     int
	}
	err := s.inShared(ctx, func(tx *gorm.DB) error {
		return tx.Table("mcp_market_categories AS c").
			Select("c.key, c.name, c.sort_order, " + counted + " AS count").
			Joins("LEFT JOIN mcp_market_servers s ON s.category = c.key").
			Group("c.key, c.name, c.sort_order").Having(counted + " > 0").Order("c.sort_order, c.key").Scan(&rows).Error
	})
	if err != nil {
		return nil, storageErr("list mcp market categories", err)
	}
	out := make([]store.McpMarketCategoryCount, 0, len(rows))
	for _, row := range rows {
		out = append(out, store.McpMarketCategoryCount{Key: row.Key, Name: row.Name, SortOrder: row.SortOrder, Count: row.Count})
	}
	return out, nil
}

func (s *Store) GetMcpMarket(ctx context.Context, tenantID, id string) (store.McpMarketDetail, error) {
	if tenantID == "" || id == "" {
		return store.McpMarketDetail{}, store.ErrNotFound
	}
	var row mcpMarketListRow
	err := s.inShared(ctx, func(tx *gorm.DB) error {
		return mcpServers(tx).Joins("LEFT JOIN mcp_market_details d ON d.id = s.id").
			Select(mcpMarketSelect+`, COALESCE(d.license, '') AS license, COALESCE(d.updated_on, '') AS updated_on,
				COALESCE(d.readme, '') AS readme, COALESCE(d.tools, '[]'::jsonb) AS tools`).
			Where("s.id = ?", id).Take(&row).Error
	})
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return store.McpMarketDetail{}, store.ErrNotFound
	}
	if err != nil {
		return store.McpMarketDetail{}, storageErr("get mcp market", err)
	}
	out := store.McpMarketDetail{McpMarketRecord: row.record(), License: row.License, UpdatedOn: row.UpdatedOn, Readme: row.Readme, Tools: []store.McpMarketTool{}}
	if len(row.Tools) > 0 {
		if err := json.Unmarshal(row.Tools, &out.Tools); err != nil {
			return store.McpMarketDetail{}, storageErr("decode mcp market tools", err)
		}
	}
	if out.Tools == nil {
		out.Tools = []store.McpMarketTool{}
	}
	return out, nil
}

const marketChunk = 200

// ReplaceMcpMarket swaps the stored plaza for the given one in one transaction. Details of servers that are not in the
// new plaza are dropped, and categories are upserted, not replaced.
func (s *Store) ReplaceMcpMarket(ctx context.Context, tenantID string, servers []store.McpMarketRecord, categories []store.McpMarketCategoryRecord, details []store.McpMarketDetailRecord) error {
	if tenantID == "" {
		return store.ErrNotFound
	}
	serverRows := make([]mcpMarketServerRow, 0, len(servers))
	known := map[string]struct{}{}
	for _, row := range servers {
		if row.ID == "" {
			continue
		}
		known[row.ID] = struct{}{}
		serverRows = append(serverRows, mcpMarketServerRow{
			ID: row.ID, Name: row.Name, Summary: row.Summary, Author: row.Author, Category: row.Category, CategoryMore: row.CategoryMore,
			Calls: row.Calls, Views: row.Views, Stars: row.Stars, Verified: row.Verified, Hosted: row.Hosted, NeedsOnline: row.NeedsOnline, Rank: row.Rank,
		})
	}
	detailRows := make([]mcpMarketDetailRow, 0, len(details))
	for _, row := range details {
		if _, ok := known[row.ID]; !ok {
			continue
		}
		tools := row.Tools
		if tools == nil {
			tools = []store.McpMarketTool{}
		}
		raw, err := json.Marshal(tools)
		if err != nil {
			return storageErr("encode mcp market tools", err)
		}
		detailRows = append(detailRows, mcpMarketDetailRow{ID: row.ID, License: row.License, UpdatedOn: row.UpdatedOn, Readme: row.Readme, Tools: raw})
	}
	categoryRows := make([]mcpMarketCategoryRow, 0, len(categories))
	for _, row := range categories {
		if row.Key != "" {
			categoryRows = append(categoryRows, mcpMarketCategoryRow{Key: row.Key, Name: row.Name, SortOrder: row.SortOrder})
		}
	}
	return s.inShared(ctx, func(tx *gorm.DB) error {
		for _, step := range []struct {
			op    string
			write func() error
		}{
			{"clear mcp market details", func() error { return tx.Where("TRUE").Delete(&mcpMarketDetailRow{}).Error }},
			{"clear mcp market", func() error { return tx.Where("TRUE").Delete(&mcpMarketServerRow{}).Error }},
			{"insert mcp market", func() error { return createInChunks(tx, serverRows) }},
			{"insert mcp market details", func() error { return createInChunks(tx, detailRows) }},
			{"upsert mcp market categories", func() error {
				if len(categoryRows) == 0 {
					return nil
				}
				return tx.Clauses(clause.OnConflict{
					Columns: []clause.Column{{Name: "key"}}, DoUpdates: clause.AssignmentColumns([]string{"name", "sort_order"}),
				}).Create(&categoryRows).Error
			}},
		} {
			if err := step.write(); err != nil {
				return storageErr(step.op, err)
			}
		}
		return nil
	})
}

func createInChunks[T any](tx *gorm.DB, rows []T) error {
	if len(rows) == 0 {
		return nil
	}
	return tx.CreateInBatches(&rows, marketChunk).Error
}

func mcpMarketFilter(q store.McpMarketQuery, withSearch bool) func(*gorm.DB) *gorm.DB {
	q = store.NormalizeMcpMarketQuery(q)
	return func(tx *gorm.DB) *gorm.DB {
		switch q.NeedsOnline {
		case "true":
			tx = tx.Where("s.needs_online")
		case "false":
			tx = tx.Where("NOT s.needs_online")
		}
		if !withSearch {
			return tx
		}
		if q.Category != "" {
			tx = tx.Where("s.category = ?", q.Category)
		}
		switch q.ServiceType {
		case "hosted":
			tx = tx.Where("s.hosted")
		case "local":
			tx = tx.Where("NOT s.hosted")
		}
		if q.Keyword != "" {
			like := likeContains(q.Keyword)
			tx = tx.Where(`(lower(s.name) LIKE ? ESCAPE '\' OR lower(s.author) LIKE ? ESCAPE '\' OR
				lower(s.summary) LIKE ? ESCAPE '\' OR lower(s.category) LIKE ? ESCAPE '\' OR
				lower(COALESCE(c.name, '')) LIKE ? ESCAPE '\')`, like, like, like, like, like)
		}
		return tx
	}
}

func likeContains(keyword string) string {
	replacer := strings.NewReplacer(`\`, `\\`, `%`, `\%`, `_`, `\_`)
	return "%" + replacer.Replace(strings.ToLower(keyword)) + "%"
}

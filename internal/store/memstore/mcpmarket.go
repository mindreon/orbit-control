package memstore

import (
	"context"
	"sort"
	"strings"

	"github.com/mindreon/orbit-control/internal/store"
)

func (s *Store) ListMcpMarket(_ context.Context, tenantID string, q store.McpMarketQuery) (store.McpMarketPage, error) {
	if tenantID == "" {
		return store.McpMarketPage{}, store.ErrNotFound
	}
	q = store.NormalizeMcpMarketQuery(q)
	s.mu.Lock()
	defer s.mu.Unlock()
	names := map[string]string{}
	for key, cat := range s.mcpCategories {
		names[key] = cat.Name
	}
	kw := strings.ToLower(q.Keyword)
	matched := make([]store.McpMarketRecord, 0)
	stored := 0
	for _, rec := range s.mcpMarket {
		if !needsOnlineOK(rec, q.NeedsOnline) {
			continue
		}
		stored++
		if !marketVisible(rec, names[rec.Category], q, kw) {
			continue
		}
		copyRec := rec
		copyRec.CategoryName = names[rec.Category]
		matched = append(matched, copyRec)
	}
	sort.Slice(matched, func(i, j int) bool {
		return marketLess(matched[i], matched[j], kw)
	})
	start := (q.Page - 1) * q.PageSize
	if start > len(matched) {
		start = len(matched)
	}
	end := start + q.PageSize
	if end > len(matched) {
		end = len(matched)
	}
	items := append([]store.McpMarketRecord(nil), matched[start:end]...)
	if items == nil {
		items = []store.McpMarketRecord{}
	}
	return store.McpMarketPage{
		Items:    items,
		Total:    len(matched),
		Stored:   stored,
		Page:     q.Page,
		PageSize: q.PageSize,
	}, nil
}

func (s *Store) ListMcpMarketCategories(_ context.Context, tenantID string, needsOnline string) ([]store.McpMarketCategoryCount, error) {
	if tenantID == "" {
		return nil, store.ErrNotFound
	}
	needsOnline = store.NormalizeMcpMarketQuery(store.McpMarketQuery{NeedsOnline: needsOnline}).NeedsOnline
	s.mu.Lock()
	defer s.mu.Unlock()
	counts := map[string]int{}
	for _, rec := range s.mcpMarket {
		if needsOnlineOK(rec, needsOnline) {
			counts[rec.Category]++
		}
	}
	out := make([]store.McpMarketCategoryCount, 0, len(s.mcpCategories))
	for key, cat := range s.mcpCategories {
		count := counts[key]
		if count == 0 {
			continue
		}
		out = append(out, store.McpMarketCategoryCount{
			Key: key, Name: cat.Name, SortOrder: cat.SortOrder, Count: count,
		})
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].SortOrder != out[j].SortOrder {
			return out[i].SortOrder < out[j].SortOrder
		}
		return out[i].Key < out[j].Key
	})
	return out, nil
}

func (s *Store) ReplaceMcpMarket(_ context.Context, tenantID string, servers []store.McpMarketRecord, categories []store.McpMarketCategoryRecord) error {
	if tenantID == "" {
		return store.ErrNotFound
	}
	nextServers := map[string]store.McpMarketRecord{}
	for _, row := range servers {
		if row.ID == "" {
			continue
		}
		nextServers[row.ID] = row
	}
	nextCats := map[string]store.McpMarketCategoryRecord{}
	for _, row := range categories {
		if row.Key == "" || row.Name == "" {
			continue
		}
		nextCats[row.Key] = row
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.mcpMarket = nextServers
	if s.mcpCategories == nil {
		s.mcpCategories = map[string]store.McpMarketCategoryRecord{}
	}
	for key, row := range nextCats {
		s.mcpCategories[key] = row
	}
	return nil
}

func needsOnlineOK(rec store.McpMarketRecord, filter string) bool {
	switch filter {
	case "true":
		return rec.NeedsOnline
	case "false":
		return !rec.NeedsOnline
	default:
		return true
	}
}

func marketVisible(rec store.McpMarketRecord, categoryName string, q store.McpMarketQuery, kw string) bool {
	if q.Category != "" && rec.Category != q.Category {
		return false
	}
	if q.ServiceType == "hosted" && !rec.Hosted {
		return false
	}
	if q.ServiceType == "local" && rec.Hosted {
		return false
	}
	if kw == "" {
		return true
	}
	return strings.Contains(strings.ToLower(rec.Name), kw) ||
		strings.Contains(strings.ToLower(rec.Author), kw) ||
		strings.Contains(strings.ToLower(rec.Summary), kw) ||
		strings.Contains(strings.ToLower(rec.Category), kw) ||
		strings.Contains(strings.ToLower(categoryName), kw)
}

func marketLess(a, b store.McpMarketRecord, kw string) bool {
	if kw != "" {
		ai, bi := 1, 1
		if nameHit(a, kw) {
			ai = 0
		}
		if nameHit(b, kw) {
			bi = 0
		}
		if ai != bi {
			return ai < bi
		}
	}
	if a.Rank != b.Rank {
		return a.Rank < b.Rank
	}
	return a.ID < b.ID
}

func nameHit(rec store.McpMarketRecord, kw string) bool {
	return strings.Contains(strings.ToLower(rec.Name), kw) || strings.Contains(strings.ToLower(rec.Author), kw)
}

package pgstore

import (
	"context"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"

	"github.com/mindreon/orbit-control/internal/store"
)

// mcp_market_servers and mcp_market_categories are shared marketplace
// metadata. They are not tenant tables: statements do not filter by
// tenant_id. tenantID is still required so a caller without a tenant cannot
// use the repository.

func (s *Store) ListMcpMarket(ctx context.Context, tenantID string, q store.McpMarketQuery) (store.McpMarketPage, error) {
	if tenantID == "" {
		return store.McpMarketPage{}, store.ErrNotFound
	}
	q = store.NormalizeMcpMarketQuery(q)
	where, args := mcpMarketWhere(q, true)
	storedWhere, storedArgs := mcpMarketWhere(store.McpMarketQuery{NeedsOnline: q.NeedsOnline}, false)
	var page store.McpMarketPage
	page.Page = q.Page
	page.PageSize = q.PageSize
	page.Items = []store.McpMarketRecord{}
	err := s.withTx(ctx, func(tx pgx.Tx) error {
		if err := tx.QueryRow(ctx, "SELECT count(*) FROM mcp_market_servers s WHERE "+storedWhere, storedArgs...).Scan(&page.Stored); err != nil {
			return storageErr("count mcp market stored", err)
		}
		if err := tx.QueryRow(ctx, "SELECT count(*) FROM mcp_market_servers s LEFT JOIN mcp_market_categories c ON c.key = s.category WHERE "+where, args...).Scan(&page.Total); err != nil {
			return storageErr("count mcp market", err)
		}
		limit := len(args) + 1
		offset := len(args) + 2
		listSQL := `SELECT s.id, s.name, s.summary, s.author, s.category, COALESCE(c.name, ''),
		                   s.category_more, s.calls, s.views, s.stars, s.verified, s.hosted, s.needs_online, s.rank
		              FROM mcp_market_servers s
		              LEFT JOIN mcp_market_categories c ON c.key = s.category
		             WHERE ` + where + `
		             ORDER BY ` + mcpMarketOrder(q, len(args)) + fmt.Sprintf(`
		             LIMIT $%d OFFSET $%d`, limit, offset)
		rows, err := tx.Query(ctx, listSQL, append(append([]any{}, args...), q.PageSize, (q.Page-1)*q.PageSize)...)
		if err != nil {
			return storageErr("list mcp market", err)
		}
		defer rows.Close()
		for rows.Next() {
			var rec store.McpMarketRecord
			if err := rows.Scan(
				&rec.ID, &rec.Name, &rec.Summary, &rec.Author, &rec.Category, &rec.CategoryName,
				&rec.CategoryMore, &rec.Calls, &rec.Views, &rec.Stars, &rec.Verified, &rec.Hosted, &rec.NeedsOnline, &rec.Rank,
			); err != nil {
				return storageErr("scan mcp market", err)
			}
			page.Items = append(page.Items, rec)
		}
		if err := rows.Err(); err != nil {
			return storageErr("list mcp market", err)
		}
		return nil
	})
	return page, err
}

func (s *Store) ListMcpMarketCategories(ctx context.Context, tenantID string, needsOnline string) ([]store.McpMarketCategoryCount, error) {
	if tenantID == "" {
		return nil, store.ErrNotFound
	}
	needsOnline = store.NormalizeMcpMarketQuery(store.McpMarketQuery{NeedsOnline: needsOnline}).NeedsOnline
	filter := "TRUE"
	switch needsOnline {
	case "true":
		filter = "s.needs_online"
	case "false":
		filter = "NOT s.needs_online"
	}
	out := []store.McpMarketCategoryCount{}
	err := s.withTx(ctx, func(tx pgx.Tx) error {
		rows, err := tx.Query(ctx, `
			SELECT c.key, c.name, c.sort_order, count(s.id) FILTER (WHERE `+filter+`)
			  FROM mcp_market_categories c
			  LEFT JOIN mcp_market_servers s ON s.category = c.key
			 GROUP BY c.key, c.name, c.sort_order
			HAVING count(s.id) FILTER (WHERE `+filter+`) > 0
			 ORDER BY c.sort_order, c.key`)
		if err != nil {
			return storageErr("list mcp market categories", err)
		}
		defer rows.Close()
		for rows.Next() {
			var rec store.McpMarketCategoryCount
			if err := rows.Scan(&rec.Key, &rec.Name, &rec.SortOrder, &rec.Count); err != nil {
				return storageErr("scan mcp market category", err)
			}
			out = append(out, rec)
		}
		return rows.Err()
	})
	return out, err
}

func (s *Store) ReplaceMcpMarket(ctx context.Context, tenantID string, servers []store.McpMarketRecord, categories []store.McpMarketCategoryRecord) error {
	if tenantID == "" {
		return store.ErrNotFound
	}
	return s.withTx(ctx, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, `DELETE FROM mcp_market_servers`); err != nil {
			return storageErr("clear mcp market", err)
		}
		const chunk = 200
		for start := 0; start < len(servers); start += chunk {
			end := start + chunk
			if end > len(servers) {
				end = len(servers)
			}
			batch := &pgx.Batch{}
			for _, row := range servers[start:end] {
				if row.ID == "" {
					continue
				}
				batch.Queue(`
					INSERT INTO mcp_market_servers (
					  id, name, summary, author, category, category_more,
					  calls, views, stars, verified, hosted, needs_online, rank
					) VALUES (
					  $1, $2, $3, $4, $5, $6,
					  $7, $8, $9, $10, $11, $12, $13
					)`,
					row.ID, row.Name, row.Summary, row.Author, row.Category, row.CategoryMore,
					row.Calls, row.Views, row.Stars, row.Verified, row.Hosted, row.NeedsOnline, row.Rank)
			}
			if batch.Len() == 0 {
				continue
			}
			if err := execBatch(ctx, tx, batch, "insert mcp market"); err != nil {
				return err
			}
		}
		if len(categories) == 0 {
			return nil
		}
		batch := &pgx.Batch{}
		for _, row := range categories {
			if row.Key == "" {
				continue
			}
			batch.Queue(`
				INSERT INTO mcp_market_categories (key, name, sort_order)
				VALUES ($1, $2, $3)
				ON CONFLICT (key) DO UPDATE SET
				  name = EXCLUDED.name,
				  sort_order = EXCLUDED.sort_order`,
				row.Key, row.Name, row.SortOrder)
		}
		if batch.Len() == 0 {
			return nil
		}
		return execBatch(ctx, tx, batch, "upsert mcp market categories")
	})
}

func mcpMarketWhere(q store.McpMarketQuery, withSearch bool) (string, []any) {
	q = store.NormalizeMcpMarketQuery(q)
	parts := []string{"TRUE"}
	args := []any{}
	switch q.NeedsOnline {
	case "true":
		parts = append(parts, "s.needs_online")
	case "false":
		parts = append(parts, "NOT s.needs_online")
	}
	if !withSearch {
		return strings.Join(parts, " AND "), args
	}
	if q.Category != "" {
		args = append(args, q.Category)
		parts = append(parts, fmt.Sprintf("s.category = $%d", len(args)))
	}
	switch q.ServiceType {
	case "hosted":
		parts = append(parts, "s.hosted")
	case "local":
		parts = append(parts, "NOT s.hosted")
	}
	if q.Keyword != "" {
		args = append(args, likeContains(q.Keyword))
		n := len(args)
		parts = append(parts, fmt.Sprintf(`(
			lower(s.name) LIKE $%d ESCAPE '\' OR lower(s.author) LIKE $%d ESCAPE '\' OR
			lower(s.summary) LIKE $%d ESCAPE '\' OR lower(s.category) LIKE $%d ESCAPE '\' OR
			lower(COALESCE(c.name, '')) LIKE $%d ESCAPE '\'
		)`, n, n, n, n, n))
	}
	return strings.Join(parts, " AND "), args
}

func mcpMarketOrder(q store.McpMarketQuery, argCount int) string {
	if q.Keyword == "" {
		return "s.rank ASC, s.id ASC"
	}
	return fmt.Sprintf(`CASE WHEN lower(s.name) LIKE $%d ESCAPE '\' OR lower(s.author) LIKE $%d ESCAPE '\' THEN 0 ELSE 1 END, s.rank ASC, s.id ASC`, argCount, argCount)
}

func likeContains(keyword string) string {
	replacer := strings.NewReplacer(`\`, `\\`, `%`, `\%`, `_`, `\_`)
	return "%" + replacer.Replace(strings.ToLower(keyword)) + "%"
}

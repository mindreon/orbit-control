package pgstore

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/mindreon/orbit-control/internal/store"
)

// skill_catalog and skill_categories are shared marketplace metadata. They
// are not [T] tables: statements do not filter by tenant_id. tenantID is
// still required so a caller without a tenant cannot use the repository.

func (s *Store) ListSkillCatalog(ctx context.Context, tenantID string, q store.SkillCatalogQuery) (store.SkillCatalogPage, error) {
	if tenantID == "" {
		return store.SkillCatalogPage{}, store.ErrNotFound
	}
	q = store.NormalizeSkillQuery(q)
	where, args := skillWhere(q)
	var page store.SkillCatalogPage
	page.Page = q.Page
	page.PageSize = q.PageSize
	page.Items = []store.SkillRecord{}
	err := s.withTx(ctx, func(tx pgx.Tx) error {
		countSQL := "SELECT count(*) FROM skill_catalog s WHERE " + where
		if err := tx.QueryRow(ctx, countSQL, args...).Scan(&page.Total); err != nil {
			return storageErr("count skill catalog", err)
		}
		var synced *time.Time
		if err := tx.QueryRow(ctx, `SELECT max(synced_at) FROM skill_catalog`).Scan(&synced); err != nil {
			return storageErr("skill catalog sync time", err)
		}
		if synced != nil {
			page.SyncedAt = *synced
		}
		limit := len(args) + 1
		offset := len(args) + 2
		listSQL := `SELECT s.id, s.slug, s.handle, s.name, s.description, s.category,
		                   COALESCE(c.name, ''), s.icon_url, s.downloads, s.stars, s.source,
		                   s.version, s.requires_api_key, s.paid, s.score, s.updated_at, s.trending_rank
		              FROM skill_catalog s
		              LEFT JOIN skill_categories c ON c.key = s.category
		             WHERE ` + where + `
		             ORDER BY ` + skillOrder(q.Sort) + fmt.Sprintf(`
		             LIMIT $%d OFFSET $%d`, limit, offset)
		rows, err := tx.Query(ctx, listSQL, append(append([]any{}, args...), q.PageSize, (q.Page-1)*q.PageSize)...)
		if err != nil {
			return storageErr("list skill catalog", err)
		}
		defer rows.Close()
		for rows.Next() {
			var rec store.SkillRecord
			if err := rows.Scan(
				&rec.ID, &rec.Slug, &rec.Handle, &rec.Name, &rec.Description, &rec.Category,
				&rec.CategoryName, &rec.IconURL, &rec.Downloads, &rec.Stars, &rec.Source,
				&rec.Version, &rec.RequiresAPIKey, &rec.Paid, &rec.Score, &rec.UpdatedAt, &rec.TrendingRank,
			); err != nil {
				return storageErr("scan skill catalog", err)
			}
			page.Items = append(page.Items, rec)
		}
		if err := rows.Err(); err != nil {
			return storageErr("list skill catalog", err)
		}
		return nil
	})
	return page, err
}

func (s *Store) UpsertSkillCatalog(ctx context.Context, tenantID string, rows []store.SkillRecord) error {
	if tenantID == "" {
		return store.ErrNotFound
	}
	return s.upsertSkills(ctx, rows, false)
}

func (s *Store) ReplaceSkillTrending(ctx context.Context, tenantID string, rows []store.SkillRecord) error {
	if tenantID == "" {
		return store.ErrNotFound
	}
	return s.withTx(ctx, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, `UPDATE skill_catalog SET trending_rank = 0 WHERE trending_rank <> 0`); err != nil {
			return storageErr("clear skill trending", err)
		}
		return s.queueSkills(ctx, tx, rows, true)
	})
}

func (s *Store) ListSkillCategories(ctx context.Context, tenantID string) ([]store.SkillCategoryRecord, error) {
	if tenantID == "" {
		return nil, store.ErrNotFound
	}
	out := []store.SkillCategoryRecord{}
	err := s.withTx(ctx, func(tx pgx.Tx) error {
		rows, err := tx.Query(ctx, `
			SELECT key, name, name_en, sort_order
			  FROM skill_categories
			 ORDER BY sort_order, key`)
		if err != nil {
			return storageErr("list skill categories", err)
		}
		defer rows.Close()
		for rows.Next() {
			var rec store.SkillCategoryRecord
			if err := rows.Scan(&rec.Key, &rec.Name, &rec.NameEn, &rec.SortOrder); err != nil {
				return storageErr("scan skill category", err)
			}
			out = append(out, rec)
		}
		if err := rows.Err(); err != nil {
			return storageErr("list skill categories", err)
		}
		return nil
	})
	return out, err
}

func (s *Store) UpsertSkillCategories(ctx context.Context, tenantID string, rows []store.SkillCategoryRecord) error {
	if tenantID == "" {
		return store.ErrNotFound
	}
	if len(rows) == 0 {
		return nil
	}
	return s.withTx(ctx, func(tx pgx.Tx) error {
		batch := &pgx.Batch{}
		for _, row := range rows {
			batch.Queue(`
				INSERT INTO skill_categories (key, name, name_en, sort_order)
				VALUES ($1, $2, $3, $4)
				ON CONFLICT (key) DO UPDATE SET
				  name = EXCLUDED.name,
				  name_en = EXCLUDED.name_en,
				  sort_order = EXCLUDED.sort_order`,
				row.Key, row.Name, row.NameEn, row.SortOrder)
		}
		return execBatch(ctx, tx, batch, "upsert skill categories")
	})
}

func (s *Store) upsertSkills(ctx context.Context, rows []store.SkillRecord, withRank bool) error {
	if len(rows) == 0 {
		return nil
	}
	return s.withTx(ctx, func(tx pgx.Tx) error {
		return s.queueSkills(ctx, tx, rows, withRank)
	})
}

func (s *Store) queueSkills(ctx context.Context, tx pgx.Tx, rows []store.SkillRecord, withRank bool) error {
	if len(rows) == 0 {
		return nil
	}
	batch := &pgx.Batch{}
	for _, row := range rows {
		if withRank {
			batch.Queue(`
				INSERT INTO skill_catalog (
				  id, slug, handle, name, description, category, icon_url,
				  downloads, stars, source, version, requires_api_key, paid,
				  score, updated_at, synced_at, trending_rank
				) VALUES (
				  $1, $2, $3, $4, $5, $6, $7,
				  $8, $9, $10, $11, $12, $13,
				  $14, $15, now(), $16
				)
				ON CONFLICT (id) DO UPDATE SET
				  slug = EXCLUDED.slug,
				  handle = EXCLUDED.handle,
				  name = EXCLUDED.name,
				  description = EXCLUDED.description,
				  category = EXCLUDED.category,
				  icon_url = EXCLUDED.icon_url,
				  downloads = EXCLUDED.downloads,
				  stars = EXCLUDED.stars,
				  source = EXCLUDED.source,
				  version = EXCLUDED.version,
				  requires_api_key = EXCLUDED.requires_api_key,
				  paid = EXCLUDED.paid,
				  score = EXCLUDED.score,
				  updated_at = EXCLUDED.updated_at,
				  synced_at = now(),
				  trending_rank = EXCLUDED.trending_rank`,
				row.ID, row.Slug, row.Handle, row.Name, row.Description, row.Category, row.IconURL,
				row.Downloads, row.Stars, row.Source, row.Version, row.RequiresAPIKey, row.Paid,
				row.Score, row.UpdatedAt, row.TrendingRank)
			continue
		}
		batch.Queue(`
			INSERT INTO skill_catalog (
			  id, slug, handle, name, description, category, icon_url,
			  downloads, stars, source, version, requires_api_key, paid,
			  score, updated_at, synced_at
			) VALUES (
			  $1, $2, $3, $4, $5, $6, $7,
			  $8, $9, $10, $11, $12, $13,
			  $14, $15, now()
			)
			ON CONFLICT (id) DO UPDATE SET
			  slug = EXCLUDED.slug,
			  handle = EXCLUDED.handle,
			  name = EXCLUDED.name,
			  description = EXCLUDED.description,
			  category = EXCLUDED.category,
			  icon_url = EXCLUDED.icon_url,
			  downloads = EXCLUDED.downloads,
			  stars = EXCLUDED.stars,
			  source = EXCLUDED.source,
			  version = EXCLUDED.version,
			  requires_api_key = EXCLUDED.requires_api_key,
			  paid = EXCLUDED.paid,
			  score = EXCLUDED.score,
			  updated_at = EXCLUDED.updated_at,
			  synced_at = now()`,
			row.ID, row.Slug, row.Handle, row.Name, row.Description, row.Category, row.IconURL,
			row.Downloads, row.Stars, row.Source, row.Version, row.RequiresAPIKey, row.Paid,
			row.Score, row.UpdatedAt)
	}
	return execBatch(ctx, tx, batch, "upsert skill catalog")
}

func (s *Store) withTx(ctx context.Context, fn func(pgx.Tx) error) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return storageErr("begin", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if err := fn(tx); err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return storageErr("commit", err)
	}
	return nil
}

func execBatch(ctx context.Context, tx pgx.Tx, batch *pgx.Batch, op string) error {
	results := tx.SendBatch(ctx, batch)
	defer results.Close()
	for range batch.Len() {
		if _, err := results.Exec(); err != nil {
			return storageErr(op, err)
		}
	}
	return nil
}

func skillWhere(q store.SkillCatalogQuery) (string, []any) {
	cond := []string{"TRUE"}
	args := []any{}
	if q.Category != "" {
		args = append(args, q.Category)
		cond = append(cond, fmt.Sprintf("s.category = $%d", len(args)))
	}
	if q.Source != "" {
		args = append(args, q.Source)
		cond = append(cond, fmt.Sprintf("s.source = $%d", len(args)))
	}
	if q.Keyword != "" {
		args = append(args, "%"+escapeLike(q.Keyword)+"%")
		cond = append(cond, fmt.Sprintf(`(s.name ILIKE $%d ESCAPE E'\\' OR s.description ILIKE $%d ESCAPE E'\\')`, len(args), len(args)))
	}
	if q.RequiresAPIKey != "" {
		args = append(args, q.RequiresAPIKey == "true")
		cond = append(cond, fmt.Sprintf("s.requires_api_key = $%d", len(args)))
	}
	if q.Paid != "" {
		args = append(args, q.Paid == "true")
		cond = append(cond, fmt.Sprintf("s.paid = $%d", len(args)))
	}
	if q.Sort == "trending" {
		cond = append(cond, "s.trending_rank > 0")
	}
	return strings.Join(cond, " AND "), args
}

func skillOrder(sort string) string {
	switch sort {
	case "downloads":
		return "s.downloads DESC, s.id"
	case "updated_at":
		return "s.updated_at DESC, s.id"
	case "stars":
		return "s.stars DESC, s.id"
	case "trending":
		return "s.trending_rank ASC, s.id"
	default:
		return "s.score DESC, s.id"
	}
}

func escapeLike(s string) string {
	return strings.NewReplacer(`\`, `\\`, `%`, `\%`, `_`, `\_`).Replace(s)
}

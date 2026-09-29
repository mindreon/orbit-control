package pgstore

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"time"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"

	"github.com/mindreon/orbit-control/internal/store"
)

// skill_catalog and skill_categories are shared marketplace metadata. They are not tenant tables: statements do not
// filter by tenant_id, and no tenant GUC is set. tenantID is still required so a caller without a tenant cannot use
// the repository.

type skillRow struct {
	ID                string `gorm:"primaryKey"`
	Slug              string
	Handle            string
	Name              string
	Description       string
	Category          string
	IconURL           string
	Downloads         int64
	Stars             int64
	Source            string
	Version           string
	NeedsUpstreamAuth bool
	Paid              bool
	Score             float64
	UpdatedAt         time.Time `gorm:"autoUpdateTime:false"`
	SyncedAt          time.Time `gorm:"autoUpdateTime:false"`
	TrendingRank      int
	TextFiles         []byte `gorm:"type:jsonb"`
	DetailCopy        []byte `gorm:"type:jsonb"`
}

func (skillRow) TableName() string { return "skill_catalog" }

// skillListRow is a catalog row with the label of its category: only what a list shows, flat (gorm skips embedded
// structs of unexported type).
type skillListRow struct {
	ID                string
	Slug              string
	Handle            string
	Name              string
	Description       string
	Category          string
	CategoryName      string
	IconURL           string
	Downloads         int64
	Stars             int64
	Source            string
	Version           string
	NeedsUpstreamAuth bool
	Paid              bool
	Score             float64
	UpdatedAt         time.Time
	TrendingRank      int
}

func (r skillListRow) record() store.SkillRecord {
	return store.SkillRecord{
		ID: r.ID, Slug: r.Slug, Handle: r.Handle, Name: r.Name, Description: r.Description, Category: r.Category,
		CategoryName: r.CategoryName, IconURL: r.IconURL, Downloads: r.Downloads, Stars: r.Stars, Source: r.Source,
		Version: r.Version, RequiresAPIKey: r.NeedsUpstreamAuth, Paid: r.Paid, Score: r.Score, UpdatedAt: r.UpdatedAt,
		TrendingRank: r.TrendingRank,
	}
}

type skillCategoryRow struct {
	Key       string `gorm:"primaryKey"`
	Name      string
	NameEn    string
	SortOrder int
}

func (skillCategoryRow) TableName() string { return "skill_categories" }

const skillSelect = `s.id, s.slug, s.handle, s.name, s.description, s.category, COALESCE(c.name, '') AS category_name,
	s.icon_url, s.downloads, s.stars, s.source, s.version, s.needs_upstream_auth, s.paid, s.score, s.updated_at, s.trending_rank`

// inShared runs fn in one transaction on the shared (non-tenant) tables.
func (s *Store) inShared(ctx context.Context, fn func(tx *gorm.DB) error) error {
	return s.db.WithContext(ctx).Transaction(fn)
}

func skills(tx *gorm.DB) *gorm.DB {
	return tx.Table("skill_catalog AS s").Joins("LEFT JOIN skill_categories c ON c.key = s.category")
}

func (s *Store) ListSkillCatalog(ctx context.Context, tenantID string, q store.SkillCatalogQuery) (store.SkillCatalogPage, error) {
	if tenantID == "" {
		return store.SkillCatalogPage{}, store.ErrNotFound
	}
	q = store.NormalizeSkillQuery(q)
	page := store.SkillCatalogPage{Page: q.Page, PageSize: q.PageSize, Items: []store.SkillRecord{}}
	err := s.inShared(ctx, func(tx *gorm.DB) error {
		var total int64
		if err := skills(tx).Scopes(skillFilter(q)).Count(&total).Error; err != nil {
			return storageErr("count skill catalog", err)
		}
		page.Total = int(total)
		var synced *time.Time
		if err := tx.Model(&skillRow{}).Select("max(synced_at)").Scan(&synced).Error; err != nil {
			return storageErr("skill catalog sync time", err)
		}
		if synced != nil {
			page.SyncedAt = *synced
		}
		var rows []skillListRow
		if err := skills(tx).Select(skillSelect).Scopes(skillFilter(q)).Order(skillOrder(q.Sort)).
			Limit(q.PageSize).Offset((q.Page - 1) * q.PageSize).Find(&rows).Error; err != nil {
			return storageErr("list skill catalog", err)
		}
		for _, row := range rows {
			page.Items = append(page.Items, row.record())
		}
		return nil
	})
	return page, err
}

func (s *Store) GetSkill(ctx context.Context, tenantID, id string) (store.SkillRecord, error) {
	if tenantID == "" || id == "" {
		return store.SkillRecord{}, store.ErrNotFound
	}
	var row skillListRow
	err := s.inShared(ctx, func(tx *gorm.DB) error {
		return skills(tx).Select(skillSelect).Where("s.id = ?", id).Take(&row).Error
	})
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return store.SkillRecord{}, store.ErrNotFound
	}
	if err != nil {
		return store.SkillRecord{}, storageErr("get skill", err)
	}
	return row.record(), nil
}

// skillColumn reads one column of one catalog row: ErrNotFound when there is no such skill, nil bytes when the column
// has never been filled.
func (s *Store) skillColumn(ctx context.Context, id, column, op string) ([]byte, error) {
	var row skillRow
	err := s.inShared(ctx, func(tx *gorm.DB) error {
		return tx.Select(column).Where("id = ?", id).Take(&row).Error
	})
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, store.ErrNotFound
	}
	if err != nil {
		return nil, storageErr(op, err)
	}
	if column == "text_files" {
		return row.TextFiles, nil
	}
	return row.DetailCopy, nil
}

func (s *Store) saveSkillColumn(ctx context.Context, id, column string, raw []byte, op string) error {
	return s.inShared(ctx, func(tx *gorm.DB) error {
		res := tx.Model(&skillRow{}).Where("id = ?", id).Update(column, raw)
		if res.Error != nil {
			return storageErr(op, res.Error)
		}
		if res.RowsAffected == 0 {
			return store.ErrNotFound
		}
		return nil
	})
}

func (s *Store) GetSkillTextFiles(ctx context.Context, tenantID, id string) ([]store.SkillFile, bool, error) {
	if tenantID == "" || id == "" {
		return nil, false, store.ErrNotFound
	}
	raw, err := s.skillColumn(ctx, id, "text_files", "get skill text")
	if err != nil || raw == nil {
		return nil, false, err
	}
	var files []store.SkillFile
	if err := json.Unmarshal(raw, &files); err != nil {
		return nil, false, storageErr("decode skill text", err)
	}
	if files == nil {
		files = []store.SkillFile{}
	}
	return files, true, nil
}

func (s *Store) SaveSkillTextFiles(ctx context.Context, tenantID, id string, files []store.SkillFile) error {
	if tenantID == "" || id == "" {
		return store.ErrNotFound
	}
	if files == nil {
		files = []store.SkillFile{}
	}
	raw, err := json.Marshal(files)
	if err != nil {
		return store.ErrStorage
	}
	return s.saveSkillColumn(ctx, id, "text_files", raw, "save skill text")
}

func (s *Store) GetSkillDetail(ctx context.Context, tenantID, id string) ([]byte, bool, error) {
	if tenantID == "" || id == "" {
		return nil, false, store.ErrNotFound
	}
	raw, err := s.skillColumn(ctx, id, "detail_copy", "get skill detail")
	if err != nil || raw == nil {
		return nil, false, err
	}
	return raw, true, nil
}

func (s *Store) SaveSkillDetail(ctx context.Context, tenantID, id string, raw []byte) error {
	if tenantID == "" || id == "" {
		return store.ErrNotFound
	}
	if raw == nil {
		raw = []byte("{}")
	}
	return s.saveSkillColumn(ctx, id, "detail_copy", raw, "save skill detail")
}

func (s *Store) UpsertSkillCatalog(ctx context.Context, tenantID string, rows []store.SkillRecord) error {
	if tenantID == "" {
		return store.ErrNotFound
	}
	if len(rows) == 0 {
		return nil
	}
	return s.inShared(ctx, func(tx *gorm.DB) error { return upsertSkills(tx, rows, false) })
}

func (s *Store) ReplaceSkillTrending(ctx context.Context, tenantID string, rows []store.SkillRecord) error {
	if tenantID == "" {
		return store.ErrNotFound
	}
	return s.inShared(ctx, func(tx *gorm.DB) error {
		if err := tx.Model(&skillRow{}).Where("trending_rank <> 0").Update("trending_rank", 0).Error; err != nil {
			return storageErr("clear skill trending", err)
		}
		return upsertSkills(tx, rows, true)
	})
}

func (s *Store) ListSkillCategories(ctx context.Context, tenantID string) ([]store.SkillCategoryRecord, error) {
	if tenantID == "" {
		return nil, store.ErrNotFound
	}
	var rows []skillCategoryRow
	err := s.inShared(ctx, func(tx *gorm.DB) error { return tx.Order("sort_order, key").Find(&rows).Error })
	if err != nil {
		return nil, storageErr("list skill categories", err)
	}
	out := make([]store.SkillCategoryRecord, 0, len(rows))
	for _, row := range rows {
		out = append(out, store.SkillCategoryRecord{Key: row.Key, Name: row.Name, NameEn: row.NameEn, SortOrder: row.SortOrder})
	}
	return out, nil
}

func (s *Store) UpsertSkillCategories(ctx context.Context, tenantID string, rows []store.SkillCategoryRecord) error {
	if tenantID == "" {
		return store.ErrNotFound
	}
	if len(rows) == 0 {
		return nil
	}
	models := make([]skillCategoryRow, 0, len(rows))
	for _, row := range rows {
		models = append(models, skillCategoryRow{Key: row.Key, Name: row.Name, NameEn: row.NameEn, SortOrder: row.SortOrder})
	}
	return s.inShared(ctx, func(tx *gorm.DB) error {
		err := tx.Clauses(clause.OnConflict{
			Columns: []clause.Column{{Name: "key"}}, DoUpdates: clause.AssignmentColumns([]string{"name", "name_en", "sort_order"}),
		}).Create(&models).Error
		if err != nil {
			return storageErr("upsert skill categories", err)
		}
		return nil
	})
}

var skillUpsertColumns = []string{
	"slug", "handle", "name", "description", "category", "icon_url", "downloads", "stars", "source", "version",
	"needs_upstream_auth", "paid", "score", "updated_at", "synced_at",
}

func upsertSkills(tx *gorm.DB, rows []store.SkillRecord, withRank bool) error {
	if len(rows) == 0 {
		return nil
	}
	now := time.Now().UTC()
	models := make([]skillRow, 0, len(rows))
	for _, row := range rows {
		if row.Handle != "" && row.Slug != "" && row.ID == row.Handle+"/"+row.Slug {
			// A skill that arrived without an author handle was stored under the slug alone. Once the handle is
			// known, keep that same row.
			err := tx.Model(&skillRow{}).
				Where("id = ? AND handle = '' AND slug = ? AND NOT EXISTS (SELECT 1 FROM skill_catalog existing WHERE existing.id = ?)", row.Slug, row.Slug, row.ID).
				Updates(map[string]any{"id": row.ID, "handle": row.Handle}).Error
			if err != nil {
				return storageErr("upsert skill catalog", err)
			}
		}
		models = append(models, skillRow{
			ID: row.ID, Slug: row.Slug, Handle: row.Handle, Name: row.Name, Description: row.Description, Category: row.Category,
			IconURL: row.IconURL, Downloads: row.Downloads, Stars: row.Stars, Source: row.Source, Version: row.Version,
			NeedsUpstreamAuth: row.RequiresAPIKey, Paid: row.Paid, Score: row.Score, UpdatedAt: row.UpdatedAt, SyncedAt: now,
			TrendingRank: row.TrendingRank,
		})
	}
	// The trending rank is only written by the trending refresh; the plain catalog sync leaves it alone.
	columns := append([]string{"id"}, skillUpsertColumns...)
	updates := skillUpsertColumns
	if withRank {
		columns, updates = append(columns, "trending_rank"), append(append([]string{}, skillUpsertColumns...), "trending_rank")
	}
	err := tx.Clauses(clause.OnConflict{Columns: []clause.Column{{Name: "id"}}, DoUpdates: clause.AssignmentColumns(updates)}).
		Select(columns).CreateInBatches(&models, 200).Error
	if err != nil {
		return storageErr("upsert skill catalog", err)
	}
	return nil
}

func skillFilter(q store.SkillCatalogQuery) func(*gorm.DB) *gorm.DB {
	return func(tx *gorm.DB) *gorm.DB {
		if q.Category != "" {
			tx = tx.Where("s.category = ?", q.Category)
		}
		if q.Source != "" {
			tx = tx.Where("s.source = ?", q.Source)
		}
		if q.Keyword != "" {
			like := "%" + escapeLike(q.Keyword) + "%"
			tx = tx.Where(`(s.name ILIKE ? ESCAPE E'\\' OR s.description ILIKE ? ESCAPE E'\\')`, like, like)
		}
		if q.RequiresAPIKey != "" {
			tx = tx.Where("s.needs_upstream_auth = ?", q.RequiresAPIKey == "true")
		}
		if q.Paid != "" {
			tx = tx.Where("s.paid = ?", q.Paid == "true")
		}
		if q.Sort == "trending" {
			tx = tx.Where("s.trending_rank > 0")
		}
		return tx
	}
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
		return "s.score DESC, s.downloads DESC, s.stars DESC, s.id"
	}
}

func escapeLike(s string) string {
	return strings.NewReplacer(`\`, `\\`, `%`, `\%`, `_`, `\_`).Replace(s)
}

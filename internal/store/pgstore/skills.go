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

// skill_catalog and skill_categories are shared marketplace metadata (the
// ModelScope snapshot). They are not tenant tables: statements do not filter
// by tenant_id, and no tenant GUC is set. tenantID is still required so a
// caller without a tenant cannot use the repository.

type skillRow struct {
	ID            string `gorm:"primaryKey"`
	Handle        string
	Slug          string
	Name          string
	Description   string
	DescriptionEn string
	Category      string
	Tags          []byte `gorm:"type:jsonb"`
	License       string
	IconURL       string
	SourceURL     string
	Downloads     int64
	Visits        int64
	Likes         int64
	UpdatedAt     time.Time `gorm:"autoUpdateTime:false"`
	Source        string
	TextFiles     []byte    `gorm:"type:jsonb"`
	InstalledAt   time.Time `gorm:"autoUpdateTime:false"`
}

func (skillRow) TableName() string { return "skill_catalog" }

// skillListRow is a catalog row with the label of its category: only what a
// list shows, flat (gorm skips embedded structs of unexported type).
type skillListRow struct {
	ID            string
	Handle        string
	Slug          string
	Name          string
	Description   string
	DescriptionEn string
	Category      string
	CategoryName  string
	Tags          []byte
	License       string
	IconURL       string
	SourceURL     string
	Downloads     int64
	Visits        int64
	Likes         int64
	UpdatedAt     time.Time
	Source        string
}

func (r skillListRow) record() store.SkillRecord {
	return store.SkillRecord{
		ID: r.ID, Handle: r.Handle, Slug: r.Slug, Name: r.Name, Description: r.Description, DescriptionEn: r.DescriptionEn,
		Category: r.Category, CategoryName: r.CategoryName, Tags: decodeStrings(r.Tags), License: r.License,
		IconURL: r.IconURL, SourceURL: r.SourceURL, Downloads: r.Downloads, Visits: r.Visits, Likes: r.Likes,
		UpdatedAt: r.UpdatedAt, Source: r.Source,
	}
}

type skillCategoryRow struct {
	Key       string `gorm:"primaryKey"`
	Name      string
	NameEn    string
	SortOrder int
}

func (skillCategoryRow) TableName() string { return "skill_categories" }

const skillSelect = `s.id, s.handle, s.slug, s.name, s.description, s.description_en, s.category,
	COALESCE(c.name, '') AS category_name, s.tags, s.license, s.icon_url, s.source_url,
	s.downloads, s.visits, s.likes, s.updated_at, s.source`

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
		var installed *time.Time
		if err := tx.Model(&skillRow{}).Select("max(installed_at)").Scan(&installed).Error; err != nil {
			return storageErr("skill catalog install time", err)
		}
		if installed != nil {
			page.InstalledAt = *installed
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

func (s *Store) GetSkillTextFiles(ctx context.Context, tenantID, id string) ([]store.SkillFile, bool, error) {
	if tenantID == "" || id == "" {
		return nil, false, store.ErrNotFound
	}
	var row skillRow
	err := s.inShared(ctx, func(tx *gorm.DB) error {
		return tx.Select("text_files").Where("id = ?", id).Take(&row).Error
	})
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, false, store.ErrNotFound
	}
	if err != nil {
		return nil, false, storageErr("get skill text", err)
	}
	if row.TextFiles == nil {
		return nil, false, nil
	}
	var files []store.SkillFile
	if err := json.Unmarshal(row.TextFiles, &files); err != nil {
		return nil, false, storageErr("decode skill text", err)
	}
	if files == nil {
		files = []store.SkillFile{}
	}
	return files, true, nil
}

// textChunk is how many skills one UPDATE statement fills. One row can carry
// hundreds of kilobytes of text, so the chunk stays small.
const textChunk = 50

// SaveSkillTextFilesBatch fills the text_files column of known skills. Rows
// with unknown ids update nothing; empty file lists mark the row known-empty.
func (s *Store) SaveSkillTextFilesBatch(ctx context.Context, tenantID string, rows []store.SkillTextFilesRow) error {
	if tenantID == "" {
		return store.ErrNotFound
	}
	if len(rows) == 0 {
		return nil
	}
	type blob struct {
		id  string
		raw []byte
	}
	blobs := make([]blob, 0, len(rows))
	for _, row := range rows {
		if row.ID == "" {
			continue
		}
		files := row.Files
		if files == nil {
			files = []store.SkillFile{}
		}
		raw, err := json.Marshal(files)
		if err != nil {
			return store.ErrStorage
		}
		blobs = append(blobs, blob{id: row.ID, raw: raw})
	}
	return s.inShared(ctx, func(tx *gorm.DB) error {
		for start := 0; start < len(blobs); start += textChunk {
			end := min(start+textChunk, len(blobs))
			query := strings.Builder{}
			query.WriteString("UPDATE skill_catalog AS s SET text_files = v.f FROM (VALUES ")
			args := make([]any, 0, 2*(end-start))
			for i := start; i < end; i++ {
				if i > start {
					query.WriteString(", ")
				}
				query.WriteString("(?, ?::jsonb)")
				args = append(args, blobs[i].id, string(blobs[i].raw))
			}
			query.WriteString(") AS v(id, f) WHERE s.id = v.id")
			if err := tx.Exec(query.String(), args...).Error; err != nil {
				return storageErr("save skill text", err)
			}
		}
		return nil
	})
}

// ReplaceSkills swaps the stored skill snapshot for the given one in one
// transaction. Text files are filled afterwards by SaveSkillTextFilesBatch.
func (s *Store) ReplaceSkills(ctx context.Context, tenantID string, rows []store.SkillRecord, categories []store.SkillCategoryRecord) error {
	if tenantID == "" {
		return store.ErrNotFound
	}
	now := time.Now().UTC()
	skillRows := make([]skillRow, 0, len(rows))
	for _, row := range rows {
		if row.ID == "" {
			continue
		}
		tags := row.Tags
		if tags == nil {
			tags = []string{}
		}
		raw, err := json.Marshal(tags)
		if err != nil {
			return store.ErrStorage
		}
		skillRows = append(skillRows, skillRow{
			ID: row.ID, Handle: row.Handle, Slug: row.Slug, Name: row.Name, Description: row.Description,
			DescriptionEn: row.DescriptionEn, Category: row.Category, Tags: raw, License: row.License,
			IconURL: row.IconURL, SourceURL: row.SourceURL, Downloads: row.Downloads, Visits: row.Visits,
			Likes: row.Likes, UpdatedAt: row.UpdatedAt, Source: row.Source, InstalledAt: now,
		})
	}
	categoryRows := make([]skillCategoryRow, 0, len(categories))
	for _, row := range categories {
		if row.Key != "" {
			categoryRows = append(categoryRows, skillCategoryRow{Key: row.Key, Name: row.Name, NameEn: row.NameEn, SortOrder: row.SortOrder})
		}
	}
	return s.inShared(ctx, func(tx *gorm.DB) error {
		if err := tx.Where("TRUE").Delete(&skillRow{}).Error; err != nil {
			return storageErr("clear skill catalog", err)
		}
		if err := tx.CreateInBatches(&skillRows, 200).Error; err != nil {
			return storageErr("insert skill catalog", err)
		}
		if len(categoryRows) == 0 {
			return nil
		}
		if err := tx.Clauses(clause.OnConflict{
			Columns: []clause.Column{{Name: "key"}}, DoUpdates: clause.AssignmentColumns([]string{"name", "name_en", "sort_order"}),
		}).Create(&categoryRows).Error; err != nil {
			return storageErr("upsert skill categories", err)
		}
		return nil
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
			tx = tx.Where(`(s.name ILIKE ? ESCAPE E'\\' OR s.description ILIKE ? ESCAPE E'\\' OR s.description_en ILIKE ? ESCAPE E'\\')`, like, like, like)
		}
		return tx
	}
}

func skillOrder(sort string) string {
	switch sort {
	case "updated_at":
		return "s.updated_at DESC, s.id"
	case "likes":
		return "s.likes DESC, s.id"
	default:
		return "s.downloads DESC, s.id"
	}
}

func escapeLike(s string) string {
	return strings.NewReplacer(`\`, `\\`, `%`, `\%`, `_`, `\_`).Replace(s)
}

// decodeStrings reads a JSONB string array; a missing column decodes to nil.
func decodeStrings(raw []byte) []string {
	if len(raw) == 0 {
		return nil
	}
	var out []string
	if err := json.Unmarshal(raw, &out); err != nil {
		return nil
	}
	return out
}

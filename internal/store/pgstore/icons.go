package pgstore

import (
	"context"
	"errors"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"

	"github.com/mindreon/orbit-control/internal/store"
)

// catalog_icons holds the icon bytes the snapshot sidecar shipped, keyed by the
// source URL. It is shared marketplace data, not a tenant table. tenantID is
// still required so a caller without a tenant cannot use the repository.

type catalogIconRow struct {
	URL         string `gorm:"primaryKey"`
	ContentType string
	Data        []byte
}

func (catalogIconRow) TableName() string { return "catalog_icons" }

const iconChunk = 100

// ReplaceCatalogIcons upserts icon bytes by URL. Icons are additive: rows for
// URLs the new sidecar does not name stay in place.
func (s *Store) ReplaceCatalogIcons(ctx context.Context, tenantID string, rows []store.CatalogIcon) error {
	if tenantID == "" {
		return store.ErrNotFound
	}
	if len(rows) == 0 {
		return nil
	}
	models := make([]catalogIconRow, 0, len(rows))
	for _, row := range rows {
		if row.URL == "" || len(row.Data) == 0 {
			continue
		}
		models = append(models, catalogIconRow{URL: row.URL, ContentType: row.ContentType, Data: row.Data})
	}
	if len(models) == 0 {
		return nil
	}
	return s.inShared(ctx, func(tx *gorm.DB) error {
		for start := 0; start < len(models); start += iconChunk {
			end := min(start+iconChunk, len(models))
			if err := tx.Clauses(clause.OnConflict{
				Columns:   []clause.Column{{Name: "url"}},
				DoUpdates: clause.AssignmentColumns([]string{"content_type", "data"}),
			}).CreateInBatches(models[start:end], 50).Error; err != nil {
				return storageErr("upsert catalog icons", err)
			}
		}
		return nil
	})
}

// CatalogIcon reads one icon by source URL.
func (s *Store) CatalogIcon(ctx context.Context, tenantID, url string) (string, []byte, error) {
	if tenantID == "" || url == "" {
		return "", nil, store.ErrNotFound
	}
	var row catalogIconRow
	err := s.inShared(ctx, func(tx *gorm.DB) error {
		return tx.Where("url = ?", url).Take(&row).Error
	})
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return "", nil, store.ErrNotFound
	}
	if err != nil {
		return "", nil, storageErr("get catalog icon", err)
	}
	return row.ContentType, row.Data, nil
}

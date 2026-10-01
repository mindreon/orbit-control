package pgstore

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"

	"github.com/mindreon/orbit-control/internal/store"
)

// agent_catalog is shared marketplace metadata (the ModelScope agents
// snapshot). It is not a tenant table: statements do not filter by tenant_id.
// tenantID is still required so a caller without a tenant cannot use the
// repository.

type agentRow struct {
	ID            string `gorm:"primaryKey"`
	Handle        string
	Slug          string
	Name          string
	Description   string
	Framework     string
	License       string
	LogoURL       string
	Catalogues    []byte `gorm:"type:jsonb"`
	Models        []byte `gorm:"type:jsonb"`
	Mcps          []byte `gorm:"type:jsonb"`
	Skills        []byte `gorm:"type:jsonb"`
	SystemPrompts []byte `gorm:"type:jsonb"`
	Readme        string
	Files         []byte `gorm:"type:jsonb"`
	Stars         int64
	Downloads     int64
	Visits        int64
	UpdatedAt     time.Time `gorm:"autoUpdateTime:false"`
	Source        string
	InstalledAt   time.Time `gorm:"autoUpdateTime:false"`
}

func (agentRow) TableName() string { return "agent_catalog" }

func (r agentRow) record() store.AgentRecord {
	return store.AgentRecord{
		ID: r.ID, Handle: r.Handle, Slug: r.Slug, Name: r.Name, Description: r.Description,
		Framework: r.Framework, License: r.License, LogoURL: r.LogoURL,
		Catalogues: decodeStrings(r.Catalogues), Models: decodeAgentModels(r.Models), Mcps: decodeAgentRefs(r.Mcps),
		Skills: decodeAgentRefs(r.Skills), SystemPrompts: decodeAgentPrompts(r.SystemPrompts), Readme: r.Readme,
		Files: decodeSkillFiles(r.Files), FilesKnown: r.Files != nil,
		Stars: r.Stars, Downloads: r.Downloads, Visits: r.Visits, UpdatedAt: r.UpdatedAt, Source: r.Source,
	}
}

func decodeAgentModels(raw []byte) []store.AgentModel {
	var out []store.AgentModel
	_ = json.Unmarshal(raw, &out)
	return out
}

func decodeAgentRefs(raw []byte) []store.AgentRef {
	var out []store.AgentRef
	_ = json.Unmarshal(raw, &out)
	return out
}

func decodeAgentPrompts(raw []byte) []store.AgentPrompt {
	var out []store.AgentPrompt
	_ = json.Unmarshal(raw, &out)
	return out
}

func decodeSkillFiles(raw []byte) []store.SkillFile {
	var out []store.SkillFile
	_ = json.Unmarshal(raw, &out)
	return out
}

func agents(tx *gorm.DB) *gorm.DB {
	return tx.Table("agent_catalog AS a")
}

func (s *Store) ListAgentCatalog(ctx context.Context, tenantID string, q store.AgentCatalogQuery) (store.AgentCatalogPage, error) {
	if tenantID == "" {
		return store.AgentCatalogPage{}, store.ErrNotFound
	}
	q = store.NormalizeAgentCatalogQuery(q)
	page := store.AgentCatalogPage{Page: q.Page, PageSize: q.PageSize, Items: []store.AgentRecord{}}
	err := s.inShared(ctx, func(tx *gorm.DB) error {
		var total int64
		if err := agents(tx).Scopes(agentFilter(q)).Count(&total).Error; err != nil {
			return storageErr("count agent catalog", err)
		}
		page.Total = int(total)
		var rows []agentRow
		list := agents(tx).Select("a.*").Scopes(agentFilter(q)).Order(agentOrder(q.Sort)).
			Limit(q.PageSize).Offset((q.Page - 1) * q.PageSize)
		if err := list.Find(&rows).Error; err != nil {
			return storageErr("list agent catalog", err)
		}
		for _, row := range rows {
			page.Items = append(page.Items, row.record())
		}
		return nil
	})
	return page, err
}

func (s *Store) GetAgent(ctx context.Context, tenantID, id string) (store.AgentRecord, error) {
	if tenantID == "" || id == "" {
		return store.AgentRecord{}, store.ErrNotFound
	}
	var row agentRow
	err := s.inShared(ctx, func(tx *gorm.DB) error {
		return agents(tx).Select("a.*").Where("a.id = ?", id).Take(&row).Error
	})
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return store.AgentRecord{}, store.ErrNotFound
	}
	if err != nil {
		return store.AgentRecord{}, storageErr("get agent", err)
	}
	return row.record(), nil
}

// ReplaceAgents swaps the stored agent snapshot for the given one in one transaction.
func (s *Store) ReplaceAgents(ctx context.Context, tenantID string, rows []store.AgentRecord) error {
	if tenantID == "" {
		return store.ErrNotFound
	}
	now := time.Now().UTC()
	agentRows := make([]agentRow, 0, len(rows))
	for _, row := range rows {
		if row.ID == "" {
			continue
		}
		models, err := json.Marshal(orEmpty(row.Models))
		if err != nil {
			return store.ErrStorage
		}
		mcps, err := json.Marshal(orEmpty(row.Mcps))
		if err != nil {
			return store.ErrStorage
		}
		skills, err := json.Marshal(orEmpty(row.Skills))
		if err != nil {
			return store.ErrStorage
		}
		prompts, err := json.Marshal(orEmpty(row.SystemPrompts))
		if err != nil {
			return store.ErrStorage
		}
		catalogues, err := json.Marshal(orEmpty(row.Catalogues))
		if err != nil {
			return store.ErrStorage
		}
		files, err := json.Marshal(orEmpty(row.Files))
		if err != nil {
			return store.ErrStorage
		}
		agentRows = append(agentRows, agentRow{
			ID: row.ID, Handle: row.Handle, Slug: row.Slug, Name: row.Name, Description: row.Description,
			Framework: row.Framework, License: row.License, LogoURL: row.LogoURL,
			Catalogues: catalogues, Models: models, Mcps: mcps, Skills: skills, SystemPrompts: prompts,
			Readme: row.Readme, Files: files, Stars: row.Stars, Downloads: row.Downloads, Visits: row.Visits,
			UpdatedAt: row.UpdatedAt, Source: row.Source, InstalledAt: now,
		})
	}
	return s.inShared(ctx, func(tx *gorm.DB) error {
		if err := tx.Where("TRUE").Delete(&agentRow{}).Error; err != nil {
			return storageErr("clear agent catalog", err)
		}
		if err := tx.CreateInBatches(&agentRows, 200).Error; err != nil {
			return storageErr("insert agent catalog", err)
		}
		return nil
	})
}

func orEmpty[T any](items []T) []T {
	if items == nil {
		return []T{}
	}
	return items
}

func agentFilter(q store.AgentCatalogQuery) func(*gorm.DB) *gorm.DB {
	return func(tx *gorm.DB) *gorm.DB {
		if q.Catalogue != "" {
			tx = tx.Where("a.catalogues @> ?", cataloguesJSON(q.Catalogue))
		}
		if q.Keyword != "" {
			like := "%" + escapeLike(q.Keyword) + "%"
			tx = tx.Where(`(a.name ILIKE ? ESCAPE E'\\' OR a.description ILIKE ? ESCAPE E'\\' OR a.readme ILIKE ? ESCAPE E'\\')`, like, like, like)
		}
		return tx
	}
}

// cataloguesJSON wraps one key for the JSONB containment operator.
func cataloguesJSON(key string) string {
	raw, err := json.Marshal([]string{key})
	if err != nil {
		return "[]"
	}
	return string(raw)
}

func agentOrder(sort string) string {
	switch sort {
	case "updated_at":
		return "a.updated_at DESC, a.id"
	case "stars":
		return "a.stars DESC, a.id"
	default:
		return "a.downloads DESC, a.id"
	}
}

type catalogSnapshotRow struct {
	Name        string `gorm:"primaryKey"`
	Sha256      string
	InstalledAt time.Time `gorm:"autoUpdateTime:false"`
}

func (catalogSnapshotRow) TableName() string { return "catalog_snapshots" }

// CatalogSnapshot reads the stored hash of a named snapshot.
func (s *Store) CatalogSnapshot(ctx context.Context, tenantID, name string) (string, bool, error) {
	if tenantID == "" {
		return "", false, store.ErrNotFound
	}
	var row catalogSnapshotRow
	err := s.inShared(ctx, func(tx *gorm.DB) error {
		return tx.Where("name = ?", name).Take(&row).Error
	})
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return "", false, nil
	}
	if err != nil {
		return "", false, storageErr("get catalog snapshot", err)
	}
	return row.Sha256, true, nil
}

// SetCatalogSnapshot records the hash of a named snapshot.
func (s *Store) SetCatalogSnapshot(ctx context.Context, tenantID, name, sha256 string) error {
	if tenantID == "" {
		return store.ErrNotFound
	}
	return s.inShared(ctx, func(tx *gorm.DB) error {
		return tx.Clauses(clause.OnConflict{
			Columns: []clause.Column{{Name: "name"}}, DoUpdates: clause.AssignmentColumns([]string{"sha256", "installed_at"}),
		}).Create(&catalogSnapshotRow{Name: name, Sha256: sha256, InstalledAt: time.Now().UTC()}).Error
	})
}

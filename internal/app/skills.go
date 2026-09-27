package app

import (
	"context"

	"github.com/mindreon/orbit-control/internal/store"
)

// Skill is one locally stored SkillHub card. IconURL is empty unless it is
// an https URL on an allowlisted host. This API does not install the skill.
type Skill struct {
	ID             string  `json:"id"`
	Slug           string  `json:"slug"`
	Handle         string  `json:"handle"`
	Name           string  `json:"name"`
	Description    string  `json:"description"`
	Category       string  `json:"category"`
	CategoryName   string  `json:"categoryName"`
	IconURL        string  `json:"iconUrl"`
	Downloads      int64   `json:"downloads"`
	Stars          int64   `json:"stars"`
	Source         string  `json:"source"`
	Version        string  `json:"version"`
	RequiresAPIKey bool    `json:"requiresApiKey"`
	Paid           bool    `json:"paid"`
	Score          float64 `json:"score"`
	UpdatedAt      string  `json:"updatedAt"`
	TrendingRank   int     `json:"trendingRank"`
}

// SkillList is a page of the local catalog. SyncedAt is empty until the
// background sync has stored at least one row.
type SkillList struct {
	Items    []Skill `json:"items"`
	Total    int     `json:"total"`
	Page     int     `json:"page"`
	PageSize int     `json:"pageSize"`
	SyncedAt string  `json:"syncedAt"`
}

// SkillCategory is a label for Skill.Category.
type SkillCategory struct {
	Key       string `json:"key"`
	Name      string `json:"name"`
	NameEn    string `json:"nameEn"`
	SortOrder int    `json:"sortOrder"`
}

// ListSkills reads the stored catalog. It does not call SkillHub.
func (a *App) ListSkills(ctx context.Context, tenantID string, q store.SkillCatalogQuery) (SkillList, error) {
	page, err := a.Repo.ListSkillCatalog(ctx, tenantID, q)
	if err != nil {
		return SkillList{}, err
	}
	items := make([]Skill, 0, len(page.Items))
	for _, rec := range page.Items {
		items = append(items, skillFrom(rec))
	}
	synced := ""
	if !page.SyncedAt.IsZero() {
		synced = stamp(page.SyncedAt)
	}
	return SkillList{
		Items:    items,
		Total:    page.Total,
		Page:     page.Page,
		PageSize: page.PageSize,
		SyncedAt: synced,
	}, nil
}

// GetSkill reads one stored skill. handle may be empty when the catalog row
// has no author. A missing row and a malformed path both look the same.
func (a *App) GetSkill(ctx context.Context, tenantID, handle, slug string) (Skill, error) {
	id, ok := skillID(handle, slug)
	if !ok {
		return Skill{}, store.ErrNotFound
	}
	rec, err := a.Repo.GetSkill(ctx, tenantID, id)
	if err != nil {
		return Skill{}, err
	}
	return skillFrom(rec), nil
}

// SkillTextFile is one saved text file. Body is for reading on the page.
type SkillTextFile struct {
	Path string `json:"path"`
	Body string `json:"body"`
}

// SkillTextFiles returns text copied from the skill package. The first call
// for a row uses fetch and stores the result. Later calls only read the copy.
func (a *App) SkillTextFiles(ctx context.Context, tenantID, handle, slug string, fetch func(context.Context, string, string) ([]store.SkillFile, error)) ([]SkillTextFile, error) {
	id, ok := skillID(handle, slug)
	if !ok {
		return nil, store.ErrNotFound
	}
	files, known, err := a.Repo.GetSkillTextFiles(ctx, tenantID, id)
	if err != nil {
		return nil, err
	}
	if !known {
		if fetch == nil {
			return nil, store.ErrNotFound
		}
		files, err = fetch(ctx, handle, slug)
		if err != nil {
			return nil, err
		}
		if err := a.Repo.SaveSkillTextFiles(ctx, tenantID, id, files); err != nil {
			return nil, err
		}
	}
	out := make([]SkillTextFile, 0, len(files))
	for _, file := range files {
		out = append(out, SkillTextFile{Path: file.Path, Body: file.Body})
	}
	return out, nil
}

func skillID(handle, slug string) (string, bool) {
	if handle == "" {
		return store.SkillSlugID(slug)
	}
	return store.SkillPathID(handle, slug)
}

func skillFrom(rec store.SkillRecord) Skill {
	return Skill{
		ID:             rec.ID,
		Slug:           rec.Slug,
		Handle:         rec.Handle,
		Name:           rec.Name,
		Description:    rec.Description,
		Category:       rec.Category,
		CategoryName:   rec.CategoryName,
		IconURL:        rec.IconURL,
		Downloads:      rec.Downloads,
		Stars:          rec.Stars,
		Source:         rec.Source,
		Version:        rec.Version,
		RequiresAPIKey: rec.RequiresAPIKey,
		Paid:           rec.Paid,
		Score:          rec.Score,
		UpdatedAt:      stamp(rec.UpdatedAt),
		TrendingRank:   rec.TrendingRank,
	}
}

// ListSkillCategories reads stored labels. It does not call SkillHub.
func (a *App) ListSkillCategories(ctx context.Context, tenantID string) ([]SkillCategory, error) {
	rows, err := a.Repo.ListSkillCategories(ctx, tenantID)
	if err != nil {
		return nil, err
	}
	out := make([]SkillCategory, 0, len(rows))
	for _, row := range rows {
		out = append(out, SkillCategory{
			Key:       row.Key,
			Name:      row.Name,
			NameEn:    row.NameEn,
			SortOrder: row.SortOrder,
		})
	}
	return out, nil
}

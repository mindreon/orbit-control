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
		items = append(items, Skill{
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
		})
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

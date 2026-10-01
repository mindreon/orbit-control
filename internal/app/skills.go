package app

import (
	"context"

	"github.com/mindreon/orbit-control/internal/store"
)

// Skill is one locally stored ModelScope skill card. This API does not
// install the skill.
type Skill struct {
	ID            string   `json:"id"`
	Handle        string   `json:"handle"`
	Slug          string   `json:"slug"`
	Name          string   `json:"name"`
	Description   string   `json:"description"`
	DescriptionEn string   `json:"descriptionEn"`
	Category      string   `json:"category"`
	CategoryName  string   `json:"categoryName"`
	Tags          []string `json:"tags"`
	License       string   `json:"license"`
	IconURL       string   `json:"iconUrl"`
	SourceURL     string   `json:"sourceUrl"`
	Downloads     int64    `json:"downloads"`
	Visits        int64    `json:"visits"`
	Likes         int64    `json:"likes"`
	UpdatedAt     string   `json:"updatedAt"`
	Source        string   `json:"source"`
}

// SkillList is a page of the local catalog. InstalledAt is empty until the
// snapshot has been stored.
type SkillList struct {
	Items       []Skill `json:"items"`
	Total       int     `json:"total"`
	Page        int     `json:"page"`
	PageSize    int     `json:"pageSize"`
	InstalledAt string  `json:"installedAt"`
}

// SkillCategory is a label for Skill.Category.
type SkillCategory struct {
	Key       string `json:"key"`
	Name      string `json:"name"`
	NameEn    string `json:"nameEn"`
	SortOrder int    `json:"sortOrder"`
}

// ListSkills reads the stored snapshot. It does not call ModelScope.
func (a *App) ListSkills(ctx context.Context, tenantID string, q store.SkillCatalogQuery) (SkillList, error) {
	page, err := a.Repo.ListSkillCatalog(ctx, tenantID, q)
	if err != nil {
		return SkillList{}, err
	}
	items := make([]Skill, 0, len(page.Items))
	for _, rec := range page.Items {
		items = append(items, skillFrom(rec))
	}
	installed := ""
	if !page.InstalledAt.IsZero() {
		installed = stamp(page.InstalledAt)
	}
	return SkillList{
		Items:       items,
		Total:       page.Total,
		Page:        page.Page,
		PageSize:    page.PageSize,
		InstalledAt: installed,
	}, nil
}

// GetSkill reads one stored skill. A missing row and a malformed path both
// look the same.
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

// SkillTextFile is one stored text file. Body is for reading on the page.
type SkillTextFile struct {
	Path string `json:"path"`
	Body string `json:"body"`
}

// SkillTextFiles returns the text files the snapshot stored beside the skill.
// Without the text sidecar the catalog has no file text and the list is empty.
func (a *App) SkillTextFiles(ctx context.Context, tenantID, handle, slug string) ([]SkillTextFile, error) {
	id, ok := skillID(handle, slug)
	if !ok {
		return nil, store.ErrNotFound
	}
	files, known, err := a.Repo.GetSkillTextFiles(ctx, tenantID, id)
	if err != nil {
		return nil, err
	}
	if !known {
		return []SkillTextFile{}, nil
	}
	out := make([]SkillTextFile, 0, len(files))
	for _, file := range files {
		out = append(out, SkillTextFile{Path: file.Path, Body: file.Body})
	}
	return out, nil
}

// SkillIcon returns the content type and bytes of one skill's card icon. A
// skill without an icon, and an icon the sidecar did not ship, both 404.
func (a *App) SkillIcon(ctx context.Context, tenantID, handle, slug string) (string, []byte, error) {
	rec, err := a.GetSkill(ctx, tenantID, handle, slug)
	if err != nil {
		return "", nil, err
	}
	return a.Repo.CatalogIcon(ctx, tenantID, rec.IconURL)
}

func skillID(handle, slug string) (string, bool) {
	return store.SkillPathID(handle, slug)
}

func skillFrom(rec store.SkillRecord) Skill {
	tags := rec.Tags
	if tags == nil {
		tags = []string{}
	}
	return Skill{
		ID:            rec.ID,
		Handle:        rec.Handle,
		Slug:          rec.Slug,
		Name:          rec.Name,
		Description:   rec.Description,
		DescriptionEn: rec.DescriptionEn,
		Category:      rec.Category,
		CategoryName:  rec.CategoryName,
		Tags:          tags,
		License:       rec.License,
		IconURL:       rec.IconURL,
		SourceURL:     rec.SourceURL,
		Downloads:     rec.Downloads,
		Visits:        rec.Visits,
		Likes:         rec.Likes,
		UpdatedAt:     stamp(rec.UpdatedAt),
		Source:        rec.Source,
	}
}

// ListSkillCategories reads stored labels. It does not call ModelScope.
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

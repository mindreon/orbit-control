package msmarket

import (
	"time"

	"github.com/mindreon/orbit-control/internal/store"
)

// skillRow is one line of skills.json.gz. Field names are the export
// contract with tools/modelscope-crawler's export-orbit.
type skillRow struct {
	ID             string   `json:"id"`
	Handle         string   `json:"handle"`
	Slug           string   `json:"slug"`
	Name           string   `json:"name"`
	Description    string   `json:"description"`
	DescriptionEn  string   `json:"descriptionEn"`
	Category       string   `json:"category"`
	CategoryName   string   `json:"categoryName"`
	CategoryNameEn string   `json:"categoryNameEn"`
	CategoryOrder  int      `json:"categoryOrder"`
	Tags           []string `json:"tags"`
	License        string   `json:"license"`
	IconURL        string   `json:"iconUrl"`
	SourceURL      string   `json:"sourceUrl"`
	Downloads      int64    `json:"downloads"`
	Visits         int64    `json:"visits"`
	Likes          int64    `json:"likes"`
	UpdatedAt      int64    `json:"updatedAt"`
	Source         string   `json:"source"`
}

type skillCategory struct {
	key       string
	name      string
	nameEn    string
	sortOrder int
}

// loadSkills reads the embedded skills snapshot and derives the category
// labels from the rows themselves (every row carries its L1 label).
func loadSkills() ([]store.SkillRecord, []store.SkillCategoryRecord, error) {
	rows, err := readJSONL[skillRow](mustEmbed("skills.json.gz"))
	if err != nil {
		return nil, nil, err
	}
	categories := map[string]skillCategory{}
	out := make([]store.SkillRecord, 0, len(rows))
	for _, row := range rows {
		id, ok := store.SkillPathID(row.Handle, row.Slug)
		if !ok || id != row.ID || row.Name == "" {
			continue
		}
		if row.Category != "" {
			name := row.CategoryName
			if name == "" {
				name = row.Category
			}
			categories[row.Category] = skillCategory{key: row.Category, name: name, nameEn: row.CategoryNameEn, sortOrder: row.CategoryOrder}
		}
		out = append(out, store.SkillRecord{
			ID: id, Handle: row.Handle, Slug: row.Slug, Name: row.Name, Description: row.Description,
			DescriptionEn: row.DescriptionEn, Category: row.Category, Tags: row.Tags, License: row.License,
			IconURL: row.IconURL, SourceURL: row.SourceURL, Downloads: row.Downloads, Visits: row.Visits,
			Likes: row.Likes, UpdatedAt: time.Unix(row.UpdatedAt, 0).UTC(), Source: source(row.Source),
		})
	}
	labels := make([]store.SkillCategoryRecord, 0, len(categories))
	for _, cat := range categories {
		labels = append(labels, store.SkillCategoryRecord{Key: cat.key, Name: cat.name, NameEn: cat.nameEn, SortOrder: cat.sortOrder})
	}
	return out, labels, nil
}

// source normalizes the snapshot's source flag.
func source(raw string) string {
	if raw == "nexa" {
		return "nexa"
	}
	return "common"
}

// skillTextRow is one line of the skills_text.json.gz sidecar.
type skillTextRow struct {
	ID    string            `json:"id"`
	Files []store.SkillFile `json:"files"`
}

func (row skillTextRow) storeFiles() []store.SkillFile {
	if row.Files == nil {
		return []store.SkillFile{}
	}
	return row.Files
}

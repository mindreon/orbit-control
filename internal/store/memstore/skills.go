package memstore

import (
	"context"
	"sort"
	"strings"
	"time"

	"github.com/mindreon/orbit-control/internal/store"
)

func (s *Store) ListSkillCatalog(_ context.Context, tenantID string, q store.SkillCatalogQuery) (store.SkillCatalogPage, error) {
	if tenantID == "" {
		return store.SkillCatalogPage{}, store.ErrNotFound
	}
	q = store.NormalizeSkillQuery(q)
	s.mu.Lock()
	defer s.mu.Unlock()
	names := map[string]string{}
	for key, cat := range s.skillCategories {
		names[key] = cat.Name
	}
	matched := make([]store.SkillRecord, 0, len(s.skills))
	var installed time.Time
	for _, rec := range s.skills {
		if rec.InstalledAt.After(installed) {
			installed = rec.InstalledAt
		}
		if !skillVisible(rec, q) {
			continue
		}
		copyRec := rec
		copyRec.CategoryName = names[rec.Category]
		matched = append(matched, copyRec)
	}
	sort.Slice(matched, func(i, j int) bool {
		return skillLess(matched[i], matched[j], q.Sort)
	})
	start := (q.Page - 1) * q.PageSize
	if start > len(matched) {
		start = len(matched)
	}
	end := start + q.PageSize
	if end > len(matched) {
		end = len(matched)
	}
	items := append([]store.SkillRecord(nil), matched[start:end]...)
	if items == nil {
		items = []store.SkillRecord{}
	}
	return store.SkillCatalogPage{
		Items:       items,
		Total:       len(matched),
		Page:        q.Page,
		PageSize:    q.PageSize,
		InstalledAt: installed,
	}, nil
}

func (s *Store) GetSkill(_ context.Context, tenantID, id string) (store.SkillRecord, error) {
	if tenantID == "" || id == "" {
		return store.SkillRecord{}, store.ErrNotFound
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	rec, ok := s.skills[id]
	if !ok {
		return store.SkillRecord{}, store.ErrNotFound
	}
	if cat, found := s.skillCategories[rec.Category]; found {
		rec.CategoryName = cat.Name
	}
	return rec, nil
}

func (s *Store) GetSkillTextFiles(_ context.Context, tenantID, id string) ([]store.SkillFile, bool, error) {
	if tenantID == "" || id == "" {
		return nil, false, store.ErrNotFound
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	rec, ok := s.skills[id]
	if !ok {
		return nil, false, store.ErrNotFound
	}
	if !rec.FilesKnown {
		return nil, false, nil
	}
	files := append([]store.SkillFile(nil), rec.TextFiles...)
	if files == nil {
		files = []store.SkillFile{}
	}
	return files, true, nil
}

// SaveSkillTextFilesBatch fills text for known skills; unknown ids are ignored.
func (s *Store) SaveSkillTextFilesBatch(_ context.Context, tenantID string, rows []store.SkillTextFilesRow) error {
	if tenantID == "" {
		return store.ErrNotFound
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, row := range rows {
		rec, ok := s.skills[row.ID]
		if !ok {
			continue
		}
		rec.TextFiles = append([]store.SkillFile(nil), row.Files...)
		rec.FilesKnown = true
		s.skills[row.ID] = rec
	}
	return nil
}

// ReplaceSkills swaps the stored skill snapshot for the given one.
func (s *Store) ReplaceSkills(_ context.Context, tenantID string, rows []store.SkillRecord, categories []store.SkillCategoryRecord) error {
	if tenantID == "" {
		return store.ErrNotFound
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	now := time.Now().UTC()
	s.skills = map[string]store.SkillRecord{}
	for _, row := range rows {
		if row.ID == "" {
			continue
		}
		if row.InstalledAt.IsZero() {
			row.InstalledAt = now
		}
		s.skills[row.ID] = row
	}
	for _, row := range categories {
		if row.Key == "" {
			continue
		}
		s.skillCategories[row.Key] = row
	}
	return nil
}

func (s *Store) ListSkillCategories(_ context.Context, tenantID string) ([]store.SkillCategoryRecord, error) {
	if tenantID == "" {
		return nil, store.ErrNotFound
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]store.SkillCategoryRecord, 0, len(s.skillCategories))
	for _, rec := range s.skillCategories {
		out = append(out, rec)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].SortOrder == out[j].SortOrder {
			return out[i].Key < out[j].Key
		}
		return out[i].SortOrder < out[j].SortOrder
	})
	return out, nil
}

func skillVisible(rec store.SkillRecord, q store.SkillCatalogQuery) bool {
	if q.Category != "" && rec.Category != q.Category {
		return false
	}
	if q.Source != "" && rec.Source != q.Source {
		return false
	}
	if q.Keyword != "" {
		needle := strings.ToLower(q.Keyword)
		if !strings.Contains(strings.ToLower(rec.Name), needle) &&
			!strings.Contains(strings.ToLower(rec.Description), needle) &&
			!strings.Contains(strings.ToLower(rec.DescriptionEn), needle) {
			return false
		}
	}
	return true
}

func skillLess(a, b store.SkillRecord, sortBy string) bool {
	switch sortBy {
	case "updated_at":
		if !a.UpdatedAt.Equal(b.UpdatedAt) {
			return a.UpdatedAt.After(b.UpdatedAt)
		}
	case "likes":
		if a.Likes != b.Likes {
			return a.Likes > b.Likes
		}
	default:
		if a.Downloads != b.Downloads {
			return a.Downloads > b.Downloads
		}
	}
	return a.ID < b.ID
}

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
	var synced time.Time
	for _, rec := range s.skills {
		if rec.SyncedAt.After(synced) {
			synced = rec.SyncedAt
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
		Items:    items,
		Total:    len(matched),
		Page:     q.Page,
		PageSize: q.PageSize,
		SyncedAt: synced,
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

func (s *Store) SaveSkillTextFiles(_ context.Context, tenantID, id string, files []store.SkillFile) error {
	if tenantID == "" || id == "" {
		return store.ErrNotFound
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	rec, ok := s.skills[id]
	if !ok {
		return store.ErrNotFound
	}
	if files == nil {
		files = []store.SkillFile{}
	}
	rec.TextFiles = append([]store.SkillFile(nil), files...)
	rec.FilesKnown = true
	s.skills[id] = rec
	return nil
}

func (s *Store) UpsertSkillCatalog(_ context.Context, tenantID string, rows []store.SkillRecord) error {
	if tenantID == "" {
		return store.ErrNotFound
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.ensureSkills()
	now := time.Now().UTC()
	for _, row := range rows {
		if row.ID == "" {
			continue
		}
		if prev, ok := s.skills[row.ID]; ok {
			row.TrendingRank = prev.TrendingRank
		}
		s.keepSkillFiles(&row)
		if row.SyncedAt.IsZero() {
			row.SyncedAt = now
		}
		s.skills[row.ID] = row
	}
	return nil
}

// keepSkillFiles moves a slug-only row onto handle/slug and keeps text that
// was already copied. The caller holds s.mu.
func (s *Store) keepSkillFiles(row *store.SkillRecord) {
	if row.Handle != "" && row.Slug != "" && row.ID == row.Handle+"/"+row.Slug {
		if prev, ok := s.skills[row.Slug]; ok && prev.Handle == "" && prev.Slug == row.Slug {
			if _, exists := s.skills[row.ID]; !exists {
				delete(s.skills, row.Slug)
				row.TextFiles = prev.TextFiles
				row.FilesKnown = prev.FilesKnown
			}
		}
	}
	if prev, ok := s.skills[row.ID]; ok && prev.FilesKnown {
		row.TextFiles = prev.TextFiles
		row.FilesKnown = true
	}
}

func (s *Store) ReplaceSkillTrending(_ context.Context, tenantID string, rows []store.SkillRecord) error {
	if tenantID == "" {
		return store.ErrNotFound
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.ensureSkills()
	for id, rec := range s.skills {
		rec.TrendingRank = 0
		s.skills[id] = rec
	}
	now := time.Now().UTC()
	for _, row := range rows {
		if row.ID == "" {
			continue
		}
		s.keepSkillFiles(&row)
		if row.SyncedAt.IsZero() {
			row.SyncedAt = now
		}
		s.skills[row.ID] = row
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

func (s *Store) UpsertSkillCategories(_ context.Context, tenantID string, rows []store.SkillCategoryRecord) error {
	if tenantID == "" {
		return store.ErrNotFound
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.ensureSkills()
	for _, row := range rows {
		if row.Key == "" {
			continue
		}
		s.skillCategories[row.Key] = row
	}
	return nil
}

func (s *Store) ensureSkills() {
	if s.skills == nil {
		s.skills = map[string]store.SkillRecord{}
	}
	if s.skillCategories == nil {
		s.skillCategories = map[string]store.SkillCategoryRecord{}
	}
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
		if !strings.Contains(strings.ToLower(rec.Name), needle) && !strings.Contains(strings.ToLower(rec.Description), needle) {
			return false
		}
	}
	if q.RequiresAPIKey == "true" && !rec.RequiresAPIKey {
		return false
	}
	if q.RequiresAPIKey == "false" && rec.RequiresAPIKey {
		return false
	}
	if q.Paid == "true" && !rec.Paid {
		return false
	}
	if q.Paid == "false" && rec.Paid {
		return false
	}
	if q.Sort == "trending" && rec.TrendingRank <= 0 {
		return false
	}
	return true
}

func skillLess(a, b store.SkillRecord, sortBy string) bool {
	switch sortBy {
	case "downloads":
		if a.Downloads != b.Downloads {
			return a.Downloads > b.Downloads
		}
	case "updated_at":
		if !a.UpdatedAt.Equal(b.UpdatedAt) {
			return a.UpdatedAt.After(b.UpdatedAt)
		}
	case "stars":
		if a.Stars != b.Stars {
			return a.Stars > b.Stars
		}
	case "trending":
		if a.TrendingRank != b.TrendingRank {
			return a.TrendingRank < b.TrendingRank
		}
	default:
		if a.Score != b.Score {
			return a.Score > b.Score
		}
		if a.Downloads != b.Downloads {
			return a.Downloads > b.Downloads
		}
		if a.Stars != b.Stars {
			return a.Stars > b.Stars
		}
	}
	return a.ID < b.ID
}

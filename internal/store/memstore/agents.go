package memstore

import (
	"context"
	"sort"
	"strings"

	"github.com/mindreon/orbit-control/internal/store"
)

func (s *Store) ListAgentCatalog(_ context.Context, tenantID string, q store.AgentCatalogQuery) (store.AgentCatalogPage, error) {
	if tenantID == "" {
		return store.AgentCatalogPage{}, store.ErrNotFound
	}
	q = store.NormalizeAgentCatalogQuery(q)
	s.mu.Lock()
	defer s.mu.Unlock()
	matched := make([]store.AgentRecord, 0, len(s.agents))
	for _, rec := range s.agents {
		if !agentVisible(rec, q) {
			continue
		}
		matched = append(matched, rec)
	}
	sort.Slice(matched, func(i, j int) bool {
		return agentLess(matched[i], matched[j], q.Sort)
	})
	start := (q.Page - 1) * q.PageSize
	if start > len(matched) {
		start = len(matched)
	}
	end := start + q.PageSize
	if end > len(matched) {
		end = len(matched)
	}
	items := append([]store.AgentRecord(nil), matched[start:end]...)
	if items == nil {
		items = []store.AgentRecord{}
	}
	return store.AgentCatalogPage{
		Items: items, Total: len(matched), Page: q.Page, PageSize: q.PageSize,
	}, nil
}

func (s *Store) GetAgent(_ context.Context, tenantID, id string) (store.AgentRecord, error) {
	if tenantID == "" || id == "" {
		return store.AgentRecord{}, store.ErrNotFound
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	rec, ok := s.agents[id]
	if !ok {
		return store.AgentRecord{}, store.ErrNotFound
	}
	return rec, nil
}

// ReplaceAgents swaps the stored agent snapshot for the given one.
func (s *Store) ReplaceAgents(_ context.Context, tenantID string, rows []store.AgentRecord) error {
	if tenantID == "" {
		return store.ErrNotFound
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.agents = map[string]store.AgentRecord{}
	for _, row := range rows {
		if row.ID == "" {
			continue
		}
		s.agents[row.ID] = row
	}
	return nil
}

func agentVisible(rec store.AgentRecord, q store.AgentCatalogQuery) bool {
	if q.Catalogue != "" {
		found := false
		for _, key := range rec.Catalogues {
			if key == q.Catalogue {
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}
	if q.Keyword != "" {
		needle := strings.ToLower(q.Keyword)
		if !strings.Contains(strings.ToLower(rec.Name), needle) &&
			!strings.Contains(strings.ToLower(rec.Description), needle) &&
			!strings.Contains(strings.ToLower(rec.Readme), needle) {
			return false
		}
	}
	return true
}

func agentLess(a, b store.AgentRecord, sortBy string) bool {
	switch sortBy {
	case "updated_at":
		if !a.UpdatedAt.Equal(b.UpdatedAt) {
			return a.UpdatedAt.After(b.UpdatedAt)
		}
	case "stars":
		if a.Stars != b.Stars {
			return a.Stars > b.Stars
		}
	default:
		if a.Downloads != b.Downloads {
			return a.Downloads > b.Downloads
		}
	}
	return a.ID < b.ID
}

// CatalogSnapshot reads the stored hash of a named snapshot.
func (s *Store) CatalogSnapshot(_ context.Context, tenantID, name string) (string, bool, error) {
	if tenantID == "" {
		return "", false, store.ErrNotFound
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	sha, ok := s.snapshots[name]
	return sha, ok, nil
}

// SetCatalogSnapshot records the hash of a named snapshot.
func (s *Store) SetCatalogSnapshot(_ context.Context, tenantID, name, sha string) error {
	if tenantID == "" {
		return store.ErrNotFound
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.snapshots[name] = sha
	return nil
}

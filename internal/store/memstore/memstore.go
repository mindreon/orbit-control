// Package memstore is the in-process Repository used by tests and local dev without ORBIT_CONTROL_DB_URL. It holds
// the catalog (assistants, connectors, skills, MCP market); tasks live in the task projection, not here.
package memstore

import (
	"context"
	"sort"
	"sync"

	"github.com/mindreon/orbit-control/internal/store"
)

type personaRow struct {
	tenantID string
	rec      store.PersonaRecord
}

type connectorRow struct {
	tenantID string
	rec      store.McpConnectorRecord
}

type Store struct {
	mu              sync.Mutex
	personas        map[string]personaRow
	connectors      map[string]connectorRow
	skills          map[string]store.SkillRecord
	skillCategories map[string]store.SkillCategoryRecord
	mcpMarket       map[string]store.McpMarketRecord
	mcpCategories   map[string]store.McpMarketCategoryRecord
	mcpDetails      map[string]store.McpMarketDetailRecord
}

var _ store.Repository = (*Store)(nil)

func New() *Store {
	return &Store{
		personas:        map[string]personaRow{},
		connectors:      map[string]connectorRow{},
		skills:          map[string]store.SkillRecord{},
		skillCategories: map[string]store.SkillCategoryRecord{},
		mcpMarket:       map[string]store.McpMarketRecord{},
		mcpCategories:   map[string]store.McpMarketCategoryRecord{},
		mcpDetails:      map[string]store.McpMarketDetailRecord{},
	}
}

func (s *Store) Close() {}

func (s *Store) CheckTenant(_ context.Context, tenantID string) error {
	if tenantID == "" {
		return store.ErrNotFound
	}
	return nil
}

func copyStrings(in []string) []string {
	if len(in) == 0 {
		return []string{}
	}
	return append([]string(nil), in...)
}

func (s *Store) ListPersonas(_ context.Context, tenantID string) ([]store.PersonaRecord, error) {
	if tenantID == "" {
		return nil, store.ErrNotFound
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	out := []store.PersonaRecord{}
	for _, row := range s.personas {
		if row.tenantID != tenantID {
			continue
		}
		rec := row.rec
		rec.McpConnectorIDs = copyStrings(row.rec.McpConnectorIDs)
		out = append(out, rec)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].CreatedAt.Equal(out[j].CreatedAt) {
			return out[i].ID < out[j].ID
		}
		return out[i].CreatedAt.After(out[j].CreatedAt)
	})
	return out, nil
}

func (s *Store) CreatePersona(_ context.Context, tenantID string, p store.PersonaRecord) error {
	if tenantID == "" || p.ID == "" {
		return store.ErrNotFound
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.personas[p.ID]; ok {
		return store.ErrStorage
	}
	p.McpConnectorIDs = copyStrings(p.McpConnectorIDs)
	s.personas[p.ID] = personaRow{tenantID: tenantID, rec: p}
	return nil
}

func (s *Store) ListMcpConnectors(_ context.Context, tenantID string) ([]store.McpConnectorRecord, error) {
	if tenantID == "" {
		return nil, store.ErrNotFound
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	out := []store.McpConnectorRecord{}
	for _, row := range s.connectors {
		if row.tenantID != tenantID {
			continue
		}
		rec := row.rec
		rec.Args = copyStrings(row.rec.Args)
		rec.EnvRefs = copyStrings(row.rec.EnvRefs)
		rec.HeaderRefs = copyStrings(row.rec.HeaderRefs)
		out = append(out, rec)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].CreatedAt.Equal(out[j].CreatedAt) {
			return out[i].ID < out[j].ID
		}
		return out[i].CreatedAt.After(out[j].CreatedAt)
	})
	return out, nil
}

func (s *Store) CreateMcpConnector(_ context.Context, tenantID string, c store.McpConnectorRecord) error {
	if tenantID == "" || c.ID == "" {
		return store.ErrNotFound
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.connectors[c.ID]; ok {
		return store.ErrStorage
	}
	c.Args = copyStrings(c.Args)
	c.EnvRefs = copyStrings(c.EnvRefs)
	c.HeaderRefs = copyStrings(c.HeaderRefs)
	if c.Transport == "" {
		c.Transport = "stdio"
	}
	s.connectors[c.ID] = connectorRow{tenantID: tenantID, rec: c}
	return nil
}

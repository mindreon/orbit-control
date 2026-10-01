package memstore

import (
	"context"

	"github.com/mindreon/orbit-control/internal/store"
)

func (s *Store) ReplaceCatalogIcons(_ context.Context, tenantID string, rows []store.CatalogIcon) error {
	if tenantID == "" {
		return store.ErrNotFound
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.icons == nil {
		s.icons = map[string]store.CatalogIcon{}
	}
	for _, row := range rows {
		if row.URL == "" || len(row.Data) == 0 {
			continue
		}
		s.icons[row.URL] = row
	}
	return nil
}

func (s *Store) CatalogIcon(_ context.Context, tenantID, url string) (string, []byte, error) {
	if tenantID == "" || url == "" {
		return "", nil, store.ErrNotFound
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	row, ok := s.icons[url]
	if !ok {
		return "", nil, store.ErrNotFound
	}
	return row.ContentType, row.Data, nil
}

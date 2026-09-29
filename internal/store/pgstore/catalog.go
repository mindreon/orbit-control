package pgstore

import (
	"context"
	"time"

	"github.com/lib/pq"
	"gorm.io/gorm"

	"github.com/mindreon/orbit-control/internal/store"
)

type personaRow struct {
	ID              string `gorm:"primaryKey"`
	TenantID        string
	Name            string
	Instructions    string
	McpConnectorIDs pq.StringArray `gorm:"type:text[]"`
	CreatedAt       time.Time      `gorm:"autoCreateTime:false"`
}

func (personaRow) TableName() string { return "personas" }

type mcpConnectorRow struct {
	ID          string `gorm:"primaryKey"`
	TenantID    string
	Name        string
	Transport   string
	Command     string
	Args        pq.StringArray `gorm:"type:text[]"`
	EnvRefs     pq.StringArray `gorm:"type:text[]"`
	URL         string
	HeaderRefs  pq.StringArray `gorm:"type:text[]"`
	DefaultOpen bool
	CreatedAt   time.Time `gorm:"autoCreateTime:false"`
}

func (mcpConnectorRow) TableName() string { return "mcp_connectors" }

// stored is a slice for writing: a missing one is the empty array, since the columns are NOT NULL.
func stored(items []string) pq.StringArray {
	if items == nil {
		return pq.StringArray{}
	}
	return items
}

// list is a stored array as a slice, never nil (the API renders [] rather than null).
func list(items pq.StringArray) []string {
	if items == nil {
		return []string{}
	}
	return items
}

func (r personaRow) record() store.PersonaRecord {
	return store.PersonaRecord{ID: r.ID, Name: r.Name, Instructions: r.Instructions, McpConnectorIDs: list(r.McpConnectorIDs), CreatedAt: r.CreatedAt}
}

func (r mcpConnectorRow) record() store.McpConnectorRecord {
	return store.McpConnectorRecord{
		ID: r.ID, Name: r.Name, Transport: r.Transport, Command: r.Command, Args: list(r.Args), EnvRefs: list(r.EnvRefs),
		URL: r.URL, HeaderRefs: list(r.HeaderRefs), DefaultOpen: r.DefaultOpen, CreatedAt: r.CreatedAt,
	}
}

func (s *Store) ListPersonas(ctx context.Context, tenantID string) ([]store.PersonaRecord, error) {
	var rows []personaRow
	err := s.inTenant(ctx, tenantID, func(tx *gorm.DB) error {
		return tx.Where("tenant_id = ?", tenantID).Order("created_at DESC, id").Find(&rows).Error
	})
	if err != nil {
		return nil, storageErr("list personas", err)
	}
	out := make([]store.PersonaRecord, 0, len(rows))
	for _, row := range rows {
		out = append(out, row.record())
	}
	return out, nil
}

func (s *Store) CreatePersona(ctx context.Context, tenantID string, p store.PersonaRecord) error {
	if p.ID == "" {
		return store.ErrNotFound
	}
	row := personaRow{ID: p.ID, TenantID: tenantID, Name: p.Name, Instructions: p.Instructions, McpConnectorIDs: stored(p.McpConnectorIDs), CreatedAt: p.CreatedAt}
	if err := s.inTenant(ctx, tenantID, func(tx *gorm.DB) error { return tx.Create(&row).Error }); err != nil {
		return storageErr("insert persona", err)
	}
	return nil
}

func (s *Store) ListMcpConnectors(ctx context.Context, tenantID string) ([]store.McpConnectorRecord, error) {
	var rows []mcpConnectorRow
	err := s.inTenant(ctx, tenantID, func(tx *gorm.DB) error {
		return tx.Where("tenant_id = ?", tenantID).Order("created_at DESC, id").Find(&rows).Error
	})
	if err != nil {
		return nil, storageErr("list mcp connectors", err)
	}
	out := make([]store.McpConnectorRecord, 0, len(rows))
	for _, row := range rows {
		out = append(out, row.record())
	}
	return out, nil
}

func (s *Store) CreateMcpConnector(ctx context.Context, tenantID string, c store.McpConnectorRecord) error {
	if c.ID == "" {
		return store.ErrNotFound
	}
	if c.Transport == "" {
		c.Transport = "stdio"
	}
	row := mcpConnectorRow{
		ID: c.ID, TenantID: tenantID, Name: c.Name, Transport: c.Transport, Command: c.Command, Args: stored(c.Args), EnvRefs: stored(c.EnvRefs),
		URL: c.URL, HeaderRefs: stored(c.HeaderRefs), DefaultOpen: c.DefaultOpen, CreatedAt: c.CreatedAt,
	}
	if err := s.inTenant(ctx, tenantID, func(tx *gorm.DB) error { return tx.Create(&row).Error }); err != nil {
		return storageErr("insert mcp connector", err)
	}
	return nil
}

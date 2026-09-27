package pgstore

import (
	"context"

	"github.com/jackc/pgx/v5"

	"github.com/mindreon/orbit-control/internal/store"
)

func scanPersona(row pgx.Row) (store.PersonaRecord, error) {
	var rec store.PersonaRecord
	var ids []string
	err := row.Scan(&rec.ID, &rec.Name, &rec.Instructions, &ids, &rec.CreatedAt)
	if ids == nil {
		ids = []string{}
	}
	rec.McpConnectorIDs = ids
	return rec, err
}

func scanConnector(row pgx.Row) (store.McpConnectorRecord, error) {
	var rec store.McpConnectorRecord
	var args, refs, headers []string
	err := row.Scan(
		&rec.ID, &rec.Name, &rec.Transport, &rec.Command, &args, &refs,
		&rec.URL, &headers, &rec.DefaultOpen, &rec.CreatedAt,
	)
	if args == nil {
		args = []string{}
	}
	if refs == nil {
		refs = []string{}
	}
	if headers == nil {
		headers = []string{}
	}
	rec.Args = args
	rec.EnvRefs = refs
	rec.HeaderRefs = headers
	return rec, err
}

func (s *Store) ListPersonas(ctx context.Context, tenantID string) ([]store.PersonaRecord, error) {
	out := []store.PersonaRecord{}
	err := s.inTenantTx(ctx, tenantID, func(tx pgx.Tx) error {
		rows, err := tx.Query(ctx, `
			SELECT id, name, instructions, mcp_connector_ids, created_at
			  FROM personas
			 WHERE tenant_id = $1
			 ORDER BY created_at DESC, id`,
			tenantID)
		if err != nil {
			return storageErr("list personas", err)
		}
		defer rows.Close()
		for rows.Next() {
			rec, err := scanPersona(rows)
			if err != nil {
				return storageErr("scan persona", err)
			}
			out = append(out, rec)
		}
		if err := rows.Err(); err != nil {
			return storageErr("list personas", err)
		}
		return nil
	})
	return out, err
}

func (s *Store) CreatePersona(ctx context.Context, tenantID string, p store.PersonaRecord) error {
	if p.ID == "" {
		return store.ErrNotFound
	}
	ids := p.McpConnectorIDs
	if ids == nil {
		ids = []string{}
	}
	return s.inTenantTx(ctx, tenantID, func(tx pgx.Tx) error {
		_, err := tx.Exec(ctx, `
			INSERT INTO personas (id, tenant_id, name, instructions, mcp_connector_ids, created_at)
			VALUES ($1, $2, $3, $4, $5, $6)`,
			p.ID, tenantID, p.Name, p.Instructions, ids, p.CreatedAt)
		if err != nil {
			return storageErr("insert persona", err)
		}
		return nil
	})
}

func (s *Store) ListMcpConnectors(ctx context.Context, tenantID string) ([]store.McpConnectorRecord, error) {
	out := []store.McpConnectorRecord{}
	err := s.inTenantTx(ctx, tenantID, func(tx pgx.Tx) error {
		rows, err := tx.Query(ctx, `
			SELECT id, name, transport, command, args, env_refs, url, header_refs, default_open, created_at
			  FROM mcp_connectors
			 WHERE tenant_id = $1
			 ORDER BY created_at DESC, id`,
			tenantID)
		if err != nil {
			return storageErr("list mcp connectors", err)
		}
		defer rows.Close()
		for rows.Next() {
			rec, err := scanConnector(rows)
			if err != nil {
				return storageErr("scan mcp connector", err)
			}
			out = append(out, rec)
		}
		if err := rows.Err(); err != nil {
			return storageErr("list mcp connectors", err)
		}
		return nil
	})
	return out, err
}

func (s *Store) CreateMcpConnector(ctx context.Context, tenantID string, c store.McpConnectorRecord) error {
	if c.ID == "" {
		return store.ErrNotFound
	}
	args := c.Args
	if args == nil {
		args = []string{}
	}
	refs := c.EnvRefs
	if refs == nil {
		refs = []string{}
	}
	headers := c.HeaderRefs
	if headers == nil {
		headers = []string{}
	}
	transport := c.Transport
	if transport == "" {
		transport = "stdio"
	}
	return s.inTenantTx(ctx, tenantID, func(tx pgx.Tx) error {
		_, err := tx.Exec(ctx, `
			INSERT INTO mcp_connectors (
			  id, tenant_id, name, transport, command, args, env_refs, url, header_refs, default_open, created_at
			) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)`,
			c.ID, tenantID, c.Name, transport, c.Command, args, refs, c.URL, headers, c.DefaultOpen, c.CreatedAt)
		if err != nil {
			return storageErr("insert mcp connector", err)
		}
		return nil
	})
}

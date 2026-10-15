package assets

import (
	"context"
	"encoding/json/jsontext"
	"errors"

	"github.com/mindreon/orbit-control/internal/platform/db"
	"github.com/mindreon/orbit-control/internal/platform/httpx"
	"github.com/mindreon/orbit-control/internal/policy"
)

type Page[T any] struct {
	Items      []T    `json:"items"`
	NextCursor string `json:"next_cursor"`
}
type Filter struct{ Kind, Visibility, Query, WorkspaceID string }

func (s *Service) List(ctx context.Context, page httpx.Page, f Filter) (Page[Asset], error) {
	result := []Asset{}
	cursor := page.Cursor
	for len(result) <= page.Limit {
		rows, e := db.Tx(ctx).
			Query(ctx, "SELECT id::text FROM assets WHERE ($1='' OR id>NULLIF($1,'')::uuid) AND ($2='' OR "+
				"kind=$2) AND ($3='' OR visibility=$3) AND ($4='' OR name ILIKE '%'||$4||'%') AND ($5='' "+
				"OR workspace_id=NULLIF($5,'')::uuid) ORDER BY id LIMIT 100", cursor, f.Kind, f.Visibility, f.Query, f.WorkspaceID)
		if e != nil {
			return Page[Asset]{}, e
		}
		batch := []string{}
		for rows.Next() {
			var id string
			if e = rows.Scan(&id); e != nil {
				rows.Close()
				return Page[Asset]{}, e
			}
			batch = append(batch, id)
		}
		e = rows.Err()
		rows.Close()
		if e != nil {
			return Page[Asset]{}, e
		}
		if len(batch) == 0 {
			break
		}
		for _, id := range batch {
			cursor = id
			a, e := s.Get(ctx, id, "asset.read")
			if status, ok := errors.AsType[*httpx.Error](e); ok && status.Status == 404 {
				continue
			}
			if e != nil {
				return Page[Asset]{}, e
			}
			result = append(result, a)
			if len(result) > page.Limit {
				break
			}
		}
	}
	next := ""
	if len(result) > page.Limit {
		result = result[:page.Limit]
		next = httpx.Cursor(result[len(result)-1].ID)
	}
	return Page[Asset]{Items: result, NextCursor: next}, nil
}
func (s *Service) Versions(ctx context.Context, p httpx.Page, value string, review bool) (Page[Version], error) {
	filter := "asset_id=$1"
	if review {
		filter = "state='in_review' AND tenant_id=$1"
	}
	items := []Version{}
	cursor := p.Cursor
	for len(items) <= p.Limit {
		rows, e := db.Tx(ctx).
			Query(ctx,
				`SELECT id::text,asset_id::text FROM asset_versions WHERE `+
					filter+
					(" AND ($2='' OR id>NULLIF($2,'')::uuid) ORDER BY id LIMIT 100"),
				value,
				cursor)
		if e != nil {
			return Page[Version]{}, e
		}
		batch := [][2]string{}
		for rows.Next() {
			var v [2]string
			if e = rows.Scan(&v[0], &v[1]); e != nil {
				rows.Close()
				return Page[Version]{}, e
			}
			batch = append(batch, v)
		}
		e = rows.Err()
		rows.Close()
		if e != nil {
			return Page[Version]{}, e
		}
		if len(batch) == 0 {
			break
		}
		for _, row := range batch {
			cursor = row[0]
			if review {
				if _, e = s.Get(ctx, row[1], "asset.review"); e != nil {
					if v, ok := errors.AsType[*httpx.Error](e); ok && (v.Status == 403 || v.Status == 404) {
						continue
					}
					return Page[Version]{}, e
				}
			}
			v, e := s.Version(ctx, row[1], row[0])
			if e != nil {
				return Page[Version]{}, e
			}
			items = append(items, v)
			if len(items) > p.Limit {
				break
			}
		}
	}
	next := ""
	if len(items) > p.Limit {
		items = items[:p.Limit]
		next = httpx.Cursor(items[len(items)-1].ID)
	}
	return Page[Version]{Items: items, NextCursor: next}, nil
}
func jsonPage(ctx context.Context, p httpx.Page, query string, arg any) (Page[jsontext.Value], error) {
	rows, err := db.Tx(ctx).Query(ctx, query, arg, p.Cursor, p.Limit+1)
	if err != nil {
		return Page[jsontext.Value]{}, err
	}
	defer rows.Close()
	items := []jsontext.Value{}
	keys := []string{}
	for rows.Next() {
		var id string
		var raw []byte
		if err = rows.Scan(&id, &raw); err != nil {
			return Page[jsontext.Value]{}, err
		}
		keys = append(keys, id)
		items = append(items, jsontext.Value(raw))
	}
	if err = rows.Err(); err != nil {
		return Page[jsontext.Value]{}, err
	}
	next := ""
	if len(items) > p.Limit {
		items = items[:p.Limit]
		next = httpx.Cursor(keys[p.Limit-1])
	}
	return Page[jsontext.Value]{Items: items, NextCursor: next}, nil
}
func (s *Service) Checks(ctx context.Context, p httpx.Page, id string) (Page[jsontext.Value], error) {
	return jsonPage(
		ctx,
		p,
		"SELECT id::text,jsonb_build_object('id',id,'report',findings,'status',status) FROM "+
			"asset_check_reports WHERE version_id=$1 AND ($2='' OR id>NULLIF($2,'')::uuid) ORDER BY id "+
			"LIMIT $3",
		id,
	)
}

type AssetChannel struct {
	Name    string `json:"channel"`
	Version string `json:"version_id"`
}

func (s *Service) Channels(ctx context.Context, id string) ([]AssetChannel, error) {
	rows, err := db.Tx(ctx).
		Query(ctx, `SELECT channel,version_id::text FROM asset_channels WHERE asset_id=$1 ORDER BY channel`, id)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	items := []AssetChannel{}
	for rows.Next() {
		var c AssetChannel
		if err = rows.Scan(&c.Name, &c.Version); err != nil {
			return nil, err
		}
		items = append(items, c)
	}
	if err = rows.Err(); err != nil {
		return nil, err
	}
	return items, nil
}
func (s *Service) Grants(ctx context.Context, id string) ([]Grant, error) {
	rows, err := db.Tx(ctx).
		Query(ctx, "SELECT grantee_type,grantee_id::text,permission FROM asset_grants WHERE asset_id=$1 ORDER "+
			"BY grantee_type,grantee_id,permission", id)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	items := []Grant{}
	for rows.Next() {
		var g Grant
		if err = rows.Scan(&g.Type, &g.ID, &g.Permission); err != nil {
			return nil, err
		}
		items = append(items, g)
	}
	if err = rows.Err(); err != nil {
		return nil, err
	}
	return items, nil
}
func (s *Service) Installs(ctx context.Context, p httpx.Page, scope, id string) (Page[jsontext.Value], error) {
	action := "tenant.read"
	if scope == "workspace" {
		action = "workspace.read"
	}
	if err := s.Authz.Require(ctx, action, policy.Resource{Type: scope, ID: id}); err != nil {
		return Page[jsontext.Value]{}, err
	}
	return jsonPage(
		ctx,
		p,
		"SELECT id::text,to_jsonb(i)-'tenant_id' FROM asset_installs i WHERE scope_id=$1 AND "+
			"scope_type='"+scope+`' AND ($2='' OR id>NULLIF($2,'')::uuid) ORDER BY id LIMIT $3`,
		id,
	)
}

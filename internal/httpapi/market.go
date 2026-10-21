package httpapi

import (
	"errors"
	"net/http"
	"strconv"

	"github.com/jackc/pgx/v5"

	"github.com/mindreon/orbit-control/internal/auth"
	"github.com/mindreon/orbit-control/internal/market"
	"github.com/mindreon/orbit-control/internal/platform/db"
	"github.com/mindreon/orbit-control/internal/platform/httpx"
)

func (h *Handler) marketSkills(w http.ResponseWriter, r *http.Request) error {
	page, err := market.ListSkills(r.URL.Query())
	if err != nil {
		return err
	}
	httpx.JSON(w, 200, page)
	return nil
}

func (h *Handler) marketSkill(w http.ResponseWriter, r *http.Request) error {
	item, ok, err := market.GetSkill(r.PathValue("handle"), r.PathValue("slug"))
	if err != nil {
		return err
	}
	if !ok {
		return httpx.E(404, "NOT_FOUND", "Skill not found")
	}
	httpx.JSON(w, 200, item)
	return nil
}

func (h *Handler) marketSkillFiles(w http.ResponseWriter, r *http.Request) error {
	_, ok, err := market.GetSkill(r.PathValue("handle"), r.PathValue("slug"))
	if err != nil {
		return err
	}
	if !ok {
		return httpx.E(404, "NOT_FOUND", "Skill not found")
	}
	files, err := market.SkillFiles(r.PathValue("handle"), r.PathValue("slug"))
	if err != nil {
		return err
	}
	httpx.JSON(w, 200, map[string]any{"items": files})
	return nil
}

func (h *Handler) marketAgents(w http.ResponseWriter, r *http.Request) error {
	page, err := market.ListAgents(r.URL.Query())
	if err != nil {
		return err
	}
	httpx.JSON(w, 200, page)
	return nil
}

func (h *Handler) marketAgent(w http.ResponseWriter, r *http.Request) error {
	item, ok, err := market.GetAgent(r.PathValue("handle"), r.PathValue("slug"))
	if err != nil {
		return err
	}
	if !ok {
		return httpx.E(404, "NOT_FOUND", "Agent not found")
	}
	httpx.JSON(w, 200, item)
	return nil
}

func (h *Handler) marketRunnableAgents(w http.ResponseWriter, r *http.Request) error {
	q := r.URL.Query()
	limit := 24
	if raw := q.Get("limit"); raw != "" {
		n, err := strconv.Atoi(raw)
		if err != nil || n < 1 || n > 60 {
			return httpx.E(422, "INVALID_INPUT", "Invalid limit")
		}
		limit = n
	}
	items := []market.AgentCard{}
	for page := 1; page <= 30 && len(items) < limit; page++ {
		q.Set("page", strconv.Itoa(page))
		q.Set("limit", "60")
		listed, err := market.ListAgents(q)
		if err != nil {
			return err
		}
		if len(listed.Items) == 0 {
			break
		}
		names := make([]string, len(listed.Items))
		for i, item := range listed.Items {
			names[i] = market.AssetName(item.ID, item.Slug, item.Name)
		}
		rows, err := db.Tx(r.Context()).Query(r.Context(), `SELECT a.name
 FROM assets a
 JOIN asset_channels c ON c.tenant_id=a.tenant_id AND c.asset_id=a.id AND c.channel='stable'
 WHERE a.tenant_id=$1 AND a.kind='agent' AND a.status='active' AND a.name = ANY($2)`,
			auth.Tenant(r.Context()), names)
		if err != nil {
			return err
		}
		ready := map[string]struct{}{}
		for rows.Next() {
			var name string
			if err = rows.Scan(&name); err != nil {
				rows.Close()
				return err
			}
			ready[name] = struct{}{}
		}
		if err = rows.Err(); err != nil {
			rows.Close()
			return err
		}
		rows.Close()
		for _, item := range listed.Items {
			if _, ok := ready[market.AssetName(item.ID, item.Slug, item.Name)]; !ok {
				continue
			}
			items = append(items, item)
			if len(items) == limit {
				break
			}
		}
		if page*60 >= listed.Total {
			break
		}
	}
	httpx.JSON(w, 200, map[string]any{"items": items})
	return nil
}

func (h *Handler) marketAgentEntry(w http.ResponseWriter, r *http.Request) error {
	item, ok, err := market.GetAgent(r.PathValue("handle"), r.PathValue("slug"))
	if err != nil {
		return err
	}
	if !ok {
		return httpx.E(404, "NOT_FOUND", "Agent not found")
	}
	name := market.AssetName(item.ID, item.Slug, item.Name)
	var publisher string
	err = db.Tx(r.Context()).QueryRow(r.Context(), `SELECT publisher
 FROM assets
 WHERE tenant_id=$1 AND kind='agent' AND name=$2 AND status='active'
 ORDER BY created_at
 LIMIT 1`, auth.Tenant(r.Context()), name).Scan(&publisher)
	if errors.Is(err, pgx.ErrNoRows) {
		return httpx.E(404, "NOT_FOUND", "Expert is not installed")
	}
	if err != nil {
		return err
	}
	var published bool
	err = db.Tx(r.Context()).QueryRow(r.Context(), `SELECT EXISTS(
 SELECT 1 FROM assets a
 JOIN asset_channels c ON c.tenant_id=a.tenant_id AND c.asset_id=a.id AND c.channel='stable'
 WHERE a.tenant_id=$1 AND a.kind='agent' AND a.publisher=$2 AND a.name=$3 AND a.status='active')`,
		auth.Tenant(r.Context()), publisher, name).Scan(&published)
	if err != nil {
		return err
	}
	if !published {
		return httpx.E(404, "NOT_FOUND", "Expert is not installed")
	}
	httpx.JSON(w, 200, map[string]string{
		"entry_kind": "agent",
		"entry_ref":  publisher + "/" + name,
		"name":       item.Name,
	})
	return nil
}

func (h *Handler) marketMCP(w http.ResponseWriter, r *http.Request) error {
	page, err := market.ListMCP(r.URL.Query())
	if err != nil {
		return err
	}
	httpx.JSON(w, 200, page)
	return nil
}

func (h *Handler) marketMCPServer(w http.ResponseWriter, r *http.Request) error {
	item, ok, err := market.GetMCP(r.PathValue("id"))
	if err != nil {
		return err
	}
	if !ok {
		return httpx.E(404, "NOT_FOUND", "MCP server not found")
	}
	httpx.JSON(w, 200, item)
	return nil
}

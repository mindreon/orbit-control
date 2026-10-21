package market

import (
	"net/url"
	"slices"
	"strings"
)

// Catalogue labels are the ones the expert page shows. A key without a label stays off the chips.
var agentCatalogues = []Category{
	{Key: "development-tools", Name: "开发工具"},
	{Key: "others", Name: "其他"},
	{Key: "marketing", Name: "市场营销"},
	{Key: "education", Name: "教育"},
	{Key: "finance", Name: "金融"},
	{Key: "design", Name: "设计"},
	{Key: "product", Name: "产品"},
	{Key: "sales", Name: "销售"},
	{Key: "entertainment", Name: "娱乐"},
	{Key: "life-assistant", Name: "生活助手"},
}

type AgentModel struct {
	Name     string `json:"name"`
	Supplier string `json:"supplier"`
	Protocol string `json:"protocol"`
}

type AgentRef struct {
	Name        string `json:"name"`
	Description string `json:"description"`
}

type AgentPrompt struct {
	Filename string `json:"filename"`
	Content  string `json:"content"`
}

type AgentCard struct {
	ID          string   `json:"id"`
	Handle      string   `json:"handle"`
	Slug        string   `json:"slug"`
	Name        string   `json:"name"`
	Description string   `json:"description"`
	Framework   string   `json:"framework"`
	Catalogues  []string `json:"catalogues"`
	Stars       int64    `json:"stars"`
	Downloads   int64    `json:"downloads"`
	IconURL     string   `json:"icon_url,omitempty"`
}

type AgentDetail struct {
	AgentCard
	License string        `json:"license,omitempty"`
	Source  string        `json:"source,omitempty"`
	Updated int64         `json:"updated_at,omitempty"`
	Readme  string        `json:"readme"`
	Models  []AgentModel  `json:"models"`
	Mcps    []AgentRef    `json:"mcps"`
	Skills  []AgentRef    `json:"skills"`
	Prompts []AgentPrompt `json:"prompts"`
	Files   []SkillFile   `json:"files"`
}

type AgentPage struct {
	Items      []AgentCard `json:"items"`
	Total      int         `json:"total"`
	Page       int         `json:"page"`
	Categories []Category  `json:"categories"`
}

type agentRow struct {
	ID            string        `json:"id"`
	Handle        string        `json:"handle"`
	Slug          string        `json:"slug"`
	Name          string        `json:"name"`
	Description   string        `json:"description"`
	Framework     string        `json:"framework"`
	License       string        `json:"license"`
	LogoURL       string        `json:"logoUrl"`
	Catalogues    []string      `json:"catalogues"`
	Models        []AgentModel  `json:"models"`
	Mcps          []AgentRef    `json:"mcps"`
	Skills        []AgentRef    `json:"skills"`
	SystemPrompts []AgentPrompt `json:"systemPrompts"`
	Readme        string        `json:"readme"`
	Files         []SkillFile   `json:"files"`
	Stars         int64         `json:"stars"`
	Downloads     int64         `json:"downloads"`
	UpdatedAt     int64         `json:"updatedAt"`
	Source        string        `json:"source"`
}

func loadAgents(out *plaza) error {
	rows, err := readGZ[agentRow]("snapshots/agents.json.gz")
	if err != nil {
		return err
	}
	seen := map[string]struct{}{}
	counts := map[string]int{}
	for _, row := range rows {
		if row.ID == "" || row.Name == "" || strings.Contains(row.Handle, "/") || strings.Contains(row.Slug, "/") {
			continue
		}
		if _, ok := seen[row.ID]; ok {
			continue
		}
		seen[row.ID] = struct{}{}
		source := "common"
		if row.Source == "nexa" {
			source = "nexa"
		}
		catalogues := row.Catalogues
		if catalogues == nil {
			catalogues = []string{}
		}
		for _, key := range catalogues {
			counts[key]++
		}
		card := AgentDetail{AgentCard: AgentCard{
			ID: row.ID, Handle: row.Handle, Slug: row.Slug, Name: row.Name, Description: row.Description,
			Framework: row.Framework, Catalogues: catalogues, Stars: row.Stars, Downloads: row.Downloads, IconURL: row.LogoURL,
		}, License: row.License, Source: source, Updated: row.UpdatedAt, Readme: row.Readme,
			Models: emptyModels(row.Models), Mcps: emptyRefs(row.Mcps), Skills: emptyRefs(row.Skills),
			Prompts: emptyPrompts(row.SystemPrompts), Files: emptyFiles(row.Files)}
		out.agents = append(out.agents, card)
		out.agentHay = append(out.agentHay, strings.ToLower(row.Name+" "+row.Description))
	}
	out.agentCats = agentCategories(counts)
	sortAgents(out, "downloads")
	return nil
}

func agentCategories(counts map[string]int) []Category {
	out := make([]Category, 0, len(agentCatalogues))
	for _, cat := range agentCatalogues {
		count := counts[cat.Key]
		if count == 0 {
			continue
		}
		cat.Count = count
		out = append(out, cat)
	}
	return out
}

func sortAgents(out *plaza, sortBy string) {
	type pair struct {
		item AgentDetail
		hay  string
	}
	rows := make([]pair, len(out.agents))
	for i := range out.agents {
		rows[i] = pair{out.agents[i], out.agentHay[i]}
	}
	slices.SortStableFunc(rows, func(a, b pair) int {
		var delta int64
		switch sortBy {
		case "stars":
			delta = b.item.Stars - a.item.Stars
		case "updated":
			delta = b.item.Updated - a.item.Updated
		default:
			delta = b.item.Downloads - a.item.Downloads
		}
		if delta != 0 {
			if delta > 0 {
				return 1
			}
			return -1
		}
		return strings.Compare(a.item.Name, b.item.Name)
	})
	for i := range rows {
		out.agents[i] = rows[i].item
		out.agentHay[i] = rows[i].hay
	}
}

func ListAgents(q url.Values) (AgentPage, error) {
	p, err := plazaIndex()
	if err != nil {
		return AgentPage{}, err
	}
	needle, catalogue, page, limit := queryPage(q)
	sortBy := q.Get("sort")
	if sortBy != "stars" && sortBy != "updated" {
		sortBy = "downloads"
	}
	matched := make([]int, 0)
	for i, item := range p.agents {
		if catalogue != "" && !slices.Contains(item.Catalogues, catalogue) {
			continue
		}
		if needle != "" && !strings.Contains(p.agentHay[i], needle) {
			continue
		}
		matched = append(matched, i)
	}
	if sortBy != "downloads" {
		slices.SortStableFunc(matched, func(a, b int) int {
			left, right := p.agents[a], p.agents[b]
			var delta int64
			if sortBy == "stars" {
				delta = right.Stars - left.Stars
			} else {
				delta = right.Updated - left.Updated
			}
			if delta != 0 {
				if delta > 0 {
					return 1
				}
				return -1
			}
			return strings.Compare(left.Name, right.Name)
		})
	}
	start, end := window(len(matched), page, limit)
	items := make([]AgentCard, 0, end-start)
	for _, i := range matched[start:end] {
		items = append(items, p.agents[i].AgentCard)
	}
	return AgentPage{Items: items, Total: len(matched), Page: page, Categories: p.agentCats}, nil
}

func GetAgent(handle, slug string) (AgentDetail, bool, error) {
	p, err := plazaIndex()
	if err != nil {
		return AgentDetail{}, false, err
	}
	id := handle + "/" + slug
	for _, item := range p.agents {
		if item.ID == id || item.Handle == handle && item.Slug == slug {
			return item, true, nil
		}
	}
	return AgentDetail{}, false, nil
}

func emptyModels(items []AgentModel) []AgentModel {
	if items == nil {
		return []AgentModel{}
	}
	return items
}

func emptyRefs(items []AgentRef) []AgentRef {
	if items == nil {
		return []AgentRef{}
	}
	return items
}

func emptyPrompts(items []AgentPrompt) []AgentPrompt {
	if items == nil {
		return []AgentPrompt{}
	}
	return items
}

func emptyFiles(items []SkillFile) []SkillFile {
	if items == nil {
		return []SkillFile{}
	}
	return items
}

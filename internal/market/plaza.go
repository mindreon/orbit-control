package market

import (
	"bufio"
	"compress/gzip"
	"embed"
	"encoding/json/v2"
	"io"
	"net/url"
	"slices"
	"strconv"
	"strings"
	"sync"
)

// The ModelScope plaza is a read-only catalog. It is not turned into assets:
// tens of thousands of skills and MCP servers would each start a release check.

//go:embed snapshots/skills.json.gz snapshots/mcp.json.gz snapshots/agents.json.gz
var plazaFiles embed.FS

type SkillCard struct {
	ID           string `json:"id"`
	Handle       string `json:"handle"`
	Slug         string `json:"slug"`
	Name         string `json:"name"`
	Description  string `json:"description"`
	Category     string `json:"category"`
	CategoryName string `json:"category_name"`
	Downloads    int64  `json:"downloads"`
	Likes        int64  `json:"likes"`
}

type SkillDetail struct {
	SkillCard
	DescriptionEn string   `json:"description_en,omitempty"`
	Tags          []string `json:"tags"`
	SourceURL     string   `json:"source_url,omitempty"`
	License       string   `json:"license,omitempty"`
	IconURL       string   `json:"icon_url,omitempty"`
	Source        string   `json:"source,omitempty"`
	Updated       int64    `json:"updated_at,omitempty"`
}

type MCPCard struct {
	ID       string `json:"id"`
	Name     string `json:"name"`
	Summary  string `json:"summary"`
	Author   string `json:"author"`
	Category string `json:"category"`
	Hosted   bool   `json:"hosted"`
	Calls    int64  `json:"calls"`
	Stars    int64  `json:"stars"`
	Views    int64  `json:"views"`
}

type MCPTool struct {
	Name        string `json:"name"`
	Description string `json:"description"`
}

type MCPDetail struct {
	MCPCard
	Readme string    `json:"readme"`
	Tools  []MCPTool `json:"tools"`
}

type Category struct {
	Key   string `json:"key"`
	Name  string `json:"name"`
	Count int    `json:"count"`
}

type SkillPage struct {
	Items      []SkillCard `json:"items"`
	Total      int         `json:"total"`
	Page       int         `json:"page"`
	Categories []Category  `json:"categories"`
}

type MCPPage struct {
	Items      []MCPCard  `json:"items"`
	Total      int        `json:"total"`
	Page       int        `json:"page"`
	Categories []Category `json:"categories"`
}

type skillRow struct {
	ID            string   `json:"id"`
	Handle        string   `json:"handle"`
	Slug          string   `json:"slug"`
	Name          string   `json:"name"`
	Description   string   `json:"description"`
	DescriptionEn string   `json:"descriptionEn"`
	Category      string   `json:"category"`
	CategoryName  string   `json:"categoryName"`
	Tags          []string `json:"tags"`
	SourceURL     string   `json:"sourceUrl"`
	License       string   `json:"license"`
	IconURL       string   `json:"iconUrl"`
	Downloads     int64    `json:"downloads"`
	Likes         int64    `json:"likes"`
	UpdatedAt     int64    `json:"updatedAt"`
	Source        string   `json:"source"`
}

type mcpRow struct {
	ID       string `json:"id"`
	Name     string `json:"name"`
	Summary  string `json:"summary"`
	Author   string `json:"author"`
	Category string `json:"category"`
	Calls    int64  `json:"calls"`
	Stars    int64  `json:"stars"`
	Views    int64  `json:"views"`
	Hosted   bool   `json:"hosted"`
	Readme   string `json:"readme"`
	Tools    []struct {
		Name        string `json:"name"`
		Description string `json:"description"`
	} `json:"tools"`
}

type plaza struct {
	skills    []SkillDetail
	skillHay  []string
	skillCats []Category
	mcp       []MCPDetail
	mcpHay    []string
	mcpCats   []Category
	agents    []AgentDetail
	agentHay  []string
	agentCats []Category
}

var (
	plazaOnce sync.Once
	plazaData *plaza
	plazaErr  error
)

func plazaIndex() (*plaza, error) {
	plazaOnce.Do(func() {
		plazaData, plazaErr = loadPlaza()
	})
	return plazaData, plazaErr
}

func loadPlaza() (*plaza, error) {
	out := &plaza{}
	skills, err := readGZ[skillRow]("snapshots/skills.json.gz")
	if err != nil {
		return nil, err
	}
	seen := map[string]struct{}{}
	skillCount := map[string]*Category{}
	for _, row := range skills {
		if row.ID == "" || row.Name == "" || strings.Contains(row.Handle, "/") || strings.Contains(row.Slug, "/") {
			continue
		}
		if _, ok := seen[row.ID]; ok {
			continue
		}
		seen[row.ID] = struct{}{}
		name := row.CategoryName
		if name == "" {
			name = row.Category
		}
		if name == "" {
			name = "其他"
		}
		key := row.Category
		if key == "" {
			key = "other"
		}
		source := "common"
		if row.Source == "nexa" {
			source = "nexa"
		}
		card := SkillDetail{SkillCard: SkillCard{
			ID: row.ID, Handle: row.Handle, Slug: row.Slug, Name: row.Name,
			Description: row.Description, Category: key, CategoryName: name, Downloads: row.Downloads, Likes: row.Likes,
		}, DescriptionEn: row.DescriptionEn, Tags: row.Tags, SourceURL: row.SourceURL, License: row.License, IconURL: row.IconURL, Source: source, Updated: row.UpdatedAt}
		if card.Tags == nil {
			card.Tags = []string{}
		}
		out.skills = append(out.skills, card)
		out.skillHay = append(out.skillHay, strings.ToLower(row.Name+" "+row.Description))
		cat := skillCount[key]
		if cat == nil {
			cat = &Category{Key: key, Name: name}
			skillCount[key] = cat
		}
		cat.Count++
	}
	out.skillCats = catsOf(skillCount)
	sortSkills(out)

	servers, err := readGZ[mcpRow]("snapshots/mcp.json.gz")
	if err != nil {
		return nil, err
	}
	seen = map[string]struct{}{}
	mcpCount := map[string]*Category{}
	for _, row := range servers {
		if row.ID == "" || row.Name == "" || strings.Contains(row.ID, "/") {
			continue
		}
		if _, ok := seen[row.ID]; ok {
			continue
		}
		seen[row.ID] = struct{}{}
		key := row.Category
		if key == "" {
			key = "other"
		}
		tools := make([]MCPTool, 0, len(row.Tools))
		for _, tool := range row.Tools {
			if tool.Name == "" {
				continue
			}
			tools = append(tools, MCPTool{Name: tool.Name, Description: clipText(tool.Description, 400)})
		}
		card := MCPDetail{MCPCard: MCPCard{
			ID: row.ID, Name: row.Name, Summary: row.Summary, Author: row.Author,
			Category: key, Hosted: row.Hosted, Calls: row.Calls, Stars: row.Stars, Views: row.Views,
		}, Readme: clipText(row.Readme, 20000), Tools: tools}
		out.mcp = append(out.mcp, card)
		out.mcpHay = append(out.mcpHay, strings.ToLower(row.Name+" "+row.Summary+" "+row.Author))
		cat := mcpCount[key]
		if cat == nil {
			cat = &Category{Key: key, Name: mcpCategoryName(key)}
			mcpCount[key] = cat
		}
		cat.Count++
	}
	out.mcpCats = catsOf(mcpCount)
	sortMCP(out)
	if err := loadAgents(out); err != nil {
		return nil, err
	}
	return out, nil
}

func readGZ[T any](name string) ([]T, error) {
	f, err := plazaFiles.Open(name)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	gz, err := gzip.NewReader(f)
	if err != nil {
		return nil, err
	}
	defer gz.Close()
	scanner := bufio.NewScanner(io.LimitReader(gz, 1<<30))
	scanner.Buffer(make([]byte, 64<<10), 16<<20)
	var rows []T
	for scanner.Scan() {
		var row T
		if err := jsonUnmarshal(scanner.Bytes(), &row); err != nil {
			continue
		}
		rows = append(rows, row)
	}
	return rows, scanner.Err()
}

func jsonUnmarshal(raw []byte, dst any) error {
	return json.Unmarshal(raw, dst)
}

func clipText(s string, n int) string {
	r := []rune(s)
	if len(r) <= n {
		return s
	}
	return string(r[:n])
}

func mcpCategoryName(key string) string {
	names := map[string]string{
		"browser-automation":         "浏览器自动化",
		"search":                     "搜索工具",
		"communication":              "交流协作工具",
		"developer-tools":            "开发者工具",
		"entertainment-and-media":    "娱乐与多媒体",
		"file-systems":               "文件系统",
		"finance":                    "金融",
		"knowledge-and-memory":       "知识管理与记忆",
		"location-services":          "位置服务",
		"art-and-culture":            "文化与艺术",
		"research-and-data":          "学术研究",
		"calendar-management":        "日程管理",
		"scientific-tool":            "科研工具",
		"databases":                  "数据库",
		"cloud-platforms":            "云平台",
		"note-taking":                "笔记",
		"image-and-video-processing": "图像与视频",
		"version-control":            "版本控制",
		"os-automation":              "系统自动化",
		"monitoring":                 "监控",
		"security-and-iam":           "安全与身份",
		"app-automation":             "应用自动化",
		"rag-systems":                "检索增强",
		"agent-orchestration":        "智能体编排",
		"other":                      "其他",
	}
	if name, ok := names[key]; ok {
		return name
	}
	return key
}

func catsOf(in map[string]*Category) []Category {
	out := make([]Category, 0, len(in))
	for _, cat := range in {
		out = append(out, *cat)
	}
	slices.SortFunc(out, func(a, b Category) int {
		if a.Count != b.Count {
			return b.Count - a.Count
		}
		return strings.Compare(a.Name, b.Name)
	})
	return out
}

func sortSkills(p *plaza) {
	type pair struct {
		item SkillDetail
		hay  string
	}
	rows := make([]pair, len(p.skills))
	for i := range p.skills {
		rows[i] = pair{p.skills[i], p.skillHay[i]}
	}
	slices.SortStableFunc(rows, func(a, b pair) int {
		if a.item.Downloads != b.item.Downloads {
			if a.item.Downloads > b.item.Downloads {
				return -1
			}
			return 1
		}
		return strings.Compare(a.item.Name, b.item.Name)
	})
	for i := range rows {
		p.skills[i] = rows[i].item
		p.skillHay[i] = rows[i].hay
	}
}

func sortMCP(p *plaza) {
	type pair struct {
		item MCPDetail
		hay  string
	}
	rows := make([]pair, len(p.mcp))
	for i := range p.mcp {
		rows[i] = pair{p.mcp[i], p.mcpHay[i]}
	}
	slices.SortStableFunc(rows, func(a, b pair) int {
		if a.item.Calls != b.item.Calls {
			if a.item.Calls > b.item.Calls {
				return -1
			}
			return 1
		}
		return strings.Compare(a.item.Name, b.item.Name)
	})
	for i := range rows {
		p.mcp[i] = rows[i].item
		p.mcpHay[i] = rows[i].hay
	}
}

func queryPage(q url.Values) (string, string, int, int) {
	limit, _ := strconv.Atoi(q.Get("limit"))
	if limit <= 0 || limit > 60 {
		limit = 24
	}
	page, _ := strconv.Atoi(q.Get("page"))
	if page < 1 {
		page = 1
	}
	return strings.ToLower(strings.TrimSpace(q.Get("q"))), q.Get("category"), page, limit
}

func window(n, page, limit int) (int, int) {
	start := (page - 1) * limit
	if start > n {
		start = n
	}
	end := start + limit
	if end > n {
		end = n
	}
	return start, end
}

func ListSkills(q url.Values) (SkillPage, error) {
	p, err := plazaIndex()
	if err != nil {
		return SkillPage{}, err
	}
	needle, category, page, limit := queryPage(q)
	source := q.Get("source")
	if source != "common" && source != "nexa" {
		source = ""
	}
	sortBy := q.Get("sort")
	matched := make([]int, 0)
	for i, item := range p.skills {
		if source != "" && item.Source != source {
			continue
		}
		if category != "" && item.Category != category {
			continue
		}
		if needle != "" && !strings.Contains(p.skillHay[i], needle) {
			continue
		}
		matched = append(matched, i)
	}
	if sortBy == "likes" || sortBy == "updated" {
		slices.SortStableFunc(matched, func(a, b int) int {
			left, right := p.skills[a], p.skills[b]
			var delta int64
			if sortBy == "likes" {
				delta = right.Likes - left.Likes
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
	items := make([]SkillCard, 0, end-start)
	for _, i := range matched[start:end] {
		items = append(items, p.skills[i].SkillCard)
	}
	return SkillPage{Items: items, Total: len(matched), Page: page, Categories: p.skillCats}, nil
}

func GetSkill(handle, slug string) (SkillDetail, bool, error) {
	p, err := plazaIndex()
	if err != nil {
		return SkillDetail{}, false, err
	}
	id := handle + "/" + slug
	for _, item := range p.skills {
		if item.ID == id || item.Handle == handle && item.Slug == slug {
			return item, true, nil
		}
	}
	return SkillDetail{}, false, nil
}

func ListMCP(q url.Values) (MCPPage, error) {
	p, err := plazaIndex()
	if err != nil {
		return MCPPage{}, err
	}
	needle, category, page, limit := queryPage(q)
	matched := make([]MCPCard, 0)
	for i, item := range p.mcp {
		if category != "" && item.Category != category {
			continue
		}
		if needle != "" && !strings.Contains(p.mcpHay[i], needle) {
			continue
		}
		matched = append(matched, item.MCPCard)
	}
	start, end := window(len(matched), page, limit)
	items := matched[start:end]
	if items == nil {
		items = []MCPCard{}
	}
	return MCPPage{Items: items, Total: len(matched), Page: page, Categories: p.mcpCats}, nil
}

func GetMCP(id string) (MCPDetail, bool, error) {
	p, err := plazaIndex()
	if err != nil {
		return MCPDetail{}, false, err
	}
	for _, item := range p.mcp {
		if item.ID == id {
			return item, true, nil
		}
	}
	return MCPDetail{}, false, nil
}

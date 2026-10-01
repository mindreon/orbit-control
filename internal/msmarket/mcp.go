package msmarket

import (
	"strings"

	"github.com/mindreon/orbit-control/internal/store"
)

// mcpRow is one line of mcp.json.gz: the plaza card and its display detail
// merged. Field names are the export contract with
// tools/modelscope-crawler's export-orbit.
type mcpRow struct {
	ID          string                `json:"id"`
	Name        string                `json:"name"`
	Summary     string                `json:"summary"`
	Author      string                `json:"author"`
	Category    string                `json:"category"`
	Calls       int64                 `json:"calls"`
	Views       int64                 `json:"views"`
	Stars       int64                 `json:"stars"`
	Verified    bool                  `json:"verified"`
	Hosted      bool                  `json:"hosted"`
	NeedsOnline bool                  `json:"needsOnline"`
	Rank        int                   `json:"rank"`
	Source      string                `json:"source"`
	IconURL     string                `json:"iconUrl"`
	License     string                `json:"license"`
	UpdatedOn   string                `json:"updatedOn"`
	Readme      string                `json:"readme"`
	Tools       []store.McpMarketTool `json:"tools"`
}

// Categories is the plaza sidebar, in the same order as modelscope.cn/mcp.
func Categories() []store.McpMarketCategoryRecord {
	labels := []struct{ key, name string }{
		{"browser-automation", "浏览器自动化"},
		{"search", "搜索工具"},
		{"communication", "交流协作工具"},
		{"developer-tools", "开发者工具"},
		{"entertainment-and-media", "娱乐与多媒体"},
		{"file-systems", "文件系统"},
		{"finance", "金融"},
		{"knowledge-and-memory", "知识管理与记忆"},
		{"location-services", "位置服务"},
		{"art-and-culture", "文化与艺术"},
		{"research-and-data", "学术研究"},
		{"calendar-management", "日程管理"},
		{"scientific-tool", "科研工具"},
		{"other", "其他"},
	}
	out := make([]store.McpMarketCategoryRecord, len(labels))
	for i, item := range labels {
		out[i] = store.McpMarketCategoryRecord{Key: item.key, Name: item.name, SortOrder: i + 1}
	}
	return out
}

// loadMcp splits the merged rows into plaza cards and detail text. Free text
// keeps no URLs: the plaza displays but never launches, and a URL must not
// become a channel out of the page. A URL in a card field refuses the snapshot.
func loadMcp() ([]store.McpMarketRecord, []store.McpMarketDetailRecord, error) {
	rows, err := readJSONL[mcpRow](mustEmbed("mcp.json.gz"))
	if err != nil {
		return nil, nil, err
	}
	sidebar := map[string]bool{}
	for _, cat := range Categories() {
		sidebar[cat.Key] = true
	}
	servers := make([]store.McpMarketRecord, 0, len(rows))
	details := make([]store.McpMarketDetailRecord, 0, len(rows))
	seen := map[string]struct{}{}
	for _, row := range rows {
		if row.ID == "" {
			continue
		}
		if _, dup := seen[row.ID]; dup {
			return nil, nil, errDuplicate
		}
		seen[row.ID] = struct{}{}
		categoryMore := 0
		if !sidebar[row.Category] {
			categoryMore = 1
		}
		if containsURL(row.ID, row.Name, row.Summary, row.Author, row.Category) {
			return nil, nil, errURL
		}
		servers = append(servers, store.McpMarketRecord{
			ID: row.ID, Name: row.Name, Summary: row.Summary, Author: row.Author, Category: row.Category,
			CategoryMore: categoryMore, Calls: row.Calls, Views: row.Views, Stars: row.Stars,
			Verified: row.Verified, Hosted: row.Hosted, NeedsOnline: row.NeedsOnline, Source: source(row.Source),
			IconURL: row.IconURL, Rank: row.Rank,
		})
		tools := row.Tools
		if tools == nil {
			tools = []store.McpMarketTool{}
		}
		for i := range tools {
			tools[i].Name = dropURLs(tools[i].Name)
			tools[i].Description = dropURLs(tools[i].Description)
			for j := range tools[i].Params {
				tools[i].Params[j].Name = dropURLs(tools[i].Params[j].Name)
				tools[i].Params[j].Type = dropURLs(tools[i].Params[j].Type)
				tools[i].Params[j].Description = dropURLs(tools[i].Params[j].Description)
			}
		}
		detail := store.McpMarketDetailRecord{
			ID: row.ID, License: dropURLs(row.License), UpdatedOn: dropURLs(row.UpdatedOn),
			Readme: dropURLs(row.Readme), Tools: tools,
		}
		if containsURL(detail.License, detail.UpdatedOn, detail.Readme) {
			return nil, nil, errURL
		}
		details = append(details, detail)
	}
	if len(servers) == 0 {
		return nil, nil, errEmpty
	}
	return servers, details, nil
}

// dropURLs removes the http(s):// scheme wherever it appears, leaving the
// rest of the text in place. The match is case-insensitive on the original
// bytes: lowercasing first could change the byte length of non-ASCII text.
func dropURLs(s string) string {
	var b strings.Builder
	b.Grow(len(s))
	for i := 0; i < len(s); {
		if n, hit := schemeAt(s[i:]); hit {
			i += n
			continue
		}
		b.WriteByte(s[i])
		i++
	}
	return b.String()
}

// schemeAt reports how many bytes of t hold a URL scheme at position 0.
func schemeAt(t string) (int, bool) {
	for _, scheme := range []string{"https://", "http://"} {
		if len(t) >= len(scheme) && strings.EqualFold(t[:len(scheme)], scheme) {
			return len(scheme), true
		}
	}
	return 0, false
}

func containsURL(parts ...string) bool {
	for _, part := range parts {
		lower := strings.ToLower(part)
		if strings.Contains(lower, "https://") || strings.Contains(lower, "http://") {
			return true
		}
	}
	return false
}

type installError string

func (e installError) Error() string { return string(e) }

const (
	errDuplicate installError = "mcp market snapshot has a duplicate id"
	errURL       installError = "mcp market snapshot contains a URL"
	errEmpty     installError = "mcp market snapshot is empty"
)

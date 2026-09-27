// Package mcpmarket copies the shipped ModelScope plaza snapshot into the
// local store. Listing reads that store. This package does not call
// modelscope.cn.
package mcpmarket

import (
	"bytes"
	"compress/gzip"
	"context"
	"embed"
	"encoding/json"
	"io"
	"strings"

	"github.com/mindreon/orbit-control/internal/store"
)

// PlazaTotal is the count labeled on modelscope.cn/mcp on 2026-09-27.
// The public list API does not return every row, so the stored snapshot is smaller.
const PlazaTotal = 12525

//go:embed catalog.json details.json.gz
var catalogJSON embed.FS

type snapshotRow struct {
	ID           string `json:"id"`
	Name         string `json:"name"`
	Summary      string `json:"summary"`
	Author       string `json:"author"`
	Category     string `json:"category"`
	CategoryMore int    `json:"categoryMore"`
	Calls        int64  `json:"calls"`
	Views        int64  `json:"views"`
	Stars        int64  `json:"stars"`
	Verified     bool   `json:"verified"`
	Hosted       bool   `json:"hosted"`
	NeedsOnline  bool   `json:"needsOnline"`
	Rank         int    `json:"rank"`
}

// Categories is the plaza sidebar, in the same order as modelscope.cn/mcp.
func Categories() []store.McpMarketCategoryRecord {
	labels := []struct {
		key, name string
	}{
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

// Install replaces the stored plaza with the shipped snapshot.
func Install(ctx context.Context, repo store.Repository, tenantID string) error {
	raw, err := catalogJSON.ReadFile("catalog.json")
	if err != nil {
		return err
	}
	var rows []snapshotRow
	if err := json.Unmarshal(raw, &rows); err != nil {
		return err
	}
	servers := make([]store.McpMarketRecord, 0, len(rows))
	seen := map[string]struct{}{}
	for _, row := range rows {
		rec := store.McpMarketRecord{
			ID:           strings.TrimSpace(row.ID),
			Name:         strings.TrimSpace(row.Name),
			Summary:      row.Summary,
			Author:       row.Author,
			Category:     strings.TrimSpace(row.Category),
			CategoryMore: row.CategoryMore,
			Calls:        row.Calls,
			Views:        row.Views,
			Stars:        row.Stars,
			Verified:     row.Verified,
			Hosted:       row.Hosted,
			NeedsOnline:  row.NeedsOnline,
			Rank:         row.Rank,
		}
		if rec.ID == "" {
			continue
		}
		if rec.Name == "" {
			rec.Name = rec.ID
		}
		if _, ok := seen[rec.ID]; ok {
			return errDuplicate
		}
		if containsURL(rec.ID, rec.Name, rec.Summary, rec.Author, rec.Category) {
			return errURL
		}
		seen[rec.ID] = struct{}{}
		servers = append(servers, rec)
	}
	if len(servers) == 0 {
		return errEmpty
	}
	details, err := readDetails(seen)
	if err != nil {
		return err
	}
	return repo.ReplaceMcpMarket(ctx, tenantID, servers, Categories(), details)
}

func readDetails(allowed map[string]struct{}) ([]store.McpMarketDetailRecord, error) {
	raw, err := catalogJSON.ReadFile("details.json.gz")
	if err != nil {
		return nil, err
	}
	zr, err := gzip.NewReader(bytes.NewReader(raw))
	if err != nil {
		return nil, err
	}
	defer zr.Close()
	body, err := io.ReadAll(zr)
	if err != nil {
		return nil, err
	}
	var rows []store.McpMarketDetailRecord
	if err := json.Unmarshal(body, &rows); err != nil {
		return nil, err
	}
	out := make([]store.McpMarketDetailRecord, 0, len(rows))
	seen := map[string]struct{}{}
	for _, row := range rows {
		row.ID = strings.TrimSpace(row.ID)
		if row.ID == "" {
			continue
		}
		if _, ok := allowed[row.ID]; !ok {
			continue
		}
		if _, ok := seen[row.ID]; ok {
			return nil, errDuplicate
		}
		seen[row.ID] = struct{}{}
		row.License = dropURLs(row.License)
		row.UpdatedOn = dropURLs(row.UpdatedOn)
		row.Readme = dropURLs(row.Readme)
		for i := range row.Tools {
			row.Tools[i].Name = dropURLs(row.Tools[i].Name)
			row.Tools[i].Description = dropURLs(row.Tools[i].Description)
			for j := range row.Tools[i].Params {
				row.Tools[i].Params[j].Name = dropURLs(row.Tools[i].Params[j].Name)
				row.Tools[i].Params[j].Type = dropURLs(row.Tools[i].Params[j].Type)
				row.Tools[i].Params[j].Description = dropURLs(row.Tools[i].Params[j].Description)
			}
		}
		if containsURL(row.License, row.Readme, row.UpdatedOn) {
			return nil, errURL
		}
		out = append(out, row)
	}
	return out, nil
}

func dropURLs(s string) string {
	lower := strings.ToLower(s)
	var b strings.Builder
	b.Grow(len(s))
	for i := 0; i < len(s); {
		switch {
		case strings.HasPrefix(lower[i:], "https://"):
			i += len("https://")
		case strings.HasPrefix(lower[i:], "http://"):
			i += len("http://")
		default:
			b.WriteByte(s[i])
			i++
		}
	}
	return b.String()
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

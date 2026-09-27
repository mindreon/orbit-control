package skillhub

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"net/url"
	"strings"

	"github.com/mindreon/orbit-control/internal/store"
)

const (
	maxPageFiles    = 500
	maxPageVersions = 40
	maxPageSubs     = 12
)

// PageCopy reads the public fields the skill page shows besides the package
// text: summary, tags, score, version list, file names, and the evaluation
// summary. It does not store signed report URLs or package bytes.
func (c *Client) PageCopy(ctx context.Context, handle, slug string) ([]byte, error) {
	if !pageToken(handle) && handle != "" {
		return nil, errors.New("skillhub skill")
	}
	if !pageToken(slug) {
		return nil, errors.New("skillhub skill")
	}
	detailBody, err := c.get(ctx, skillQuery("/api/v1/skills/"+url.PathEscape(slug), handle, ""))
	if err != nil {
		return nil, err
	}
	var detail detailEnv
	if err := json.Unmarshal(detailBody, &detail); err != nil || (detail.Skill.Slug == "" && detail.Slug == "") {
		return nil, errors.New("skillhub detail payload")
	}
	version := versionToken(detail.LatestVersion.Version)
	evalBody, evalErr := c.get(ctx, skillQuery("/api/v1/skills/"+url.PathEscape(slug)+"/evaluation", handle, ""))
	if evalErr != nil && !statusIs(evalErr, 404) {
		return nil, evalErr
	}
	versionBody, versionErr := c.get(ctx, skillQuery("/api/v1/skills/"+url.PathEscape(slug)+"/versions", handle, ""))
	if versionErr != nil && !statusIs(versionErr, 404) {
		return nil, versionErr
	}
	fileBody, fileErr := c.get(ctx, skillQuery("/api/v1/skills/"+url.PathEscape(slug)+"/files", handle, version))
	if fileErr != nil && !statusIs(fileErr, 404) {
		return nil, fileErr
	}
	copy := buildPageCopy(detail, evalBody, versionBody, fileBody)
	raw, err := json.Marshal(copy)
	if err != nil {
		return nil, errors.New("skillhub detail payload")
	}
	return raw, nil
}

func skillQuery(path, handle, version string) string {
	q := url.Values{}
	if handle != "" {
		q.Set("namespace", handle)
	}
	if version != "" {
		q.Set("version", version)
	}
	if encoded := q.Encode(); encoded != "" {
		return path + "?" + encoded
	}
	return path
}

func statusIs(err error, code int) bool {
	return err != nil && strings.Contains(err.Error(), fmt.Sprintf("skillhub status %d", code))
}

func pageToken(s string) bool {
	if s == "" || s == "." || s == ".." || strings.Contains(s, "..") {
		return false
	}
	if _, ok := store.SkillSlugID(s); !ok {
		return false
	}
	return true
}

func versionToken(s string) string {
	s = strings.TrimSpace(s)
	if len(s) > 32 || !pageToken(s) {
		return ""
	}
	return s
}

type detailEnv struct {
	Slug          string `json:"slug"`
	LatestVersion struct {
		Changelog string `json:"changelog"`
		CreatedAt int64  `json:"createdAt"`
		Version   string `json:"version"`
	} `json:"latestVersion"`
	SecurityReports map[string]statusOnly `json:"securityReports"`
	Skill           struct {
		Slug          string `json:"slug"`
		Summary       string `json:"summary"`
		SummaryZh     string `json:"summary_zh"`
		UpdatedAt     int64  `json:"updatedAt"`
		SubCategories []struct {
			Key  string `json:"key"`
			Name string `json:"name"`
		} `json:"subCategories"`
	} `json:"skill"`
}

type statusOnly struct {
	Status string `json:"status"`
}

type evalItem struct {
	Score      *float64 `json:"score"`
	UserReason string   `json:"userReason"`
}

type evalEnv struct {
	CreatedAt   int64  `json:"createdAt"`
	UserSummary string `json:"userSummary"`
	Dimensions  map[string]struct {
		UserReason string              `json:"userReason"`
		Items      map[string]evalItem `json:"items"`
	} `json:"dimensions"`
}

type versionsEnv struct {
	Versions []struct {
		Changelog string `json:"changelog"`
		CreatedAt int64  `json:"createdAt"`
		Version   string `json:"version"`
	} `json:"versions"`
}

type filesEnv struct {
	Files []struct {
		Path string `json:"path"`
		Size int64  `json:"size"`
	} `json:"files"`
}

type pageCopy struct {
	Summary          string        `json:"summary,omitempty"`
	SummaryZh        string        `json:"summaryZh,omitempty"`
	SubCategories    []subCopy     `json:"subCategories,omitempty"`
	Safe             bool          `json:"safe,omitempty"`
	Score            *float64      `json:"score,omitempty"`
	Version          string        `json:"version,omitempty"`
	UpdatedAt        int64         `json:"updatedAt,omitempty"`
	VersionCreatedAt int64         `json:"versionCreatedAt,omitempty"`
	FileIndex        []fileCopy    `json:"fileIndex,omitempty"`
	Versions         []versionCopy `json:"versions,omitempty"`
	Evaluation       *evalCopy     `json:"evaluation,omitempty"`
}

type subCopy struct {
	Key  string `json:"key"`
	Name string `json:"name"`
}

type fileCopy struct {
	Path string `json:"path"`
	Size int64  `json:"size"`
}

type versionCopy struct {
	Version   string `json:"version"`
	Changelog string `json:"changelog,omitempty"`
	CreatedAt int64  `json:"createdAt,omitempty"`
}

type evalCopy struct {
	UserSummary string    `json:"userSummary,omitempty"`
	CreatedAt   int64     `json:"createdAt,omitempty"`
	Dimensions  []dimCopy `json:"dimensions,omitempty"`
	Score       float64   `json:"score"`
}

type dimCopy struct {
	Key         string     `json:"key"`
	Label       string     `json:"label"`
	LabelZh     string     `json:"labelZh"`
	Description string     `json:"description,omitempty"`
	Score       float64    `json:"score"`
	Summary     string     `json:"summary,omitempty"`
	Items       []itemCopy `json:"items,omitempty"`
}

type itemCopy struct {
	Key    string  `json:"key"`
	Name   string  `json:"name"`
	Score  float64 `json:"score"`
	Reason string  `json:"reason,omitempty"`
}

func buildPageCopy(detail detailEnv, evalBody, versionBody, fileBody []byte) pageCopy {
	out := pageCopy{
		Summary:          clipRunes(detail.Skill.Summary, 2000),
		SummaryZh:        clipRunes(detail.Skill.SummaryZh, 2000),
		Version:          versionToken(detail.LatestVersion.Version),
		UpdatedAt:        detail.Skill.UpdatedAt,
		VersionCreatedAt: detail.LatestVersion.CreatedAt,
		Safe:             securityBenign(detail.SecurityReports),
	}
	for _, sub := range detail.Skill.SubCategories {
		if len(out.SubCategories) >= maxPageSubs {
			break
		}
		key := versionToken(sub.Key)
		name := clipRunes(sub.Name, 40)
		if key == "" || name == "" {
			continue
		}
		out.SubCategories = append(out.SubCategories, subCopy{Key: key, Name: name})
	}
	if len(versionBody) > 0 {
		var versions versionsEnv
		if json.Unmarshal(versionBody, &versions) == nil {
			for _, row := range versions.Versions {
				if len(out.Versions) >= maxPageVersions {
					break
				}
				version := versionToken(row.Version)
				if version == "" {
					continue
				}
				out.Versions = append(out.Versions, versionCopy{
					Version:   version,
					Changelog: clipRunes(row.Changelog, 800),
					CreatedAt: row.CreatedAt,
				})
			}
		}
	}
	if len(fileBody) > 0 {
		var files filesEnv
		if json.Unmarshal(fileBody, &files) == nil {
			for _, row := range files.Files {
				if len(out.FileIndex) >= maxPageFiles {
					break
				}
				path, ok := safePagePath(row.Path)
				if !ok {
					continue
				}
				size := row.Size
				if size < 0 {
					size = 0
				}
				out.FileIndex = append(out.FileIndex, fileCopy{Path: path, Size: size})
			}
		}
	}
	if len(evalBody) > 0 {
		if eval, score, ok := buildEval(evalBody); ok {
			out.Evaluation = eval
			out.Score = &score
		}
	}
	return out
}

func securityBenign(reports map[string]statusOnly) bool {
	for _, name := range []string{"keen", "sanbu"} {
		if strings.EqualFold(reports[name].Status, "benign") {
			return true
		}
	}
	return false
}

func safePagePath(name string) (string, bool) {
	name = strings.ReplaceAll(name, "\\", "/")
	name = strings.TrimPrefix(name, "/")
	if name == "" || strings.Contains(name, "\x00") || strings.Contains(name, "..") {
		return "", false
	}
	if len(name) > 200 {
		return "", false
	}
	return name, true
}

var traceDims = []struct {
	key, dimKey, label, labelZh, description string
}{
	{"T", "trust", "Trust", "可信任度", "安全、合规、可控。衡量 Skill 在安全性和环境适配方面的表现。"},
	{"R", "reliability", "Reliability", "可靠性", "重复执行一致稳定，功能完整可用。衡量 Skill 在多次执行中是否表现一致。"},
	{"A", "adaptability", "Adaptability", "适用性", "场景适配，恰当调用，避免误触发。衡量 Skill 是否在正确的场景被正确使用。"},
	{"C", "convention", "Convention", "规范性", "结构清晰，文档精良，具备持续维护和迭代的基础。"},
	{"E", "effectiveness", "Effectiveness", "有效性", "真正解决用户问题，产出质量达标可用。最终价值交付。"},
}

var itemNames = map[string]string{
	"scan":           "安全性扫描",
	"domestic":       "国内适配性",
	"stability":      "运行稳定性",
	"func":           "功能完善性",
	"errorHandling":  "异常处理",
	"boundary":       "能力边界定义",
	"trigger":        "触发方式",
	"progressive":    "渐进式披露",
	"structure":      "结构清晰",
	"docQuality":     "文档质量",
	"antiPatternFaq": "反模式与FAQ",
	"accuracy":       "输出准确性",
	"completeness":   "内容完整度",
	"usability":      "开箱即用度",
	"creativity":     "创造力与增值",
}

var itemOrder = []string{
	"scan", "domestic", "stability", "func", "errorHandling", "boundary", "trigger",
	"progressive", "structure", "docQuality", "antiPatternFaq", "accuracy",
	"completeness", "usability", "creativity",
}

func buildEval(body []byte) (*evalCopy, float64, bool) {
	var env evalEnv
	if err := json.Unmarshal(body, &env); err != nil || env.Dimensions == nil {
		return nil, 0, false
	}
	out := &evalCopy{
		UserSummary: clipRunes(env.UserSummary, 2000),
		CreatedAt:   env.CreatedAt,
	}
	var total float64
	for _, dim := range traceDims {
		raw := env.Dimensions[dim.dimKey]
		score := itemMean(raw.Items)
		total += score
		out.Dimensions = append(out.Dimensions, dimCopy{
			Key:         dim.key,
			Label:       dim.label,
			LabelZh:     dim.labelZh,
			Description: dim.description,
			Score:       round1(score),
			Summary:     clipRunes(raw.UserReason, 800),
			Items:       itemCopies(raw.Items),
		})
	}
	score := round1(total / float64(len(traceDims)))
	out.Score = score
	return out, score, true
}

func itemMean(items map[string]evalItem) float64 {
	var sum float64
	var n int
	for _, item := range items {
		if item.Score == nil {
			continue
		}
		sum += *item.Score
		n++
	}
	if n == 0 {
		return 0
	}
	return sum / float64(n)
}

func itemCopies(items map[string]evalItem) []itemCopy {
	seen := map[string]bool{}
	out := make([]itemCopy, 0, len(items))
	add := func(key string) {
		item, ok := items[key]
		if !ok || seen[key] {
			return
		}
		seen[key] = true
		name := itemNames[key]
		if name == "" {
			name = key
		}
		score := 0.0
		if item.Score != nil {
			score = round1(*item.Score)
		}
		out = append(out, itemCopy{
			Key:    key,
			Name:   name,
			Score:  score,
			Reason: clipRunes(item.UserReason, 800),
		})
	}
	for _, key := range itemOrder {
		add(key)
	}
	rest := make([]string, 0)
	for key := range items {
		if !seen[key] {
			rest = append(rest, key)
		}
	}
	// Stable leftover order so two copies of the same payload match.
	for i := 0; i < len(rest); i++ {
		for j := i + 1; j < len(rest); j++ {
			if rest[j] < rest[i] {
				rest[i], rest[j] = rest[j], rest[i]
			}
		}
	}
	for _, key := range rest {
		add(key)
	}
	if len(out) > 20 {
		out = out[:20]
	}
	return out
}

func round1(v float64) float64 {
	return math.Round(v*10) / 10
}

// Package skillhub copies the public SkillHub skill list into the local
// catalog. It never downloads skill packages.
package skillhub

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/mindreon/orbit-control/internal/store"
)

const (
	defaultBase    = "https://api.skillhub.cn"
	userAgent      = "orbit-skillhub/1.0"
	maxBody        = 8 << 20
	requestTimeout = 30 * time.Second
)

// Client talks only to the SkillHub host it was built with. The process
// default is api.skillhub.cn. Callers do not pass request URLs.
type Client struct {
	Base string
	HTTP *http.Client
}

// NewClient uses https://api.skillhub.cn when base is empty. A non-empty base
// is for tests.
func NewClient(base string) *Client {
	if strings.TrimSpace(base) == "" {
		base = defaultBase
	}
	c := &Client{Base: strings.TrimRight(base, "/")}
	c.HTTP = &http.Client{
		Timeout: requestTimeout,
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			if len(via) == 0 || req.URL.Host != via[0].URL.Host {
				return errors.New("skillhub redirect left the api host")
			}
			if len(via) >= 2 {
				return errors.New("skillhub redirect")
			}
			return nil
		},
	}
	return c
}

// ListSkills reads one page of GET /api/skills. sortBy is score, downloads,
// updated_at, or stars.
func (c *Client) ListSkills(ctx context.Context, page, pageSize int, sortBy string) ([]store.SkillRecord, int, error) {
	q := url.Values{}
	q.Set("sortBy", sortBy)
	q.Set("order", "desc")
	q.Set("page", fmt.Sprintf("%d", page))
	q.Set("pageSize", fmt.Sprintf("%d", pageSize))
	body, err := c.get(ctx, "/api/skills?"+q.Encode())
	if err != nil {
		return nil, 0, err
	}
	var env struct {
		Code int `json:"code"`
		Data struct {
			Skills []rawSkill `json:"skills"`
			Total  int        `json:"total"`
		} `json:"data"`
	}
	if err := json.Unmarshal(body, &env); err != nil || env.Code != 0 {
		return nil, 0, errors.New("skillhub list payload")
	}
	out := make([]store.SkillRecord, 0, len(env.Data.Skills))
	for _, raw := range env.Data.Skills {
		if rec, ok := raw.record(); ok {
			out = append(out, rec)
		}
	}
	return out, env.Data.Total, nil
}

// ListCategories reads GET /api/v1/categories.
func (c *Client) ListCategories(ctx context.Context) ([]store.SkillCategoryRecord, error) {
	body, err := c.get(ctx, "/api/v1/categories")
	if err != nil {
		return nil, err
	}
	var env struct {
		Items []struct {
			Key       string `json:"key"`
			Name      string `json:"name"`
			NameEn    string `json:"nameEn"`
			SortOrder int    `json:"sortOrder"`
			Active    bool   `json:"active"`
		} `json:"items"`
	}
	if err := json.Unmarshal(body, &env); err != nil {
		return nil, errors.New("skillhub categories payload")
	}
	out := make([]store.SkillCategoryRecord, 0, len(env.Items))
	for _, item := range env.Items {
		if !item.Active {
			continue
		}
		key := store.NormalizeSkillQuery(store.SkillCatalogQuery{Category: item.Key}).Category
		name := clipRunes(item.Name, 80)
		if key == "" || name == "" {
			continue
		}
		out = append(out, store.SkillCategoryRecord{
			Key:       key,
			Name:      name,
			NameEn:    clipRunes(item.NameEn, 80),
			SortOrder: item.SortOrder,
		})
	}
	return out, nil
}

// ListTrending reads the showcase list used for 「近期飙升」. The upstream
// list endpoint does not accept sortBy=trending.
func (c *Client) ListTrending(ctx context.Context) ([]store.SkillRecord, error) {
	body, err := c.get(ctx, "/api/v1/showcase/trending")
	if err != nil {
		return nil, err
	}
	var env struct {
		Skills []rawSkill `json:"skills"`
	}
	if err := json.Unmarshal(body, &env); err != nil {
		return nil, errors.New("skillhub trending payload")
	}
	out := make([]store.SkillRecord, 0, len(env.Skills))
	for i, raw := range env.Skills {
		rec, ok := raw.record()
		if !ok {
			continue
		}
		rec.TrendingRank = i + 1
		out = append(out, rec)
	}
	return out, nil
}

func (c *Client) get(ctx context.Context, path string) ([]byte, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.Base+path, nil)
	if err != nil {
		return nil, errors.New("skillhub request")
	}
	req.Header.Set("User-Agent", userAgent)
	req.Header.Set("Accept", "application/json")
	resp, err := c.HTTP.Do(req)
	if err != nil {
		return nil, errors.New("skillhub unreachable")
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, maxBody+1))
	if err != nil {
		return nil, errors.New("skillhub read")
	}
	if len(body) > maxBody {
		return nil, errors.New("skillhub response too large")
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("skillhub status %d", resp.StatusCode)
	}
	return body, nil
}

type labelMap map[string]string

func (m *labelMap) UnmarshalJSON(b []byte) error {
	if string(b) == "null" {
		return nil
	}
	var raw map[string]any
	if err := json.Unmarshal(b, &raw); err != nil {
		*m = nil
		return nil
	}
	out := make(map[string]string, len(raw))
	for key, value := range raw {
		switch typed := value.(type) {
		case string:
			out[key] = typed
		case bool:
			if typed {
				out[key] = "true"
			} else {
				out[key] = "false"
			}
		}
	}
	*m = out
	return nil
}

type rawSkill struct {
	Category      string   `json:"category"`
	Description   string   `json:"description"`
	DescriptionZh string   `json:"description_zh"`
	Downloads     int64    `json:"downloads"`
	IconURL       string   `json:"iconUrl"`
	Name          string   `json:"name"`
	Slug          string   `json:"slug"`
	Source        string   `json:"source"`
	Stars         int64    `json:"stars"`
	Score         float64  `json:"score"`
	Version       string   `json:"version"`
	UpdatedAt     float64  `json:"updated_at"`
	Labels        labelMap `json:"labels"`
	Namespace     struct {
		Handle        string `json:"handle"`
		CanonicalName string `json:"canonicalName"`
	} `json:"namespace"`
}

func (raw rawSkill) record() (store.SkillRecord, bool) {
	slug := store.NormalizeSkillQuery(store.SkillCatalogQuery{Category: raw.Slug}).Category
	handle := skillHandle(raw.Namespace.Handle, raw.Namespace.CanonicalName)
	if slug == "" {
		return store.SkillRecord{}, false
	}
	id := slug
	if handle != "" {
		id = handle + "/" + slug
	}
	description := raw.DescriptionZh
	if strings.TrimSpace(description) == "" {
		description = raw.Description
	}
	source := strings.ToLower(strings.TrimSpace(raw.Source))
	switch source {
	case "clawhub", "community", "enterprise":
	default:
		source = ""
	}
	return store.SkillRecord{
		ID:             id,
		Slug:           slug,
		Handle:         handle,
		Name:           clipRunes(raw.Name, 200),
		Description:    clipRunes(description, 4000),
		Category:       store.NormalizeSkillQuery(store.SkillCatalogQuery{Category: raw.Category}).Category,
		IconURL:        safeIconURL(raw.IconURL),
		Downloads:      raw.Downloads,
		Stars:          raw.Stars,
		Source:         source,
		Version:        clipRunes(strings.TrimSpace(raw.Version), 40),
		RequiresAPIKey: raw.Labels["requires_api_key"] == "true",
		Paid:           raw.Labels["pricing_type"] == "paid",
		Score:          raw.Score,
		UpdatedAt:      unixTime(raw.UpdatedAt),
	}, true
}

func skillHandle(handle, canonical string) string {
	handle = store.NormalizeSkillQuery(store.SkillCatalogQuery{Category: handle}).Category
	if handle != "" {
		return handle
	}
	name := strings.TrimPrefix(strings.TrimSpace(canonical), "@")
	if i := strings.Index(name, "/"); i > 0 {
		return store.NormalizeSkillQuery(store.SkillCatalogQuery{Category: name[:i]}).Category
	}
	return ""
}

func clipRunes(s string, n int) string {
	s = strings.TrimSpace(strings.ReplaceAll(s, "\x00", ""))
	r := []rune(s)
	if len(r) > n {
		return string(r[:n])
	}
	return s
}

func unixTime(v float64) time.Time {
	if v <= 0 {
		return time.Unix(0, 0).UTC()
	}
	if v < 1e12 {
		return time.Unix(int64(v), 0).UTC()
	}
	return time.UnixMilli(int64(v)).UTC()
}

func safeIconURL(raw string) string {
	u, err := url.Parse(strings.TrimSpace(raw))
	if err != nil || u.Scheme != "https" || u.User != nil || u.Hostname() == "" {
		return ""
	}
	switch strings.ToLower(u.Hostname()) {
	case "cloudcache.tencent-cloud.com", "cloudcache.tencent-cloud.cn",
		"cloudcache.tencentcs.com", "cloudcache.tencentcs.cn",
		"skillhub.cn", "api.skillhub.cn":
		return u.String()
	default:
		return ""
	}
}

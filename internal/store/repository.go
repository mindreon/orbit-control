package store

import (
	"context"
	"errors"
	"strings"
	"time"
)

// ErrNotFound covers "does not exist", "belongs to another tenant or user",
// and "soft-deleted". Callers must not distinguish these (contract §16/§17).
var ErrNotFound = errors.New("not found")

// ErrStorage wraps unexpected database failures. Its text is for server logs
// only and must not be echoed to clients.
var ErrStorage = errors.New("storage error")

// Repository is the tenant-scoped persistence boundary for control.
//
// Every method that reads or writes a tenant table takes tenantID explicitly;
// implementations must scope every statement by it (§18.5 query layer). The
// Postgres implementation additionally relies on RLS as a backstop.
type Repository interface {
	// CheckTenant returns ErrNotFound when the tenant does not exist. Control
	// never creates tenants; ops does (deploy/postgres/ensure-tenant.sql).
	CheckTenant(ctx context.Context, tenantID string) error

	ListPersonas(ctx context.Context, tenantID string) ([]PersonaRecord, error)
	// CreatePersona inserts one assistant. Rows are not updated or deleted.
	CreatePersona(ctx context.Context, tenantID string, p PersonaRecord) error
	// ListMcpConnectors returns this tenant's connectors, newest first.
	ListMcpConnectors(ctx context.Context, tenantID string) ([]McpConnectorRecord, error)
	// CreateMcpConnector inserts one connector. EnvRefs are names, never values.
	CreateMcpConnector(ctx context.Context, tenantID string, c McpConnectorRecord) error

	// ListSkillCatalog reads the shared SkillHub copy. tenantID is required so
	// only an authenticated caller can ask; rows are not scoped by tenant.
	ListSkillCatalog(ctx context.Context, tenantID string, q SkillCatalogQuery) (SkillCatalogPage, error)
	// GetSkill reads one stored row by id (handle/slug, or slug when there is no handle).
	// It does not call SkillHub.
	GetSkill(ctx context.Context, tenantID, id string) (SkillRecord, error)
	// GetSkillTextFiles reads text copied out of a skill package. known is false
	// until the first successful copy. It does not call SkillHub.
	GetSkillTextFiles(ctx context.Context, tenantID, id string) (files []SkillFile, known bool, err error)
	// SaveSkillTextFiles stores that copy. Later reads do not download the package again.
	SaveSkillTextFiles(ctx context.Context, tenantID, id string, files []SkillFile) error
	// GetSkillDetail reads the extra public page fields copied once. known is
	// false until that copy exists. It does not call SkillHub.
	GetSkillDetail(ctx context.Context, tenantID, id string) (raw []byte, known bool, err error)
	// SaveSkillDetail stores that copy. Later reads do not call SkillHub again.
	SaveSkillDetail(ctx context.Context, tenantID, id string, raw []byte) error
	// UpsertSkillCatalog inserts or refreshes catalog rows. An existing
	// trending rank is left as it is.
	UpsertSkillCatalog(ctx context.Context, tenantID string, rows []SkillRecord) error
	// ReplaceSkillTrending marks the current trending order. Ranks not in rows
	// are cleared. This does not call SkillHub.
	ReplaceSkillTrending(ctx context.Context, tenantID string, rows []SkillRecord) error
	// ListSkillCategories returns category labels for the shared catalog.
	ListSkillCategories(ctx context.Context, tenantID string) ([]SkillCategoryRecord, error)
	// UpsertSkillCategories stores category labels. It does not delete keys
	// that disappeared upstream.
	UpsertSkillCategories(ctx context.Context, tenantID string, rows []SkillCategoryRecord) error

	// ListMcpMarket reads the shared ModelScope snapshot. tenantID is required
	// so only an authenticated caller can ask; rows are not scoped by tenant.
	ListMcpMarket(ctx context.Context, tenantID string, q McpMarketQuery) (McpMarketPage, error)
	// ListMcpMarketCategories returns plaza labels with counts for the same
	// needsOnline filter as the list. A category with no rows is omitted.
	ListMcpMarketCategories(ctx context.Context, tenantID string, needsOnline string) ([]McpMarketCategoryCount, error)
	// GetMcpMarket returns one stored server and its display detail.
	// A missing detail row still returns the card, with an empty readme.
	GetMcpMarket(ctx context.Context, tenantID, id string) (McpMarketDetail, error)
	// ReplaceMcpMarket replaces the shared snapshot. It does not call ModelScope.
	ReplaceMcpMarket(ctx context.Context, tenantID string, servers []McpMarketRecord, categories []McpMarketCategoryRecord, details []McpMarketDetailRecord) error

	Close()
}

// PersonaRecord is one assistant persona. Env secrets are never stored here.
type PersonaRecord struct {
	ID              string
	Name            string
	Instructions    string
	McpConnectorIDs []string
	CreatedAt       time.Time
}

// McpConnectorRecord is one MCP launcher. EnvRefs and HeaderRefs are names
// only. HeaderRefs are stored as "Header-Name:ENV_NAME".
type McpConnectorRecord struct {
	ID          string
	Name        string
	Transport   string
	Command     string
	Args        []string
	EnvRefs     []string
	URL         string
	HeaderRefs  []string
	DefaultOpen bool
	CreatedAt   time.Time
}

// SkillFile is one text file copied from a skill package for display.
// The process does not run it.
type SkillFile struct {
	Path string `json:"path"`
	Body string `json:"body"`
}

// SkillRecord is one row of the shared SkillHub catalog. It is display
// metadata only: no package bytes and no secret values.
type SkillRecord struct {
	ID             string
	Slug           string
	Handle         string
	Name           string
	Description    string
	Category       string
	CategoryName   string
	IconURL        string
	Downloads      int64
	Stars          int64
	Source         string
	Version        string
	RequiresAPIKey bool
	Paid           bool
	Score          float64
	UpdatedAt      time.Time
	SyncedAt       time.Time
	TrendingRank   int
	TextFiles      []SkillFile
	FilesKnown     bool
	DetailJSON     []byte
	DetailKnown    bool
}

// SkillCategoryRecord is a display label for SkillRecord.Category.
type SkillCategoryRecord struct {
	Key       string
	Name      string
	NameEn    string
	SortOrder int
}

// SkillCatalogQuery selects a page of the local catalog.
// Sort is score, downloads, updated_at, stars, or trending.
// RequiresAPIKey and Paid are "", "true", or "false".
type SkillCatalogQuery struct {
	Sort           string
	Category       string
	Source         string
	Keyword        string
	RequiresAPIKey string
	Paid           string
	Page           int
	PageSize       int
}

// SkillCatalogPage is one page plus the newest sync time (zero when empty).
type SkillCatalogPage struct {
	Items    []SkillRecord
	Total    int
	Page     int
	PageSize int
	SyncedAt time.Time
}

// NormalizeSkillQuery clamps a catalog query to the values the stores implement.
func NormalizeSkillQuery(q SkillCatalogQuery) SkillCatalogQuery {
	switch q.Sort {
	case "downloads", "updated_at", "stars", "trending":
	default:
		q.Sort = "score"
	}
	q.Category = clipToken(q.Category, 64)
	switch q.Source {
	case "clawhub", "community", "enterprise":
	default:
		q.Source = ""
	}
	q.Keyword = clipText(q.Keyword, 80)
	q.RequiresAPIKey = boolWord(q.RequiresAPIKey)
	q.Paid = boolWord(q.Paid)
	if q.Page < 1 {
		q.Page = 1
	}
	if q.Page > 10000 {
		q.Page = 10000
	}
	if q.PageSize < 1 {
		q.PageSize = 24
	}
	if q.PageSize > 48 {
		q.PageSize = 48
	}
	return q
}

// McpMarketRecord is one row of the shared ModelScope plaza snapshot.
// It is display metadata only: no launch command, hosted URL, or secret.
type McpMarketRecord struct {
	ID           string
	Name         string
	Summary      string
	Author       string
	Category     string
	CategoryName string
	CategoryMore int
	Calls        int64
	Views        int64
	Stars        int64
	Verified     bool
	Hosted       bool
	NeedsOnline  bool
	Rank         int
}

// McpMarketTool is one tool shown on the detail page. Parameter defaults and
// secret values are not stored.
type McpMarketTool struct {
	Name        string               `json:"name"`
	Description string               `json:"description"`
	Params      []McpMarketToolParam `json:"params"`
}

// McpMarketToolParam is one input field of a tool.
type McpMarketToolParam struct {
	Name        string `json:"name"`
	Type        string `json:"type"`
	Required    bool   `json:"required"`
	Description string `json:"description"`
}

// McpMarketDetailRecord is the text stored beside a plaza card.
type McpMarketDetailRecord struct {
	ID        string          `json:"id"`
	License   string          `json:"license"`
	UpdatedOn string          `json:"updatedOn"`
	Readme    string          `json:"readme"`
	Tools     []McpMarketTool `json:"tools"`
}

// McpMarketDetail is a card plus that text.
type McpMarketDetail struct {
	McpMarketRecord
	License   string
	UpdatedOn string
	Readme    string
	Tools     []McpMarketTool
}

// McpMarketCategoryRecord is a plaza sidebar label.
type McpMarketCategoryRecord struct {
	Key       string
	Name      string
	SortOrder int
}

// McpMarketCategoryCount is a label plus how many stored servers use it.
type McpMarketCategoryCount struct {
	Key       string
	Name      string
	SortOrder int
	Count     int
}

// McpMarketQuery selects a page of the stored plaza.
// ServiceType is "", "hosted", or "local".
// NeedsOnline is "", "true", or "false".
type McpMarketQuery struct {
	Keyword     string
	Category    string
	ServiceType string
	NeedsOnline string
	Page        int
	PageSize    int
}

// McpMarketPage is one page. Stored is the catalog size after the needsOnline
// filter and before keyword, category, and service type.
type McpMarketPage struct {
	Items    []McpMarketRecord
	Total    int
	Stored   int
	Page     int
	PageSize int
}

// NormalizeMcpMarketQuery clamps a plaza query to the values the stores implement.
func NormalizeMcpMarketQuery(q McpMarketQuery) McpMarketQuery {
	q.Keyword = clipText(q.Keyword, 80)
	q.Category = clipToken(q.Category, 64)
	switch q.ServiceType {
	case "hosted", "local":
	default:
		q.ServiceType = ""
	}
	q.NeedsOnline = boolWord(q.NeedsOnline)
	if q.Page < 1 {
		q.Page = 1
	}
	if q.Page > 10000 {
		q.Page = 10000
	}
	if q.PageSize < 1 {
		q.PageSize = 30
	}
	if q.PageSize > 48 {
		q.PageSize = 48
	}
	return q
}

func boolWord(v string) string {
	switch strings.TrimSpace(strings.ToLower(v)) {
	case "true", "false":
		return strings.TrimSpace(strings.ToLower(v))
	default:
		return ""
	}
}

func clipText(s string, n int) string {
	s = strings.TrimSpace(strings.ReplaceAll(s, "\x00", ""))
	r := []rune(s)
	if len(r) > n {
		return string(r[:n])
	}
	return s
}

// SkillSlugID is the catalog id for a skill that has no author handle.
func SkillSlugID(slug string) (string, bool) {
	slug = NormalizeSkillQuery(SkillCatalogQuery{Category: slug}).Category
	if slug == "" {
		return "", false
	}
	return slug, true
}

// SkillPathID builds the catalog id from a URL handle and slug. Both parts
// must be a single token. A slash, space, or empty part is rejected.
func SkillPathID(handle, slug string) (string, bool) {
	handle = NormalizeSkillQuery(SkillCatalogQuery{Category: handle}).Category
	slug = NormalizeSkillQuery(SkillCatalogQuery{Category: slug}).Category
	if handle == "" || slug == "" {
		return "", false
	}
	return handle + "/" + slug, true
}

func clipToken(s string, n int) string {
	s = clipText(s, n)
	if s == "" {
		return ""
	}
	for _, r := range s {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9', r == '.' || r == '_' || r == '-':
		default:
			return ""
		}
	}
	return s
}

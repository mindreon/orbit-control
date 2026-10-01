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

	// ListSkillCatalog reads the shared ModelScope snapshot. tenantID is
	// required so only an authenticated caller can ask; rows are not scoped by tenant.
	ListSkillCatalog(ctx context.Context, tenantID string, q SkillCatalogQuery) (SkillCatalogPage, error)
	// GetSkill reads one stored row by id (handle/slug). It does not call ModelScope.
	GetSkill(ctx context.Context, tenantID, id string) (SkillRecord, error)
	// GetSkillTextFiles reads the text files stored beside a skill row. known is
	// false until the snapshot text pass has filled them. It does not call ModelScope.
	GetSkillTextFiles(ctx context.Context, tenantID, id string) (files []SkillFile, known bool, err error)
	// SaveSkillTextFilesBatch stores text for many skills in one call. Rows
	// with unknown ids are ignored. An empty file list marks the row as known-empty.
	SaveSkillTextFilesBatch(ctx context.Context, tenantID string, rows []SkillTextFilesRow) error
	// ReplaceSkills swaps the stored skill snapshot for the given one in one transaction.
	ReplaceSkills(ctx context.Context, tenantID string, rows []SkillRecord, categories []SkillCategoryRecord) error
	// ListSkillCategories returns category labels for the shared catalog.
	ListSkillCategories(ctx context.Context, tenantID string) ([]SkillCategoryRecord, error)

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

	// ReplaceCatalogIcons upserts icon bytes keyed by the source URL. Rows for
	// other URLs are kept: the icon sidecar is additive.
	ReplaceCatalogIcons(ctx context.Context, tenantID string, rows []CatalogIcon) error
	// CatalogIcon reads one icon's content type and bytes by source URL.
	CatalogIcon(ctx context.Context, tenantID, url string) (contentType string, data []byte, err error)

	// ListAgentCatalog reads the shared agent snapshot. tenantID is required so
	// only an authenticated caller can ask; rows are not scoped by tenant.
	ListAgentCatalog(ctx context.Context, tenantID string, q AgentCatalogQuery) (AgentCatalogPage, error)
	// GetAgent reads one stored agent row by id (handle/slug). It does not call ModelScope.
	GetAgent(ctx context.Context, tenantID, id string) (AgentRecord, error)
	// ReplaceAgents swaps the stored agent snapshot for the given one in one transaction.
	ReplaceAgents(ctx context.Context, tenantID string, rows []AgentRecord) error

	// CatalogSnapshot returns the stored hash of a named snapshot, or "" with
	// known=false when that snapshot has never been stored.
	CatalogSnapshot(ctx context.Context, tenantID, name string) (sha256 string, known bool, err error)
	// SetCatalogSnapshot records the hash of a named snapshot after it was stored.
	SetCatalogSnapshot(ctx context.Context, tenantID, name, sha256 string) error

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

// SkillRecord is one row of the shared ModelScope skills snapshot. It is
// display metadata only: no secret values. TextFiles carries the snapshot's
// copy of the skill's text files when the text pass has filled it.
type SkillRecord struct {
	ID            string
	Handle        string
	Slug          string
	Name          string
	Description   string
	DescriptionEn string
	Category      string
	CategoryName  string
	Tags          []string
	License       string
	IconURL       string
	SourceURL     string
	Downloads     int64
	Visits        int64
	Likes         int64
	UpdatedAt     time.Time
	Source        string // "common", or "nexa" for the NEXA curated set
	TextFiles     []SkillFile
	FilesKnown    bool
	InstalledAt   time.Time
}

// SkillFile is one text file stored beside a skill or agent row. The process
// does not run it.
type SkillFile struct {
	Path string `json:"path"`
	Body string `json:"body"`
}

// SkillTextFilesRow is the text of one skill for a batch write.
type SkillTextFilesRow struct {
	ID    string
	Files []SkillFile
}

// SkillCategoryRecord is a display label for SkillRecord.Category.
type SkillCategoryRecord struct {
	Key       string
	Name      string
	NameEn    string
	SortOrder int
}

// SkillCatalogQuery selects a page of the local catalog.
// Sort is downloads, likes, or updated_at. Source is "", "common", or "nexa".
type SkillCatalogQuery struct {
	Sort     string
	Category string
	Source   string
	Keyword  string
	Page     int
	PageSize int
}

// SkillCatalogPage is one page plus the newest install time (zero when empty).
type SkillCatalogPage struct {
	Items       []SkillRecord
	Total       int
	Page        int
	PageSize    int
	InstalledAt time.Time
}

// NormalizeSkillQuery clamps a catalog query to the values the stores implement.
func NormalizeSkillQuery(q SkillCatalogQuery) SkillCatalogQuery {
	switch q.Sort {
	case "downloads", "updated_at", "likes":
	default:
		q.Sort = "downloads"
	}
	q.Category = clipToken(q.Category, 64)
	switch q.Source {
	case "common", "nexa":
	default:
		q.Source = ""
	}
	q.Keyword = clipText(q.Keyword, 80)
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

// SkillPathID builds the catalog id from a URL handle and slug. The handle is
// an optional "@" plus one token; both parts reject slashes and spaces.
func SkillPathID(handle, slug string) (string, bool) {
	handle = clipHandle(handle, 100)
	slug = clipSlug(slug, 200)
	if handle == "" || slug == "" {
		return "", false
	}
	return handle + "/" + slug, true
}

// clipHandle keeps one token with an optional leading "@".
func clipHandle(s string, n int) string {
	at := ""
	if strings.HasPrefix(s, "@") {
		at = "@"
		s = s[1:]
	}
	body := clipToken(s, n)
	if body == "" {
		return ""
	}
	return at + body
}

// clipSlug keeps one token.
func clipSlug(s string, n int) string {
	return clipToken(s, n)
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
	Source       string // "common", or "nexa" for the NEXA curated set
	IconURL      string // Source URL of the card icon; bytes live in catalog_icons.
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

// CatalogIcon is one icon's bytes keyed by the source URL it came from.
type CatalogIcon struct {
	URL         string
	ContentType string
	Data        []byte
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
// Source is "", "common", or "nexa".
type McpMarketQuery struct {
	Keyword     string
	Category    string
	ServiceType string
	NeedsOnline string
	Source      string
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
	switch q.Source {
	case "common", "nexa":
	default:
		q.Source = ""
	}
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

// AgentRecord is one row of the shared ModelScope agents snapshot. It is
// display metadata only: no secret values. Files carries the snapshot's copy
// of the agent's text files when the snapshot provided them.
type AgentRecord struct {
	ID            string
	Handle        string
	Slug          string
	Name          string
	Description   string
	Framework     string
	License       string
	LogoURL       string
	Catalogues    []string
	Models        []AgentModel
	Mcps          []AgentRef
	Skills        []AgentRef
	SystemPrompts []AgentPrompt
	Readme        string
	Files         []SkillFile
	FilesKnown    bool
	Stars         int64
	Downloads     int64
	Visits        int64
	UpdatedAt     time.Time
	Source        string // "common" (the agents plaza has no nexa view)
}

// AgentModel is one model an agent declares.
type AgentModel struct {
	Name     string `json:"name"`
	Supplier string `json:"supplier"`
	Protocol string `json:"protocol"`
}

// AgentRef is one skill or MCP server an agent declares, by name.
type AgentRef struct {
	Name        string `json:"name"`
	Description string `json:"description"`
}

// AgentPrompt is one prompt file of an agent.
type AgentPrompt struct {
	Filename string `json:"filename"`
	Content  string `json:"content"`
}

// AgentCatalogQuery selects a page of the stored agents.
// Sort is downloads, stars, or updated_at. Source is "", "common", or "nexa".
type AgentCatalogQuery struct {
	Sort      string
	Catalogue string
	Keyword   string
	Page      int
	PageSize  int
}

// AgentCatalogPage is one page.
type AgentCatalogPage struct {
	Items    []AgentRecord
	Total    int
	Page     int
	PageSize int
}

// NormalizeAgentCatalogQuery clamps an agent query to the values the stores implement.
func NormalizeAgentCatalogQuery(q AgentCatalogQuery) AgentCatalogQuery {
	switch q.Sort {
	case "downloads", "updated_at", "stars":
	default:
		q.Sort = "downloads"
	}
	q.Catalogue = clipToken(q.Catalogue, 64)
	q.Keyword = clipText(q.Keyword, 80)
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

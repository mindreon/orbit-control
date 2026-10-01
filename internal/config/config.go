// Package config loads process settings from the environment.
//
// Viper reads each variable when Load is called, so tests can change the
// environment and see the new value on the next call. Invalid numbers are
// ignored (the previous manual parsers did the same) and logged once.
package config

import (
	"log"
	"path/filepath"
	"strconv"
	"strings"
	"sync"

	"github.com/spf13/viper"
)

const (
	defaultPort         = "8080"
	defaultInternalAddr = "127.0.0.1:8081"
	defaultIngestMax    = 1 << 20
)

// Config is the typed view of the environment this process understands.
type Config struct {
	LogFormat  string
	PublicAddr string
	Port       string
	// AllowUnauthenticatedBind is true only when the env value is exactly "1".
	AllowUnauthenticatedBind bool
	InternalAddr             string

	Env      string
	AuthMode string

	ControlDBURL   string
	MigrateOnStart bool
	MigrateDBURL   string

	ArtifactDir      string
	DataDir          string
	ArtifactMaxBytes int64
	DefaultTenant    string
	IngestMaxBytes   int64

	ObjectStorePublicEndpoint string
	ObjectStoreEndpoint       string
	ObjectStoreAccessKey      string
	ObjectStoreSecretKey      string
	ObjectStoreBucket         string
	ObjectStoreSecure         bool

	TemporalAddress   string
	TemporalNamespace string
	TemporalTaskQueue string

	// CatalogDir is where an optional skills_text.json.gz sidecar lives
	// (ORBIT_CATALOG_DIR). Without it the skill catalog runs without file text.
	CatalogDir string

	// SkillsDir is the skill library (ORBIT_SKILLS_DIR): <dir>/<handle>/<slug>/SKILL.md and the files beside it. A skill
	// found there is served from disk; it is how skills are updated without re-importing anything.
	SkillsDir string

	TaskMembers     []string
	TaskMemberID    string
	InternalMembers []string
	MembersFile     string
	// MembersRefreshSeconds is set when the env var is a positive integer.
	// MembersRefreshConfigured is set whenever the env var is non-empty,
	// including a value that cannot be parsed.
	MembersRefreshSeconds    int
	MembersRefreshConfigured bool

	AllowedOrigins []string
	InternalToken  string
}

// Load reads the current environment. It does not cache, so a later change
// to the process environment is visible to the next caller.
func Load() Config {
	v := viper.New()
	v.AutomaticEnv()
	v.SetDefault("PORT", defaultPort)
	v.SetDefault("ORBIT_INTERNAL_ADDR", defaultInternalAddr)

	refreshRaw := strings.TrimSpace(v.GetString("ORBIT_CONTROL_MEMBERS_REFRESH_SECONDS"))
	refreshSeconds := 0
	if refreshRaw != "" {
		if n, err := strconv.Atoi(refreshRaw); err == nil && n > 0 {
			refreshSeconds = n
		} else {
			warnOnce("ORBIT_CONTROL_MEMBERS_REFRESH_SECONDS", refreshRaw, "a positive integer")
		}
	}

	return Config{
		LogFormat:                strings.TrimSpace(v.GetString("ORBIT_LOG_FORMAT")),
		PublicAddr:               strings.TrimSpace(v.GetString("ORBIT_PUBLIC_ADDR")),
		Port:                     strings.TrimSpace(v.GetString("PORT")),
		AllowUnauthenticatedBind: strings.TrimSpace(v.GetString("ORBIT_ALLOW_UNAUTHENTICATED_BIND")) == "1",
		InternalAddr:             strings.TrimSpace(v.GetString("ORBIT_INTERNAL_ADDR")),

		Env:      strings.TrimSpace(v.GetString("ORBIT_ENV")),
		AuthMode: strings.TrimSpace(v.GetString("ORBIT_AUTH_MODE")),

		ControlDBURL:   strings.TrimSpace(v.GetString("ORBIT_CONTROL_DB_URL")),
		MigrateOnStart: strings.TrimSpace(v.GetString("ORBIT_CONTROL_MIGRATE_ON_START")) == "1",
		MigrateDBURL:   strings.TrimSpace(v.GetString("ORBIT_CONTROL_MIGRATE_DB_URL")),

		ArtifactDir:      strings.TrimSpace(v.GetString("ORBIT_ARTIFACT_DIR")),
		DataDir:          strings.TrimSpace(v.GetString("ORBIT_DATA_DIR")),
		ArtifactMaxBytes: positiveInt64(v, "ORBIT_ARTIFACT_MAX_BYTES", 0),
		DefaultTenant:    strings.TrimSpace(v.GetString("ORBIT_DEFAULT_TENANT")),
		IngestMaxBytes:   positiveInt64(v, "ORBIT_INGEST_MAX_BYTES", defaultIngestMax),

		ObjectStorePublicEndpoint: strings.TrimSpace(v.GetString("ORBIT_OBJECT_STORE_PUBLIC_ENDPOINT")),
		ObjectStoreEndpoint:       strings.TrimSpace(v.GetString("ORBIT_OBJECT_STORE_ENDPOINT")),
		ObjectStoreAccessKey:      v.GetString("ORBIT_OBJECT_STORE_ACCESS_KEY"),
		ObjectStoreSecretKey:      v.GetString("ORBIT_OBJECT_STORE_SECRET_KEY"),
		ObjectStoreBucket:         v.GetString("ORBIT_OBJECT_STORE_BUCKET"),
		ObjectStoreSecure:         strings.TrimSpace(v.GetString("ORBIT_OBJECT_STORE_SECURE")) == "1",

		TemporalAddress:   strings.TrimSpace(v.GetString("TEMPORAL_ADDRESS")),
		TemporalNamespace: strings.TrimSpace(v.GetString("TEMPORAL_NAMESPACE")),
		TemporalTaskQueue: strings.TrimSpace(v.GetString("TEMPORAL_TASK_QUEUE")),

		CatalogDir: strings.TrimSpace(v.GetString("ORBIT_CATALOG_DIR")),
		SkillsDir:  strings.TrimSpace(v.GetString("ORBIT_SKILLS_DIR")),

		TaskMembers:              SplitCSV(v.GetString("ORBIT_CONTROL_MEMBERS")),
		TaskMemberID:             strings.TrimSpace(v.GetString("ORBIT_CONTROL_MEMBER_ID")),
		InternalMembers:          SplitCSV(v.GetString("ORBIT_CONTROL_INTERNAL_MEMBERS")),
		MembersFile:              strings.TrimSpace(v.GetString("ORBIT_CONTROL_MEMBERS_FILE")),
		MembersRefreshSeconds:    refreshSeconds,
		MembersRefreshConfigured: refreshRaw != "",

		AllowedOrigins: SplitCSV(v.GetString("ORBIT_ALLOWED_ORIGINS")),
		InternalToken:  strings.TrimSpace(v.GetString("ORBIT_INTERNAL_TOKEN")),
	}
}

// PublicListenAddr is ORBIT_PUBLIC_ADDR, otherwise 127.0.0.1:$PORT.
func (c Config) PublicListenAddr() string {
	if c.PublicAddr != "" {
		return c.PublicAddr
	}
	port := c.Port
	if port == "" {
		port = defaultPort
	}
	return "127.0.0.1:" + port
}

// InternalListenAddr defaults to loopback port 8081.
func (c Config) InternalListenAddr() string {
	if c.InternalAddr != "" {
		return c.InternalAddr
	}
	return defaultInternalAddr
}

// Prod is true when the process is configured as production or OIDC.
func (c Config) Prod() bool {
	return strings.EqualFold(c.Env, "prod") || strings.EqualFold(c.AuthMode, "oidc")
}

// OIDC is true when ORBIT_AUTH_MODE=oidc.
func (c Config) OIDC() bool {
	return strings.EqualFold(c.AuthMode, "oidc")
}

// ArtifactDirPath is ORBIT_ARTIFACT_DIR, or $ORBIT_DATA_DIR/artifacts.
func (c Config) ArtifactDirPath() string {
	if c.ArtifactDir != "" {
		return c.ArtifactDir
	}
	if c.DataDir != "" {
		return filepath.Join(c.DataDir, "artifacts")
	}
	return ""
}

// ObjectStoreURL prefers the public endpoint, then the internal one.
func (c Config) ObjectStoreURL() string {
	if c.ObjectStorePublicEndpoint != "" {
		return c.ObjectStorePublicEndpoint
	}
	return c.ObjectStoreEndpoint
}

// SplitCSV splits a comma-separated list and drops empty items.
func SplitCSV(raw string) []string {
	items := strings.Split(raw, ",")
	out := make([]string, 0, len(items))
	for _, item := range items {
		item = strings.TrimSpace(item)
		if item != "" {
			out = append(out, item)
		}
	}
	return out
}

func positiveInt64(v *viper.Viper, key string, fallback int64) int64 {
	raw := strings.TrimSpace(v.GetString(key))
	if raw == "" {
		return fallback
	}
	n, err := strconv.ParseInt(raw, 10, 64)
	if err != nil || n <= 0 {
		warnOnce(key, raw, "a positive integer")
		return fallback
	}
	return n
}

var warned sync.Map

func warnOnce(key, raw, want string) {
	if _, loaded := warned.LoadOrStore(key+"\x00"+raw, struct{}{}); loaded {
		return
	}
	log.Printf("ignoring %s=%q: want %s", key, raw, want)
}

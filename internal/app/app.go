// Package app is what the HTTP layer calls: the task service and the tenant catalog (assistants, connectors, skills,
// MCP market).
package app

import (
	"errors"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/oklog/ulid/v2"

	"github.com/mindreon/orbit-control/internal/store"
	"github.com/mindreon/orbit-control/internal/store/memstore"
	taskruntime "github.com/mindreon/orbit-control/internal/task"
)

const (
	DefaultTenantID = "default"

	defaultArtifactMaxBytes = 32 << 20
)

// ErrInvalid marks caller errors (400).
var ErrInvalid = errors.New("invalid request")

func invalidf(format string, args ...any) error {
	return fmt.Errorf("%w: %s", ErrInvalid, fmt.Sprintf(format, args...))
}

// ErrNotFound is the repository sentinel.
var ErrNotFound = store.ErrNotFound

// Principal is the authenticated caller. TenantID and UserID come only from
// the authenticator (session), never from request bodies (§17.5).
type Principal struct {
	TenantID string
	UserID   string
}

type Options struct {
	Repo          store.Repository
	Log           *log.Logger
	DefaultTenant string
	// ArtifactDir is the content-addressed blob root ({dir}/{tenant}/{sha256}).
	// Empty disables POST /internal/artifact-blobs.
	ArtifactDir string
	// ArtifactMaxBytes caps one blob body. Zero uses 32 MiB.
	ArtifactMaxBytes int64
	Tasks            *taskruntime.Service
	TaskClient       taskruntime.TaskClient
	ArtifactSigner   taskruntime.ArtifactSigner
}

type App struct {
	Repo             store.Repository
	Log              *log.Logger
	DefaultTenant    string
	ArtifactDir      string
	ArtifactMaxBytes int64
	IngestMaxBytes   int64
	Tasks            *taskruntime.Service
}

func NewWithOptions(opts Options) *App {
	if opts.Repo == nil {
		opts.Repo = memstore.New()
	}
	if opts.Log == nil {
		opts.Log = log.Default()
	}
	if opts.DefaultTenant == "" {
		opts.DefaultTenant = DefaultTenantID
	}
	if opts.ArtifactMaxBytes <= 0 {
		opts.ArtifactMaxBytes = defaultArtifactMaxBytes
	}
	a := &App{
		Repo:             opts.Repo,
		Log:              opts.Log,
		DefaultTenant:    opts.DefaultTenant,
		ArtifactDir:      opts.ArtifactDir,
		ArtifactMaxBytes: opts.ArtifactMaxBytes,
		IngestMaxBytes:   ingestMaxBytes(),
		Tasks:            opts.Tasks,
	}
	if a.Tasks == nil {
		projection, _ := opts.Repo.(taskruntime.ProjectionStore)
		a.Tasks = taskruntime.NewWithProjection(opts.TaskClient, projection)
	}
	if opts.ArtifactSigner != nil {
		a.Tasks.SetArtifactSigner(opts.ArtifactSigner)
	}
	return a
}

func id(prefix string) string { return prefix + strings.ToLower(ulid.Make().String()) }

func stamp(t time.Time) string { return t.UTC().Format(time.RFC3339Nano) }

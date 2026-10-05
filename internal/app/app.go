// Package app is what the HTTP layer calls: the task service and the tenant catalog (assistants, connectors, skills,
// MCP market).
package app

import (
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/google/uuid"

	"github.com/mindreon/orbit-control/internal/skillstore"
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

// FieldError is a caller error that says what is wrong and where: a stable Code, the request Field it is about
// ("members[2].expert") and a human Reason. It is an ErrInvalid, so it is a 400 wherever those are.
type FieldError struct {
	Code   string
	Field  string
	Reason string
}

func (e *FieldError) Error() string {
	return fmt.Sprintf("%s: %s: %s (%s)", ErrInvalid, e.Field, e.Reason, e.Code)
}
func (e *FieldError) Unwrap() error { return ErrInvalid }

func invalidField(code, field, reason string) error {
	return &FieldError{Code: code, Field: field, Reason: reason}
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
	// Skills is the skill library on disk, if one is configured.
	Skills *skillstore.Store
}

type App struct {
	Repo             store.Repository
	Log              *log.Logger
	DefaultTenant    string
	ArtifactDir      string
	ArtifactMaxBytes int64
	IngestMaxBytes   int64
	Tasks            *taskruntime.Service
	Skills           *skillstore.Store
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
		Skills:           opts.Skills,
	}
	if a.Tasks == nil {
		projection, _ := opts.Repo.(taskruntime.ProjectionStore)
		a.Tasks = taskruntime.NewWithProjection(opts.TaskClient, projection)
	}
	a.Tasks.SetLog(a.Log)
	if opts.ArtifactSigner != nil {
		a.Tasks.SetArtifactSigner(opts.ArtifactSigner)
	}
	return a
}

func id(prefix string) string {
	value, err := uuid.NewV7()
	if err != nil {
		panic(err)
	}
	return prefix + value.String()
}

func stamp(t time.Time) string { return t.UTC().Format(time.RFC3339Nano) }

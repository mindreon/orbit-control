// Package task is the control plane's v3 task boundary.
package task

import (
	"log"
	"sync"

	"github.com/google/uuid"
	lru "github.com/hashicorp/golang-lru/v2"
)

type Service struct {
	mu             sync.Mutex
	appendMu       sync.Mutex
	orch           TaskClient
	projection     ProjectionStore
	artifactSigner ArtifactSigner
	log            *log.Logger
	tasks          map[string]*Task
	events         map[string][]Event
	subs           map[string]map[*subscriber]struct{}
	seenMessage    map[string]map[string]uint64
	ledger         CommandLedger
	nextSeq        map[string]uint64
	seenEvents     *lru.Cache[string, struct{}]
	entityVersion  map[string]map[string]int64
	profiles       map[string]map[string]Profile
	configs        map[string]localConfig  // dev only: with an orchestrator the workflow owns a task's configuration
	userSettings   map[string]UserSettings // dev only: without a projection store, keyed by tenant and user
	manifests      map[string]ArtifactManifest
	maxEvents      int
}

type subscriber struct {
	ch     chan Event
	closed chan struct{}
	once   sync.Once
}

func New(client TaskClient) *Service {
	return NewWithProjection(client, nil)
}

func NewWithProjection(client TaskClient, projection ProjectionStore) *Service {
	return &Service{
		orch: client, projection: projection, tasks: map[string]*Task{}, events: map[string][]Event{},
		subs: map[string]map[*subscriber]struct{}{}, seenMessage: map[string]map[string]uint64{},
		ledger: newCommandLedger(projection), nextSeq: map[string]uint64{},
		seenEvents:    mustCache[string, struct{}](seenEventCacheLimit),
		entityVersion: map[string]map[string]int64{}, profiles: map[string]map[string]Profile{}, configs: map[string]localConfig{}, userSettings: map[string]UserSettings{}, manifests: map[string]ArtifactManifest{}, maxEvents: 5000,
	}
}

func (s *Service) SetArtifactSigner(signer ArtifactSigner) {
	s.artifactSigner = signer
}

// SetLog wires the process logger for background work that has no caller to
// report to. A nil logger is fine; the work just stays silent.
func (s *Service) SetLog(log *log.Logger) {
	s.log = log
}

// isClosed is true only for a cancelled task. A task is a conversation: COMPLETED means its plan is done for now, and the
// next message starts another round.
func isClosed(status string) bool {
	return status == "CANCELLED"
}

// newID is prefix plus a canonical 36-character UUIDv7 (task_018f...-....).
// UUIDv7 is time-ordered, so ids created later sort after ids created earlier.
func newID(prefix string) string { return prefix + "_" + mustUUIDv7() }

func mustUUIDv7() string {
	id, err := uuid.NewV7()
	if err != nil {
		panic(err)
	}
	return id.String()
}

func mustCache[K comparable, V any](size int) *lru.Cache[K, V] {
	cache, err := lru.New[K, V](size)
	if err != nil {
		panic(err)
	}
	return cache
}

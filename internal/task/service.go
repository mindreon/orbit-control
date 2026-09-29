// Package task is the control plane's v3 task boundary.
package task

import (
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
	tasks          map[string]*Task
	events         map[string][]Event
	subs           map[string]map[*subscriber]struct{}
	seenMessage    map[string]map[string]uint64
	ledger         CommandLedger
	nextSeq        map[string]uint64
	seenEvents     *lru.Cache[string, struct{}]
	entityVersion  map[string]map[string]int64
	profiles       map[string]map[string]Profile
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
		entityVersion: map[string]map[string]int64{}, profiles: map[string]map[string]Profile{}, manifests: map[string]ArtifactManifest{}, maxEvents: 5000,
	}
}

func (s *Service) SetArtifactSigner(signer ArtifactSigner) {
	s.artifactSigner = signer
}

func isClosed(status string) bool {
	return status == "COMPLETED" || status == "FAILED" || status == "CANCELLED"
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

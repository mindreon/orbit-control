package task

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"time"

	lru "github.com/hashicorp/golang-lru/v2"
)

// commandLease is how long a claimed API command is considered in flight. The orchestrator call is bounded by 5s
// (orch.Client.UpdateTask), so a claim older than this belongs to a process that died. The next request takes it over
// and sends the Update again: the Update ID is the command_id, so the workflow deduplicates it (10 §1).
const commandLease = 30 * time.Second

// commandLedgerLimit bounds the in-memory ledger used without a database (dev and tests).
const commandLedgerLimit = 4096

// ErrCommandInProgress means another request holds an unfinished claim on the same command_id. The client retries
// with the same id; the API answers 409 IN_PROGRESS.
var ErrCommandInProgress = errors.New("command is already being processed")

// CommandLedger is the API command idempotency record (idempotency_ledger, scope api_command, 10 §1). A claim moves
// through: claimed (started, owned) -> completed (succeeded, with the response) or released (started, no owner, so the
// same command_id can be retried).
type CommandLedger interface {
	// ClaimCommand records the command as started by owner. It returns claimed=true when the caller must run the
	// command. With claimed=false the command already succeeded and result is the stored response. It returns
	// ErrIdempotencyConflict when the key was used with another request hash, and ErrCommandInProgress when another
	// live owner holds it. A released claim or one older than lease is taken over.
	ClaimCommand(ctx context.Context, tenantID, key, hash, owner string, lease time.Duration) (result json.RawMessage, claimed bool, err error)
	// CompleteCommand stores the response and marks the command succeeded. It changes nothing if owner no longer
	// holds the claim (it lost it after the lease passed).
	CompleteCommand(ctx context.Context, tenantID, key, owner string, result json.RawMessage) error
	// ReleaseCommand gives up owner's claim after a failure so the same command_id can be retried at once.
	ReleaseCommand(ctx context.Context, tenantID, key, owner string) error
}

// commandKey is the ledger key: tenant, task and command_id (the table's primary key is global, not per tenant).
func commandKey(p Principal, taskID, commandID string) string {
	return p.TenantID + "/" + taskID + "/" + commandID
}

type memoryEntry struct {
	hash   string
	status string // started or succeeded
	owner  string // empty when released
	seen   time.Time
	result json.RawMessage
}

// memoryLedger is the CommandLedger without a database. It behaves like the table but lives in one process, so it
// cannot deduplicate across replicas or restarts.
type memoryLedger struct {
	mu      sync.Mutex
	now     func() time.Time
	entries *lru.Cache[string, memoryEntry]
}

func newMemoryLedger(limit int, now func() time.Time) *memoryLedger {
	return &memoryLedger{now: now, entries: mustCache[string, memoryEntry](limit)}
}

func (m *memoryLedger) len() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.entries.Len()
}

func (m *memoryLedger) ClaimCommand(_ context.Context, _ string, key, hash, owner string, lease time.Duration) (json.RawMessage, bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	now := m.now()
	entry, ok := m.entries.Get(key)
	if !ok {
		m.entries.Add(key, memoryEntry{hash: hash, status: "started", owner: owner, seen: now})
		return nil, true, nil
	}
	if entry.hash != hash {
		return nil, false, ErrIdempotencyConflict
	}
	if entry.status == "succeeded" {
		return append(json.RawMessage(nil), entry.result...), false, nil
	}
	if entry.owner != "" && now.Sub(entry.seen) < lease {
		return nil, false, ErrCommandInProgress
	}
	entry.owner, entry.seen = owner, now
	m.entries.Add(key, entry)
	return nil, true, nil
}

func (m *memoryLedger) CompleteCommand(_ context.Context, _ string, key, owner string, result json.RawMessage) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	entry, ok := m.entries.Get(key)
	if !ok || entry.status != "started" || entry.owner != owner {
		return nil
	}
	entry.status, entry.seen, entry.result = "succeeded", m.now(), append(json.RawMessage(nil), result...)
	m.entries.Add(key, entry)
	return nil
}

func (m *memoryLedger) ReleaseCommand(_ context.Context, _ string, key, owner string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	entry, ok := m.entries.Get(key)
	if !ok || entry.status != "started" || entry.owner != owner {
		return nil
	}
	entry.owner = ""
	m.entries.Add(key, entry)
	return nil
}

func newCommandLedger(projection ProjectionStore) CommandLedger {
	if projection != nil {
		return projection
	}
	return newMemoryLedger(commandLedgerLimit, time.Now)
}

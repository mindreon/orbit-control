package app

import (
	"bytes"
	"encoding/json"
	"sort"
	"sync"
)

// Envelope is one room (task) event as stored and streamed: typed routing
// fields plus the producer's payload. Worker payloads are passed through
// unchanged (whitespace compacted), so fields control does not know survive.
type Envelope struct {
	// ID is the global event id and the SSE `id:`. Live-only events have none.
	ID     uint64 `json:"id,omitempty"`
	Type   string `json:"type"`
	TaskID string `json:"taskId"`
	// TS is the payload's occurredAt when present, else when control received it.
	TS      string          `json:"ts"`
	Source  string          `json:"source"`
	Payload json.RawMessage `json:"payload"`
}

// EncodeEnvelope renders env for SSE and audit without HTML-escaping, so the
// payload bytes are exactly the (compacted) bytes the producer sent.
func EncodeEnvelope(env Envelope) ([]byte, error) {
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(env); err != nil {
		return nil, err
	}
	return bytes.TrimSuffix(buf.Bytes(), []byte("\n")), nil
}

// EventLog stores durable room (task) events for activity, SSE ids, and
// Last-Event-ID replay. Ids come from one global monotonic sequence shared by
// all tasks; every read filters by task id. Replay and reset rules live in
// App and use only this interface, so a persistent implementation can replace
// MemoryEventLog without changing them.
//
// Implementations must make appended events visible in id order: replay reads
// "id > cursor" and would permanently skip an id that became visible late.
type EventLog interface {
	// Append assigns ev the next global id (ev.ID) and stores it under taskID.
	Append(taskID string, ev Envelope) (Envelope, error)
	// After returns taskID's retained events with id > after, oldest first.
	After(taskID string, after uint64) ([]Envelope, error)
	// Contains reports whether id is a retained event of taskID.
	Contains(taskID string, id uint64) (bool, error)
	// EvictedThrough is the id of taskID's newest evicted event, or 0 if none.
	EvictedThrough(taskID string) (uint64, error)
	// Head is taskID's latest event id, or 0 if it has none.
	Head(taskID string) (uint64, error)
	// LastID is the latest id issued for any task.
	LastID() (uint64, error)
}

// MemoryEventLog is the W1 EventLog. The sequence lives only in this process:
// after a restart it starts again and every previously issued id is unknown.
type MemoryEventLog struct {
	mu     sync.Mutex
	retain int
	lastID uint64
	tasks  map[string]*memoryTask
}

type memoryTask struct {
	events         []Envelope
	evictedThrough uint64
}

// NewMemoryEventLog keeps the newest retainPerTask events of each task.
func NewMemoryEventLog(retainPerTask int) *MemoryEventLog {
	return &MemoryEventLog{retain: retainPerTask, tasks: map[string]*memoryTask{}}
}

func (l *MemoryEventLog) Append(taskID string, ev Envelope) (Envelope, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.lastID++
	ev.ID = l.lastID
	t := l.tasks[taskID]
	if t == nil {
		t = &memoryTask{}
		l.tasks[taskID] = t
	}
	t.events = append(t.events, ev)
	if drop := len(t.events) - l.retain; drop > 0 {
		t.evictedThrough = t.events[drop-1].ID
		t.events = append([]Envelope(nil), t.events[drop:]...)
	}
	return ev, nil
}

func (l *MemoryEventLog) After(taskID string, after uint64) ([]Envelope, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	t := l.tasks[taskID]
	if t == nil {
		return nil, nil
	}
	i := sort.Search(len(t.events), func(i int) bool { return t.events[i].ID > after })
	return append([]Envelope(nil), t.events[i:]...), nil
}

func (l *MemoryEventLog) Contains(taskID string, id uint64) (bool, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	t := l.tasks[taskID]
	if t == nil {
		return false, nil
	}
	i := sort.Search(len(t.events), func(i int) bool { return t.events[i].ID >= id })
	return i < len(t.events) && t.events[i].ID == id, nil
}

func (l *MemoryEventLog) EvictedThrough(taskID string) (uint64, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if t := l.tasks[taskID]; t != nil {
		return t.evictedThrough, nil
	}
	return 0, nil
}

func (l *MemoryEventLog) Head(taskID string) (uint64, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if t := l.tasks[taskID]; t != nil && len(t.events) > 0 {
		return t.events[len(t.events)-1].ID, nil
	}
	return 0, nil
}

func (l *MemoryEventLog) LastID() (uint64, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.lastID, nil
}

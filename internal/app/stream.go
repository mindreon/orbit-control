package app

import (
	"encoding/json"
	"math"
	"sort"
	"strconv"
	"strings"
)

// StreamFrame is one SSE-bound event for a room.
//
// Durable frames are ActivityEvents: persisted to the audit log and retained
// in the bounded activity window; Seq is their global event id.
// Non-durable frames (assistant.delta) are fanned out live only; Seq is the
// global event head at emission time.
type StreamFrame struct {
	Seq     uint64
	Durable bool
	Data    []byte
}

// subscriberBuffer is large enough for normal bursts; overflow is recovered
// from history by the stream (see Subscription.Lagged), never silently lost.
const subscriberBuffer = 256

type subscriber struct {
	frames chan StreamFrame
	lagged chan struct{}
}

type Subscription struct {
	Frames <-chan StreamFrame
	// Lagged receives a token when a frame could not be buffered, before any
	// later frame is buffered. The reader must re-read history after its
	// cursor before delivering another frame.
	Lagged <-chan struct{}
	// Head is the room's latest event id when the subscription started (0 if
	// none). Every durable frame on Frames has a greater id.
	Head  uint64
	close func()
}

func (s *Subscription) Close() { s.close() }

// SubscribeRoom registers a live buffer for roomID. It returns false when the
// room does not exist.
func (a *App) SubscribeRoom(roomID string) (*Subscription, bool) {
	sub := &subscriber{
		frames: make(chan StreamFrame, subscriberBuffer),
		lagged: make(chan struct{}, 1),
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if _, ok := a.Rooms[roomID]; !ok {
		return nil, false
	}
	if a.subs[roomID] == nil {
		a.subs[roomID] = map[*subscriber]struct{}{}
	}
	a.subs[roomID][sub] = struct{}{}
	return &Subscription{
		Frames: sub.frames,
		Lagged: sub.lagged,
		Head:   a.roomHeadLocked(roomID),
		close: func() {
			a.mu.Lock()
			delete(a.subs[roomID], sub)
			a.mu.Unlock()
		},
	}, true
}

func (a *App) broadcastLocked(roomID string, frame StreamFrame) {
	for sub := range a.subs[roomID] {
		select {
		case sub.frames <- frame:
		default:
			select {
			case sub.lagged <- struct{}{}:
			default:
			}
		}
	}
}

// ResetReason says why a resume cursor could not be honoured.
type ResetReason string

const (
	ResetMalformed ResetReason = "malformed"
	ResetUnknown   ResetReason = "unknown"
	ResetExpired   ResetReason = "expired"
)

type CursorError struct {
	Reason ResetReason
}

func (e *CursorError) Error() string { return "event cursor " + string(e.Reason) }

// FormatEventID is the public SSE `id:` of a durable event: its global event id.
func FormatEventID(id uint64) string { return strconv.FormatUint(id, 10) }

// ParseEventID accepts only the canonical decimal form FormatEventID emits.
// Whether the id belongs to a given room is decided by EventsAfter.
func ParseEventID(raw string) (uint64, error) {
	id, err := strconv.ParseUint(raw, 10, 64)
	if err != nil || strconv.FormatUint(id, 10) != raw {
		return 0, &CursorError{Reason: ResetMalformed}
	}
	return id, nil
}

// EventsAfter returns the retained durable events of roomID with id > after,
// oldest first, plus the room's latest event id.
//
// after must be 0 (before the room's first event) or the id of an event of
// this room; anything else is ResetUnknown, so an id from another room never
// selects this room's events. ResetExpired means events after the cursor were
// evicted from the retained window.
func (a *App) EventsAfter(roomID string, after uint64) ([]StreamFrame, uint64, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	head := a.roomHeadLocked(roomID)
	if after > a.eventHeadLocked() {
		return nil, head, &CursorError{Reason: ResetUnknown}
	}
	history := a.Activity[roomID]
	start := sort.Search(len(history), func(i int) bool { return history[i].Sequence > after })
	switch {
	case after == 0:
		oldestRetained := uint64(math.MaxUint64)
		if len(history) > 0 {
			oldestRetained = history[0].Sequence
		}
		if a.persistedRoomEventLocked(roomID, func(id uint64) bool { return id < oldestRetained }) {
			return nil, head, &CursorError{Reason: ResetExpired}
		}
	case start > 0 && history[start-1].Sequence == after:
	case a.persistedRoomEventLocked(roomID, func(id uint64) bool { return id == after }):
		return nil, head, &CursorError{Reason: ResetExpired}
	default:
		return nil, head, &CursorError{Reason: ResetUnknown}
	}
	frames := make([]StreamFrame, 0, len(history)-start)
	for _, item := range history[start:] {
		raw, err := json.Marshal(item)
		if err != nil {
			return nil, head, err
		}
		frames = append(frames, StreamFrame{Seq: item.Sequence, Durable: true, Data: raw})
	}
	return frames, head, nil
}

func (a *App) roomHeadLocked(roomID string) uint64 {
	history := a.Activity[roomID]
	if len(history) == 0 {
		return 0
	}
	return history[len(history)-1].Sequence
}

// persistedRoomEventLocked reports whether the room's audit log (the durable
// event table filtered by task) holds an id matching match. It only runs for
// cursors outside the retained window.
func (a *App) persistedRoomEventLocked(roomID string, match func(uint64) bool) bool {
	a.ensureCatalog()
	found := false
	_ = a.Store.ReadJSONL(auditPath(roomID), func(line []byte) error {
		if !found {
			if id, ok := auditSequence(line); ok && match(id) {
				found = true
			}
		}
		return nil
	})
	return found
}

// nextEventIDLocked allocates from the single global event sequence shared by
// all rooms (the stand-in for a database identity column).
func (a *App) nextEventIDLocked() uint64 {
	a.eventHeadLocked()
	a.eventSeq++
	return a.eventSeq
}

// eventHeadLocked returns the last allocated global event id. The first call
// in a process recovers it as the max id across all persisted audit logs, so
// ids keep increasing across control restarts.
func (a *App) eventHeadLocked() uint64 {
	if a.eventSeqLoaded {
		return a.eventSeq
	}
	a.ensureCatalog()
	names, _ := a.Store.List("audit")
	for _, name := range names {
		if !strings.HasSuffix(name, ".jsonl") {
			continue
		}
		_ = a.Store.ReadJSONL("audit/"+name, func(line []byte) error {
			if id, ok := auditSequence(line); ok && id > a.eventSeq {
				a.eventSeq = id
			}
			return nil
		})
	}
	a.eventSeqLoaded = true
	return a.eventSeq
}

func auditPath(roomID string) string { return "audit/" + roomID + ".jsonl" }

func auditSequence(line []byte) (uint64, bool) {
	var item struct {
		Sequence uint64 `json:"sequence"`
	}
	if json.Unmarshal(line, &item) != nil || item.Sequence == 0 {
		return 0, false
	}
	return item.Sequence, true
}

// AssistantDelta is the live-only streaming draft frame (contract §2.1).
// It is never persisted to activity or audit, so it is never replayed.
type AssistantDelta struct {
	Type            string `json:"type"`
	RoomID          string `json:"roomId"`
	SessionID       string `json:"sessionId,omitempty"`
	TurnID          string `json:"turnId,omitempty"`
	AgentID         string `json:"agentId,omitempty"`
	AgentPath       string `json:"agentPath,omitempty"`
	BlockID         string `json:"blockId,omitempty"`
	Seq             int64  `json:"seq"`
	Delta           string `json:"delta"`
	ActivityAttempt int64  `json:"activityAttempt,omitempty"`
	Source          string `json:"source"`
	OccurredAt      string `json:"occurredAt"`
}

func (a *App) publishEphemeral(roomID string, ev Event, source string) {
	occurredAt := eventString(ev, "occurredAt")
	if occurredAt == "" {
		occurredAt = now()
	}
	raw, err := json.Marshal(AssistantDelta{
		Type:            eventString(ev, "type"),
		RoomID:          roomID,
		SessionID:       eventString(ev, "sessionId"),
		TurnID:          eventString(ev, "turnId"),
		AgentID:         eventString(ev, "agentId"),
		AgentPath:       eventString(ev, "agentPath"),
		BlockID:         eventString(ev, "blockId"),
		Seq:             eventInt(ev, "seq"),
		Delta:           eventString(ev, "delta"),
		ActivityAttempt: eventInt(ev, "activityAttempt"),
		Source:          source,
		OccurredAt:      occurredAt,
	})
	if err != nil {
		return
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if _, ok := a.Rooms[roomID]; !ok {
		return
	}
	a.broadcastLocked(roomID, StreamFrame{Seq: a.eventHeadLocked(), Data: raw})
}

func eventInt(ev Event, key string) int64 {
	switch v := ev[key].(type) {
	case float64:
		return int64(v)
	case int:
		return int64(v)
	case int64:
		return v
	}
	return 0
}

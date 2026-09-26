package app

import (
	"encoding/json"
	"strconv"
	"strings"
)

// StreamFrame is one SSE-bound event for a room.
//
// Durable frames are ActivityEvents: persisted to the audit log and retained
// in the bounded activity window, Seq is their per-room sequence.
// Non-durable frames (assistant.delta) are fanned out live only; Seq is the
// room's sequence at emission time, i.e. the last durable event before it.
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
	// Lagged receives a token when a frame could not be buffered. The reader
	// must re-read history after its cursor before trusting the buffer again.
	Lagged <-chan struct{}
	// Head is the room sequence at the instant the subscription started.
	// Every durable frame on Frames has Seq > Head.
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
		Head:   a.sequenceLocked(roomID),
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

// FormatEventID is the public SSE `id:` for a durable event: "<roomId>:<sequence>".
// The room prefix scopes ids per task so a cursor from one room is never
// interpreted against another room's sequence.
func FormatEventID(roomID string, seq uint64) string {
	return roomID + ":" + strconv.FormatUint(seq, 10)
}

// ParseEventID accepts only the exact form FormatEventID emits for roomID.
func ParseEventID(roomID, raw string) (uint64, error) {
	i := strings.LastIndexByte(raw, ':')
	if i <= 0 {
		return 0, &CursorError{Reason: ResetMalformed}
	}
	seq, err := strconv.ParseUint(raw[i+1:], 10, 64)
	if err != nil || strconv.FormatUint(seq, 10) != raw[i+1:] {
		return 0, &CursorError{Reason: ResetMalformed}
	}
	if raw[:i] != roomID {
		return 0, &CursorError{Reason: ResetUnknown}
	}
	return seq, nil
}

// EventsAfter returns the retained durable events of roomID with sequence >
// after, oldest first, plus the room head. It fails with ResetUnknown when
// after is beyond anything this room issued, and ResetExpired when events
// between after and the retained window were already evicted.
func (a *App) EventsAfter(roomID string, after uint64) ([]StreamFrame, uint64, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	head := a.sequenceLocked(roomID)
	if after > head {
		return nil, head, &CursorError{Reason: ResetUnknown}
	}
	if after == head {
		return nil, head, nil
	}
	history := a.Activity[roomID]
	if len(history) == 0 || history[0].Sequence > after+1 {
		return nil, head, &CursorError{Reason: ResetExpired}
	}
	pending := history[after+1-history[0].Sequence:]
	frames := make([]StreamFrame, 0, len(pending))
	for _, item := range pending {
		raw, err := json.Marshal(item)
		if err != nil {
			return nil, head, err
		}
		frames = append(frames, StreamFrame{Seq: item.Sequence, Durable: true, Data: raw})
	}
	return frames, head, nil
}

// sequenceLocked returns the room's last issued sequence. The first lookup in
// a process recovers the high-water mark from the persisted audit log, so ids
// keep increasing for a room across control restarts.
func (a *App) sequenceLocked(roomID string) uint64 {
	if seq, ok := a.sequences[roomID]; ok {
		return seq
	}
	var last uint64
	a.ensureCatalog()
	_ = a.Store.ReadJSONL("audit/"+roomID+".jsonl", func(line []byte) error {
		var item struct {
			Sequence uint64 `json:"sequence"`
		}
		if json.Unmarshal(line, &item) == nil && item.Sequence > last {
			last = item.Sequence
		}
		return nil
	})
	a.sequences[roomID] = last
	return last
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
	a.broadcastLocked(roomID, StreamFrame{Seq: a.sequenceLocked(roomID), Data: raw})
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

package app

import (
	"errors"
	"strconv"
)

// StreamFrame is one SSE-bound event for a room.
//
// Durable frames are Envelopes stored in the EventLog; Seq is their global
// event id. Non-durable frames (assistant.delta) are fanned out live only;
// Seq is the global LastID at emission time.
type StreamFrame struct {
	Seq     uint64
	Durable bool
	Data    []byte
}

// subscriberBuffer is large enough for normal bursts; overflow is recovered
// from history by the stream (see Subscription.Lagged), never silently lost.
const subscriberBuffer = 256

var ErrRoomNotFound = errors.New("room not found")

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

// SubscribeRoom registers a live buffer for roomID.
func (a *App) SubscribeRoom(roomID string) (*Subscription, error) {
	sub := &subscriber{
		frames: make(chan StreamFrame, subscriberBuffer),
		lagged: make(chan struct{}, 1),
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if _, ok := a.Rooms[roomID]; !ok {
		return nil, ErrRoomNotFound
	}
	head, err := a.Events.Head(roomID)
	if err != nil {
		return nil, err
	}
	if a.subs[roomID] == nil {
		a.subs[roomID] = map[*subscriber]struct{}{}
	}
	a.subs[roomID][sub] = struct{}{}
	return &Subscription{
		Frames: sub.frames,
		Lagged: sub.lagged,
		Head:   head,
		close: func() {
			a.mu.Lock()
			delete(a.subs[roomID], sub)
			a.mu.Unlock()
		},
	}, nil
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

// EventsAfter returns roomID's retained durable events with id > after,
// oldest first, plus the room's latest event id.
//
// after must be 0 (before the room's first event) or a retained event id of
// this room. Otherwise it is ResetExpired when it is older than the room's
// retained events, and ResetUnknown in every other case: another room's id,
// an id beyond LastID, or an id issued before a control restart.
func (a *App) EventsAfter(roomID string, after uint64) ([]StreamFrame, uint64, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	head, err := a.Events.Head(roomID)
	if err != nil {
		return nil, 0, err
	}
	if err := a.checkCursorLocked(roomID, after); err != nil {
		return nil, head, err
	}
	events, err := a.Events.After(roomID, after)
	if err != nil {
		return nil, head, err
	}
	frames := make([]StreamFrame, 0, len(events))
	for _, item := range events {
		raw, err := EncodeEnvelope(item)
		if err != nil {
			return nil, head, err
		}
		frames = append(frames, StreamFrame{Seq: item.ID, Durable: true, Data: raw})
	}
	return frames, head, nil
}

func (a *App) checkCursorLocked(roomID string, after uint64) error {
	last, err := a.Events.LastID()
	if err != nil {
		return err
	}
	if after > last {
		return &CursorError{Reason: ResetUnknown}
	}
	evictedThrough, err := a.Events.EvictedThrough(roomID)
	if err != nil {
		return err
	}
	if after == 0 {
		if evictedThrough > 0 {
			return &CursorError{Reason: ResetExpired}
		}
		return nil
	}
	ok, err := a.Events.Contains(roomID, after)
	switch {
	case err != nil:
		return err
	case ok:
		return nil
	case after <= evictedThrough:
		return &CursorError{Reason: ResetExpired}
	default:
		return &CursorError{Reason: ResetUnknown}
	}
}

// publishLive fans out a live-only event (assistant.delta). It is never
// stored in the EventLog or audit, takes no id, and is never replayed.
func (a *App) publishLive(env Envelope) {
	raw, err := EncodeEnvelope(env)
	if err != nil {
		return
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if _, ok := a.Rooms[env.TaskID]; !ok {
		return
	}
	last, err := a.Events.LastID()
	if err != nil {
		return
	}
	a.broadcastLocked(env.TaskID, StreamFrame{Seq: last, Data: raw})
}

package app

import (
	"errors"
	"strconv"
	"time"
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

var (
	ErrRoomNotFound      = errors.New("room not found")
	ErrRoomStreamLimit   = errors.New("too many streams on this room")
	ErrClientStreamLimit = errors.New("too many streams from this client")
)

type subscriber struct {
	frames chan StreamFrame
	lagged chan struct{}
	closed chan struct{}
	client string
	ended  bool
}

type Subscription struct {
	Frames <-chan StreamFrame
	// Lagged receives a token when a frame could not be buffered, before any
	// later frame is buffered. The reader must re-read history after its
	// cursor before delivering another frame.
	Lagged <-chan struct{}
	// Closed is closed when the room closes; frames already buffered are
	// still readable.
	Closed <-chan struct{}
	// Head is the room's latest event id when the subscription started (0 if
	// none). Every durable frame on Frames has a greater id.
	Head  uint64
	close func()
}

func (s *Subscription) Close() { s.close() }

// SubscribeRoom registers a live buffer for roomID on behalf of client,
// within Limits.MaxStreamsPerRoom and Limits.MaxStreamsPerClient.
func (a *App) SubscribeRoom(roomID, client string) (*Subscription, error) {
	sub := &subscriber{
		frames: make(chan StreamFrame, subscriberBuffer),
		lagged: make(chan struct{}, 1),
		closed: make(chan struct{}),
		client: client,
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	room, ok := a.Rooms[roomID]
	if !ok {
		return nil, ErrRoomNotFound
	}
	if len(a.subs[roomID]) >= a.Limits.MaxStreamsPerRoom {
		return nil, ErrRoomStreamLimit
	}
	if a.clientStreams[client] >= a.Limits.MaxStreamsPerClient {
		return nil, ErrClientStreamLimit
	}
	head, err := a.Events.Head(roomID)
	if err != nil {
		return nil, err
	}
	if a.subs[roomID] == nil {
		a.subs[roomID] = map[*subscriber]struct{}{}
	}
	a.subs[roomID][sub] = struct{}{}
	a.clientStreams[client]++
	if room.State == RoomClosed {
		sub.end()
	}
	return &Subscription{
		Frames: sub.frames,
		Lagged: sub.lagged,
		Closed: sub.closed,
		Head:   head,
		close: func() {
			a.mu.Lock()
			defer a.mu.Unlock()
			if _, ok := a.subs[roomID][sub]; !ok {
				return
			}
			delete(a.subs[roomID], sub)
			if len(a.subs[roomID]) == 0 {
				delete(a.subs, roomID)
			}
			if a.clientStreams[client]--; a.clientStreams[client] <= 0 {
				delete(a.clientStreams, client)
			}
		},
	}, nil
}

func (s *subscriber) end() {
	if !s.ended {
		s.ended = true
		close(s.closed)
	}
}

// roomClosed ends the room's streams after their buffered frames and frees
// its event log once Limits.ClosedRoomLogTTL has passed. Call it after the
// room's closing session.status has been published.
func (a *App) roomClosed(roomID string) {
	a.mu.Lock()
	for sub := range a.subs[roomID] {
		sub.end()
	}
	ttl := a.Limits.ClosedRoomLogTTL
	a.mu.Unlock()
	free := func() {
		a.mu.Lock()
		defer a.mu.Unlock()
		if room, ok := a.Rooms[roomID]; ok && room.State == RoomClosed {
			_ = a.Events.Drop(roomID)
			a.freedLogs[roomID] = struct{}{}
		}
	}
	if ttl <= 0 {
		free()
		return
	}
	time.AfterFunc(ttl, free)
}

// RoomHead is the room's latest event id (0 if none).
func (a *App) RoomHead(roomID string) (uint64, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.Events.Head(roomID)
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
	// ResetLagging is sent before a stream is closed because its reader fell
	// behind the live buffer Limits.MaxConsecutiveLags times in a row.
	ResetLagging ResetReason = "lagging"
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
// this room. Otherwise it is ResetExpired when it was one of this room's
// events and has been evicted, and ResetUnknown in every other case: another
// room's id, an id beyond LastID, or an id issued before a control restart.
func (a *App) EventsAfter(roomID string, after uint64) ([]StreamFrame, uint64, error) {
	a.mu.Lock()
	head, err := a.Events.Head(roomID)
	if err == nil {
		err = a.checkCursorLocked(roomID, after)
	}
	var events []Envelope
	if err == nil {
		events, err = a.Events.After(roomID, after)
	}
	a.mu.Unlock()
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
	if ok, err := a.Events.Contains(roomID, after); err != nil || ok {
		return err
	}
	evicted, err := a.Events.WasEvicted(roomID, after)
	switch {
	case err != nil:
		return err
	case evicted:
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

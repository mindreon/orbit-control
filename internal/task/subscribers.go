package task

import (
	"context"
	"errors"
)

// subscriberBuffer is the room a subscriber has for live events beyond its replay.
const subscriberBuffer = 256

func (s *Service) Subscribe(ctx context.Context, p Principal, id string, after uint64) (*Subscriber, error) {
	if _, err := s.Get(ctx, p, id); err != nil {
		return nil, err
	}
	items, head, err := s.EventsAfter(ctx, p, id, after)
	if err != nil && !errors.Is(err, ErrCursor) {
		return nil, err
	}
	if err != nil {
		items = nil
	}
	// The replay is queued below while the service lock is held: the buffer must take all of it, or a replay longer than the
	// buffer blocks here with the lock held and stops every call of the service.
	sub := &subscriber{ch: make(chan Event, subscriberBuffer+len(items)), closed: make(chan struct{})}
	s.mu.Lock()
	if s.subs[id] == nil {
		s.subs[id] = map[*subscriber]struct{}{}
	}
	s.subs[id][sub] = struct{}{}
	for _, item := range items {
		sub.ch <- item
	}
	s.mu.Unlock()
	return &Subscriber{Events: sub.ch, Closed: sub.closed, Head: head, Close: func() { s.removeSubscriber(id, sub) }}, nil
}

// CloseAllSubscribers forces active task streams to reconnect. The caller uses
// this when task ownership changes so clients resume from Last-Event-ID on the
// new owner instead of staying attached to a replica that no longer owns the
// task.
func (s *Service) CloseAllSubscribers() {
	s.mu.Lock()
	defer s.mu.Unlock()
	for taskID, subscribers := range s.subs {
		for sub := range subscribers {
			delete(subscribers, sub)
			sub.once.Do(func() {
				close(sub.closed)
				close(sub.ch)
			})
		}
		delete(s.subs, taskID)
	}
}

func (s *Service) removeSubscriber(id string, sub *subscriber) {
	s.mu.Lock()
	if _, ok := s.subs[id][sub]; ok {
		delete(s.subs[id], sub)
		sub.once.Do(func() { close(sub.closed) })
		close(sub.ch)
	}
	s.mu.Unlock()
}

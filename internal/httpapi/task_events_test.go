package httpapi

import (
	"errors"
	"reflect"
	"testing"

	taskruntime "github.com/mindreon/orbit-control/internal/task"
)

func durable(seq uint64) taskruntime.Event {
	return taskruntime.Event{Seq: seq, Durable: true, Type: "t"}
}

func newTestSender(lastSent uint64, log []taskruntime.Event, fetchErr error) (*eventSender, *[]taskruntime.Event) {
	var sent []taskruntime.Event
	return &eventSender{
		lastSent: lastSent,
		fetch: func(after uint64) ([]taskruntime.Event, error) {
			if fetchErr != nil {
				return nil, fetchErr
			}
			var out []taskruntime.Event
			for _, e := range log {
				if e.Seq == 0 || e.Seq > after {
					out = append(out, e)
				}
			}
			return out, nil
		},
		emit: func(e taskruntime.Event) error { sent = append(sent, e); return nil },
	}, &sent
}

func seqs(events []taskruntime.Event) []uint64 {
	out := make([]uint64, 0, len(events))
	for _, e := range events {
		out = append(out, e.Seq)
	}
	return out
}

func TestDeliverFillsSeqGapFromLogBeforeLiveEvent(t *testing.T) {
	log := []taskruntime.Event{durable(5), durable(6), durable(7), durable(8), durable(9)}
	s2, sent2 := newTestSender(4, log, nil)
	if err := s2.deliver(durable(9)); err != nil {
		t.Fatal(err)
	}
	if got, want := seqs(*sent2), []uint64{5, 6, 7, 8, 9}; !reflect.DeepEqual(got, want) {
		t.Fatalf("sent seqs = %v, want %v", got, want)
	}
	// The same live event arriving again is a duplicate.
	if err := s2.deliver(durable(9)); err != nil || len(*sent2) != 5 {
		t.Fatalf("duplicate was sent: err=%v sent=%v", err, seqs(*sent2))
	}
}

func TestDeliverSendsNextEventWithoutReadingLog(t *testing.T) {
	s, sent := newTestSender(4, nil, errors.New("log must not be read"))
	if err := s.deliver(durable(5)); err != nil {
		t.Fatal(err)
	}
	if got := seqs(*sent); !reflect.DeepEqual(got, []uint64{5}) || s.lastSent != 5 {
		t.Fatalf("sent = %v lastSent = %d", got, s.lastSent)
	}
}

func TestEphemeralEventNeverMovesCursorOrTriggersCatchUp(t *testing.T) {
	s, sent := newTestSender(7, nil, errors.New("log must not be read"))
	live := taskruntime.Event{Seq: 0, Type: "token.delta"}
	if err := s.deliver(live); err != nil {
		t.Fatal(err)
	}
	if len(*sent) != 1 || s.lastSent != 7 {
		t.Fatalf("sent = %d lastSent = %d", len(*sent), s.lastSent)
	}
}

func TestCatchUpSkipsEphemeralAndSurvivesReadError(t *testing.T) {
	log := []taskruntime.Event{{Seq: 0, Type: "token.delta"}, durable(3)}
	s, sent := newTestSender(2, log, nil)
	if err := s.catchUp(); err != nil {
		t.Fatal(err)
	}
	if got := seqs(*sent); !reflect.DeepEqual(got, []uint64{3}) {
		t.Fatalf("catchUp sent %v, want only seq 3", got)
	}
	failing, sentFailing := newTestSender(2, nil, errors.New("db down"))
	if err := failing.catchUp(); err != nil || len(*sentFailing) != 0 || failing.lastSent != 2 {
		t.Fatalf("read error must be retried on the next tick: err=%v sent=%d lastSent=%d", err, len(*sentFailing), failing.lastSent)
	}
}

func TestCatchUpReturnsWriteError(t *testing.T) {
	boom := errors.New("client gone")
	s := &eventSender{
		lastSent: 0,
		fetch:    func(uint64) ([]taskruntime.Event, error) { return []taskruntime.Event{durable(1)}, nil },
		emit:     func(taskruntime.Event) error { return boom },
	}
	if err := s.catchUp(); !errors.Is(err, boom) {
		t.Fatalf("err = %v, want write error", err)
	}
}
